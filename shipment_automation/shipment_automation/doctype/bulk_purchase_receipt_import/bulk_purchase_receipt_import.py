import frappe
from frappe.model.document import Document
from frappe.utils import flt, nowdate, getdate
import openpyxl
from io import BytesIO


@frappe.whitelist()
def download_template():
    """Generates and downloads the Bulk Purchase Receipt Import Excel template."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Bulk PR Import Template"
    
    headers = [
        "Supplier Name", "Purchase Order Number", "Item Code", 
        "Item Name", "Description", "Quantity", "Rate", "Line Number"
    ]
    ws.append(headers)
    
    from openpyxl.styles import Font
    for cell in ws[1]:
        cell.font = Font(bold=True)

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    
    frappe.response['filename'] = "Bulk_PR_Import_Template.xlsx"
    frappe.response['filecontent'] = output.getvalue()
    frappe.response['type'] = 'binary'


class BulkPurchaseReceiptImport(Document):

    @frappe.whitelist()
    def start_validation(self):
        self.db_set("status", "Validating")
        self.db_set("validation_log", "⏳ Validation in progress. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_purchase_receipt_import.bulk_purchase_receipt_import.run_validation",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Validation started. Refresh in a few seconds."

    @frappe.whitelist()
    def start_processing(self):
        if self.status != "Validated":
            frappe.throw("Please validate the data first.")
        if not self.pr_naming_series:
            frappe.throw("Please select a Purchase Receipt Naming Series.")
        self.db_set("status", "Processing")
        self.db_set("receipts_log", "⏳ Creating Purchase Receipt. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_purchase_receipt_import.bulk_purchase_receipt_import.run_processing",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Processing started. Refresh in a few seconds."

    @frappe.whitelist()
    def create_purchase_invoice_and_boe(self):
        if not self.receipt_name:
            frappe.throw("No Purchase Receipt linked.")
        pr_doc = frappe.get_doc("Purchase Receipt", self.receipt_name)
        if pr_doc.docstatus != 1:
            frappe.throw(f"Purchase Receipt {self.receipt_name} must be Submitted first.")
        if not self.pi_naming_series:
            frappe.throw("Please select a Purchase Invoice Naming Series.")
        
        try:
            from erpnext.stock.stock_ledger import make_purchase_invoice
            pi = make_purchase_invoice(self.receipt_name)
            pi.naming_series = self.pi_naming_series
            pi.insert(ignore_permissions=True)
            self.db_set("invoice_name", pi.name)
            
            boe = frappe.new_doc("Bill of Entry")
            boe.purchase_invoice = pi.name
            boe.posting_date = nowdate()
            boe.company = pr_doc.company
            boe.supplier = pr_doc.supplier
            boe.insert(ignore_permissions=True, ignore_mandatory=True)
            self.db_set("bill_of_entry_name", boe.name)
            
            frappe.db.commit()
            return {"invoice": pi.name, "bill_of_entry": boe.name}
        except Exception as exc:
            frappe.log_error(frappe.get_traceback(), "Bulk PR Import – Create PI/BOE Error")
            frappe.throw(f"Error: {exc}")


def get_column_map(sheet):
    header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
    mapping = {}
    expected = {
        "supplier": ["Supplier Name", "Supplier"],
        "po_num": ["Purchase Order Number", "PO Number"],
        "item_code": ["Item Code"],
        "item_name": ["Item Name"],
        "description": ["Description"],
        "quantity": ["Quantity", "Qty"],
        "rate": ["Rate"],
        "line_number": ["Line Number", "Line #"]
    }
    for idx, cell in enumerate(header_row):
        if not cell: continue
        clean = str(cell).strip().lower()
        for key, aliases in expected.items():
            if any(a.lower() == clean for a in aliases):
                mapping[key] = idx
    return mapping


def run_validation(docname):
    doc = frappe.get_doc("Bulk Purchase Receipt Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active
        col_map = get_column_map(sheet)
        
        errors = []
        ok_rows = 0
        
        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not any(row): continue
            
            po_num = str(row[col_map["po_num"]]).strip() if col_map.get("po_num") is not None else ""
            item_code = str(row[col_map["item_code"]]).strip() if col_map.get("item_code") is not None else ""
            item_name = str(row[col_map["item_name"]]).strip() if col_map.get("item_name") is not None else ""
            description = str(row[col_map["description"]]).strip() if col_map.get("description") is not None else ""
            qty_exc = flt(row[col_map["quantity"]]) if col_map.get("quantity") is not None else 0
            rate_exc = flt(row[col_map["rate"]]) if col_map.get("rate") is not None else 0
            line_val = str(row[col_map["line_number"]]).strip() if col_map.get("line_number") is not None and row[col_map["line_number"]] else ""

            if not po_num or not frappe.db.exists("Purchase Order", po_num):
                errors.append(f"Row {row_idx} ❌ Purchase Order '{po_num}' not found.")
                continue

            # Find matching PO Line
            filters = {"parent": po_num, "item_code": item_code}
            if line_val:
                # Support both direct line number or matching custom line_number field
                po_item_name = frappe.db.get_value("Purchase Order Item", {"parent": po_num, "line_number": line_val}, "name")
                if not po_item_name:
                    po_item_name = frappe.db.get_value("Purchase Order Item", {"parent": po_num, "custom_line_number": line_val}, "name")
                if not po_item_name:
                    errors.append(f"Row {row_idx} ❌ Line Number '{line_val}' not found in PO '{po_num}'.")
                    continue
                filters["name"] = po_item_name
            
            po_items = frappe.get_all("Purchase Order Item", filters=filters, fields=["name", "item_name", "description", "qty", "rate"])
            
            if not po_items:
                errors.append(f"Row {row_idx} ❌ Item '{item_code}' not found in PO '{po_num}'.")
                continue
            
            # Match 2 out of (Item Name, Description)
            match_found = False
            for pi in po_items:
                score = 0
                if pi.item_name == item_name: score += 1
                if pi.description == description: score += 1
                
                if score >= 1: # Basic match for now, or ensure Qty/Rate matches
                    if abs(pi.qty - qty_exc) > 0.01:
                        errors.append(f"Row {row_idx} ❌ Quantity mismatch: Excel {qty_exc} vs PO {pi.qty}")
                    elif abs(pi.rate - rate_exc) > 0.01:
                        errors.append(f"Row {row_idx} ❌ Rate mismatch: Excel {rate_exc} vs PO {pi.rate}")
                    else:
                        match_found = True
                        break
            
            if not match_found:
                errors.append(f"Row {row_idx} ❌ Data mismatch (Name/Desc/Qty/Rate) with PO.")
            else:
                ok_rows += 1

        if not errors:
            doc.db_set("status", "Validated")
            doc.db_set("validation_log", f"✅ {ok_rows} row(s) validated successfully.")
        else:
            doc.db_set("status", "Failed")
            doc.db_set("validation_log", f"❌ Issues found:\n\n" + "\n".join(errors))
        frappe.db.commit()
    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("validation_log", f"❌ Error:\n{frappe.get_traceback()}")
        frappe.db.commit()


def run_processing(docname):
    doc = frappe.get_doc("Bulk Purchase Receipt Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active
        col_map = get_column_map(sheet)
        
        pr = frappe.new_doc("Purchase Receipt")
        pr.naming_series = doc.pr_naming_series
        pr.supplier = doc.supplier
        pr.company = frappe.db.get_value("Supplier", doc.supplier, "default_company") or frappe.db.get_single_value("Global Defaults", "default_company")
        
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not any(row): continue
            
            po_num = str(row[col_map["po_num"]]).strip()
            item_code = str(row[col_map["item_code"]]).strip()
            line_val = str(row[col_map["line_number"]]).strip() if col_map.get("line_number") is not None and row[col_map["line_number"]] else ""
            
            # Find the specific PO Item name again for mapping
            filters = {"parent": po_num, "item_code": item_code}
            if line_val:
                po_item_name = frappe.db.get_value("Purchase Order Item", {"parent": po_num, "line_number": line_val}, "name") or \
                               frappe.db.get_value("Purchase Order Item", {"parent": po_num, "custom_line_number": line_val}, "name")
                filters["name"] = po_item_name
            
            po_item = frappe.get_doc("Purchase Order Item", filters)
            
            pr_item = pr.append("items", {
                "item_code": item_code,
                "qty": flt(row[col_map["quantity"]]),
                "rate": flt(row[col_map["rate"]]),
                "purchase_order": po_num,
                "purchase_order_item": po_item.name,
                "warehouse": po_item.warehouse or doc.target_warehouse
            })
            pr_item.run_method("set_missing_values")

        pr.run_method("set_missing_values")
        pr.run_method("calculate_taxes_and_totals")
        pr.insert(ignore_permissions=True)
        
        doc.db_set("status", "Completed")
        doc.db_set("receipt_name", pr.name)
        doc.db_set("receipts_log", f"✅ Purchase Receipt {pr.name} created successfully.")
        frappe.db.commit()
    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("receipts_log", f"❌ Error:\n{frappe.get_traceback()}")
        frappe.db.commit()
