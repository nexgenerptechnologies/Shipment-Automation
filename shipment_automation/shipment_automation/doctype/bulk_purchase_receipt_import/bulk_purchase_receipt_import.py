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
    def start_creation(self):
        if self.status != "Validated":
            frappe.throw("Please validate the data first.")
        self.db_set("status", "Processing")
        self.db_set("receipts_log", "⏳ Creating Purchase Receipts. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_purchase_receipt_import.bulk_purchase_receipt_import.run_processing",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Processing started. Refresh in a few seconds."

    @frappe.whitelist()
    def create_purchase_invoice_and_boe(self):
        if not self.receipts_log:
            frappe.throw("No Purchase Receipts found in the log.")
        
        import re
        # Find all PR names in the log (matches PR- followed by any characters until a space or end of line)
        receipts = re.findall(r'(PR-[^\s\n✅❌]+)', self.receipts_log)
        
        if not receipts:
             frappe.throw("Could not find Purchase Receipt names in the processing log.")

        created = []
        for pr_name in receipts:
            try:
                pr_doc = frappe.get_doc("Purchase Receipt", pr_name)
                if pr_doc.docstatus != 1:
                    continue
                
                from erpnext.stock.stock_ledger import make_purchase_invoice
                pi = make_purchase_invoice(pr_name)
                # Auto-set default Invoice Naming Series
                pi.naming_series = "PINV-.YY.-"
                pi.insert(ignore_permissions=True)
                
                boe = frappe.new_doc("Bill of Entry")
                boe.purchase_invoice = pi.name
                boe.posting_date = nowdate()
                boe.company = pr_doc.company
                boe.supplier = pr_doc.supplier
                boe.insert(ignore_permissions=True, ignore_mandatory=True)
                
                created.append(f"✅ Invoice {pi.name} and BOE {boe.name} created for {pr_name}")
            except Exception as e:
                created.append(f"❌ Error for {pr_name}: {str(e)}")
        
        frappe.db.commit()
        return {"summary": "\n".join(created)}


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
            
            supplier_name = str(row[col_map["supplier"]]).strip() if col_map.get("supplier") is not None else ""
            po_num = str(row[col_map["po_num"]]).strip() if col_map.get("po_num") is not None else ""
            item_code = str(row[col_map["item_code"]]).strip() if col_map.get("item_code") is not None else ""
            item_name = str(row[col_map["item_name"]]).strip() if col_map.get("item_name") is not None else ""
            description = str(row[col_map["description"]]).strip() if col_map.get("description") is not None else ""
            qty_exc = flt(row[col_map["quantity"]]) if col_map.get("quantity") is not None else 0
            rate_exc = flt(row[col_map["rate"]]) if col_map.get("rate") is not None else 0
            line_val = str(row[col_map["line_number"]]).strip() if col_map.get("line_number") is not None and row[col_map["line_number"]] else ""

            if not supplier_name or not frappe.db.exists("Supplier", supplier_name):
                errors.append(f"Row {row_idx} ❌ Supplier '{supplier_name}' not found.")
                continue

            if not po_num or not frappe.db.exists("Purchase Order", po_num):
                errors.append(f"Row {row_idx} ❌ Purchase Order '{po_num}' not found.")
                continue

            po_supplier = frappe.db.get_value("Purchase Order", po_num, "supplier")
            if po_supplier != supplier_name:
                errors.append(f"Row {row_idx} ❌ PO '{po_num}' belongs to '{po_supplier}', not '{supplier_name}'.")
                continue

            filters = {"parent": po_num, "item_code": item_code}
            if line_val:
                po_item_name = frappe.db.get_value("Purchase Order Item", {"parent": po_num, "line_number": line_val}, "name") or \
                               frappe.db.get_value("Purchase Order Item", {"parent": po_num, "custom_line_number": line_val}, "name")
                if not po_item_name:
                    errors.append(f"Row {row_idx} ❌ Line Number '{line_val}' not found in PO '{po_num}'.")
                    continue
                filters["name"] = po_item_name
            
            po_items = frappe.get_all("Purchase Order Item", filters=filters, fields=["name", "item_name", "description", "qty", "rate"])
            
            if not po_items:
                errors.append(f"Row {row_idx} ❌ Item '{item_code}' not found in PO '{po_num}'.")
                continue
            
            match_found = False
            for pi in po_items:
                score = 0
                if pi.item_name == item_name: score += 1
                if pi.description == description: score += 1
                
                if score >= 1:
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
        
        supplier_map = {}
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not any(row): continue
            s_name = str(row[col_map["supplier"]]).strip()
            if s_name not in supplier_map:
                supplier_map[s_name] = []
            supplier_map[s_name].append(row)

        created_receipts = []
        for s_name, rows in supplier_map.items():
            pr = frappe.new_doc("Purchase Receipt")
            # Auto-set default Receipt Naming Series
            pr.naming_series = "PR-.YY.-"
            pr.supplier = s_name
            pr.company = frappe.db.get_value("Supplier", s_name, "default_company") or frappe.db.get_single_value("Global Defaults", "default_company")
            pr.posting_date = nowdate()

            for row in rows:
                po_num = str(row[col_map["po_num"]]).strip()
                item_code = str(row[col_map["item_code"]]).strip()
                line_val = str(row[col_map["line_number"]]).strip() if col_map.get("line_number") is not None and row[col_map["line_number"]] else ""
                
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
                    "warehouse": po_item.warehouse
                })
                pr_item.run_method("set_missing_values")

            pr.run_method("set_missing_values")
            pr.run_method("calculate_taxes_and_totals")
            pr.insert(ignore_permissions=True)
            created_receipts.append(pr.name)

        doc.db_set("status", "Completed")
        doc.db_set("receipts_log", "SUMMARY:\n" + "\n".join([f"✅ {name}" for name in created_receipts]))
        frappe.db.commit()
    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("receipts_log", f"❌ Error:\n{frappe.get_traceback()}")
        frappe.db.commit()
