import frappe
from frappe.model.document import Document
from frappe.utils import flt, nowdate, getdate
import openpyxl
from io import BytesIO
import datetime


@frappe.whitelist()
def download_template():
    """Generates and downloads the Bulk Purchase Receipt Import Excel template."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Bulk PR Import Template"
    
    # Updated headers as requested
    headers = [
        "Purchase Receipt Number", "Purchase Receipt Date", "Supplier Name", 
        "Purchase Order Number", "Item Code", "Item Name", 
        "Description", "Quantity", "Rate", "Line Number"
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
        receipts = re.findall(r'([A-Z0-9\-/]+\-[0-9]+)', self.receipts_log)
        
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
        "pr_num": ["Purchase Receipt Number", "PR Number"],
        "pr_date": ["Purchase Receipt Date", "PR Date"],
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


def find_po_item_name(po_num, item_code, line_val=None):
    """Safely finds a PO Item name, checking multiple field possibilities WITHOUT crashing on missing columns."""
    meta = frappe.get_meta("Purchase Order Item")
    available_fields = [f.fieldname for f in meta.fields]
    
    if line_val:
        if "line_number" in available_fields:
            res = frappe.db.get_value("Purchase Order Item", {"parent": po_num, "line_number": line_val}, "name")
            if res: return res
        
        if "custom_line_number" in available_fields:
            res = frappe.db.get_value("Purchase Order Item", {"parent": po_num, "custom_line_number": line_val}, "name")
            if res: return res
        
        import re
        match = re.search(r'(\d+)$', po_num)
        if match:
            suffix = match.group(1)
            if line_val.startswith(f"{suffix}-"):
                try:
                    idx_part = int(line_val.split("-")[-1])
                    res = frappe.db.get_value("Purchase Order Item", {"parent": po_num, "idx": idx_part}, "name")
                    if res: return res
                except: pass

    return frappe.db.get_value("Purchase Order Item", {"parent": po_num, "item_code": item_code}, "name")


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
            
            pr_num = str(row[col_map["pr_num"]]).strip() if col_map.get("pr_num") is not None and row[col_map["pr_num"]] else ""
            supplier_name = str(row[col_map["supplier"]]).strip() if col_map.get("supplier") is not None else ""
            po_num = str(row[col_map["po_num"]]).strip() if col_map.get("po_num") is not None else ""
            item_code = str(row[col_map["item_code"]]).strip() if col_map.get("item_code") is not None else ""
            item_name = str(row[col_map["item_name"]]).strip() if col_map.get("item_name") is not None else ""
            description = str(row[col_map["description"]]).strip() if col_map.get("description") is not None else ""
            qty_exc = flt(row[col_map["quantity"]]) if col_map.get("quantity") is not None else 0
            rate_exc = flt(row[col_map["rate"]]) if col_map.get("rate") is not None else 0
            line_val = str(row[col_map["line_number"]]).strip() if col_map.get("line_number") is not None and row[col_map["line_number"]] else ""

            if not pr_num:
                errors.append(f"Row {row_idx} ❌ Purchase Receipt Number is missing.")
                continue

            if frappe.db.exists("Purchase Receipt", pr_num):
                errors.append(f"Row {row_idx} ❌ Purchase Receipt '{pr_num}' already exists.")
                continue

            if not po_num or not frappe.db.exists("Purchase Order", po_num):
                errors.append(f"Row {row_idx} ❌ Purchase Order '{po_num}' not found.")
                continue

            # Check if ANY Purchase Receipt (even Draft) already exists for this PO
            # Note: Checking if items from this PO are already partially or fully received
            # But the user specifically asked: "if the Purchase Receipt already created against Purchase Order"
            existing_pr = frappe.db.get_value("Purchase Receipt Item", {"purchase_order": po_num, "docstatus": ["<", 2]}, "parent")
            if existing_pr:
                errors.append(f"Row {row_idx} ❌ A Purchase Receipt '{existing_pr}' already exists for PO '{po_num}'. Duplicate not allowed.")
                continue

            po_supplier = frappe.db.get_value("Purchase Order", po_num, "supplier")
            if po_supplier != supplier_name:
                errors.append(f"Row {row_idx} ❌ PO '{po_num}' belongs to '{po_supplier}', not '{supplier_name}'.")
                continue

            target_item_name = find_po_item_name(po_num, item_code, line_val)
            if not target_item_name:
                errors.append(f"Row {row_idx} ❌ Item '{item_code}' not found in PO '{po_num}'.")
                continue
            
            pi = frappe.get_doc("Purchase Order Item", target_item_name)
            
            score = 0
            if pi.item_name == item_name: score += 1
            if pi.description == description: score += 1
            
            if score >= 1:
                if abs(pi.qty - qty_exc) > 0.01:
                    errors.append(f"Row {row_idx} ❌ Quantity mismatch: Excel {qty_exc} vs PO {pi.qty}")
                elif abs(pi.rate - rate_exc) > 0.01:
                    errors.append(f"Row {row_idx} ❌ Rate mismatch: Excel {rate_exc} vs PO {pi.rate}")
                else:
                    ok_rows += 1
            else:
                errors.append(f"Row {row_idx} ❌ Data mismatch (Name/Description) with PO.")

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
        
        # Group by PR Number
        pr_map = {}
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not any(row): continue
            pr_id = str(row[col_map["pr_num"]]).strip()
            if pr_id not in pr_map:
                pr_map[pr_id] = []
            pr_map[pr_id].append(row)

        created_receipts = []
        for pr_id, rows in pr_map.items():
            first_row = rows[0]
            s_name = str(first_row[col_map["supplier"]]).strip()
            po_num_first = str(first_row[col_map["po_num"]]).strip()
            pr_date_raw = first_row[col_map["pr_date"]]
            
            po_header = frappe.db.get_value("Purchase Order", po_num_first, ["company", "currency", "conversion_rate"], as_dict=True)
            
            pr = frappe.new_doc("Purchase Receipt")
            pr.name = pr_id # FORCE PR NUMBER FROM EXCEL
            
            # Series Matching logic
            available_series = frappe.get_meta("Purchase Receipt").get_field("naming_series").options.split("\n")
            for series in available_series:
                prefix = series.replace(".####", "").replace(".YY.", "").replace(".YYYY.", "").strip()
                if pr_id.startswith(prefix):
                    pr.naming_series = series
                    break

            pr.supplier = s_name
            pr.company = po_header.company or frappe.db.get_single_value("Global Defaults", "default_company")
            pr.currency = po_header.currency
            pr.conversion_rate = po_header.conversion_rate or 1.0
            pr.posting_date = getdate(pr_date_raw) if pr_date_raw else nowdate()

            for row in rows:
                p_num = str(row[col_map["po_num"]]).strip()
                i_code = str(row[col_map["item_code"]]).strip()
                l_val = str(row[col_map["line_number"]]).strip() if col_map.get("line_number") is not None and row[col_map["line_number"]] else ""
                
                target_item_name = find_po_item_name(p_num, i_code, l_val)
                po_item = frappe.get_doc("Purchase Order Item", target_item_name)
                
                pr_item = pr.append("items", {
                    "item_code": i_code,
                    "qty": flt(row[col_map["quantity"]]),
                    "rate": flt(row[col_map["rate"]]),
                    "purchase_order": p_num,
                    "purchase_order_item": po_item.name,
                    "warehouse": po_item.warehouse
                })
                pr_item.run_method("set_missing_values")
                
                # Logic for line_number or custom_line_number
                l_field = "line_number"
                if not hasattr(pr_item, "line_number"):
                    if hasattr(pr_item, "custom_line_number"):
                        l_field = "custom_line_number"
                
                # If Excel has a line value, put it in. If empty, ignore it.
                if l_val:
                    setattr(pr_item, l_field, l_val)

            pr.run_method("set_missing_values")
            pr.run_method("calculate_taxes_and_totals")
            pr.flags.ignore_permissions = True
            
            # Deep Insert to bypass naming series
            pr.db_insert()
            for child in pr.get_all_children():
                child.db_insert()
            pr.run_method("on_update")
            
            created_receipts.append(pr.name)

        doc.db_set("status", "Completed")
        doc.db_set("receipts_log", "SUMMARY:\n" + "\n".join([f"✅ {name}" for name in created_receipts]))
        frappe.db.commit()
    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("receipts_log", f"❌ Error:\n{frappe.get_traceback()}")
        frappe.db.commit()
