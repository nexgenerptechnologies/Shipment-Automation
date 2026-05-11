import frappe
from frappe.model.document import Document
from frappe.utils import flt, nowdate, getdate
import openpyxl
from io import BytesIO
import datetime


@frappe.whitelist()
def download_template():
    """Generates and downloads the Bulk Purchase Invoice & BOE Import Excel template."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Bulk PI and BOE Import"
    
    headers = [
        "Purchase Receipt Number",
        "Purchase Invoice Number", "Purchase Invoice Date",
        "Bill of Entry Number", "Bill of Entry Date"
    ]
    ws.append(headers)
    
    from openpyxl.styles import Font
    for cell in ws[1]:
        cell.font = Font(bold=True)

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    
    frappe.response['filename'] = "Bulk_Invoice_BOE_Import_Template.xlsx"
    frappe.response['filecontent'] = output.getvalue()
    frappe.response['type'] = 'binary'


class BulkPurchaseInvoiceImport(Document):

    @frappe.whitelist()
    def start_validation(self):
        self.db_set("status", "Validating")
        self.db_set("validation_log", "⏳ Validation in progress. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_purchase_invoice_import.bulk_purchase_invoice_import.run_validation",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Validation started. Refresh in a few seconds."

    @frappe.whitelist()
    def start_processing(self):
        if self.status != "Validated":
            frappe.throw("Please validate the data first.")
        self.db_set("status", "Processing")
        self.db_set("processing_log", "⏳ Creating Invoices & BOEs. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_purchase_invoice_import.bulk_purchase_invoice_import.run_processing",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Processing started. Refresh in a few seconds."


def get_column_map(sheet):
    header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
    mapping = {}
    expected = {
        "pr_num": ["Purchase Receipt Number", "PR Number"],
        "pi_num": ["Purchase Invoice Number", "PI Number", "Invoice No"],
        "pi_date": ["Purchase Invoice Date", "PI Date", "Invoice Date"],
        "boe_num": ["Bill of Entry Number", "BOE Number", "BOE No"],
        "boe_date": ["Bill of Entry Date", "BOE Date"]
    }
    for idx, cell in enumerate(header_row):
        if not cell: continue
        clean = str(cell).strip().lower()
        for key, aliases in expected.items():
            if any(alias.lower() == clean for alias in aliases):
                mapping[key] = idx
    return mapping


def parse_excel_date(date_val):
    """STRICTLY forces DD/MM/YYYY parsing even if Excel auto-converted it to MM/DD/YYYY."""
    if not date_val:
        return None
    
    if isinstance(date_val, (datetime.datetime, datetime.date)):
        if date_val.day <= 12:
             try:
                 return datetime.date(date_val.year, date_val.day, date_val.month).strftime("%Y-%m-%d")
             except ValueError:
                 return date_val.strftime("%Y-%m-%d")
        return date_val.strftime("%Y-%m-%d")

    if isinstance(date_val, str):
        date_str = date_val.strip()
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"]:
            try:
                dt = datetime.datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    try:
        res = getdate(date_val)
        return res.strftime("%Y-%m-%d") if res else None
    except:
        return None


def run_validation(docname):
    doc = frappe.get_doc("Bulk Purchase Invoice Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active
        col_map = get_column_map(sheet)
        
        errors = []
        ok_rows = 0
        today = nowdate()
        
        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not any(row): continue
            
            row_errors = []
            
            pr_id = str(row[col_map["pr_num"]]).strip() if col_map.get("pr_num") is not None and row[col_map["pr_num"]] else ""
            pi_id = str(row[col_map["pi_num"]]).strip() if col_map.get("pi_num") is not None and row[col_map["pi_num"]] else ""
            pi_date = parse_excel_date(row[col_map["pi_date"]]) if col_map.get("pi_date") is not None else None
            boe_id = str(row[col_map["boe_num"]]).strip() if col_map.get("boe_num") is not None and row[col_map["boe_num"]] else ""
            boe_date = parse_excel_date(row[col_map["boe_date"]]) if col_map.get("boe_date") is not None else None

            if not pr_id:
                row_errors.append("Purchase Receipt Number missing.")
            elif not frappe.db.exists("Purchase Receipt", pr_id):
                row_errors.append(f"Purchase Receipt '{pr_id}' not found in system.")

            if not pi_id:
                row_errors.append("Purchase Invoice Number missing.")
            elif frappe.db.exists("Purchase Invoice", pi_id):
                row_errors.append(f"Duplicate Error: Purchase Invoice '{pi_id}' already exists.")

            if pi_date and getdate(pi_date) > getdate(today):
                row_errors.append(f"Purchase Invoice Date '{pi_date}' is a future date.")

            if not boe_id:
                row_errors.append("Bill of Entry Number missing.")
            elif frappe.db.exists("Bill of Entry", boe_id):
                row_errors.append(f"Duplicate Error: Bill of Entry '{boe_id}' already exists.")

            if row_errors:
                errors.append(f"Row {row_idx} ❌ " + " | ".join(row_errors))
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
    doc = frappe.get_doc("Bulk Purchase Invoice Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active
        col_map = get_column_map(sheet)
        
        created = []
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not any(row): continue
            
            pr_id = str(row[col_map["pr_num"]]).strip()
            pi_id = str(row[col_map["pi_num"]]).strip()
            pi_date = parse_excel_date(row[col_map["pi_date"]])
            boe_id = str(row[col_map["boe_num"]]).strip()
            boe_date = parse_excel_date(row[col_map["boe_date"]])

            try:
                pr_doc = frappe.get_doc("Purchase Receipt", pr_id)
                
                # 1. Create Purchase Invoice
                from erpnext.buying.doctype.purchase_receipt.purchase_receipt import make_purchase_invoice
                pi = make_purchase_invoice(pr_id)
                pi.name = pi_id
                pi.posting_date = pi_date or nowdate()
                pi.bill_no = pi_id
                
                # Set Naming Series
                pi_series = frappe.get_meta("Purchase Invoice").get_field("naming_series").options.split("\n")
                pi.naming_series = pi_series[0]
                for s in pi_series:
                    prefix = s.replace(".####", "").replace(".YY.", "").replace(".YYYY.", "").strip()
                    if prefix and pi_id.startswith(prefix):
                        pi.naming_series = s
                        break

                pi.flags.ignore_permissions = True
                pi.db_insert()
                for item in pi.get("items"): item.db_insert()
                for tax in pi.get("taxes"): tax.db_insert()
                pi.run_method("on_update")
                pi.submit()

                # 2. Create Bill of Entry
                boe = frappe.new_doc("Bill of Entry")
                boe.name = boe_id
                boe.purchase_invoice = pi.name
                boe.posting_date = boe_date or nowdate()
                boe.bill_of_entry_number = boe_id
                boe.bill_of_entry_date = boe_date or nowdate()
                boe.company = pr_doc.company
                boe.supplier = pr_doc.supplier
                boe.flags.ignore_permissions = True
                boe.insert(ignore_mandatory=True)
                
                created.append(f"✅ {pr_id} -> PI {pi.name}, BOE {boe.name}")
            except Exception as e:
                created.append(f"❌ {pr_id}: {str(e)}")

        doc.db_set("status", "Completed")
        doc.db_set("processing_log", "SUMMARY:\n" + "\n".join(created))
        frappe.db.commit()
    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("processing_log", f"❌ Error:\n{frappe.get_traceback()}")
        frappe.db.commit()
