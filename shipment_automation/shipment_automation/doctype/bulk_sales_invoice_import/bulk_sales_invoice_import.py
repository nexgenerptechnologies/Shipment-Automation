import frappe
from frappe.model.document import Document
from frappe.utils import flt, nowdate, getdate
import openpyxl
from io import BytesIO
import datetime
from shipment_automation.shipment_automation.utils import parse_excel_date, validate_2_of_3


@frappe.whitelist()
def download_template():
    """Generates and downloads the Bulk Sales Invoice Import Excel template."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Bulk Sales Invoice Import"
    
    headers = [
        "Sales Invoice Number (Optional)", "Posting Date", "Customer Name", 
        "Delivery Note Number", "Sales Order Number", "Item Code", "Item Name", 
        "Description", "Quantity", "Rate", "Update Stock (Yes/No)"
    ]
    ws.append(headers)
    
    from openpyxl.styles import Font
    for cell in ws[1]:
        cell.font = Font(bold=True)

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    
    frappe.response['filename'] = "Bulk_Sales_Invoice_Import_Template.xlsx"
    frappe.response['filecontent'] = output.getvalue()
    frappe.response['type'] = 'binary'


class BulkSalesInvoiceImport(Document):

    @frappe.whitelist()
    def start_validation(self):
        self.db_set("status", "Validating")
        self.db_set("validation_log", "⏳ Validation in progress. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_sales_invoice_import.bulk_sales_invoice_import.run_validation",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Validation started. Refresh in a few seconds."

    @frappe.whitelist()
    def start_creation(self):
        if self.status != "Validated":
            frappe.throw("Please validate the data first.")
        self.db_set("status", "Processing")
        self.db_set("si_log", "⏳ Creating Sales Invoices. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_sales_invoice_import.bulk_sales_invoice_import.run_processing",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Processing started. Refresh in a few seconds."


def get_column_map(sheet):
    header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
    mapping = {}
    expected = {
        "si_num": ["Sales Invoice Number (Optional)", "Sales Invoice Number", "SI Number"],
        "posting_date": ["Posting Date"],
        "customer": ["Customer Name", "Customer"],
        "dn_num": ["Delivery Note Number", "DN Number"],
        "so_num": ["Sales Order Number", "SO Number"],
        "item_code": ["Item Code"],
        "item_name": ["Item Name"],
        "description": ["Description"],
        "quantity": ["Quantity", "Qty"],
        "rate": ["Rate"],
        "update_stock": ["Update Stock (Yes/No)", "Update Stock"]
    }
    for idx, cell in enumerate(header_row):
        if not cell: continue
        clean = str(cell).strip().lower()
        for key, aliases in expected.items():
            if any(alias.lower() == clean for alias in aliases):
                mapping[key] = idx
    return mapping


def run_validation(docname):
    doc = frappe.get_doc("Bulk Sales Invoice Import", docname)
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
            si_id = str(row[col_map["si_num"]]).strip() if col_map.get("si_num") is not None and row[col_map["si_num"]] else ""
            customer = str(row[col_map["customer"]]).strip() if col_map.get("customer") is not None else ""
            dn_num = str(row[col_map["dn_num"]]).strip() if col_map.get("dn_num") is not None and row[col_map["dn_num"]] else ""
            so_num = str(row[col_map["so_num"]]).strip() if col_map.get("so_num") is not None and row[col_map["so_num"]] else ""
            item_code = str(row[col_map["item_code"]]).strip() if col_map.get("item_code") is not None else ""
            item_name = str(row[col_map["item_name"]]).strip() if col_map.get("item_name") is not None else ""
            description = str(row[col_map["description"]]).strip() if col_map.get("description") is not None else ""
            
            posting_date = parse_excel_date(row[col_map["posting_date"]])

            if not customer or not frappe.db.exists("Customer", customer):
                row_errors.append(f"Customer '{customer}' not found.")
            
            if dn_num and not frappe.db.exists("Delivery Note", dn_num):
                row_errors.append(f"Delivery Note '{dn_num}' not found.")
            
            if so_num and not frappe.db.exists("Sales Order", so_num):
                row_errors.append(f"Sales Order '{so_num}' not found.")
            
            if not item_code or not frappe.db.exists("Item", item_code):
                row_errors.append(f"Item '{item_code}' not found.")
            else:
                # 2-of-3 column match
                it_doc = frappe.get_doc("Item", item_code)
                if not validate_2_of_3(it_doc, item_code, item_name, description):
                    row_errors.append("2-of-3 match failed (Code/Name/Description).")

            if posting_date and getdate(posting_date) > getdate(today):
                row_errors.append(f"Posting Date '{posting_date}' is a future date.")

            if si_id and frappe.db.exists("Sales Invoice", si_id):
                row_errors.append(f"Duplicate Sales Invoice Number '{si_id}' already exists.")

            if row_errors:
                errors.append(f"Row {row_idx} ❌ " + " | ".join(row_errors))
            else:
                ok_rows += 1

        if not errors:
            doc.db_set("status", "Validated")
            doc.db_set("validation_log", f"✅ {ok_rows} row(s) validated successfully.")
        else:
            doc.db_set("status", "Failed")
            doc.db_set("validation_log", "❌ Issues found:\n\n" + "\n".join(errors))
        frappe.db.commit()
    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("validation_log", f"❌ Error:\n{frappe.get_traceback()}")
        frappe.db.commit()


def run_processing(docname):
    doc = frappe.get_doc("Bulk Sales Invoice Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active
        col_map = get_column_map(sheet)
        
        # Group by SI ID if provided, else by Customer + Posting Date
        si_groups = {}
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not any(row): continue
            si_id = str(row[col_map["si_num"]]).strip() if col_map.get("si_num") is not None and row[col_map["si_num"]] else ""
            cust = str(row[col_map["customer"]]).strip()
            date = parse_excel_date(row[col_map["posting_date"]]) or nowdate()
            
            group_key = si_id if si_id else (cust, date)
            if group_key not in si_groups: si_groups[group_key] = []
            si_groups[group_key].append(row)

        created = []
        for key, rows in si_groups.items():
            try:
                first = rows[0]
                customer = str(first[col_map["customer"]]).strip()
                si_id = str(first[col_map["si_num"]]).strip() if col_map.get("si_num") is not None and first[col_map["si_num"]] else ""
                posting_date = parse_excel_date(first[col_map["posting_date"]]) or nowdate()
                update_stock = 1 if str(first[col_map["update_stock"]]).strip().lower() in ["yes", "y", "1"] else 0
                
                si = frappe.new_doc("Sales Invoice")
                if si_id:
                    si.name = si_id
                
                si.customer = customer
                si.posting_date = posting_date
                si.update_stock = update_stock
                
                for r in rows:
                    dn_num = str(r[col_map["dn_num"]]).strip() if col_map.get("dn_num") is not None and r[col_map["dn_num"]] else ""
                    so_num = str(r[col_map["so_num"]]).strip() if col_map.get("so_num") is not None and r[col_map["so_num"]] else ""
                    item_code = str(r[col_map["item_code"]]).strip()
                    qty = flt(r[col_map["quantity"]])
                    rate = flt(r[col_map["rate"]])
                    
                    item_row = si.append("items", {
                        "item_code": item_code,
                        "qty": qty,
                        "rate": rate,
                        "delivery_note": dn_num,
                        "sales_order": so_num
                    })
                    
                    # Logic to pull from DN or SO if provided
                    if dn_num:
                        dn_item = frappe.db.get_value("Delivery Note Item", 
                            {"parent": dn_num, "item_code": item_code}, 
                            ["name", "rate", "warehouse"], as_dict=True)
                        if dn_item:
                            item_row.dn_detail = dn_item.name
                            item_row.warehouse = dn_item.warehouse
                            if not rate: item_row.rate = dn_item.rate
                    
                    elif so_num:
                        so_item = frappe.db.get_value("Sales Order Item", 
                            {"parent": so_num, "item_code": item_code}, 
                            ["name", "rate"], as_dict=True)
                        if so_item:
                            item_row.so_detail = so_item.name
                            if not rate: item_row.rate = so_item.rate

                si.flags.ignore_permissions = True
                si.insert()
                # si.submit() # Optional
                created.append(f"✅ {si.name}")
            except Exception as e:
                created.append(f"❌ Error creating SI: {str(e)}")

        doc.db_set("status", "Completed")
        doc.db_set("si_log", "SUMMARY:\n" + "\n".join(created))
        frappe.db.commit()
    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("si_log", f"❌ Error:\n{frappe.get_traceback()}")
        frappe.db.commit()
