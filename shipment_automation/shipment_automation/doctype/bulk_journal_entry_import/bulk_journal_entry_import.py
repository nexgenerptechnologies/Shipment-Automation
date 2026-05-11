import frappe
from frappe.model.document import Document
from frappe.utils import flt, nowdate, getdate
import openpyxl
from io import BytesIO
import datetime
from shipment_automation.shipment_automation.utils import parse_excel_date


@frappe.whitelist()
def download_template():
    """Generates and downloads the Bulk Journal Entry Import Excel template."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Bulk Journal Entry Import"
    
    headers = [
        "Voucher ID (Link rows)", "Voucher Type", "Posting Date", 
        "Account", "Party Type (Optional)", "Party (Optional)", 
        "Debit", "Credit", "User Remark"
    ]
    ws.append(headers)
    
    from openpyxl.styles import Font
    for cell in ws[1]:
        cell.font = Font(bold=True)

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    
    frappe.response['filename'] = "Bulk_Journal_Entry_Import_Template.xlsx"
    frappe.response['filecontent'] = output.getvalue()
    frappe.response['type'] = 'binary'


class BulkJournalEntryImport(Document):

    @frappe.whitelist()
    def start_validation(self):
        self.db_set("status", "Validating")
        self.db_set("validation_log", "⏳ Validation in progress. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_journal_entry_import.bulk_journal_entry_import.run_validation",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Validation started. Refresh in a few seconds."

    @frappe.whitelist()
    def start_creation(self):
        if self.status != "Validated":
            frappe.throw("Please validate the data first.")
        self.db_set("status", "Processing")
        self.db_set("je_log", "⏳ Creating Journal Entries. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_journal_entry_import.bulk_journal_entry_import.run_processing",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Processing started. Refresh in a few seconds."


def get_column_map(sheet):
    header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
    mapping = {}
    expected = {
        "v_id": ["Voucher ID (Link rows)", "Voucher ID"],
        "v_type": ["Voucher Type"],
        "posting_date": ["Posting Date"],
        "account": ["Account"],
        "party_type": ["Party Type (Optional)", "Party Type"],
        "party": ["Party (Optional)", "Party"],
        "debit": ["Debit"],
        "credit": ["Credit"],
        "remark": ["User Remark", "Remark"]
    }
    for idx, cell in enumerate(header_row):
        if not cell: continue
        clean = str(cell).strip().lower()
        for key, aliases in expected.items():
            if any(alias.lower() == clean for alias in aliases):
                mapping[key] = idx
    return mapping


def run_validation(docname):
    doc = frappe.get_doc("Bulk Journal Entry Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active
        col_map = get_column_map(sheet)
        
        errors = []
        voucher_groups = {}
        
        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not any(row): continue
            
            row_errors = []
            v_id = str(row[col_map["v_id"]]).strip() if col_map.get("v_id") is not None and row[col_map["v_id"]] else "SINGLE"
            v_type = str(row[col_map["v_type"]]).strip() if col_map.get("v_type") is not None else ""
            account = str(row[col_map["account"]]).strip() if col_map.get("account") is not None else ""
            p_type = str(row[col_map["party_type"]]).strip() if col_map.get("party_type") is not None and row[col_map["party_type"]] else ""
            party = str(row[col_map["party"]]).strip() if col_map.get("party") is not None and row[col_map["party"]] else ""
            debit = flt(row[col_map["debit"]])
            credit = flt(row[col_map["credit"]])
            
            posting_date = parse_excel_date(row[col_map["posting_date"]])

            if not v_type: row_errors.append("Voucher Type is mandatory.")
            if not account or not frappe.db.exists("Account", account):
                row_errors.append(f"Account '{account}' not found.")
            
            if p_type and party:
                if not frappe.db.exists(p_type, party):
                    row_errors.append(f"Party '{party}' of type '{p_type}' not found.")
            
            if not posting_date:
                row_errors.append("Invalid Posting Date (Use DD/MM/YYYY).")
            elif getdate(posting_date) > getdate(nowdate()):
                row_errors.append(f"Posting Date '{posting_date}' is a future date.")

            if row_errors:
                errors.append(f"Row {row_idx} ❌ " + " | ".join(row_errors))
            
            # Sum debits/credits per voucher to check balance later
            if v_id not in voucher_groups: voucher_groups[v_id] = {"d": 0, "c": 0}
            voucher_groups[v_id]["d"] += debit
            voucher_groups[v_id]["c"] += credit

        # Final Balance Check
        for vid, sums in voucher_groups.items():
            if abs(sums["d"] - sums["c"]) > 0.01:
                errors.append(f"Voucher ID '{vid}' ❌ Out of balance! Total Debit: {sums['d']} | Total Credit: {sums['c']}")

        if not errors:
            doc.db_set("status", "Validated")
            doc.db_set("validation_log", f"✅ All vouchers are balanced and validated.")
        else:
            doc.db_set("status", "Failed")
            doc.db_set("validation_log", "❌ Issues found:\n\n" + "\n".join(errors))
        frappe.db.commit()
    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("validation_log", f"❌ Error:\n{frappe.get_traceback()}")
        frappe.db.commit()


def run_processing(docname):
    doc = frappe.get_doc("Bulk Journal Entry Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active
        col_map = get_column_map(sheet)
        
        # Group by Voucher ID
        groups = {}
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not any(row): continue
            v_id = str(row[col_map["v_id"]]).strip() if col_map.get("v_id") is not None and row[col_map["v_id"]] else "SINGLE"
            if v_id not in groups: groups[v_id] = []
            groups[v_id].append(row)

        created = []
        for v_id, rows in groups.items():
            try:
                first = rows[0]
                je = frappe.new_doc("Journal Entry")
                je.voucher_type = str(first[col_map["v_type"]]).strip()
                je.posting_date = parse_excel_date(first[col_map["posting_date"]])
                je.user_remark = str(first[col_map["remark"]]).strip() if col_map.get("remark") is not None else f"Bulk Import {v_id}"
                
                for r in rows:
                    je.append("accounts", {
                        "account": str(r[col_map["account"]]).strip(),
                        "party_type": str(r[col_map["party_type"]]).strip() if col_map.get("party_type") is not None and r[col_map["party_type"]] else None,
                        "party": str(r[col_map["party"]]).strip() if col_map.get("party") is not None and r[col_map["party"]] else None,
                        "debit_in_account_currency": flt(r[col_map["debit"]]),
                        "credit_in_account_currency": flt(r[col_map["credit"]])
                    })

                je.flags.ignore_permissions = True
                je.insert()
                # je.submit() # Keep in Draft for accountant review
                created.append(f"✅ {je.name}")
            except Exception as e:
                created.append(f"❌ Voucher '{v_id}': {str(e)}")

        doc.db_set("status", "Completed")
        doc.db_set("je_log", "SUMMARY:\n" + "\n".join(created))
        frappe.db.commit()
    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("je_log", f"❌ Error:\n{frappe.get_traceback()}")
        frappe.db.commit()
