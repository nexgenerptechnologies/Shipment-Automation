import frappe
from frappe.utils import getdate, flt
import datetime

def parse_excel_date(date_val):
    """
    STRICTLY forces DD/MM/YYYY parsing.
    Handles Excel date objects (swapping MM/DD if needed) and string formats.
    """
    if not date_val:
        return None
    
    # CASE 1: Date Object from Excel
    if isinstance(date_val, (datetime.datetime, datetime.date)):
        # If day <= 12, check if Excel/System swapped it to MM/DD
        if date_val.day <= 12:
             try:
                 return datetime.date(date_val.year, date_val.day, date_val.month).strftime("%Y-%m-%d")
             except ValueError:
                 return date_val.strftime("%Y-%m-%d")
        return date_val.strftime("%Y-%m-%d")

    # CASE 2: String/Text
    if isinstance(date_val, str):
        date_str = date_val.strip()
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%Y-%m-%d"]:
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

def validate_2_of_3(doc_item, excel_code, excel_name, excel_desc):
    """
    Validates that at least 2 out of 3 fields match between the ERPNext Item and Excel row.
    """
    score = 0
    if str(doc_item.item_code).strip() == str(excel_code).strip(): score += 1
    if str(doc_item.item_name).strip() == str(excel_name).strip(): score += 1
    if str(doc_item.description).strip() == str(excel_desc).strip(): score += 1
    return score >= 2

def check_rate_precision(val1, val2, precision=7):
    """
    Compares two rates with high decimal precision.
    """
    diff = abs(flt(val1) - flt(val2))
    return diff <= (10 ** -precision)

def cleanup_old_import_logs():
    """
    Daily scheduled job to delete import logs older than 60 days.
    Also deletes the associated physical files to save disk space.
    """
    days = 60
    doctypes = [
        "Bulk PO Import", "Bulk Purchase Receipt Import", "Bulk Purchase Invoice Import",
        "Bulk Supplier Import", "Bulk Customer Import", "Bulk Sales Order Import",
        "Bulk Delivery Note Import", "Bulk Sales Invoice Import", "Bulk Journal Entry Import",
        "Bulk Stock Entry Import", "Bulk Employee Import"
    ]
    
    for dt in doctypes:
        if not frappe.db.exists("DocType", dt): continue
        
        # Find records older than 60 days
        old_records = frappe.get_all(dt, filters={
            "creation": ["<", frappe.utils.add_days(nowdate(), -days)]
        })
        
        for record in old_records:
            doc = frappe.get_doc(dt, record.name)
            
            # Delete attached files first
            attachments = frappe.get_all("File", filters={
                "attached_to_doctype": dt,
                "attached_to_name": record.name
            })
            for f in attachments:
                frappe.delete_doc("File", f.name, ignore_permissions=True)
            
            # Delete the record itself
            frappe.delete_doc(dt, record.name, ignore_permissions=True)
            
    frappe.db.commit()
