import frappe
import openpyxl
from frappe.model.document import Document
from frappe.utils import flt, get_site_path

class BulkBOMImport(Document):
    @frappe.whitelist()
    def start_validation(self):
        self.db_set("status", "Validating")
        self.db_set("validation_log", "⏳ Validating BOM structure...")
        frappe.db.commit()
        frappe.enqueue("shipment_automation.shipment_automation.doctype.bulk_bom_import.bulk_bom_import.run_validation", docname=self.name)

    @frappe.whitelist()
    def start_processing(self):
        if self.status != "Validated":
            frappe.throw("Please validate the data first.")
        self.db_set("status", "Processing")
        self.db_set("processing_log", "⏳ Creating Bill of Materials records. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue("shipment_automation.shipment_automation.doctype.bulk_bom_import.bulk_bom_import.run_processing", docname=self.name)

    @frappe.whitelist()
    def download_template(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "BOM Import Template"
        
        headers = [
            "Parent Item Code", "BOM Name (Optional)", "Quantity to Manufacture", 
            "Child Item Code", "Child Quantity", "UOM"
        ]
        ws.append(headers)
        
        # Add sample data for one BOM with two items
        ws.append(["PARENT-01", "Main Assembly", "1", "RAW-001", "5", "Nos"])
        ws.append(["PARENT-01", "", "1", "RAW-002", "2", "Nos"])
        
        file_path = get_site_path("public", "files", "Bulk_BOM_Import_Template.xlsx")
        wb.save(file_path)
        
        return "/files/Bulk_BOM_Import_Template.xlsx"

def clean_val(val):
    if val is None or str(val).lower() == "none":
        return ""
    return str(val).strip()

def run_validation(docname):
    doc = frappe.get_doc("Bulk BOM Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.bom_excel})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active
        
        errors = []
        for i, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            parent = clean_val(row[0])
            child = clean_val(row[3])
            
            if not parent:
                errors.append(f"Row {i}: Parent Item Code is missing.")
            elif not frappe.db.exists("Item", parent):
                errors.append(f"Row {i}: Parent Item '{parent}' does not exist.")
                
            if not child:
                errors.append(f"Row {i}: Child Item Code is missing.")
            elif not frappe.db.exists("Item", child):
                errors.append(f"Row {i}: Child Item '{child}' does not exist.")

        if errors:
            doc.db_set("status", "Failed")
            doc.db_set("validation_log", "❌ Validation Errors:\n" + "\n".join(errors))
        else:
            doc.db_set("status", "Validated")
            doc.db_set("validation_log", "✅ All items validated. Parent and Child items exist.")
        frappe.db.commit()
        
    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("validation_log", f"❌ Error:\n{frappe.get_traceback()}")
        frappe.db.commit()

def run_processing(docname):
    doc = frappe.get_doc("Bulk BOM Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.bom_excel})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active
        
        # Group by Parent Item
        boms = {}
        for row in sheet.iter_rows(min_row=2, values_only=True):
            parent = clean_val(row[0])
            if not parent: continue
            
            if parent not in boms:
                boms[parent] = {
                    "name": clean_val(row[1]) or f"BOM for {parent}",
                    "qty": flt(row[2]) or 1.0,
                    "items": []
                }
            
            boms[parent]["items"].append({
                "item_code": clean_val(row[3]),
                "qty": flt(row[4]),
                "uom": clean_val(row[5])
            })

        summary = []
        for parent_code, data in boms.items():
            try:
                # Create BOM
                bom = frappe.new_doc("BOM")
                bom.item = parent_code
                bom.quantity = data["qty"]
                bom.is_active = 1
                bom.is_default = 1
                
                for item_data in data["items"]:
                    bom.append("items", {
                        "item_code": item_data["item_code"],
                        "qty": item_data["qty"],
                        "uom": item_data["uom"] or frappe.db.get_value("Item", item_data["item_code"], "stock_uom")
                    })
                
                bom.insert(ignore_permissions=True)
                bom.submit()
                summary.append(f"✅ {parent_code}: BOM Created and Submitted")
                
            except Exception as e:
                summary.append(f"❌ {parent_code}: {str(e)}")

        doc.db_set("status", "Completed")
        doc.db_set("processing_log", "SUMMARY:\n" + "\n".join(summary))
        frappe.db.commit()

    except Exception:
        doc.db_set("status", "Failed")
        doc.db_set("processing_log", f"❌ Critical Error:\n{frappe.get_traceback()}")
        frappe.db.commit()
