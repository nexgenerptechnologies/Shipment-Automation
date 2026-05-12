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
            "Parent Item Code", "Item Code (Component)", "Quantity", "UOM",
            "Is Scrap (Yes/No)", "Operation Name", "Workstation", "Operation Time (Mins)", "Rate"
        ]
        ws.append(headers)
        
        # Sample data based on user image
        ws.append(["FGD00167", "RMT00059", "0.010970", "KGS", "No", "", "", "", "68.00"])
        ws.append(["FGD00167", "SCP00006", "-0.004230", "KGS", "Yes", "", "", "", "32.00"])
        ws.append(["FGD00167", "", "", "", "No", "Cutting & Bending", "PP45T08", "0.022222", "19.40"])
        
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
            child = clean_val(row[1])
            op_name = clean_val(row[5])
            workstation = clean_val(row[6])
            
            if not parent:
                errors.append(f"Row {i}: Parent Item Code is missing.")
            elif not frappe.db.exists("Item", parent):
                errors.append(f"Row {i}: Parent Item '{parent}' does not exist.")
            
            if child and not frappe.db.exists("Item", child):
                errors.append(f"Row {i}: Component Item '{child}' does not exist.")
            
            if op_name and workstation and not frappe.db.exists("Workstation", workstation):
                errors.append(f"Row {i}: Workstation '{workstation}' does not exist.")

        if errors:
            doc.db_set("status", "Failed")
            doc.db_set("validation_log", "❌ Validation Errors:\n" + "\n".join(errors))
        else:
            doc.db_set("status", "Validated")
            doc.db_set("validation_log", "✅ Validation Successful.")
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
                boms[parent] = {"items": [], "operations": []}
            
            child = clean_val(row[1])
            if child:
                boms[parent]["items"].append({
                    "item_code": child,
                    "qty": flt(row[2]),
                    "uom": clean_val(row[3]),
                    "is_scrap": 1 if clean_val(row[4]).lower() == "yes" else 0,
                    "rate": flt(row[8])
                })
            
            op_name = clean_val(row[5])
            if op_name:
                boms[parent]["operations"].append({
                    "operation": op_name,
                    "workstation": clean_val(row[6]),
                    "time_in_mins": flt(row[7]) * 60, # Assuming user input is hours as per image
                    "hour_rate": flt(row[8])
                })

        summary = []
        for parent_code, data in boms.items():
            try:
                # Create BOM
                bom = frappe.new_doc("BOM")
                bom.item = parent_code
                bom.quantity = 1.0
                bom.with_operations = 1 if data["operations"] else 0
                
                for item_data in data["items"]:
                    if item_data["is_scrap"]:
                        bom.append("scrap_items", {
                            "item_code": item_data["item_code"],
                            "stock_qty": item_data["qty"],
                            "rate": item_data["rate"]
                        })
                    else:
                        bom.append("items", {
                            "item_code": item_data["item_code"],
                            "qty": item_data["qty"],
                            "uom": item_data["uom"] or frappe.db.get_value("Item", item_data["item_code"], "stock_uom"),
                            "rate": item_data["rate"]
                        })
                
                for op_data in data["operations"]:
                    # Check if operation exists, if not create it
                    if not frappe.db.exists("Operation", op_data["operation"]):
                        new_op = frappe.new_doc("Operation")
                        new_op.operation = op_data["operation"]
                        new_op.insert(ignore_permissions=True)
                    
                    bom.append("operations", {
                        "operation": op_data["operation"],
                        "workstation": op_data["workstation"],
                        "time_in_mins": op_data["time_in_mins"],
                        "hour_rate": op_data["hour_rate"]
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
