import frappe
import openpyxl
from frappe.model.document import Document
from frappe.utils import flt, get_site_path
from io import BytesIO

class BulkBOMImport(Document):
    @frappe.whitelist()
    def start_validation(self):
        self.db_set("status", "Validating")
        self.db_set("validation_log", "⏳ Validating BOM structure...")
        frappe.db.commit()
        frappe.enqueue("shipment_automation.shipment_automation.doctype.bulk_bom_import.bulk_bom_import.run_validation", docname=self.name, queue="long", timeout=3600)

    @frappe.whitelist()
    def start_processing(self):
        if self.status != "Validated":
            frappe.throw("Please validate the data first.")
        self.db_set("status", "Processing")
        self.db_set("processing_log", "⏳ Creating Bill of Materials records. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue("shipment_automation.shipment_automation.doctype.bulk_bom_import.bulk_bom_import.run_processing", docname=self.name, queue="long", timeout=3600)

    @frappe.whitelist()
    def download_template(self):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Bulk BOM Import"
        
        headers = [
            "Item to Manufacture", "BOM Qty", "Row Type (Material/Operation)", 
            "Code / Name", "Qty / Time (Mins)", "Workstation"
        ]
        ws.append(headers)
        
        file_path = get_site_path("public", "files", "Bulk_BOM_Import_Minimal.xlsx")
        wb.save(file_path)
        
        return "/files/Bulk_BOM_Import_Minimal.xlsx"

def clean_val(val):
    if val is None or str(val).lower() == "none":
        return ""
    return str(val).strip()

def get_col_map(sheet):
    header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
    mapping = {}
    for idx, cell in enumerate(header_row):
        if not cell: continue
        clean = str(cell).strip().lower()
        if "manufacture" in clean or "parent" in clean: mapping["parent"] = idx
        elif "bom qty" in clean: mapping["bom_qty"] = idx
        elif "row type" in clean: mapping["row_type"] = idx
        elif "code" in clean or "name" in clean: mapping["code_name"] = idx
        elif "qty" in clean or "time" in clean: mapping["qty_time"] = idx
        elif "workstation" in clean: mapping["workstation"] = idx
    return mapping

def run_validation(docname):
    doc = frappe.get_doc("Bulk BOM Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.bom_excel})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active
        col_map = get_col_map(sheet)
        
        errors = []
        current_parent = ""
        
        for i, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not any(row): continue
            
            parent_raw = clean_val(row[col_map.get("parent", 0)])
            if parent_raw:
                current_parent = parent_raw
            
            parent = current_parent
            if not parent:
                errors.append(f"Row {i}: Item to Manufacture is missing.")
                continue
            elif not frappe.db.exists("Item", parent):
                errors.append(f"Row {i}: Item to Manufacture '{parent}' does not exist.")
            
            row_type = clean_val(row[col_map.get("row_type", -1)]).lower()
            code_name = clean_val(row[col_map.get("code_name", -1)])
            qty_time = flt(row[col_map.get("qty_time", -1)])
            workstation = clean_val(row[col_map.get("workstation", -1)])
            
            if "material" in row_type or "scrap" in row_type:
                if not code_name:
                    errors.append(f"Row {i}: Code / Name is required for Materials.")
                elif not frappe.db.exists("Item", code_name):
                    errors.append(f"Row {i}: Item '{code_name}' does not exist.")
            
            elif "operation" in row_type:
                if not code_name:
                    errors.append(f"Row {i}: Code / Name (Operation Name) is required for Operations.")
                if workstation and not frappe.db.exists("Workstation", workstation):
                    errors.append(f"Row {i}: Workstation '{workstation}' does not exist.")

        if errors:
            doc.db_set("status", "Failed")
            doc.db_set("validation_log", "❌ Validation Errors:\n" + "\n".join(errors[:50]) + ("\n...and more" if len(errors)>50 else ""))
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
        col_map = get_col_map(sheet)
        
        boms = {}
        dependencies = {} # parent -> set of child items
        current_parent = ""
        
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not any(row): continue
            
            parent_raw = clean_val(row[col_map.get("parent", 0)])
            if parent_raw:
                current_parent = parent_raw
            
            parent = current_parent
            if not parent: continue
            
            bom_qty = flt(row[col_map.get("bom_qty", -1)]) if "bom_qty" in col_map else 0
            
            if parent not in boms:
                boms[parent] = {"qty": bom_qty or 1.0, "items": [], "operations": [], "scrap": []}
                dependencies[parent] = set()
            elif bom_qty > 0:
                boms[parent]["qty"] = bom_qty # Update if redefined
            
            row_type = clean_val(row[col_map.get("row_type", -1)]).lower()
            code_name = clean_val(row[col_map.get("code_name", -1)])
            qty_time = flt(row[col_map.get("qty_time", -1)])
            workstation = clean_val(row[col_map.get("workstation", -1)])
            
            if not code_name: continue
            
            if "material" in row_type:
                dependencies[parent].add(code_name)
                boms[parent]["items"].append({
                    "item_code": code_name,
                    "qty": qty_time or 1.0
                })
            elif "scrap" in row_type:
                boms[parent]["scrap"].append({
                    "item_code": code_name,
                    "qty": qty_time or 1.0
                })
            elif "operation" in row_type:
                boms[parent]["operations"].append({
                    "operation": code_name,
                    "workstation": workstation,
                    "time_in_mins": qty_time
                })

        # Topological sort for bottom-up creation
        def topological_sort(dep_graph):
            visited = set()
            temp_mark = set()
            order = []
            
            def visit(node):
                if node in temp_mark: return # Circular dependency
                if node not in visited:
                    temp_mark.add(node)
                    for child in dep_graph.get(node, []):
                        if child in dep_graph: # Only visit if child is a parent BOM in this import
                            visit(child)
                    temp_mark.remove(node)
                    visited.add(node)
                    order.append(node)
                    
            for node in dep_graph:
                if node not in visited:
                    visit(node)
            return order

        creation_order = topological_sort(dependencies)
        
        summary = []
        for parent_code in creation_order:
            data = boms[parent_code]
            try:
                bom = frappe.new_doc("BOM")
                bom.item = parent_code
                bom.quantity = data["qty"]
                bom.is_active = 1
                bom.is_default = 1
                bom.with_operations = 1 if data["operations"] else 0
                
                for item_data in data["items"]:
                    uom = frappe.db.get_value("Item", item_data["item_code"], "stock_uom") or "Nos"
                    bom.append("items", {
                        "item_code": item_data["item_code"],
                        "qty": item_data["qty"],
                        "uom": uom
                    })
                
                for scrap_data in data["scrap"]:
                    bom.append("scrap_items", {
                        "item_code": scrap_data["item_code"],
                        "stock_qty": scrap_data["qty"]
                    })
                
                for op_data in data["operations"]:
                    if not frappe.db.exists("Operation", op_data["operation"]):
                        new_op = frappe.new_doc("Operation")
                        new_op.operation = op_data["operation"]
                        new_op.insert(ignore_permissions=True)
                    
                    bom.append("operations", {
                        "operation": op_data["operation"],
                        "workstation": op_data["workstation"],
                        "time_in_mins": op_data["time_in_mins"],
                        "operating_cost": 0 # ERPNext will auto-calculate if workstation has an hour rate
                    })
                
                bom.insert(ignore_permissions=True)
                bom.submit()
                summary.append(f"✅ {parent_code}: BOM Created")
                
            except Exception as e:
                msg = str(e)
                if hasattr(e, 'message'): msg = e.message
                elif hasattr(e, 'args') and e.args: msg = e.args[0]
                from frappe.utils import strip_html
                msg = strip_html(str(msg))
                summary.append(f"❌ {parent_code}: {msg}")

        status = "Completed" if not any("❌" in log for log in summary) else "Failed"
        doc.db_set("status", status)
        doc.db_set("processing_log", "SUMMARY:\n" + "\n".join(summary))
        frappe.db.commit()

    except Exception as e:
        doc.db_set("status", "Failed")
        doc.db_set("processing_log", f"❌ Critical Error:\n{str(e)}")
        frappe.db.commit()
