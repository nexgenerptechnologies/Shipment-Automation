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
            "Item to Manufacture (Item Code)", "Item Name", "Qty", "UOM", 
            "Item Consume", "Qty Required", "Item UOM", "Operation", 
            "Scrap Code", "Scrap Qty", "Scrap UOM", "Workstation Type", 
            "Workstation", "Operation Time", "Hour Rate", "Is Subcontracted"
        ]
        ws.append(headers)
        
        file_path = get_site_path("public", "files", "Bulk_BOM_Import_Template.xlsx")
        wb.save(file_path)
        
        return "/files/Bulk_BOM_Import_Template.xlsx"

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
        elif "item name" in clean: mapping["item_name"] = idx
        elif clean == "qty" or "qty to manufacture" in clean: mapping["qty"] = idx
        elif clean == "uom" or clean == "parent uom": mapping["uom"] = idx
        elif "consume" in clean or "component" in clean: mapping["child"] = idx
        elif "qty required" in clean or "child qty" in clean: mapping["child_qty"] = idx
        elif "item uom" in clean or "child uom" in clean: mapping["child_uom"] = idx
        elif "operation" in clean and "time" not in clean: mapping["operation"] = idx
        elif "scrap code" in clean: mapping["scrap_code"] = idx
        elif "scrap qty" in clean: mapping["scrap_qty"] = idx
        elif "scrap uom" in clean: mapping["scrap_uom"] = idx
        elif "workstation" in clean and "type" not in clean: mapping["workstation"] = idx
        elif "operation time" in clean: mapping["op_time"] = idx
        elif "hour rate" in clean: mapping["hour_rate"] = idx
        elif "subcontracted" in clean: mapping["is_subcontracted"] = idx
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
            child = clean_val(row[col_map.get("child", -1)]) if "child" in col_map else ""
            scrap = clean_val(row[col_map.get("scrap_code", -1)]) if "scrap_code" in col_map else ""
            op_name = clean_val(row[col_map.get("operation", -1)]) if "operation" in col_map else ""
            workstation = clean_val(row[col_map.get("workstation", -1)]) if "workstation" in col_map else ""
            
            if not parent:
                errors.append(f"Row {i}: Item to Manufacture is missing.")
            elif not frappe.db.exists("Item", parent):
                errors.append(f"Row {i}: Item to Manufacture '{parent}' does not exist.")
            
            if child and not frappe.db.exists("Item", child):
                errors.append(f"Row {i}: Item Consume '{child}' does not exist.")
                
            if scrap and not frappe.db.exists("Item", scrap):
                errors.append(f"Row {i}: Scrap Item '{scrap}' does not exist.")
            
            if op_name and workstation and not frappe.db.exists("Workstation", workstation):
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
        current_parent = ""
        dependencies = {} # parent -> set of child items (which are also parents)
        
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not any(row): continue
            
            parent_raw = clean_val(row[col_map.get("parent", 0)])
            if parent_raw:
                current_parent = parent_raw
            parent = current_parent
            
            if not parent: continue
            
            if parent not in boms:
                qty = flt(row[col_map.get("qty", -1)]) if "qty" in col_map else 1.0
                if qty == 0: qty = 1.0
                boms[parent] = {"qty": qty, "items": [], "operations": [], "scrap": []}
                if parent not in dependencies:
                    dependencies[parent] = set()
            
            child = clean_val(row[col_map.get("child", -1)]) if "child" in col_map else ""
            if child:
                dependencies[parent].add(child)
                boms[parent]["items"].append({
                    "item_code": child,
                    "qty": flt(row[col_map.get("child_qty", -1)]),
                    "uom": clean_val(row[col_map.get("child_uom", -1)])
                })
            
            scrap = clean_val(row[col_map.get("scrap_code", -1)]) if "scrap_code" in col_map else ""
            if scrap:
                boms[parent]["scrap"].append({
                    "item_code": scrap,
                    "qty": flt(row[col_map.get("scrap_qty", -1)])
                })
            
            op_name = clean_val(row[col_map.get("operation", -1)]) if "operation" in col_map else ""
            if op_name:
                sub = clean_val(row[col_map.get("is_subcontracted", -1)]) if "is_subcontracted" in col_map else ""
                boms[parent]["operations"].append({
                    "operation": op_name,
                    "workstation": clean_val(row[col_map.get("workstation", -1)]),
                    "time_in_mins": flt(row[col_map.get("op_time", -1)]) * 60,
                    "hour_rate": flt(row[col_map.get("hour_rate", -1)]),
                    "is_subcontracted": 1 if sub.lower() in ["yes", "y", "1", "true"] else 0
                })

        # Topological sort for bottom-up creation
        def topological_sort(dep_graph):
            visited = set()
            temp_mark = set()
            order = []
            
            def visit(node):
                if node in temp_mark:
                    return # Circular dependency detected, ignore for sort
                if node not in visited:
                    temp_mark.add(node)
                    for child in dep_graph.get(node, []):
                        if child in dep_graph: # Only if child is also a parent in this import
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
                    bom.append("items", {
                        "item_code": item_data["item_code"],
                        "qty": item_data["qty"] or 1,
                        "uom": item_data["uom"] or frappe.db.get_value("Item", item_data["item_code"], "stock_uom")
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
                    
                    op_row = bom.append("operations", {
                        "operation": op_data["operation"],
                        "workstation": op_data["workstation"],
                        "time_in_mins": op_data["time_in_mins"],
                        "hour_rate": op_data["hour_rate"]
                    })
                    
                    if op_data["is_subcontracted"] and hasattr(op_row, "is_subcontracted"):
                        op_row.is_subcontracted = 1
                
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
