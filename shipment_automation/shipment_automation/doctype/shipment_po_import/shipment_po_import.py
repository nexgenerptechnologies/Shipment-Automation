import frappe
from frappe.model.document import Document
from frappe.utils import flt, nowdate
import openpyxl


class ShipmentPOImport(Document):

    @frappe.whitelist()
    def start_validation(self):
        if not self.po_naming_series:
            frappe.throw("Please select a PO Naming Series before uploading.")
        if not self.item_naming_series:
            frappe.throw("Please select an Item Naming Series before uploading.")
        self.db_set("status", "Validating")
        self.db_set("creation_log", "⏳ Validation in progress. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.shipment_po_import.shipment_po_import.run_po_validation",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Validation started. Refresh in a few seconds."

    @frappe.whitelist()
    def start_creation(self):
        if self.status != "Validated":
            frappe.throw("Please validate the Excel first.")
        self.db_set("status", "Processing")
        self.db_set("creation_log", "⏳ Creating Purchase Orders. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.shipment_po_import.shipment_po_import.run_po_creation",
            queue="long", timeout=3600, docname=self.name,
        )
        return "Purchase Order creation started. Refresh in a few seconds."


def find_duplicate_item(e_code, e_name, e_desc):
    """Checks if an item matching 2 out of 3 fields exists."""
    if e_code and e_name:
        res = frappe.db.get_value("Item", {"name": e_code, "item_name": e_name}, "name")
        if res: return res
    if e_code and e_desc:
        res = frappe.db.get_value("Item", {"name": e_code, "description": e_desc}, "name")
        if res: return res
    if e_name and e_desc:
        res = frappe.db.get_value("Item", {"item_name": e_name, "description": e_desc}, "name")
        if res: return res
    return None

def run_po_validation(docname):
    doc = frappe.get_doc("Shipment PO Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.po_excel})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active

        errors = []
        ok_rows = 0
        items_to_create = []

        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not any(row):
                continue

            item_code = str(row[0]).strip() if len(row) > 0 and row[0] else ""
            item_name = str(row[1]).strip() if len(row) > 1 and row[1] else ""
            desc      = str(row[2]).strip() if len(row) > 2 and row[2] else ""
            uom       = str(row[3]).strip() if len(row) > 3 and row[3] else "Pcs"
            qty       = flt(row[4]) if len(row) > 4 else 0
            rate      = flt(row[5]) if len(row) > 5 else 0
            supplier  = str(row[6]).strip() if len(row) > 6 and row[6] else ""
            po_num    = str(row[7]).strip() if len(row) > 7 and row[7] else ""
            
            item_group = str(row[8]).strip() if len(row) > 8 and row[8] else ""
            hsn_code   = str(row[9]).strip() if len(row) > 9 and row[9] else ""

            if not po_num:
                errors.append(f"Row {row_idx} ❌  PO Number (Col H) is missing.")
                continue
                
            row_ok = True

            if not supplier:
                errors.append(f"Row {row_idx} ❌  Supplier (Col G) is empty.")
                row_ok = False
            elif not frappe.db.exists("Supplier", supplier):
                errors.append(f"Row {row_idx} ❌  Supplier '{supplier}' not found in ERPNext.")
                row_ok = False

            if qty <= 0:
                errors.append(f"Row {row_idx} ❌  Quantity must be > 0. Got: {qty}")
                row_ok = False
            if rate < 0:
                errors.append(f"Row {row_idx} ❌  Rate cannot be negative. Got: {rate}")
                row_ok = False

            # Item Check Logic
            if not item_code:
                errors.append(f"Row {row_idx} ❌  Item Code (Col A) is empty.")
                row_ok = False
            else:
                if frappe.db.exists("Item", item_code):
                    pass # Item exists perfectly, all good
                else:
                    # Check for 2-out-of-3 duplicate
                    duplicate = find_duplicate_item(item_code, item_name, desc)
                    if duplicate:
                        errors.append(f"Row {row_idx} ❌  Duplicate Item Found: ERPNext Item '{duplicate}' has the same Name/Description. Please update Excel Item Code to '{duplicate}' instead of creating a duplicate.")
                        row_ok = False
                    else:
                        # Genuine New Item
                        if not item_group or not hsn_code:
                            errors.append(f"Row {row_idx} ❌  New Item '{item_code}' needs to be created. Please add Item Group (Col I) and HSN/SAC Code (Col J).")
                            row_ok = False
                        else:
                            if not any(i['item_code'] == item_code for i in items_to_create):
                                items_to_create.append({
                                    "item_code": item_code,
                                    "item_name": item_name or item_code,
                                    "description": desc,
                                    "item_group": item_group,
                                    "hsn_code": hsn_code,
                                    "uom": "Pcs"
                                })

            if row_ok:
                ok_rows += 1

        if not errors:
            log_parts = [f"✅ All {ok_rows} row(s) validated successfully."]
            if items_to_create:
                log_parts.append(f"\n✨ {len(items_to_create)} New Items will be created:")
                for it in items_to_create:
                    log_parts.append(f"  • {it['item_code']} | {it['item_name']} | {it['description']}")
            
            doc.db_set("status", "Validated")
            doc.db_set("creation_log", "\n".join(log_parts))
        else:
            log = f"❌ {len(errors)} issue(s) found ({ok_rows} OK):\n\n" + "\n".join(errors)
            doc.db_set("status", "Failed")
            doc.db_set("creation_log", log)

        frappe.db.commit()

    except Exception:
        err = frappe.get_traceback()
        frappe.log_error(err, "Shipment PO Import – Validation Error")
        doc.db_set("status", "Failed")
        doc.db_set("creation_log", f"❌ System error:\n{err}")
        frappe.db.commit()


def run_po_creation(docname):
    doc = frappe.get_doc("Shipment PO Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.po_excel})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active

        # Step 1: Find and create new items
        items_to_create = {}
        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not any(row): continue
            item_code = str(row[0]).strip() if len(row) > 0 and row[0] else ""
            if item_code and not frappe.db.exists("Item", item_code):
                item_name = str(row[1]).strip() if len(row) > 1 and row[1] else item_code
                desc      = str(row[2]).strip() if len(row) > 2 and row[2] else ""
                item_group = str(row[8]).strip() if len(row) > 8 and row[8] else ""
                hsn_code   = str(row[9]).strip() if len(row) > 9 and row[9] else ""
                
                if item_code not in items_to_create:
                    items_to_create[item_code] = {
                        "item_name": item_name,
                        "description": desc,
                        "item_group": item_group,
                        "gst_hsn_code": hsn_code,
                        "stock_uom": "Pcs"
                    }

        created_items_log = []
        item_mapping = {} # maps excel_code -> erpnext_code
        
        for excel_code, idata in items_to_create.items():
            new_item = frappe.new_doc("Item")
            new_item.naming_series = doc.item_naming_series
            new_item.item_name = idata["item_name"]
            new_item.description = idata["description"]
            new_item.item_group = idata["item_group"]
            new_item.gst_hsn_code = idata["gst_hsn_code"]
            new_item.stock_uom = idata["stock_uom"]
            new_item.insert(ignore_permissions=True)
            
            item_mapping[excel_code] = new_item.name
            created_items_log.append(f"✨ Created Item: {new_item.name} (from Excel Code: {excel_code})")

        # Step 2: Build POs
        po_map = {}
        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not any(row) or not row[7]:
                continue

            excel_code  = str(row[0]).strip() if len(row) > 0 and row[0] else ""
            qty         = flt(row[4]) if len(row) > 4 else 0
            rate        = flt(row[5]) if len(row) > 5 else 0
            supplier    = str(row[6]).strip() if len(row) > 6 and row[6] else ""
            po_num      = str(row[7]).strip()

            if not excel_code or not supplier or qty <= 0:
                continue
                
            actual_item_code = item_mapping.get(excel_code, excel_code)

            po_map.setdefault(po_num, {"supplier": supplier, "items": []})
            po_map[po_num]["items"].append(
                {
                 "item_code": actual_item_code,
                 "qty": qty, 
                 "rate": rate
                }
            )

        created = []
        errors = []
        default_company = frappe.db.get_single_value("Global Defaults", "default_company")

        for po_num, data in po_map.items():
            try:
                po = frappe.new_doc("Purchase Order")
                po.naming_series   = doc.po_naming_series
                po.supplier        = data["supplier"]
                po.transaction_date = nowdate()
                po.schedule_date   = nowdate()
                po.company         = default_company

                for item in data["items"]:
                    po.append("items", {
                        "item_code":     item["item_code"],
                        "qty":           item["qty"],
                        "rate":          item["rate"],
                        "schedule_date": nowdate(),
                    })

                po.insert(ignore_permissions=True)
                
                # Populate the custom line_number field for each item
                for idx, item in enumerate(po.items, start=1):
                    item.db_set("line_number", f"{po.name}-{idx}")
                    
                created.append(
                    f"✅  {po.name} | Excel PO#: {po_num} | "
                    f"Supplier: {data['supplier']} | {len(data['items'])} line(s)"
                )
            except Exception as exc:
                errors.append(f"❌  Excel PO# {po_num}: {exc}")
                frappe.log_error(frappe.get_traceback(), f"Shipment PO Import – {po_num}")

        log_parts = []
        if created_items_log:
            log_parts.append("NEW ITEMS CREATED:\n" + "\n".join(created_items_log) + "\n")
            
        if created:
            log_parts.append(f"PURCHASE ORDERS CREATED ({len(created)}):\n" + "\n".join(created))
        if errors:
            log_parts.append("ERRORS:\n" + "\n".join(errors))

        doc.db_set("status", "Completed" if not errors else "Failed")
        doc.db_set("creation_log", "\n\n".join(log_parts) or "No POs created.")
        frappe.db.commit()

    except Exception:
        err = frappe.get_traceback()
        frappe.log_error(err, "Shipment PO Import – Creation Error")
        doc.db_set("status", "Failed")
        doc.db_set("creation_log", f"❌ System error:\n{err}")
        frappe.db.commit()
