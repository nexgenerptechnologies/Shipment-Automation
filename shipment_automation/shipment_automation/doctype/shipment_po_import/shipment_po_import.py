import frappe
from frappe.model.document import Document
from frappe.utils import flt, nowdate
import openpyxl


class ShipmentPOImport(Document):

    @frappe.whitelist()
    def start_validation(self):
        if not self.po_naming_series:
            frappe.throw("Please select a PO Naming Series before uploading.")
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


def run_po_validation(docname):
    """
    Validates PO Excel rows against ERPNext master data.
    Excel columns (0-indexed):
      A(0)=Item Code, B(1)=Item Name, C(2)=Description,
      D(3)=UOM, E(4)=Qty, F(5)=Rate, G(6)=Supplier, H(7)=PO Number
    """
    doc = frappe.get_doc("Shipment PO Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.po_excel})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active

        errors = []
        ok_rows = 0
        po_groups = {}

        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not any(row):
                continue

            item_code = str(row[0]).strip() if row[0] else ""
            uom       = str(row[3]).strip() if row[3] else ""
            qty       = flt(row[4])
            rate      = flt(row[5])
            supplier  = str(row[6]).strip() if row[6] else ""
            po_num    = str(row[7]).strip() if row[7] else ""

            if not po_num:
                errors.append(f"Row {row_idx} ❌  PO Number (Col H) is missing.")
                continue

            row_ok = True

            if not item_code:
                errors.append(f"Row {row_idx} ❌  Item Code (Col A) is empty.")
                row_ok = False
            elif not frappe.db.exists("Item", item_code):
                errors.append(f"Row {row_idx} ❌  Item '{item_code}' not found in ERPNext.")
                row_ok = False

            if not supplier:
                errors.append(f"Row {row_idx} ❌  Supplier (Col G) is empty.")
                row_ok = False
            elif not frappe.db.exists("Supplier", supplier):
                errors.append(f"Row {row_idx} ❌  Supplier '{supplier}' not found in ERPNext.")
                row_ok = False

            if not uom:
                errors.append(f"Row {row_idx} ❌  UOM (Col D) is empty.")
                row_ok = False
            elif not frappe.db.exists("UOM", uom):
                errors.append(f"Row {row_idx} ❌  UOM '{uom}' not found in ERPNext.")
                row_ok = False

            if qty <= 0:
                errors.append(f"Row {row_idx} ❌  Quantity must be > 0. Got: {qty}")
                row_ok = False
            if rate < 0:
                errors.append(f"Row {row_idx} ❌  Rate cannot be negative. Got: {rate}")
                row_ok = False

            if row_ok:
                ok_rows += 1
                po_groups.setdefault(po_num, []).append(row_idx)

        if not errors:
            summary = "\n".join(
                f"  • PO Group '{k}': {len(v)} item(s) (Rows: {', '.join(map(str, v))})"
                for k, v in po_groups.items()
            )
            log = f"✅ All {ok_rows} row(s) OK.\n\nPOs to create ({len(po_groups)}):\n{summary}"
            doc.db_set("status", "Validated")
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
    """Creates one Purchase Order per unique PO Number in the Excel."""
    doc = frappe.get_doc("Shipment PO Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.po_excel})
        wb = openpyxl.load_workbook(file_doc.get_full_path(), data_only=True)
        sheet = wb.active

        po_map = {}

        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not any(row) or not row[7]:
                continue

            item_code   = str(row[0]).strip() if row[0] else ""
            item_name   = str(row[1]).strip() if row[1] else ""
            description = str(row[2]).strip() if row[2] else ""
            uom         = str(row[3]).strip() if row[3] else "Nos"
            qty         = flt(row[4])
            rate        = flt(row[5])
            supplier    = str(row[6]).strip() if row[6] else ""
            po_num      = str(row[7]).strip()

            if not item_code or not supplier or qty <= 0:
                continue

            po_map.setdefault(po_num, {"supplier": supplier, "items": []})
            po_map[po_num]["items"].append(
                {"item_code": item_code, "item_name": item_name,
                 "description": description, "uom": uom,
                 "qty": qty, "rate": rate}
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
                        "item_name":     item["item_name"],
                        "description":   item["description"],
                        "uom":           item["uom"],
                        "qty":           item["qty"],
                        "rate":          item["rate"],
                        "schedule_date": nowdate(),
                    })

                po.insert(ignore_permissions=True)
                created.append(
                    f"✅  {po.name} | Excel PO#: {po_num} | "
                    f"Supplier: {data['supplier']} | {len(data['items'])} line(s)"
                )
            except Exception as exc:
                errors.append(f"❌  Excel PO# {po_num}: {exc}")
                frappe.log_error(frappe.get_traceback(), f"Shipment PO Import – {po_num}")

        log_parts = []
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
