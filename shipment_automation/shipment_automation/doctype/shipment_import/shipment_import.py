import frappe
from frappe.model.document import Document
from frappe.utils import flt, nowdate
import openpyxl


class ShipmentImport(Document):

    @frappe.whitelist()
    def start_validation(self):
        """Kick off background validation of the uploaded Excel file."""
        self.db_set("status", "Validating")
        self.db_set("validation_log", "⏳ Validation in progress. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.shipment_import.shipment_import.run_validation",
            queue="long",
            timeout=3600,
            docname=self.name,
        )
        return "Validation started in the background. Refresh the page in a few seconds."

    @frappe.whitelist()
    def start_processing(self):
        """Kick off background processing: creates Purchase Receipts for validated data."""
        if self.status != "Validated":
            frappe.throw("Please validate the data first before processing.")
        self.db_set("status", "Processing")
        self.db_set("receipts_log", "⏳ Processing in progress. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.shipment_import.shipment_import.run_processing",
            queue="long",
            timeout=3600,
            docname=self.name,
        )
        return "Processing started in the background. Refresh the page in a few seconds."


# ─────────────────────────────────────────────
# Background worker functions
# ─────────────────────────────────────────────

def run_validation(docname):
    """
    Reads every data row from the Excel file and compares
    Qty & Rate against the matching Purchase Order line.

    Expected Excel columns (1-indexed):
      Col 1 – Item description (ignored)
      Col 2 – Item code (ignored)
      Col 3 – Qty (in Excel units; will be multiplied ×1000 to match PO)
      Col 4 – Rate (in Excel units; will be divided ÷1000 to match PO)
      Col 5 – PO Reference in format  <PO_NUMBER>-<LINE_IDX>  e.g. 12345-1
    """
    doc = frappe.get_doc("Shipment Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        file_path = file_doc.get_full_path()
        wb = openpyxl.load_workbook(file_path, data_only=True)
        sheet = wb.active

        errors = []
        ok_rows = 0

        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            # Skip empty rows
            if not row[4]:
                continue

            qty_exc = flt(row[2])
            rate_exc = flt(row[3])
            po_val_exc = str(row[4]).strip()

            # ── Parse PO reference ──────────────────────────────────
            try:
                po_parts = po_val_exc.split("-")
                po_name = f"{doc.po_prefix}{po_parts[0]}"
                line_idx = int(po_parts[1])
            except Exception:
                errors.append(
                    f"Row {row_idx} ❌  Cannot parse PO reference '{po_val_exc}'. "
                    f"Expected format: PO_NUMBER-LINE_INDEX  (e.g. 12345-1)"
                )
                continue

            # ── Check PO exists ─────────────────────────────────────
            if not frappe.db.exists("Purchase Order", po_name):
                errors.append(
                    f"Row {row_idx} ❌  Purchase Order '{po_name}' not found in ERPNext."
                )
                continue

            # ── Fetch PO line item ──────────────────────────────────
            po_item = frappe.db.get_value(
                "Purchase Order Item",
                {"parent": po_name, "idx": line_idx},
                ["qty", "rate", "item_code", "name"],
                as_dict=True,
            )
            if not po_item:
                errors.append(
                    f"Row {row_idx} ❌  Line {line_idx} not found in Purchase Order '{po_name}'."
                )
                continue

            row_ok = True

            # ── Quantity check ──────────────────────────────────────
            expected_qty = qty_exc * 1000
            if abs(expected_qty - po_item.qty) > 0.01:
                errors.append(
                    f"Row {row_idx} ❌  QTY MISMATCH — Item: {po_item.item_code} | "
                    f"PO: {po_name} Line {line_idx} | "
                    f"Excel Qty: {qty_exc} (×1000 = {expected_qty}) | "
                    f"PO Qty: {po_item.qty}"
                )
                row_ok = False

            # ── Rate check ──────────────────────────────────────────
            expected_rate = rate_exc / 1000
            if abs(expected_rate - po_item.rate) > 0.000001:
                errors.append(
                    f"Row {row_idx} ❌  RATE MISMATCH — Item: {po_item.item_code} | "
                    f"PO: {po_name} Line {line_idx} | "
                    f"Excel Rate: {rate_exc} (÷1000 = {expected_rate:.6f}) | "
                    f"PO Rate: {po_item.rate:.6f}"
                )
                row_ok = False

            if row_ok:
                ok_rows += 1

        # ── Build log and set status ────────────────────────────────
        if not errors:
            log_text = f"✅ All {ok_rows} row(s) validated successfully. No mismatches found."
            doc.db_set("status", "Validated")
        else:
            log_text = (
                f"❌ Validation Failed — {len(errors)} issue(s) found "
                f"({ok_rows} row(s) were OK):\n\n"
                + "\n".join(errors)
            )
            doc.db_set("status", "Failed")

        doc.db_set("validation_log", log_text)
        frappe.db.commit()

    except Exception:
        err = frappe.get_traceback()
        frappe.log_error(err, "Shipment Import – Validation Error")
        doc.db_set("status", "Failed")
        doc.db_set("validation_log", f"❌ System error during validation:\n\n{err}")
        frappe.db.commit()


def run_processing(docname):
    """
    Groups validated rows by Purchase Order and creates one
    Purchase Receipt per PO.
    """
    doc = frappe.get_doc("Shipment Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        file_path = file_doc.get_full_path()
        wb = openpyxl.load_workbook(file_path, data_only=True)
        sheet = wb.active

        # ── Group rows by PO ────────────────────────────────────────
        po_items_map = {}   # { po_name: [ { idx, qty, rate, row_idx } ] }

        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not row[4]:
                continue

            qty_exc = flt(row[2])
            rate_exc = flt(row[3])
            po_val_exc = str(row[4]).strip()

            po_parts = po_val_exc.split("-")
            po_name = f"{doc.po_prefix}{po_parts[0]}"
            line_idx = int(po_parts[1])

            po_items_map.setdefault(po_name, []).append(
                {
                    "idx": line_idx,
                    "qty": qty_exc * 1000,
                    "rate": rate_exc / 1000,
                    "row_idx": row_idx,
                }
            )

        # ── Create one Purchase Receipt per PO ──────────────────────
        created = []
        errors = []

        for po_name, items in po_items_map.items():
            try:
                po = frappe.get_doc("Purchase Order", po_name)

                pr = frappe.new_doc("Purchase Receipt")
                pr.supplier = po.supplier
                pr.posting_date = nowdate()
                pr.company = po.company

                for item_data in items:
                    po_item = frappe.db.get_value(
                        "Purchase Order Item",
                        {"parent": po_name, "idx": item_data["idx"]},
                        [
                            "item_code", "item_name", "uom",
                            "warehouse", "name", "conversion_factor",
                        ],
                        as_dict=True,
                    )

                    if not po_item:
                        errors.append(
                            f"❌  Skipped Row {item_data['row_idx']}: "
                            f"Line {item_data['idx']} not found in PO {po_name}."
                        )
                        continue

                    default_wh = frappe.db.get_single_value("Stock Settings", "default_warehouse")

                    pr.append(
                        "items",
                        {
                            "item_code": po_item.item_code,
                            "item_name": po_item.item_name,
                            "qty": item_data["qty"],
                            "rate": item_data["rate"],
                            "uom": po_item.uom,
                            "warehouse": po_item.warehouse or default_wh,
                            "purchase_order": po_name,
                            "purchase_order_item": po_item.name,
                            "conversion_factor": po_item.conversion_factor or 1,
                        },
                    )

                pr.insert(ignore_permissions=True)
                created.append(
                    f"✅  Purchase Receipt {pr.name} created for PO {po_name} "
                    f"({len(items)} line(s))"
                )

            except Exception as exc:
                errors.append(f"❌  Failed to create receipt for PO {po_name}: {exc}")
                frappe.log_error(frappe.get_traceback(), f"Shipment Import PR – {po_name}")

        # ── Build receipts log and set status ──────────────────────
        log_parts = []
        if created:
            log_parts.append("PURCHASE RECEIPTS CREATED:\n" + "\n".join(created))
        if errors:
            log_parts.append("ERRORS:\n" + "\n".join(errors))

        receipts_log = "\n\n".join(log_parts) if log_parts else "No rows processed."

        new_status = "Completed" if not errors else "Failed"
        doc.db_set("status", new_status)
        doc.db_set("receipts_log", receipts_log)
        frappe.db.commit()

    except Exception:
        err = frappe.get_traceback()
        frappe.log_error(err, "Shipment Import – Processing Error")
        doc.db_set("status", "Failed")
        doc.db_set("receipts_log", f"❌ System error during processing:\n\n{err}")
        frappe.db.commit()
