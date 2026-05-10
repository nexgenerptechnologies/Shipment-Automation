import frappe
from frappe.model.document import Document
from frappe.utils import flt, nowdate
import openpyxl


class BulkPurchaseReceiptImport(Document):

    @frappe.whitelist()
    def start_validation(self):
        """Kick off background validation of the uploaded Excel file."""
        self.db_set("status", "Validating")
        self.db_set("validation_log", "⏳ Validation in progress. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_purchase_receipt_import.bulk_purchase_receipt_import.run_validation",
            queue="long",
            timeout=3600,
            docname=self.name,
        )
        return "Validation started in the background. Refresh the page in a few seconds."

    @frappe.whitelist()
    def start_processing(self):
        """Kick off background processing: creates a single combined Purchase Receipt (Draft)."""
        if self.status != "Validated":
            frappe.throw("Please validate the data first before processing.")
        if not self.pr_naming_series:
            frappe.throw("Please select a Purchase Receipt Naming Series before processing.")
        self.db_set("status", "Processing")
        self.db_set("receipts_log", "⏳ Processing in progress. Please refresh in a few seconds.")
        frappe.db.commit()
        frappe.enqueue(
            "shipment_automation.shipment_automation.doctype.bulk_purchase_receipt_import.bulk_purchase_receipt_import.run_processing",
            queue="long",
            timeout=3600,
            docname=self.name,
        )
        return "Processing started in the background. Refresh the page in a few seconds."

    @frappe.whitelist()
    def create_purchase_invoice_and_boe(self):
        """
        1. Creates a Purchase Invoice (Draft) from the submitted Purchase Receipt.
        2. Automatically creates a Bill of Entry (Draft) linked to that Invoice.
        """
        if not self.receipt_name:
            frappe.throw("No Purchase Receipt is linked to this import.")

        pr_doc = frappe.get_doc("Purchase Receipt", self.receipt_name)
        if pr_doc.docstatus != 1:
            frappe.throw(
                f"Purchase Receipt <b>{self.receipt_name}</b> must be <b>Submitted</b> "
                f"before creating a Purchase Invoice."
            )

        if not self.pi_naming_series:
            frappe.throw("Please select a Purchase Invoice Naming Series.")

        if self.invoice_name:
            frappe.throw(
                f"A Purchase Invoice <b>{self.invoice_name}</b> already exists."
            )

        try:
            from erpnext.stock.stock_ledger import make_purchase_invoice
            pi = make_purchase_invoice(self.receipt_name)
            pi.naming_series = self.pi_naming_series
            pi.insert(ignore_permissions=True)
            self.db_set("invoice_name", pi.name)
            frappe.db.commit()
        except Exception as exc:
            frappe.log_error(frappe.get_traceback(), "Bulk PR Import – Create PI Error")
            frappe.throw(f"Failed to create Purchase Invoice: {exc}")

        boe_name = None
        boe_error = None
        try:
            boe = frappe.new_doc("Bill of Entry")
            boe.purchase_invoice = pi.name
            boe.posting_date = nowdate()
            boe.company = pr_doc.company
            boe.supplier = pr_doc.supplier
            boe.insert(ignore_permissions=True, ignore_mandatory=True)
            boe_name = boe.name
            self.db_set("bill_of_entry_name", boe_name)
            frappe.db.commit()
        except Exception as exc:
            frappe.log_error(frappe.get_traceback(), "Bulk PR Import – Create BOE Error")
            boe_error = str(exc)

        return {
            "invoice": pi.name,
            "bill_of_entry": boe_name,
            "boe_error": boe_error,
        }


def run_validation(docname):
    doc = frappe.get_doc("Bulk Purchase Receipt Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        file_path = file_doc.get_full_path()
        wb = openpyxl.load_workbook(file_path, data_only=True)
        sheet = wb.active

        errors = []
        ok_rows = 0

        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not row[4]:
                continue

            qty_exc = flt(row[2])
            rate_exc = flt(row[3])
            po_val_exc = str(row[4]).strip()
            excel_series = str(row[5]).strip() if len(row) > 5 and row[5] else ""
            
            try:
                po_parts = po_val_exc.rsplit("-", 1)
                base_po_num = po_parts[0]
                line_idx = int(po_parts[1])
                
                if excel_series:
                    po_name = f"{excel_series}{base_po_num}" if not base_po_num.startswith(excel_series) else base_po_num
                elif doc.po_prefix:
                    po_name = f"{doc.po_prefix}{base_po_num}" if not base_po_num.startswith(doc.po_prefix) else base_po_num
                else:
                    po_name = base_po_num
                    
            except Exception:
                errors.append(f"Row {row_idx} ❌ Cannot parse PO reference '{po_val_exc}'.")
                continue

            if not frappe.db.exists("Purchase Order", po_name):
                errors.append(f"Row {row_idx} ❌ PO '{po_name}' not found.")
                continue

            po_item = frappe.db.get_value(
                "Purchase Order Item",
                {"parent": po_name, "idx": line_idx},
                ["qty", "rate", "item_code", "name"],
                as_dict=True,
            )
            if not po_item:
                errors.append(f"Row {row_idx} ❌ Line {line_idx} not found in PO '{po_name}'.")
                continue

            row_ok = True
            expected_qty = qty_exc * 1000
            if abs(expected_qty - po_item.qty) > 0.01:
                errors.append(f"Row {row_idx} ❌ QTY MISMATCH: {po_item.item_code}")
                row_ok = False

            expected_rate = rate_exc / 1000
            if abs(expected_rate - po_item.rate) > 0.000001:
                errors.append(f"Row {row_idx} ❌ RATE MISMATCH: {po_item.item_code}")
                row_ok = False

            if row_ok: ok_rows += 1

        if not errors:
            doc.db_set("status", "Validated")
            doc.db_set("validation_log", f"✅ {ok_rows} row(s) validated successfully.")
        else:
            doc.db_set("status", "Failed")
            doc.db_set("validation_log", f"❌ Issues found:\n\n" + "\n".join(errors))
        frappe.db.commit()

    except Exception:
        err = frappe.get_traceback()
        frappe.log_error(err, "Bulk PR Import – Validation Error")
        doc.db_set("status", "Failed")
        doc.db_set("validation_log", f"❌ System error:\n\n{err}")
        frappe.db.commit()


def run_processing(docname):
    doc = frappe.get_doc("Bulk Purchase Receipt Import", docname)
    try:
        file_doc = frappe.get_doc("File", {"file_url": doc.excel_file})
        file_path = file_doc.get_full_path()
        wb = openpyxl.load_workbook(file_path, data_only=True)
        sheet = wb.active

        rows_to_process = []
        for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            if not row[4]: continue
            po_val_exc = str(row[4]).strip()
            excel_series = str(row[5]).strip() if len(row) > 5 and row[5] else ""
            try:
                po_parts = po_val_exc.rsplit("-", 1)
                base_po_num = po_parts[0]
                line_idx = int(po_parts[1])
                if excel_series:
                    po_name = f"{excel_series}{base_po_num}" if not base_po_num.startswith(excel_series) else base_po_num
                elif doc.po_prefix:
                    po_name = f"{doc.po_prefix}{base_po_num}" if not base_po_num.startswith(doc.po_prefix) else base_po_num
                else:
                    po_name = base_po_num
            except Exception: continue

            rows_to_process.append({
                "po_name": po_name,
                "line_idx": line_idx,
                "qty": flt(row[2]) * 1000,
                "rate": flt(row[3]) / 1000,
                "row_idx": row_idx,
            })

        if not rows_to_process:
            doc.db_set("status", "Failed")
            doc.db_set("receipts_log", "❌ No valid rows found.")
            frappe.db.commit()
            return

        first_po = frappe.get_doc("Purchase Order", rows_to_process[0]["po_name"])
        pr = frappe.new_doc("Purchase Receipt")
        pr.naming_series = doc.pr_naming_series
        pr.supplier = doc.supplier
        pr.posting_date = nowdate()
        pr.company = first_po.company

        for item_data in rows_to_process:
            po_item = frappe.db.get_value("Purchase Order Item", {"parent": item_data["po_name"], "idx": item_data["line_idx"]}, ["item_code", "item_name", "uom", "warehouse", "name", "conversion_factor"], as_dict=True)
            if not po_item: continue
            pr.append("items", {
                "item_code": po_item.item_code,
                "qty": item_data["qty"],
                "rate": item_data["rate"],
                "uom": po_item.uom,
                "warehouse": po_item.warehouse,
                "purchase_order": item_data["po_name"],
                "purchase_order_item": po_item.name,
                "conversion_factor": po_item.conversion_factor or 1,
            })

        pr.insert(ignore_permissions=True)
        doc.db_set("status", "Completed")
        doc.db_set("receipt_name", pr.name)
        doc.db_set("receipts_log", f"✅ Purchase Receipt {pr.name} created as DRAFT.")
        frappe.db.commit()

    except Exception:
        err = frappe.get_traceback()
        frappe.log_error(err, "Bulk PR Import – Processing Error")
        doc.db_set("status", "Failed")
        doc.db_set("receipts_log", f"❌ Error:\n{err}")
        frappe.db.commit()
