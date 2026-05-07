import frappe
from frappe.model.document import Document
from frappe.utils import flt
import openpyxl
from erpnext.buying.doctype.purchase_order.purchase_order import make_purchase_receipt

class ShipmentImport(Document):
    @frappe.whitelist()
    def start_validation(self):
        self.db_set('status', 'Validating')
        frappe.enqueue(self.validate_excel, queue='long', timeout=3600)
        return 'Validation started in the background.'

    def validate_excel(self):
        try:
            file_doc = frappe.get_doc('File', {'file_url': self.excel_file})
            file_path = file_doc.get_full_path()
            wb = openpyxl.load_workbook(file_path, data_only=True)
            sheet = wb.active
            self.validation_log = [] 
            valid = True
            for row_idx, row in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
                if not row[4]: continue
                qty_exc = flt(row[2])
                rate_exc = flt(row[3])
                po_val_exc = str(row[4])
                try:
                    po_parts = po_val_exc.split('-')
                    po_name = f'{self.po_prefix}{po_parts[0]}'
                    line_idx = int(po_parts[1])
                except:
                    valid = False; continue
                po_item = frappe.db.get_value('Purchase Order Item', {'parent': po_name, 'idx': line_idx}, ['qty', 'rate', 'name'], as_dict=1)
                if not po_item:
                    valid = False; continue
                if abs((qty_exc * 1000) - po_item.qty) > 0.01: valid = False
                if abs((rate_exc / 1000) - po_item.rate) > 0.000001: valid = False
            self.db_set('status', 'Validated' if valid else 'Failed')
            self.save()
        except Exception:
            frappe.log_error(frappe.get_traceback(), 'Shipment Import Error')
