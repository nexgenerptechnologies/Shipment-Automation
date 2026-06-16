import frappe
frappe.init(site='sgecontrols.m.frappe.cloud')
frappe.connect()
print(frappe.db.exists('DocType', 'Supplier Type'))
