import frappe


@frappe.whitelist()
def get_naming_series(doctype):
    """Return naming series options for a given DocType."""
    meta = frappe.get_meta(doctype)
    field = meta.get_field("naming_series")
    if field and field.options:
        return [s.strip() for s in field.options.split("\n") if s.strip()]
    return []
