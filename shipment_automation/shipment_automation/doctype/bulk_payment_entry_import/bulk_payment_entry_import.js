frappe.ui.form.on("Bulk Payment Entry Import", {
    refresh: function(frm) {
        if (!frm.is_new()) {
            frm.add_custom_button(__("Download Template"), function() {
                window.open(
                    "/api/method/shipment_automation.shipment_automation.doctype.bulk_payment_entry_import.bulk_payment_entry_import.download_template"
                );
            }).addClass("btn-primary");
            
            if (frm.doc.status === "Draft" || frm.doc.status === "Failed") {
                frm.add_custom_button(__("Start Validation"), function() {
                    frappe.call({
                        method: "start_validation",
                        doc: frm.doc,
                        callback: function(r) {
                            if (!r.exc) {
                                frappe.msgprint(__("Validation started in background."));
                                frm.reload_doc();
                            }
                        }
                    });
                }).addClass("btn-primary");
            }
            
            if (frm.doc.status === "Validated" || frm.doc.status === "Failed") {
                frm.add_custom_button(__("Start Processing"), function() {
                    frappe.call({
                        method: "start_creation",
                        doc: frm.doc,
                        callback: function(r) {
                            if (!r.exc) {
                                frappe.msgprint(__("Processing started in background."));
                                frm.reload_doc();
                            }
                        }
                    });
                }).addClass("btn-primary");
            }
        }
    }
});
