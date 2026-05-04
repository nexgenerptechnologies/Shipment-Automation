// File 3: shipment_automation/doctype/shipment_import/shipment_import.js
frappe.ui.form.on('Shipment Import', {
    refresh: function(frm) {
        if (frm.doc.excel_file && frm.doc.status === 'Draft') {
            frm.add_custom_button(__('Validate Data'), function() {
                frm.call('start_validation').then(r => {
                    frappe.msgprint(r.message);
                    frm.reload_doc();
                });
            }).addClass('btn-primary');
        }
    }
});
