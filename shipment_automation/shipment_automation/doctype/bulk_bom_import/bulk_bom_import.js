frappe.ui.form.on('Bulk BOM Import', {
    refresh: function(frm) {
        frm.add_custom_button(__('Download Template'), function() {
            frappe.call({
                method: 'download_template',
                doc: frm.doc,
                callback: function(r) {
                    if (r.message) {
                        window.open(r.message);
                    }
                }
            });
        });

        if (frm.doc.bom_excel) {
            frm.add_custom_button(__('Run Validation'), function() {
                frm.call('start_validation');
            }, __('Actions'));

            if (frm.doc.status === 'Validated') {
                frm.add_custom_button(__('Run Processing'), function() {
                    frm.call('start_processing');
                }, __('Actions'));
            }
        }
    }
});
