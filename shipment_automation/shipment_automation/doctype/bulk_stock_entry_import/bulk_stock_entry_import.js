frappe.ui.form.on('Bulk Stock Entry Import', {
    refresh: function (frm) {
        // ── Download Template button ─────────────────────────────
        frm.add_custom_button(__('Download Template'), function() {
            window.open(frappe.urllib.get_full_url(
                "/api/method/shipment_automation.shipment_automation.doctype.bulk_stock_entry_import.bulk_stock_entry_import.download_template"
            ));
        });

        // ── Validate Data button ──
        if (frm.doc.excel_file) {
            frm.add_custom_button(__('Validate Data'), function () {
                frappe.confirm(
                    'Start validation of the uploaded Stock Entry Excel file?',
                    function () {
                        frm.call('start_validation').then(r => {
                            frappe.show_alert({ message: r.message, indicator: 'blue' });
                            setTimeout(() => frm.reload_doc(), 2000);
                        });
                    }
                );
            }).addClass('btn-primary');
        }

        // ── Process button ───────
        if (frm.doc.status === 'Validated') {
            frm.add_custom_button(__('Create Stock Entries'), function () {
                frappe.confirm(
                    'This will create <b>Stock Entries (Draft)</b> based on the Excel file.',
                    function () {
                        frm.call('start_creation').then(r => {
                            frappe.show_alert({ message: r.message, indicator: 'green' });
                            setTimeout(() => frm.reload_doc(), 2000);
                        });
                    }
                );
            }).addClass('btn-success');
        }
    },

    excel_file: function(frm) {
        if (frm.doc.excel_file) {
            frm.set_value('status', 'Draft');
            frm.save().then(() => frm.refresh());
        }
    }
});
