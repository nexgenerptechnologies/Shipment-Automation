frappe.ui.form.on('Bulk Purchase Receipt Import', {

    refresh: function (frm) {
        // ── Download Template button ─────────────────────────────
        frm.add_custom_button(__('Download Template'), function() {
            window.open(frappe.urllib.get_full_url(
                "/api/method/shipment_automation.shipment_automation.doctype.bulk_purchase_receipt_import.bulk_purchase_receipt_import.download_template"
            ));
        });

        // ── Status banner ────────────────────────────────────────
        const statusMsg = {
            'Validating': ['⏳ Validation in progress... Please refresh in a few seconds.', 'yellow'],
            'Processing': ['⏳ Creating Purchase Receipts... Please refresh in a few seconds.', 'yellow'],
            'Completed':  ['✅ Purchase Receipts created. Review them in the processing results.', 'green'],
            'Failed':     ['❌ An error occurred. See the logs below for details.', 'red'],
        };
        if (statusMsg[frm.doc.status]) {
            frm.dashboard.add_comment(statusMsg[frm.doc.status][0], statusMsg[frm.doc.status][1], true);
        }

        // ── Validate Data button (NOW ALWAYS VISIBLE IF FILE EXISTS) ──
        if (frm.doc.excel_file) {
            frm.add_custom_button(__('Validate Data'), function () {
                frappe.confirm(
                    'Start validation of the uploaded Excel file?',
                    function () {
                        frm.call('start_validation').then(r => {
                            frappe.show_alert({ message: r.message, indicator: 'blue' });
                            setTimeout(() => frm.reload_doc(), 2000);
                        });
                    }
                );
            }).addClass('btn-primary');
        }

        // ── Process Shipment button (only after Validated) ───────
        if (frm.doc.status === 'Validated') {
            frm.add_custom_button(__('Process Shipment'), function () {
                frappe.confirm(
                    'This will create separate <b>Purchase Receipts (Draft)</b> based on the Excel file.',
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

    // ── Re-show Validate button when file is changed ──
    excel_file: function(frm) {
        if (frm.doc.excel_file) {
            frm.set_value('status', 'Draft');
            frm.save().then(() => frm.refresh());
        }
    }
});
