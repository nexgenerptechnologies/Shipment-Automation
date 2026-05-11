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
            'Completed':  ['✅ Purchase Receipts created as Draft. Review and Submit them.', 'green'],
            'Failed':     ['❌ An error occurred. See the logs below for details.', 'red'],
        };
        if (statusMsg[frm.doc.status]) {
            frm.dashboard.add_comment(statusMsg[frm.doc.status][0], statusMsg[frm.doc.status][1], true);
        }

        // ── Validate Data button ─────────────────────────────────
        if (frm.doc.excel_file && (frm.doc.status === 'Draft' || frm.doc.status === 'Failed')) {
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
                    'This will create separate <b>Purchase Receipts (Draft)</b> for each supplier in the Excel.<br><br>'
                    + 'Series used: <b>PR-.YY.-</b>',
                    function () {
                        frm.call('start_creation').then(r => {
                            frappe.show_alert({ message: r.message, indicator: 'green' });
                            setTimeout(() => frm.reload_doc(), 2000);
                        });
                    }
                );
            }).addClass('btn-success');
        }

        // ── Create Invoices & BOE button ─────────────────
        if (frm.doc.status === 'Completed') {
            frm.add_custom_button(__('Create Invoices & Bills of Entry'), function () {
                frappe.confirm(
                    'Create Purchase Invoices (PINV-.YY.-) and Bills of Entry for all submitted receipts?',
                    function () {
                        frappe.show_alert({ message: 'Processing Documents...', indicator: 'blue' });
                        frm.call('create_purchase_invoice_and_boe').then(r => {
                            frappe.msgprint({ title: 'Batch Result', message: r.message.summary, indicator: 'green' });
                            frm.reload_doc();
                        });
                    }
                );
            }).addClass('btn-primary');
        }
    },
});
