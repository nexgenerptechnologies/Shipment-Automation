frappe.ui.form.on('Shipment Import', {

    refresh: function (frm) {

        // ── Status banner ────────────────────────────────────────
        const statusColor = {
            'Draft':      'blue',
            'Validating': 'yellow',
            'Validated':  'green',
            'Processing': 'yellow',
            'Completed':  'green',
            'Failed':     'red',
        };
        const statusMsg = {
            'Validating': '⏳ Validation in progress... Please refresh in a few seconds.',
            'Processing': '⏳ Creating Purchase Receipts... Please refresh in a few seconds.',
            'Completed':  '✅ Shipment processed successfully! Purchase Receipts have been created.',
            'Failed':     '❌ An error occurred. See the logs below for details.',
        };
        if (statusMsg[frm.doc.status]) {
            frm.dashboard.add_comment(statusMsg[frm.doc.status], statusColor[frm.doc.status], true);
        }

        // ── Validate Data button (Draft + file present) ──────────
        if (frm.doc.excel_file && frm.doc.status === 'Draft') {
            frm.add_custom_button(__('Validate Data'), function () {
                frappe.confirm(
                    'Start validation of the uploaded Excel file against ERPNext Purchase Orders?',
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
                    'This will create <b>Purchase Receipts</b> for all Purchase Orders in the Excel file. Continue?',
                    function () {
                        frm.call('start_processing').then(r => {
                            frappe.show_alert({ message: r.message, indicator: 'green' });
                            setTimeout(() => frm.reload_doc(), 2000);
                        });
                    }
                );
            }).addClass('btn-success');
        }

        // ── Re-validate button (after Failed) ───────────────────
        if (frm.doc.status === 'Failed' && frm.doc.excel_file) {
            frm.add_custom_button(__('Re-validate'), function () {
                frm.set_value('status', 'Draft');
                frm.save().then(() => {
                    frm.call('start_validation').then(r => {
                        frappe.show_alert({ message: r.message, indicator: 'blue' });
                        setTimeout(() => frm.reload_doc(), 2000);
                    });
                });
            });
        }
    },
});
