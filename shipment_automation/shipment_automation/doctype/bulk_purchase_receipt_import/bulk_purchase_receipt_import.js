const UTILS = "shipment_automation.shipment_automation.utils.get_naming_series";

frappe.ui.form.on('Bulk Purchase Receipt Import', {

    onload: function (frm) {
        // Populate PR naming series dynamically
        frappe.call({
            method: UTILS,
            args: { doctype: "Purchase Receipt" },
            callback: function (r) {
                if (r.message && r.message.length) {
                    frm.set_df_property("pr_naming_series", "options",
                        [""].concat(r.message).join("\n"));
                    
                    // Auto-select PR-.YY.- if available
                    let default_pr = r.message.find(s => s.startsWith("PR-"));
                    if (default_pr && !frm.doc.pr_naming_series) {
                        frm.set_value("pr_naming_series", default_pr);
                    }
                    frm.refresh_field("pr_naming_series");
                }
            }
        });

        // Populate PI naming series dynamically
        frappe.call({
            method: UTILS,
            args: { doctype: "Purchase Invoice" },
            callback: function (r) {
                if (r.message && r.message.length) {
                    frm.set_df_property("pi_naming_series", "options",
                        [""].concat(r.message).join("\n"));
                    frm.refresh_field("pi_naming_series");
                }
            }
        });

        // Populate PO Naming Series (Fallback) dynamically
        frappe.call({
            method: UTILS,
            args: { doctype: "Purchase Order" },
            callback: function (r) {
                if (r.message && r.message.length) {
                    frm.set_df_property("po_prefix", "options",
                        [""].concat(r.message).join("\n"));
                    frm.refresh_field("po_prefix");
                }
            }
        });
    },

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
        if (frm.doc.excel_file && frm.doc.status === 'Draft' && frm.doc.pr_naming_series) {
            frm.add_custom_button(__('Validate Data'), function () {
                frappe.confirm(
                    'Start validation of the uploaded Excel file for multiple suppliers?',
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
                    + 'You will need to review and submit them manually.',
                    function () {
                        frm.call('start_processing').then(r => {
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
                if (!frm.doc.pi_naming_series) {
                    frappe.msgprint({
                        title: __('Missing Naming Series'),
                        indicator: 'orange',
                        message: `Please select a <b>Purchase Invoice Naming Series</b> first.`
                    });
                    return;
                }

                frappe.confirm(
                    'Create Purchase Invoices and Bills of Entry for all submitted receipts in the log?',
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
