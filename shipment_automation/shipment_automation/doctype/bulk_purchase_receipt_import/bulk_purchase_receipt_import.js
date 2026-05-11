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

        frm.set_df_property("excel_file", "read_only",
            frm.doc.pr_naming_series ? 0 : 1);
    },

    pr_naming_series: function (frm) {
        const locked = !frm.doc.pr_naming_series;
        frm.set_df_property("excel_file", "read_only", locked ? 1 : 0);
        frm.refresh_field("excel_file");
        if (locked) {
            frappe.show_alert({ message: "⚠️ Select PR Naming Series first.", indicator: "orange" });
        }
    },

    excel_file: function (frm) {
        if (!frm.doc.pr_naming_series) {
            frappe.show_alert({ message: "⚠️ Select PR Naming Series before uploading.", indicator: "red" });
            frm.set_value("excel_file", "");
        }
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
            'Processing': ['⏳ Creating Purchase Receipt... Please refresh in a few seconds.', 'yellow'],
            'Completed':  ['✅ Purchase Receipt created as Draft. Review it, then Submit to post stock entries.', 'green'],
            'Failed':     ['❌ An error occurred. See the logs below for details.', 'red'],
        };
        if (statusMsg[frm.doc.status]) {
            frm.dashboard.add_comment(statusMsg[frm.doc.status][0], statusMsg[frm.doc.status][1], true);
        }

        // ── Quick links to linked documents ─────────────────────
        if (frm.doc.receipt_name) {
            frm.add_custom_button(__('View Purchase Receipt'), function () {
                frappe.set_route('Form', 'Purchase Receipt', frm.doc.receipt_name);
            }, __('Links'));
        }
        if (frm.doc.invoice_name) {
            frm.add_custom_button(__('View Purchase Invoice'), function () {
                frappe.set_route('Form', 'Purchase Invoice', frm.doc.invoice_name);
            }, __('Links'));
        }
        if (frm.doc.bill_of_entry_name) {
            frm.add_custom_button(__('View Bill of Entry'), function () {
                frappe.set_route('Form', 'Bill of Entry', frm.doc.bill_of_entry_name);
            }, __('Links'));
        }

        // ── Validate Data button ─────────────────────────────────
        if (frm.doc.excel_file && frm.doc.status === 'Draft' && frm.doc.pr_naming_series) {
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
                    'This will create a combined <b>Purchase Receipt (Draft)</b> for all POs in the Excel file.<br><br>'
                    + 'You will need to review and submit the receipt manually.',
                    function () {
                        frm.call('start_processing').then(r => {
                            frappe.show_alert({ message: r.message, indicator: 'green' });
                            setTimeout(() => frm.reload_doc(), 2000);
                        });
                    }
                );
            }).addClass('btn-success');
        }

        // ── Create Purchase Invoice + BOE button ─────────────────
        if (frm.doc.receipt_name && frm.doc.status === 'Completed' && !frm.doc.invoice_name) {
            frm.add_custom_button(__('Create Invoice & Bill of Entry'), function () {
                if (!frm.doc.pi_naming_series) {
                    frappe.msgprint({
                        title: __('Missing Naming Series'),
                        indicator: 'orange',
                        message: `Please select a <b>Purchase Invoice Naming Series</b> first.`
                    });
                    return;
                }

                frappe.db.get_value('Purchase Receipt', frm.doc.receipt_name, 'docstatus', (r) => {
                    if (r.docstatus !== 1) {
                        frappe.msgprint({
                            title: __('Purchase Receipt Not Submitted'),
                            indicator: 'orange',
                            message: `Purchase Receipt <b>${frm.doc.receipt_name}</b> is still in Draft. 
                                      Please open it, review, and <b>Submit</b> it first.`
                        });
                        return;
                    }
                    frappe.confirm(
                        'Create Purchase Invoice and Bill of Entry from this receipt?',
                        function () {
                            frappe.show_alert({ message: 'Creating Invoice and Bill of Entry...', indicator: 'blue' });
                            frm.call('create_purchase_invoice_and_boe').then(r => {
                                frappe.msgprint({ title: 'Success', message: 'Documents Created Successfully', indicator: 'green' });
                                frm.reload_doc();
                            });
                        }
                    );
                });
            }).addClass('btn-primary');
        }

        if (frm.doc.status === 'Failed' && frm.doc.excel_file && !frm.doc.receipt_name) {
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
