const UTILS = "shipment_automation.shipment_automation.utils.get_naming_series";

frappe.ui.form.on("Shipment PO Import", {

    onload: function (frm) {
        // Populate PO naming series dynamically from ERPNext
        frappe.call({
            method: UTILS,
            args: { doctype: "Purchase Order" },
            callback: function (r) {
                if (r.message && r.message.length) {
                    frm.set_df_property("po_naming_series", "options",
                        [""].concat(r.message).join("\n"));
                    frm.refresh_field("po_naming_series");
                }
            }
        });

        frm.set_df_property("po_excel", "read_only",
            frm.doc.po_naming_series ? 0 : 1);
    },

    po_naming_series: function (frm) {
        const locked = !frm.doc.po_naming_series;
        frm.set_df_property("po_excel", "read_only", locked ? 1 : 0);
        frm.refresh_field("po_excel");
        if (locked) {
            frappe.show_alert({ message: "⚠️ Select PO Naming Series first.", indicator: "orange" });
        }
    },

    po_excel: function (frm) {
        if (!frm.doc.po_naming_series) {
            frappe.show_alert({ message: "⚠️ Select PO Naming Series before uploading.", indicator: "red" });
            frm.set_value("po_excel", "");
        }
    },

    refresh: function (frm) {
        const banner = {
            Validating: ["⏳ Validation running... Refresh in a few seconds.", "yellow"],
            Processing: ["⏳ Creating Purchase Orders... Refresh in a few seconds.", "yellow"],
            Completed:  ["✅ Purchase Orders created as Draft. Review and submit in ERPNext.", "green"],
            Failed:     ["❌ An error occurred. See the log below.", "red"],
        };
        if (banner[frm.doc.status]) {
            frm.dashboard.add_comment(banner[frm.doc.status][0], banner[frm.doc.status][1], true);
        }

        // Download Template button
        frm.add_custom_button(__("Download Template"), function () {
            window.open(frappe.urllib.get_full_url(
                "/api/method/shipment_automation.shipment_automation.doctype.shipment_po_import.shipment_po_import.download_template"
            ));
        }, __("Actions"));

        // Validate Data
        if (frm.doc.po_excel && frm.doc.po_naming_series && frm.doc.status === "Draft") {
            frm.add_custom_button(__("Validate Data"), function () {
                frappe.confirm("Validate PO Excel against ERPNext master data?", function () {
                    frm.call("start_validation").then(r => {
                        frappe.show_alert({ message: r.message, indicator: "blue" });
                        setTimeout(() => frm.reload_doc(), 2000);
                    });
                });
            }).addClass("btn-primary");
        }

        // Create Purchase Orders
        if (frm.doc.status === "Validated") {
            frm.add_custom_button(__("Create Purchase Orders"), function () {
                frappe.confirm(
                    "Create <b>Purchase Orders (Draft)</b> from the Excel file?",
                    function () {
                        frm.call("start_creation").then(r => {
                            frappe.show_alert({ message: r.message, indicator: "green" });
                            setTimeout(() => frm.reload_doc(), 2000);
                        });
                    }
                );
            }).addClass("btn-success");
        }

        // Re-validate after failure
        if (frm.doc.status === "Failed" && frm.doc.po_excel) {
            frm.add_custom_button(__("Re-validate"), function () {
                frm.set_value("status", "Draft");
                frm.save().then(() => {
                    frm.call("start_validation").then(r => {
                        frappe.show_alert({ message: r.message, indicator: "blue" });
                        setTimeout(() => frm.reload_doc(), 2000);
                    });
                });
            });
        }
    }
});
