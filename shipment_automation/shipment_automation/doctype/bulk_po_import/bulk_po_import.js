frappe.ui.form.on("Bulk PO Import", {

    refresh: function (frm) {
        const banner = {
            Validating: ["⏳ Validation running... Refresh in a few seconds.", "yellow"],
            Processing: ["⏳ Creating Purchase Orders... Refresh in a few seconds.", "yellow"],
            Completed:  ["✅ Purchase Orders created as Draft. Review and submit.", "green"],
            Failed:     ["❌ An error occurred. See the log below.", "red"],
        };
        if (banner[frm.doc.status]) {
            frm.dashboard.add_comment(banner[frm.doc.status][0], banner[frm.doc.status][1], true);
        }

        // Download Template button
        frm.add_custom_button(__("Download Template"), function () {
            window.open(frappe.urllib.get_full_url(
                "/api/method/shipment_automation.shipment_automation.doctype.bulk_po_import.bulk_po_import.download_template"
            ));
        }, __("Actions"));

        // Validate Data
        if (frm.doc.po_excel && (frm.doc.status === "Draft" || frm.doc.status === "Failed")) {
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
    }
});
