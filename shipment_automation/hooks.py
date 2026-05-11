app_name = "shipment_automation"
app_title = "Shipment Automation"
app_publisher = "NexGen ERP Technologies"
app_description = "Automated Import Shipment Processing"
app_email = "admin@nexgenerp.com"
app_license = "mit"

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/shipment_automation/css/shipment_automation.css"
# app_include_js = "/assets/shipment_automation/js/shipment_automation.js"

# include js, css files in header of web.html
# web_include_css = "/assets/shipment_automation/css/shipment_automation.css"
# web_include_js = "/assets/shipment_automation/js/shipment_automation.js"

scheduler_events = {
    "daily": [
        "shipment_automation.shipment_automation.utils.cleanup_old_import_logs"
    ]
}
