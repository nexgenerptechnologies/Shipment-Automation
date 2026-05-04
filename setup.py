from setuptools import setup, find_packages

setup(
    name="shipment_automation",
    version="0.0.1",
    description="Automated Import Shipment Processing",
    author="NexGen ERP Technologies",
    author_email="admin@nexgenerp.com",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    install_requires=["openpyxl"],
)
