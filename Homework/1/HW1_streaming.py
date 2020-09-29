from sys import argv
import csv
from typing import Optional, Dict
from decimal import Decimal

class SaleData:
    def __init__(self, data: dict):
        self.customer_id = int(data["Customer ID"])
        self.product_id: str = data["Product ID"]
        self.item_cost: Decimal = Decimal(data["Item Cost"])

class SalesSummary:
    def __init__(self, total_revenue: Decimal = Decimal(0), customer_count: int = 0, last_recorded_cutomer: Optional[str] = None):
        self.total_revenue = total_revenue
        self.customer_count = customer_count
        self.last_recorded_cutomer = last_recorded_cutomer
    
    def update_with_sorted_customer(self, sale_data: SaleData):
        if self.last_recorded_cutomer != sale_data.customer_id:
            self.last_recorded_cutomer = sale_data.customer_id
            self.customer_count += 1
        self.total_revenue += sale_data.item_cost
        
    def dict_with_product_id(self, product_id: str):
        return {
            "Product ID": product_id,
            "Customer Count": self.customer_count,
            "Total Revenue": self.total_revenue
        }
    

def sales_data(filename):
    with open(filename, 'r') as fi:
        reader = csv.DictReader(fi)
        for row in reader:
            yield(SaleData(row))

def summarize_sales(sale_filename):
    product_sales_summaries: Dict[str, SalesSummary] = {}
    for data in sales_data(sale_filename):
        if data.product_id in product_sales_summaries:
            product_sales_summaries[data.product_id].update_with_sorted_customer(data)
        else:
            product_sales_summaries[data.product_id] = SalesSummary(total_revenue=data.item_cost, customer_count=1, last_recorded_cutomer=data.customer_id)
    for product_id in sorted(product_sales_summaries.keys()):
        yield(product_sales_summaries[product_id].dict_with_product_id(product_id))


def sale_output(sale_filename: str, output_filename: str):
    with open(output_filename, "w") as fi:
        fieldnames=["Product ID", "Customer Count", "Total Revenue"]
        csv.writer(fi).writerow(fieldnames)
        dictwriter = csv.DictWriter(f=fi, fieldnames=fieldnames)
        for summary_row in summarize_sales(sale_filename):
            dictwriter.writerow(summary_row)


sale_output(argv[1], argv[2])