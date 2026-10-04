"""Regenerate deterministic synthetic retail input CSVs; no personal data."""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "data" / "sample"
ROOT.mkdir(parents=True, exist_ok=True)


def save(name, headers, rows):
    with (ROOT / f"{name}.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(headers)
        writer.writerows(rows)


customers = [(f"C{i:03}", f"Customer {i:03}", ["North", "South", "East", "West"][i % 4])
             for i in range(1, 13)]
products = [("P001", "Laptop Stand", "Accessories", "14.00"),
            ("P002", "Wireless Mouse", "Electronics", "9.50"),
            ("P003", "USB-C Hub", "Electronics", "17.25"),
            ("P004", "Notebook", "Stationery", "2.10"),
            ("P005", "Desk Lamp", "Home Office", "19.80"),
            ("P006", "Desk Organizer", "Home Office", "6.40")]
randomizer = random.Random(2026)
orders = []
for i in range(1, 61):
    product = products[randomizer.randrange(len(products))]
    unit_cost = float(product[3])
    price = f"{unit_cost * 1.65:.2f}"
    orders.append((f"O{i:04}", customers[randomizer.randrange(len(customers))][0],
                   product[0], str(date(2026, 7, 1) + timedelta(days=randomizer.randrange(60))),
                   randomizer.randrange(1, 5), price,
                   randomizer.choice(["0.00", "0.05", "0.10", "0.15"])))
# Deliberate edge cases for the quarantine / data quality demonstration.
orders.extend([
    ("O0061", "C001", "P001", "2026-08-18", -2, "25.00", "0.00"),
    ("O0062", "C999", "P001", "2026-08-18", 1, "25.00", "0.00"),
    orders[0],
])
save("customers", ["customer_id", "customer_name", "region"], customers)
save("products", ["product_id", "product_name", "category", "unit_cost"], products)
save("orders", ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "discount_pct"], orders)
print(f"Created {len(customers)} customers, {len(products)} products, {len(orders)} orders (3 expected rejects)")
