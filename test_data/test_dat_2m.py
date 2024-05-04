import csv
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()


# Function to generate random sales data
def generate_sales_data(num_records):
    sales_data = []
    products = ['Product A', 'Product B', 'Product C', 'Product D', 'Product E']
    locations = ['Location 1', 'Location 2', 'Location 3', 'Location 4', 'Location 5']

    for _ in range(num_records):
        product = random.choice(products)
        location = random.choice(locations)
        quantity = random.randint(1, 100)
        price = round(random.uniform(10, 1000), 2)
        amount = round(quantity * price, 2)
        date = fake.date_time_between(start_date='-1y', end_date='now').strftime('%Y-%m-%d %H:%M:%S')

        sales_data.append({
            'product': product,
            'location': location,
            'quantity': quantity,
            'price': price,
            'amount': amount,
            'date': date
        })

    return sales_data


# Generate 2 million records of sales data
sales_data = generate_sales_data(2000000)

# Write data to CSV file
csv_filename = r'/test_data/sales_data.csv'
with open(csv_filename, 'w', newline='') as csvfile:
    fieldnames = ['product', 'location', 'quantity', 'price', 'amount', 'date']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for sale in sales_data:
        writer.writerow(sale)

print("Sales data has been written to", csv_filename)
