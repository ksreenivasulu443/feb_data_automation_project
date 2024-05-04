# import csv
# from faker import Faker
# from datetime import datetime
#
# # Initialize Faker
# fake = Faker()
#
#
# # Generate test data
# def generate_test_data():
#     data = {}
#     data['Identifier'] = fake.random_int(min=10000000, max=99999999)  # Generate a random 8-digit identifier
#     data['Surname'] = fake.last_name()
#     data['Given_Name'] = fake.first_name()
#     data['Middle_Initial'] = fake.random_letter().upper()
#     data['Suffix'] = fake.suffix()
#     data['Primary_Street_Number'] = fake.building_number()
#     data['Primary_Street_Name'] = fake.street_name()
#     data['City'] = fake.city()
#     data['State'] = fake.state_abbr()
#     data['Zipcode'] = fake.zipcode()
#     data['Primary_Street_Number_Prev'] = fake.building_number()
#     data['Primary_Street_Name_Prev'] = fake.street_name()
#     data['City_Prev'] = fake.city()
#     data['State_Prev'] = fake.state_abbr()
#     data['Zipcode_Prev'] = fake.zipcode()
#     data['Email'] = fake.email()
#     data['Phone'] = fake.phone_number()
#     birth_year = fake.random_int(min=1900, max=2022)  # Generate a random birth year
#     birth_month = fake.random_int(min=1, max=12)  # Generate a random birth month
#     data['Birthmonth'] = f"{birth_year}{birth_month:02d}"  # Format year and month to 6 characters
#
#     return data
#
#
# # Prompt user for the number of records
# num_records = int(input("Enter the number of records to generate: "))
#
# # Generate and store test data in CSV file
# file_name = f"contact_info_{datetime.now().strftime('%Y%m%d')}.csv"
# with open(file_name, 'w', newline='') as csvfile:
#     fieldnames = generate_test_data().keys()
#     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#
#     writer.writeheader()
#     for _ in range(num_records):
#         writer.writerow(generate_test_data())
#
# print(f"{num_records} records of test data have been saved to {file_name}.")


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
csv_filename = r'/source_files/sales_data.csv'
with open(csv_filename, 'w', newline='') as csvfile:
    fieldnames = ['product', 'location', 'quantity', 'price', 'amount', 'date']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for sale in sales_data:
        writer.writerow(sale)

print("Sales data has been written to", csv_filename)

