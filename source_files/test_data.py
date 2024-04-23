import csv
from faker import Faker
from datetime import datetime

# Initialize Faker
fake = Faker()


# Generate test data
def generate_test_data():
    data = {}
    data['Identifier'] = fake.random_int(min=10000000, max=99999999)  # Generate a random 8-digit identifier
    data['Surname'] = fake.last_name()
    data['Given_Name'] = fake.first_name()
    data['Middle_Initial'] = fake.random_letter().upper()
    data['Suffix'] = fake.suffix()
    data['Primary_Street_Number'] = fake.building_number()
    data['Primary_Street_Name'] = fake.street_name()
    data['City'] = fake.city()
    data['State'] = fake.state_abbr()
    data['Zipcode'] = fake.zipcode()
    data['Primary_Street_Number_Prev'] = fake.building_number()
    data['Primary_Street_Name_Prev'] = fake.street_name()
    data['City_Prev'] = fake.city()
    data['State_Prev'] = fake.state_abbr()
    data['Zipcode_Prev'] = fake.zipcode()
    data['Email'] = fake.email()
    data['Phone'] = fake.phone_number()
    birth_year = fake.random_int(min=1900, max=2022)  # Generate a random birth year
    birth_month = fake.random_int(min=1, max=12)  # Generate a random birth month
    data['Birthmonth'] = f"{birth_year}{birth_month:02d}"  # Format year and month to 6 characters

    return data


# Prompt user for the number of records
num_records = int(input("Enter the number of records to generate: "))

# Generate and store test data in CSV file
file_name = f"contact_info_{datetime.now().strftime('%Y%m%d')}.csv"
with open(file_name, 'w', newline='') as csvfile:
    fieldnames = generate_test_data().keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for _ in range(num_records):
        writer.writerow(generate_test_data())

print(f"{num_records} records of test data have been saved to {file_name}.")
