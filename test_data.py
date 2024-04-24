import csv
from faker import Faker

# Initialize Faker
fake = Faker()

# Generate test data
def generate_test_data(num_records):
    data = []
    for _ in range(num_records):
        record = {
            'Identifier': fake.random_number(digits=8),
            'Surname': fake.last_name(),
            'Given Name': fake.first_name(),
            'Middle Initial': fake.random_letter(),
            'Suffix': fake.suffix(),
            'Primary Street Number': fake.building_number(),
            'Primary Street Name': fake.street_name(),
            'City': fake.city(),
            'State': fake.state_abbr(),
            'Zipcode': fake.zipcode(),
            'Primary Street Number Prev': fake.building_number(),
            'Primary Street Name Prev': fake.street_name(),
            'City Prev': fake.city(),
            'State Prev': fake.state_abbr(),
            'Zipcode Prev': fake.zipcode(),
            'Email': fake.email(),
            'Phone': fake.phone_number(),
            'Birthmonth': fake.random_number(digits=2)
        }
        data.append(record)
    return data

# Save data to CSV file
def save_to_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in data:
            writer.writerow(record)

# Generate test data
num_records = 100
test_data = generate_test_data(num_records)

# Save data to CSV file
csv_filename = 'test_data.csv'
save_to_csv(test_data, csv_filename)
print(f"Test data saved to {csv_filename}")
