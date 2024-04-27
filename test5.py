import csv
import random
import string
from faker import Faker

# Initialize Faker
fake = Faker()

# Function to generate random identifier of length 8
def generate_identifier():
    return ''.join(random.choices(string.digits, k=8))

# Function to generate random surname
def generate_surname():
    return fake.last_name()

# Function to generate random given name
def generate_given_name():
    return fake.first_name()

# Function to generate random middle initial
def generate_middle_initial():
    return fake.random_letter()

# Function to generate random suffix
def generate_suffix():
    return fake.suffix()

# Function to generate random primary street number
def generate_primary_street_number():
    return fake.building_number()

# Function to generate random primary street name
def generate_primary_street_name():
    return fake.street_name()

# Function to generate random city
def generate_city():
    return fake.city()

# Function to generate random state
def generate_state():
    return fake.state_abbr()

# Function to generate random zipcode
def generate_zipcode():
    return fake.zipcode()

# Function to generate random email
def generate_email():
    return fake.email()

# Function to generate random phone number
def generate_phone():
    return fake.phone_number()

# Function to generate random birth month
def generate_birthmonth():
    return str(random.randint(1, 12))

# Generate positive test data
def generate_positive_test_data(num_records):
    data = []
    for _ in range(num_records):
        record = {
            'Identifier': generate_identifier(),
            'Surname': generate_surname(),
            'Given_Name': generate_given_name(),
            'Middle_Initial': generate_middle_initial(),
            'Suffix': generate_suffix(),
            'Primary_Street_Number': generate_primary_street_number(),
            'Primary_Street_Name': generate_primary_street_name(),
            'City': generate_city(),
            'State': generate_state(),
            'Zipcode': generate_zipcode(),
            'Primary_Street_Number_Prev': generate_primary_street_number(),
            'Primary_Street_Name_Prev': generate_primary_street_name(),
            'City_Prev': generate_city(),
            'State_Prev': generate_state(),
            'Zipcode_Prev': generate_zipcode(),
            'Email': generate_email(),
            'Phone': generate_phone(),
            'Birthmonth': generate_birthmonth()
        }
        data.append(record)
    return data

# Generate negative test data
def generate_negative_test_data(num_records):
    data = []
    for _ in range(num_records):
        record = {
            'Identifier': generate_identifier(),
            'Surname': fake.random_number(digits=5),  # Invalid surname (numeric)
            'Given_Name': fake.random_number(digits=5),  # Invalid given name (numeric)
            'Middle_Initial': fake.random_number(digits=5),  # Invalid middle initial (numeric)
            'Suffix': fake.random_number(digits=5),  # Invalid suffix (numeric)
            'Primary_Street_Number': fake.random_number(digits=5),  # Invalid primary street number (numeric)
            'Primary_Street_Name': fake.random_number(digits=5),  # Invalid primary street name (numeric)
            'City': fake.random_number(digits=5),  # Invalid city (numeric)
            'State': fake.random_number(digits=5),  # Invalid state (numeric)
            'Zipcode': fake.random_number(digits=5),  # Invalid zipcode (numeric)
            'Primary_Street_Number_Prev': fake.random_number(digits=5),  # Invalid primary street number prev (numeric)
            'Primary_Street_Name_Prev': fake.random_number(digits=5),  # Invalid primary street name prev (numeric)
            'City_Prev': fake.random_number(digits=5),  # Invalid city prev (numeric)
            'State_Prev': fake.random_number(digits=5),  # Invalid state prev (numeric)
            'Zipcode_Prev': fake.random_number(digits=5),  # Invalid zipcode prev (numeric)
            'Email': fake.word(),  # Invalid email (random word)
            'Phone': fake.random_number(digits=8),  # Invalid phone number (less than 10 digits)
            'Birthmonth': fake.random_number(digits=2)  # Invalid birthmonth (numeric)
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

# Generate positive test data
num_positive_records = 1000
positive_test_data = generate_positive_test_data(num_positive_records)

# Generate negative test data
num_negative_records = 20
negative_test_data = generate_negative_test_data(num_negative_records)

# Combine positive and negative test data
combined_test_data = positive_test_data + negative_test_data

# Save data to CSV file
csv_filename = 'source_files/contact_info_20240426.csv'
save_to_csv(combined_test_data, csv_filename)
print(f"Test data saved to {csv_filename}")
