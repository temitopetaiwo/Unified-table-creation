import re
import csv
import json
import xml.etree.ElementTree as ET
import mysql.connector

from pony.orm import Database, Required, Optional, Json, PrimaryKey, db_session

# Define the Pony ORM database
db = Database()

# MySQL connection parameters
mysql_params = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'student_cetm_23_4',
}

def read_csv(file_path):
    data = []
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def read_json(file_path):
    with open(file_path) as jsonfile:
        data = json.load(jsonfile)
    return data

def read_xml(file_path):
    data = []
    tree = ET.parse(file_path)
    root = tree.getroot()
    for user_element in root.findall('user'):
        user_data = dict(user_element.attrib)
        data.append(user_data)
    return data

def reorder_keys(record, key_to_move):
    # Move the specified key to the end
    record[key_to_move] = record.pop(key_to_move)
    return record

def remove_overlapping_attributes(record):
    # Remove overlapping attributes
    overlapping_keys = ['lastName', 'firstName', 'age', 'sex']
    for key in overlapping_keys:
        record.pop(key, None)
    return record

def update_merged_data(merged_data, firstname, lastname, attribute, value):
    for record in merged_data:
        if record['First Name'] == firstname and record['Second Name'] == lastname:
            # Update the specified attribute
            record[attribute] = value
            break
    return merged_data

def merge_data(csv_data, json_data, xml_data):
    merged_data = []
    for csv_record in csv_data:
        for json_record in json_data:
            if (
                csv_record['First Name'] == json_record['firstName']
                and csv_record['Second Name'] == json_record['lastName']
            ):
                # Handle 'debt' field in JSON
                debt_value = json_record.get('debt', 0)
                if isinstance(debt_value, dict):
                    debt_value = debt_value.get('amount', 0)
                csv_record['debt'] = float(debt_value) if debt_value else str(0)

                csv_record.update(json_record)
                break
        for xml_record in xml_data:
            if (
                csv_record['First Name'] == xml_record['firstName']
                and csv_record['Second Name'] == xml_record['lastName']
            ):
                csv_record.update(xml_record)
                break
        
        # Reorder keys to move 'debt' to the last placei
        csv_record = reorder_keys(csv_record, 'debt')

        # Remove overlapping attributes
        csv_record = remove_overlapping_attributes(csv_record)

        merged_data.append(csv_record)
    return merged_data

def process_messages(merged_data, messages_file_path):
    with open(messages_file_path, 'r') as messages_file:
        messages = messages_file.readlines()

    for message in messages:
        if 'security code is wrong' in message:
            merged_data = update_merged_data(merged_data, 'Valerie', 'Ellis', 'credit_card_security_code', '762')
        elif 'salary bump' in message:
            # Calculate the updated salary based on the current value
            merged_data = update_merged_data(merged_data, 'Charlie', 'West', 'salary', str(20131 + 2100))
        elif 'Happy Birthday' in message:
            merged_data = update_merged_data(merged_data, 'Charlie', 'Short', 'Age (Years)', '52')
        elif 'pension' in message:
            # Calculate the updated pension based on the current value
            merged_data = update_merged_data(merged_data, 'Christian', 'Martin', 'pension', str(22896 + ((0.15/100) * 22896)))

    return merged_data

# Define the data paths
csv_file_path = 'user_data_23_4.csv'
json_file_path = 'user_data_23_4.json'
xml_file_path = 'user_data_23_4.xml'
txt_file_path = 'user_data_23_4.txt'

csv_data = read_csv(csv_file_path)
json_data = read_json(json_file_path)
xml_data = read_xml(xml_file_path)

# Add missing fieldnames from JSON and XML to CSV data
csv_data[0].update({k: None for k in json_data[0] if k not in csv_data[0]})
csv_data[0].update({k: None for k in xml_data[0] if k not in csv_data[0]})

# Merge data
merged_data = merge_data(csv_data, json_data, xml_data)

# Process messages and update merged data
merged_data = process_messages(merged_data, txt_file_path)

# MySQL database connection
@db.on_connect(provider='mysql')
def mysql_on_connect(db, connection):
    connection.autocommit = True

# Define the database User entity
class User(db.Entity):
    id = PrimaryKey(int, auto=True)
    first_name = Required(str)
    last_name = Required(str)
    age = Optional(int, nullable=True)
    sex = Optional(str, nullable=True)
    vehicle_make = Optional(str, nullable=True)
    vehicle_model = Optional(str, nullable=True)
    vehicle_year = Optional(str, nullable=True)
    vehicle_type = Optional(str, nullable=True)
    iban = Optional(str, nullable=True)
    credit_card_number = Optional(str, nullable=True)
    credit_card_security_code = Optional(str, nullable=True)
    credit_card_start_date = Optional(str, nullable=True)
    credit_card_end_date = Optional(str, nullable=True)
    address_main = Optional(str, nullable=True)
    address_city = Optional(str, nullable=True)
    address_postcode = Optional(str, nullable=True)
    retired = Optional(bool, nullable=True)
    dependants = Optional(float, nullable=True)
    marital_status = Optional(str, nullable=True)
    salary = Optional(float, nullable=True)
    pension = Optional(float, nullable=True)
    company = Optional(str, nullable=True)
    commute_distance = Optional(float, nullable=True)
    debt = Optional(Json, nullable=True)  # Use Json type for flexibility

# Set the database provider (MySQL)
db.bind(provider='mysql', **mysql_params)

# Generate the database schema
db.generate_mapping(create_tables=True)

# Insert merged data into the Pony ORM database
with db_session:
    for record in merged_data:
        User(
            first_name=record['First Name'],
            last_name=record['Second Name'],
            age=record.get('Age (Years)'),
            sex=record.get('Sex'),
            vehicle_make=record.get('Vehicle Make'),
            vehicle_model=record.get('Vehicle Model'),
            vehicle_year=record.get('Vehicle Year'),
            vehicle_type=record.get('Vehicle Type'),
            iban=record.get('iban'),
            credit_card_number=record.get('credit_card_number'),
            credit_card_security_code=record.get('credit_card_security_code'),
            credit_card_start_date=record.get('credit_card_start_date'),
            credit_card_end_date=record.get('credit_card_end_date'),
            address_main=record.get('address_main'),
            address_city=record.get('address_city'),
            address_postcode=record.get('address_postcode'),
            retired=record.get('retired'),
            dependants=float(record.get('dependants')) if record.get('dependants', '').strip() else None,
            marital_status=record.get('marital_status'),
            salary=float(record.get('salary')) if record.get('salary', '').strip() else None,
            pension=float(record.get('pension')) if record.get('pension', '').strip() else None,
            company=record.get('company'),
            commute_distance=float(record.get('commute_distance')) if record.get('commute_distance', '').strip() else None,
            debt = record.get('debt'),
        )
