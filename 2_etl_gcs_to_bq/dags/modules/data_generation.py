import uuid
from faker import Faker
import pandas as pd
import os
from datetime import datetime

fake = Faker()
PII_COLUMNS = ['email', 'first_name', 'last_name', 'phone_number', 'ssn', 'credit_card_number']

def generate_fake_user_data(num_records=100, output_dir='/tmp/airflow_pii_data'):
    users = []
    for _ in range(num_records):
        user = {
            'user_id': str(uuid.uuid4()),
            'email': fake.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'phone_number': fake.phone_number(),
            'ssn': fake.ssn(),
            'credit_card_number': fake.credit_card_number(),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'state': fake.state(),
            'zip_code': fake.zipcode(),
            'country': fake.country(),
            'registration_date': fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
            'last_login': fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')
        }
        print(user)
        users.append(user)
    os.makedirs(output_dir, exist_ok=True)
    csv_file = f'{output_dir}/users_with_pii_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    df = pd.DataFrame(users)
    df.to_csv(csv_file, index=False)
    print(f"Generated CSV file with {len(users)} records: {csv_file}")
    return csv_file
