import uuid
from faker import Faker

def generate_user(fake: Faker) -> dict:
    return {
        'id': str(uuid.uuid4()),
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'phone_number': fake.phone_number(),
        'address': fake.address().replace('\n', ', '),
        'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
        'ssn': fake.ssn()
    }

def get_user_fields():
    return [
        'id', 'first_name', 'last_name', 'email', 'phone_number', 'address', 'date_of_birth', 'ssn'
    ]

def generate_users(num_users: int) -> list:
    fake = Faker()
    return [generate_user(fake) for _ in range(num_users)]
