import base64
from cryptography.fernet import Fernet
import pandas as pd
import os

ENCRYPTION_KEY = os.environ.get('PII_ENCRYPTION_KEY', Fernet.generate_key().decode())
cipher_suite = Fernet(ENCRYPTION_KEY.encode())

def encrypt_value(value):
    if pd.isna(value) or value == '':
        return value
    encrypted_value = cipher_suite.encrypt(str(value).encode())
    return base64.b64encode(encrypted_value).decode()

def decrypt_value(value):
    if pd.isna(value) or value == '':
        return value
    try:
        encrypted_value = base64.b64decode(value.encode())
        decrypted_value = cipher_suite.decrypt(encrypted_value).decode()
        return decrypted_value
    except Exception as e:
        print(f"Decryption error: {e}")
        return value
