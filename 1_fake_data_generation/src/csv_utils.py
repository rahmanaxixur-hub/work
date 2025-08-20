import os
import csv

def save_users_to_csv(users, filename, output_dir, fields):
    """
    Save a list of user dicts to a CSV file in the specified directory.
    Creates the directory if it does not exist.
    """
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(users)
    print(f"Generated {len(users)} fake users and saved to {filepath}")
    return filepath
