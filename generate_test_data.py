import os
import csv
from faker import Faker

# --- Configuration ---
NUM_ADDRESSES = 10000
OUTPUT_CSV = "test_addresses.csv"

def generate_addresses():
    """
    Generates a specified number of unique, fake addresses from different locales.
    """
    addresses = set()
    fake = Faker(['en_US', 'en_GB', 'de_DE', 'fr_FR', 'es_ES', 'ja_JP', 'en_AU'])
    
    print(f"🌍 Generating {NUM_ADDRESSES} unique addresses...")

    while len(addresses) < NUM_ADDRESSES:
        address = fake.address().replace('\n', ', ')
        addresses.add(address)

    print("✅ Generation complete.")
    return list(addresses)

def save_addresses_to_csv(addresses):
    """
    Saves the list of addresses to a single CSV file.
    """
    print(f"📝 Saving addresses to '{OUTPUT_CSV}'...")
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['address'])  # Write header
        for address in addresses:
            writer.writerow([address])
            
    print(f"✅ Successfully saved {len(addresses)} addresses to {OUTPUT_CSV}.")


if __name__ == "__main__":
    generated_addresses = generate_addresses()
    save_addresses_to_csv(generated_addresses)
    print("\n🚀 Test data generation is complete.")
    print(f"You can now run the main script on the '{OUTPUT_CSV}' file:")
    print(f"python main.py {OUTPUT_CSV} results.csv") 
