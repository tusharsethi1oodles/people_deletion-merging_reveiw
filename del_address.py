import pandas as pd
import mysql.connector
from datetime import datetime

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "oodles",
    "database": "dms_after_org"
}

# Load CSV and filter
csv_file = "complete_file2.csv"
df = pd.read_csv(csv_file)
filtered_df = df[df["to_be_deleted"].str.strip().str.lower() == "true(address)"]
people_ids = filtered_df["people_id"].dropna().unique()
print(filtered_df)

# Timestamp and log file setup
current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
log_filename = f"address_deletion_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# SQL queries
check_query = """
    SELECT id FROM addresses 
    WHERE entity_id = %s AND entity_type = 2 AND is_delete = 0
"""
update_query = """
    UPDATE addresses 
    SET is_delete = 1, deleted_at = %s 
    WHERE entity_id = %s AND entity_type = 2
"""

# Start DB connection and log file once
try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    with open(log_filename, "w") as log_file:
        log_file.write(f"--- Address Deletion Log ({current_time}) ---\n\n")

        for people_id in people_ids:
            try:
                cursor.execute(check_query, (people_id,))
                address_rows = cursor.fetchall()

                if address_rows:
                    cursor.execute(update_query, (current_time, people_id))
                    connection.commit()
                    log_file.write(f"[UPDATED] people_id={people_id} → {cursor.rowcount} address(es) marked as deleted.\n")
                else:
                    log_file.write(f"[SKIPPED] people_id={people_id} → No active address found.\n")

            except Exception as e:
                log_file.write(f"[ERROR] people_id={people_id} → Exception: {e}\n")

        log_file.write("\n--- Address deletion process completed ---\n")

except mysql.connector.Error as err:
    print(f"[FATAL ERROR] Database connection failed: {err}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
