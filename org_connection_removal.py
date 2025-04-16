import pandas as pd
import mysql.connector
from datetime import datetime

# Database configuration
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "oodles",
    "database": "dms_after_org"
}

csv_file = "complete_file2.csv"
df = pd.read_csv(csv_file)

filtered_df = df[df["to_be_deleted"] == "TRUE(ORGANISATION CONNECTION)"]

people_org_data = filtered_df[["people_id", "organisation_id"]].dropna()

# Function to execute SQL query and return affected rows
def run_query(query, params):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute(query, params)
        affected_rows = cursor.rowcount  # Check affected rows
        connection.commit()
        return affected_rows
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return -1  
    finally:
        cursor.close()
        connection.close()

current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

for _, row in people_org_data.iterrows():
    people_id = row["people_id"]
    organisation_id = row["organisation_id"]

    try:
        query1 = """
            UPDATE ledgers_role_mapping 
            SET is_delete = 1, deleted_at = %s 
            WHERE related_entity_id = %s AND related_entity_type = 2 
            AND entity_id = %s AND entity_type = 1;
        """
        rows_updated = run_query(query1, (current_time, people_id, organisation_id))

        if rows_updated == 0:
            query2 = """
                UPDATE ledgers_role_mapping 
                SET is_delete = 1, deleted_at = %s 
                WHERE entity_id = %s AND entity_type = 2 
                AND related_entity_id = %s AND related_entity_type = 1;
            """
            run_query(query2, (current_time, people_id, organisation_id))

    except Exception as e:
        print(f"Error processing people_id {people_id}, organisation_id {organisation_id}: {e}")

print("Update process completed.")
