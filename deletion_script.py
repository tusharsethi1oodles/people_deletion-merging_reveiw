import pandas as pd
import mysql.connector
from datetime import datetime

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "oodles",
    "database": "dms_after_org"
}

LOG_FILE = "new_sheet_commit_logs_2.txt"

def update_table(cursor, table, column, people_id, check_entity_type, log_file):
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if table in ["ledgers_role_mapping"]:
        update_entity_query = f"""
            UPDATE {table} 
            SET is_delete = 1, deleted_at = %s 
            WHERE entity_id = %s AND entity_type = 2
        """
        cursor.execute(update_entity_query, (current_time, people_id))
        entity_updated = cursor.rowcount 

        if entity_updated > 0:
            log_file.write(f"Updated {entity_updated} row(s) in {table}: entity_id = {people_id} time {current_time} \n")
        else:
            log_file.write(f"{people_id} not present in entity_id column of {table} time {current_time} \n")
            
        update_related_query = f"""
            UPDATE {table} 
            SET is_delete = 1, deleted_at = %s 
            WHERE related_entity_id = %s AND related_entity_type = 2
        """
        cursor.execute(update_related_query, (current_time, people_id))
        related_updated = cursor.rowcount 

        if related_updated > 0:
            log_file.write(f"Updated {related_updated} row(s) in {table}: related_entity_id = {people_id} time {current_time} \n")
        else:
            log_file.write(f"{people_id} not present in related_entity_id column of {table} time {current_time} \n")
    elif table == "leads_transaction": # table name is wrong here it should be leads_transactions
        
        # update_reminder_query = """
        #         UPDATE reminder 
        #         SET deleted_at = %s
        #         WHERE leads_transaction_id IN (
        #             SELECT id FROM leads_transaction WHERE full_name = %s
        #         );
        # """
        # cursor.execute(update_reminder_query, (current_time, people_id))
        # reminder_updated = cursor.rowcount
            
        # if reminder_updated > 0:
        #     log_file.write(f"Updated {reminder_updated} row(s) in reminder: full_name = {people_id}, time {current_time}\n")
        # else:
        #     log_file.write(f"No matching records in reminder for full_name = {people_id}, time {current_time}\n")

        update_leads_tags_query = """
            UPDATE leads_tags 
            SET deleted_at = %s, is_delete = 1
            WHERE leads_transaction_id IN (
                SELECT id FROM leads_transaction WHERE full_name = %s
            );
        """
        cursor.execute(update_leads_tags_query, (current_time, people_id))
        leads_tags_updated = cursor.rowcount
            
        if leads_tags_updated > 0:
            log_file.write(f"Updated {leads_tags_updated} row(s) in leads_tags: full_name = {people_id}, time {current_time}\n")
        else:
            log_file.write(f"No matching records in leads_tags for full_name = {people_id}, time {current_time}\n")
    else:
        query = f"""
            UPDATE {table} 
            SET is_delete = 1, deleted_at = %s 
            WHERE {column} = %s
        """
        if check_entity_type:
            query += " AND entity_type = 2"

        cursor.execute(query, (current_time, people_id))
        rows_affected = cursor.rowcount

        if rows_affected > 0:
            log_file.write(f"Updated {rows_affected} row(s) in {table} where {column} = {people_id} time {current_time} \n")
        else:
            log_file.write(f"{people_id} not present in {column} column of {table} time {current_time} \n")

tables_list = {
    "addresses": ("entity_id", True),
    "global_entity_contacts": ("entity_id", True),
    "global_people": ("id", False),
    "leads_connections": ("entity_id", True),
    "ledgers_role_mapping": ("entity_id", True), 
    "people_crm_ids": ("people_id", False),
    "leads_transactions": ("full_name", False)
}

def update_records(csv_file):
    
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    df = pd.read_csv(csv_file, dtype=str)
    df = df[df["to_be_deleted"].str.strip().str.lower() == "true"]  # Filter 'yes' cases

    with open(LOG_FILE, "w") as log_file:
        for _, row in df.iterrows():
            try:
                people_id = int(row["people_id"])  # Convert to integer
                log_file.write(f"Processing People_ID: {people_id}\n")
            except ValueError:
                log_file.write(f"Skipping invalid People ID: {row['people_id']}\n")
                continue

            for table, (column, check_entity_type) in tables_list.items():
                update_table(cursor, table, column, people_id, check_entity_type, log_file)

    conn.commit()
    cursor.close()
    conn.close()

update_records("complete_file2.csv")
