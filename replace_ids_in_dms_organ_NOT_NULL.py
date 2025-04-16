# import mysql.connector
# import json

# tables_to_update = {
#     "leads_connections": ("entity_id", True),
#     "addresses": ("entity_id", True),
#     "ledgers_role_mapping": ("entity_id", True),
#     "global_entity_contacts": ("entity_id", True),

#     "leads_notes": ("organisation_id", False),
#     "leads_tags": ("global_organisation_id", False),
#     "leads_tickets": ("global_organisation_id", False),
#     "leads_transactions": ("organisation_name", False),
#     "ledgers": ("organisation_id", False),
#     "reminders": ("organisation_id", False),
#     "users": ("global_organisation_id", False),
#     "organisation_crm_ids": ("organisation_id", False),
#     "global_organisations": ("parent_organisation_id", False)
# }

# def connect_db(database):
#     return mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="oodles",
#         database=database
#     )

# def update_ids_from_json(json_file, tables_info):
#     with open(json_file, "r") as f:
#         record_map = json.load(f)

#     conn = connect_db("dms_after_org")
#     cursor = conn.cursor()

#     for record_id, entity_id in record_map.items():
#         for table, (column, check_for_entity_type) in tables_info.items():
#             try:
#                 # Step 1: SELECT query based on whether entity_type check is needed
#                 if check_for_entity_type:
#                     select_query = f"SELECT COUNT(*) FROM {table} WHERE {column} = %s AND entity_type = 2"
#                     cursor.execute(select_query, (record_id,))
#                 else:
#                     select_query = f"SELECT COUNT(*) FROM {table} WHERE {column} = %s"
#                     cursor.execute(select_query, (record_id,))

#                 count = cursor.fetchone()[0]

#                 # Step 2: If record found, construct and run UPDATE query accordingly
#                 if count > 0:
#                     if check_for_entity_type:
#                         update_query = f"UPDATE {table} SET {column} = %s WHERE {column} = %s AND entity_type = 2"
#                         cursor.execute(update_query, (entity_id, record_id))
#                     else:
#                         update_query = f"UPDATE {table} SET {column} = %s WHERE {column} = %s"
#                         cursor.execute(update_query, (entity_id, record_id))

#                     print(f"Updated {count} row(s) in `{table}` from {record_id} to {entity_id}")
#             except Exception as e:
#                 print(f"Error updating `{table}`: {e}")

#     # conn.commit()  
#     cursor.close()
#     conn.close()


# update_ids_from_json("record_to_entity_organisation_NOT_NULL.json", tables_to_update)


# import mysql.connector
# import json

# # Tables to update and whether to check for entity_type = 2
# tables_to_update = {
#     "leads_connections": ("entity_id", True),
#     "addresses": ("entity_id", True),
#     # "ledgers_role_mapping": ("entity_id", True),
#     # "global_entity_contacts": ("entity_id", True),

#     # "leads_notes": ("organisation_id", False),
#     # "leads_tags": ("global_organisation_id", False),
#     # "leads_tickets": ("global_organisation_id", False),
#     # "leads_transactions": ("organisation_name", False),
#     # "reminders": ("organisation_id", False),
#     # "users": ("global_organisation_id", False),
#     # "organisation_crm_ids": ("organisation_id", False),

#     # "ledgers": ("organisation_id", False),
#     # "global_organisations": ("parent_organisation_id", False)
# }

# # Connect to MySQL
# def connect_db(database):
#     return mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="oodles",
#         database=database
#     )

# # Update function with batching
# def update_ids_from_json(json_file, tables_info, chunk_size=2000):
#     with open(json_file, "r") as f:
#         record_map = json.load(f)

#     # Ensure keys (old_ids) are integers
#     record_items = [(int(old_id), new_id) for old_id, new_id in record_map.items()]

#     conn = connect_db("dms_after_org")
#     cursor = conn.cursor()

#     for table, (column, check_entity_type) in tables_info.items():
#         print(f"\n▶ Updating table: {table}")

#         # Split into chunks
#         for i in range(0, len(record_items), chunk_size):
#             chunk = record_items[i:i + chunk_size]

#             if check_entity_type:
#                 query = f"""
#                     UPDATE {table}
#                     SET {column} = %s
#                     WHERE {column} = %s AND entity_type = 1
#                 """
#             else:
#                 query = f"""
#                     UPDATE {table}
#                     SET {column} = %s
#                     WHERE {column} = %s
#                 """

#             # Prepare data in (new_id, old_id) format
#             values = [(new_id, old_id) for old_id, new_id in chunk]

#             try:
#                 cursor.executemany(query, values)
#                 print(f"  ✅ Updated {cursor.rowcount} rows in chunk {i//chunk_size + 1}")
#             except Exception as e:
#                 print(f"  ❌ Error in table `{table}` chunk {i//chunk_size + 1}: {e}")

#     # conn.commit()
#     cursor.close()
#     conn.close()
#     print("\n✅ All updates completed and committed.")

# # Run the function
# update_ids_from_json("record_to_entity_organisation_NOT_NULL.json", tables_to_update)


import mysql.connector
import json
from datetime import datetime

# Tables and columns to update
tables_to_update = {
    # "leads_connections": ("entity_id", True), done 
    # "addresses": ("entity_id", True),  done 
    # "ledgers_role_mapping": ("entity_id", True), done
    # "global_entity_contacts": ("entity_id", True), done 

    # "leads_notes": ("organisation_id", False), done 
    # "leads_tags": ("global_organisation_id", False), done 
    # "leads_tickets": ("global_organisation_id", False), done
    # "leads_transactions": ("organisation_name", False), done
    # "reminders": ("organisation_id", False), done 
    # "users": ("global_organisation_id", False), done
    # "organisation_crm_ids": ("organisation_id", False), done 

    # "ledgers": ("organisation_id", False),
    # "global_organisations": ("parent_organisation_id", False)

    # done 
}

# DB connection
def connect_db(database):
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password="oodles",
            database=database
        )
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        raise

# Logging helper
def log_message(message, log_file="update_log_not_null_replacing_13april.txt"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")

# Main update logic
def update_ids_from_json(json_file, tables_info, chunk_size=2000, log_file="update_log.txt"):
    try:
        with open(json_file, "r") as f:
            record_map = json.load(f)
        record_items = [(int(old_id), new_id) for old_id, new_id in record_map.items()]
    except Exception as e:
        print(f"❌ Failed to load JSON file: {e}")
        log_message(f"Failed to load JSON file: {e}", log_file)
        return

    try:
        conn = connect_db("dms_backup_13april")
        cursor = conn.cursor()
    except Exception as e:
        log_message(f"Connection failed: {e}", log_file)
        return

    total_updates = 0

    for table, (column, check_entity_type) in tables_info.items():
        print(f"\n🔄 Processing table: `{table}`")
        log_message(f"Started updating table: {table}", log_file)

        for i in range(0, len(record_items), chunk_size):
            chunk = record_items[i:i + chunk_size]
            values = [(new_id, old_id) for old_id, new_id in chunk]

            if check_entity_type:
                query = f"UPDATE {table} SET {column} = %s WHERE {column} = %s AND entity_type = 1"
            else:
                query = f"UPDATE {table} SET {column} = %s WHERE {column} = %s"

            try:
                cursor.executemany(query, values)
                conn.commit()

                updated_rows = cursor.rowcount
                total_updates += updated_rows

                msg = f"✅ Chunk {i // chunk_size + 1}: {updated_rows} rows updated."
                print(msg)
                log_message(f"{msg} in `{table}`", log_file)

            except mysql.connector.Error as e:
                err_msg = f"❌ MySQL error in `{table}` chunk {i // chunk_size + 1}: {e}"
                print(err_msg)
                log_message(err_msg, log_file)
                conn.rollback()
                continue
            except Exception as e:
                err_msg = f"❌ Unexpected error in `{table}` chunk {i // chunk_size + 1}: {e}"
                print(err_msg)
                log_message(err_msg, log_file)
                conn.rollback()
                continue

    try:
        cursor.close()
        conn.close()
        print("\n✅ DB connection closed.")
        log_message(f"✅ Updates complete. Total rows affected: {total_updates}", log_file)
    except Exception as e:
        print(f"❌ Error closing connection: {e}")
        log_message(f"Error closing connection: {e}", log_file)

# Run the script
update_ids_from_json("records_entity_mapping_for_global_organisations_NOT_NULL.json", tables_to_update)
