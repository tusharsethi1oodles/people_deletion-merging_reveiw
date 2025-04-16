# import mysql.connector
# import json

# # Table mapping: table_name -> column_to_update
# tables_list = {
#     "addresses": "entity_id",
#     "global_entity_contacts": "entity_id",
#     "global_people": "id",  
#     "leads_connections": "entity_id",
#     "ledgers_role_mapping": "entity_id",
#     "people_crm_ids": "people_id",
#     "leads_transactions": "full_name"
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
#         for table, column in tables_info.items():
#             try:
#                 cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} = %s", (record_id,))
#                 count = cursor.fetchone()[0]

#                 if count > 0:
#                     cursor.execute(
#                         f"UPDATE {table} SET {column} = %s WHERE {column} = %s",
#                         (entity_id, record_id)
#                     )
#                     print(f"Updated {count} row(s) in `{table}` from {record_id} to {entity_id}")
#             except Exception as e:
#                 print(f"Error updating {table}: {e}")

#     # conn.commit()
#     cursor.close()
#     conn.close()

# # Run the update
# update_ids_from_json("record_to_entity.json", tables_list)


import mysql.connector
import json

# Table mapping: table_name -> column_to_update
# tables_list = {
#     "addresses": "entity_id",
#     "global_entity_contacts": "entity_id",
#     "global_people": "id",  
#     "leads_connections": "entity_id",
#     "ledgers_role_mapping": "entity_id",
#     "people_crm_ids": "people_id",
#     "leads_transactions": "full_name"
# }

# def connect_db(database):
#     return mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="oodles",
#         database=database
#     )

# def update_ids_from_json(json_file, tables_info, log_file="dms_ids_replace_people.txt", chunk_size=2000):
#     with open(json_file, "r") as f:
#         record_map = json.load(f)

#     record_items = [(int(old_id), new_id) for old_id, new_id in record_map.items()]

#     conn = connect_db("dms_after_org")
#     cursor = conn.cursor()

#     with open(log_file, "a") as log:
#         for table, column in tables_info.items():
#             for i in range(0, len(record_items), chunk_size):
#                 chunk = record_items[i:i + chunk_size]
#                 if column == "entity_id":
#                     query = f"""
#                         UPDATE {table}
#                         SET {column} = %s
#                         WHERE {column} = %s AND entity_type = 2
#                     """
#                 else:
#                     query = f"""
#                         UPDATE {table}
#                         SET {column} = %s
#                         WHERE {column} = %s
#                     """

#                 values = [(new_id, old_id) for old_id, new_id in chunk]

#                 try:
#                     cursor.executemany(query, values)
#                     for new_id, old_id in values:
#                         log.write(f"updated {old_id} in {column} to {new_id}\n")
#                 except Exception as e:
#                     log.write(f"error in `{table}` chunk {i // chunk_size + 1}: {e}\n")

#     # conn.commit()
#     cursor.close()
#     conn.close()

# # Run the update
# update_ids_from_json("record_to_entity.json", tables_list)



# -------------------------------------------------------------------------- #
import mysql.connector
import json
from datetime import datetime

# Tables and corresponding column to update
tables_list = {
    # "global_people": "id",  -- done 
    # "addresses": "entity_id", -- done
    # "global_entity_contacts": "entity_id", -- done 
    "leads_connections": "entity_id", 
    # "ledgers_role_mapping": "entity_id", done
    # "people_crm_ids": "people_id", -- done
    # "leads_transactions": "full_name" -- on it 
}

# Connect to MySQL
def connect_db(database):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database=database
    )

# Logging helper
def log_message(message, log_file):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as log:
        log.write(f"[{timestamp}] {message}\n")

# Main update logic
def update_ids_from_json(json_file, tables_info, log_file="dms_ids_replace_people_log.txt", chunk_size=2000):
    try:
        with open(json_file, "r") as f:
            record_map = json.load(f)
        record_items = [(int(old_id), new_id) for old_id, new_id in record_map.items()]
    except Exception as e:
        log_message(f"❌ Failed to load JSON file: {e}", log_file)
        return

    try:
        conn = connect_db("dms_backup_13april")
        cursor = conn.cursor()
    except Exception as e:
        log_message(f"❌ DB connection failed: {e}", log_file)
        return

    total_updates = 0

    for table, column in tables_info.items():
        print(f"\n🔄 Processing `{table}`...")
        log_message(f"Started updating table: {table}", log_file)

        for i in range(0, len(record_items), chunk_size):
            chunk = record_items[i:i + chunk_size]
            values = [(new_id, old_id) for old_id, new_id in chunk]

            if column == "entity_id":
                query = f"""
                    UPDATE {table}
                    SET {column} = %s
                    WHERE {column} = %s AND entity_type = 2
                """
            else:
                query = f"""
                    UPDATE {table}
                    SET {column} = %s
                    WHERE {column} = %s
                """

            try:
                cursor.executemany(query, values)
                conn.commit()

                updated = cursor.rowcount
                total_updates += updated

                msg = f"✅ `{table}` chunk {i // chunk_size + 1}: {updated} rows updated."
                print(msg)
                log_message(msg, log_file)

                for new_id, old_id in values:
                    log_message(f"Updated `{table}`: {column} {old_id} → {new_id}", log_file)

            except Exception as e:
                err_msg = f"❌ Error in `{table}` chunk {i // chunk_size + 1}: {e}"
                print(err_msg)
                log_message(err_msg, log_file)
                conn.rollback()
                continue

    try:
        cursor.close()
        conn.close()
        print("\n✅ DB connection closed.")
        log_message(f"✅ All updates complete. Total updated rows: {total_updates}", log_file)
    except Exception as e:
        print(f"❌ Error closing DB: {e}")
        log_message(f"Error closing DB: {e}", log_file)

# Run the update
update_ids_from_json("records_entity_mapping_for_people.json", tables_list)
