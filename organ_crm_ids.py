# import pandas as pd
# import mysql.connector
# import os 
# import json

# def connect_db(database):
#     return mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="oodles",
#         database=database
#     )

# def get_entity_details(dms_entity_id, entity_type, connection):
#     try:
#         cursor = connection.cursor(buffered=True)
#         print(f"my dms_entity_id is {dms_entity_id}")

#         dms_entity_id = str(int(dms_entity_id))  # Convert to string for key lookup

#         if entity_type == 2:
#             file_path = 'records_entity_mapping_for_people.json'
#             if not os.path.exists(file_path):
#                 raise FileNotFoundError(f"{file_path} not found.")
#             with open(file_path, 'r') as f:
#                 data = json.load(f)
#             return data.get(dms_entity_id)

#         elif entity_type == 1:
#             file_paths = [
#                 'records_entity_mapping_for_global_organisations_NOT_NULL.json',
#                 'records_entity_mapping_for_global_organisations_NULL.json'
#             ]
#             for file_path in file_paths:
#                 if not os.path.exists(file_path):
#                     continue
#                 with open(file_path, 'r') as f:
#                     data = json.load(f)
#                 if dms_entity_id in data:
#                     return data.get(dms_entity_id)

#         return None
#     except FileNotFoundError as e:
#         print(f"File error in get_entity_details: {e}")
#         return None
#     except Exception as e:
#         print(f"Error in get_entity_details: {e}")
#         return None
 


# def process_record(organisation_id, crm_id):
#     conn_db1 = connect_db("dmscopy")
#     conn_db2 = connect_db("entities_for_demo_2")

#     db1_cursor = conn_db1.cursor()
#     db2_cursor = conn_db2.cursor()

#     entity_id = get_entity_details(organisation_id,1,conn_db2)

#     if entity_id:
#         insert_into_entity_property = """ 
#         INSERT INTO entity_property (entity_id, property_id, property_value) 
#         VALUES (%s, %s, %s)
#         """
#         db2_cursor.execute(insert_into_entity_property, (entity_id, 'organisation_crm_ids', crm_id))
#         conn_db2.commit()  # Commit the transaction

#     db1_cursor.close()
#     db2_cursor.close()
#     conn_db1.close()
#     conn_db2.close()


# # Read data from MySQL
# conn_db1 = connect_db("dms_after_org")
# df = pd.read_sql("SELECT organisation_id, crm_id FROM organisation_crm_ids", conn_db1)
# conn_db1.close()

# # Process each record
# for index, row in df.iterrows():
#     process_record(row["organisation_id"], row["crm_id"])



import pandas as pd
import mysql.connector
import json
import os

LOG_FILE = "crm_insert_log_demo_2.txt"

def connect_db(database):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database=database
    )

# Load entity mappings only once
def load_entity_mappings():
    mappings = {}
    try:
        with open('records_entity_mapping_for_global_organisations_NOT_NULL.json') as f:
            mappings.update(json.load(f))
    except FileNotFoundError:
        print("NOT_NULL.json not found.")
    try:
        with open('records_entity_mapping_for_global_organisations_NULL.json') as f:
            mappings.update(json.load(f))
    except FileNotFoundError:
        print("NULL.json not found.")
    return mappings

def log_record(organisation_id, crm_id, status):
    with open(LOG_FILE, "a") as f:
        f.write(f"{organisation_id},{crm_id},{status}\n")

# Process records in chunks
def process_records_in_bulk(df, entity_mappings, batch_size=100):
    try:
        conn = connect_db("entities_for_demo_2")
        cursor = conn.cursor()

        batch = []
        for _, row in df.iterrows():
            try:
                organisation_id = str(int(row['organisation_id']))
                crm_id = row['crm_id']
                entity_id = entity_mappings.get(organisation_id)

                if entity_id:
                    batch.append((entity_id, 'CRM_ID', crm_id))  # <--- Fixed property_id here
                    # log_record(organisation_id, crm_id, "QUEUED")
                else:
                    log_record(organisation_id, crm_id, "NO_ENTITY_ID")
            except Exception as e:
                log_record(row.get('organisation_id', 'N/A'), row.get('crm_id', 'N/A'), f"ERROR: {str(e)}")

            if len(batch) >= batch_size:
                try:
                    cursor.executemany("""
                        INSERT INTO entity_property (entity_id, property_id, property_value)
                        VALUES (%s, %s, %s)
                    """, batch)
                    conn.commit()  
                    for entry in batch:
                        log_record(entry[0], entry[2], "INSERTED")
                except Exception as e:
                    for entry in batch:
                        log_record(entry[0], entry[2], f"INSERT_ERROR: {str(e)}")
                batch.clear()

        # Final batch
        if batch:
            try:
                cursor.executemany("""
                    INSERT INTO entity_property (entity_id, property_id, property_value)
                    VALUES (%s, %s, %s)
                """, batch)
                conn.commit()  
                for entry in batch:
                    log_record(entry[0], entry[2], "INSERTED")
            except Exception as e:
                for entry in batch:
                    log_record(entry[0], entry[2], f"INSERT_ERROR: {str(e)}")

    except Exception as e:
        print(f"Fatal error in bulk processing: {e}")
    finally:
        cursor.close()
        conn.close()

# Load data
try:
    conn = connect_db("dms_after_org")
    df = pd.read_sql("SELECT organisation_id, crm_id FROM organisation_crm_ids", conn)
    conn.close()
except Exception as e:
    print(f"Error loading data: {e}")
    df = pd.DataFrame()

if not df.empty:
    mappings = load_entity_mappings()
    process_records_in_bulk(df, mappings)
else:
    print("No data found to process.")
