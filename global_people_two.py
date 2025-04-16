import mysql.connector
import pandas as pd
import json
import os

def connect_db(database):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database=database
    )

def process_record(record_id):
    conn_db1 = connect_db("dms_after_org")
    conn_db2 = connect_db("entities_for_demo_2")
    cursor_db1 = conn_db1.cursor()
    cursor_db2 = conn_db2.cursor()

    try:
        insert_entity_query = """
        INSERT INTO entity (creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, type, name)
        SELECT 
            creator_ledger_id, 
            created_by, 
            updated_by, 
            created_at, 
            updated_at, 
            deleted_at, 
            2, 
            CONCAT(
                COALESCE(first_name, ''),  
                CASE 
                    WHEN COALESCE(first_name, '') != '' AND COALESCE(last_name, '') != '' THEN ' '  
                    ELSE ''  
                END,  
                COALESCE(last_name, '')
            )
        FROM dmscopy.global_people
        WHERE id = %s
        AND NOT (first_name is NULL AND last_name is NULL);
        """
        
        cursor_db2.execute(insert_entity_query, (record_id,))
        entity_last = cursor_db2.lastrowid  
        conn_db2.commit() 

        if not entity_last:
            print(f"Skipping record {record_id}(dms_id) as entity both first and last name are NULL")
            return 
        
        json_file_path = "records_entity_mapping_for_people.json"
        if os.path.exists(json_file_path):
            with open(json_file_path, "r") as f:
                data = json.load(f)
        else:
            data = {}

        data[str(record_id)] = entity_last

        with open(json_file_path, "w") as f:
            json.dump(data, f, indent=4)

        insert_people_query = """
        INSERT INTO people (entity_id, salutation, first_name, last_name, title, date_of_birth, created_by, updated_by, created_at, updated_at, type)
        SELECT 
            %s, 
            salutation, 
            first_name, 
            last_name, 
            title, 
            CASE 
                WHEN date_of_birth LIKE '%/%' THEN STR_TO_DATE(date_of_birth, '%d/%m/%Y') 
                ELSE date_of_birth 
            END, 
            created_by, 
            updated_by, 
            created_at, 
            updated_at,
            2
        FROM dmscopy.global_people
        WHERE id = %s;
        """
        try:
            cursor_db2.execute(insert_people_query, (entity_last, record_id))
            conn_db2.commit()
        except mysql.connector.Error as err:
            print(f"Error inserting into people for record {record_id}: {err}")
            return  

        properties = [
            "former_last_name", "notes", "ppsn_document_type", "photo_url", "pronounced", "signature_attachment",
            "crm_id", "exchange_ref_id", "import_people_name", "leads_transactions_id", "status_id", "industry_id","is_delete"
        ]
        
        for prop in properties:
            try:
                cursor_db2.execute("SELECT property_id FROM property WHERE property_id = %s", (prop,))
                if cursor_db2.fetchone():
                    condition = f"{prop} IS NOT NULL"
                    if prop == "leads_transactions_id":
                        condition += " AND leads_transactions_id != 0"
                    elif prop == "signature_attachment":
                        condition += " AND signature_attachment != 0"
                    elif prop == "pronounced":
                        condition += " AND pronounced != ''"

                    insert_property_query = f"""
                        INSERT INTO entity_property (entity_id, property_id, property_value)
                        SELECT %s, '{prop}', {prop}
                        FROM dmscopy.global_people
                        WHERE id = %s AND {condition};
                    """
                    try:
                        # print(f"Executing: insert_property_query with values ({entity_last}, {record_id})")
                        cursor_db2.execute(insert_property_query, (entity_last, record_id))
                    except mysql.connector.Error as err:
                        print(f"Error in property insert query: {err}")
                        print(f"Failed Query: insert_property_query with values ({entity_last}, {record_id})")
                        raise  # Re-raise the error so rollback happens

                    try:
                        select_contacts_query = """
                            SELECT gp.created_by, gp.updated_by, gec.contact_type, gec.contact_for, gec.contact_value
                            FROM dmscopy.global_entity_contacts gec
                            JOIN dmscopy.global_people gp ON gp.id = gec.entity_id
                            WHERE gec.entity_type = 2 AND gp.id = %s;
                        """
                        # print(f"Executing: select_contacts_query with values ({record_id},)")
                        cursor_db2.execute(select_contacts_query, (record_id,))
                        contacts = cursor_db2.fetchall()
                    except mysql.connector.Error as err:
                        print(f"Error in contacts select query: {err}")
                        print(f"Failed Query: select_contacts_query with values ({record_id},)")
                        raise  # Re-raise the error so rollback happens

                    for index, (created_by, updated_by, contact_type, contact_for, contact_value) in enumerate(contacts, start=1):
                        property_id = "phone_number" if contact_type.lower() == "phone" else "email" if contact_type.lower() == "email" else None
                        if not property_id:
                            continue  

                        is_primary = "Yes" if contact_for.lower() == "primary" else "No"
                        property_title = contact_for

                        insert_contact_query = """
                            INSERT INTO entity_property (entity_id, property_id, property_title, property_value, is_primary, position, created_by, updated_by)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        try:
                            # print(f"Executing: insert_contact_query with values ({entity_last}, {property_id}, {property_title}, {contact_value}, {is_primary}, {index}, {created_by}, {updated_by})")
                            cursor_db2.execute(insert_contact_query, (entity_last, property_id, property_title, contact_value, is_primary, index, created_by, updated_by))
                        except mysql.connector.Error as err:
                            print(f"Error in contact insert query: {err}")
                            print(f"Failed Query: insert_contact_query with values ({entity_last}, {property_id}, {property_title}, {contact_value}, {is_primary}, {index}, {created_by}, {updated_by})")
                            raise  # Re-raise the error so rollback happens

                conn_db2.commit()  # only if all queries succeed

            except mysql.connector.Error as err:
                conn_db2.rollback()  # Rollback everything if any error occurs
                print(f"Transaction failed for record {record_id}: {err}")
    
    except mysql.connector.Error as err:
        print(f"Error processing record {record_id}: {err}")
    
    finally:
        cursor_db1.close()
        cursor_db2.close()
        conn_db1.close()
        conn_db2.close()

# Fetch all records and process them
conn_db1 = connect_db("dms_after_org")
df = pd.read_sql("SELECT id FROM global_people where is_delete=0", conn_db1)
conn_db1.close()

for record_id in df["id"]:
    process_record(record_id)
