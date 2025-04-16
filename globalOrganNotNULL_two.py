import mysql.connector
import pandas as pd
import json
import os

def connect_db(database):
    """Connects to the MySQL database."""
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database=database
    )

def get_parent_entity_id(cursor_read, parent_organisation_id):
    """Fetches the parent entity ID if it exists."""
    # if parent_organisation_id is None:
    #     return None

    # cursor_read.execute("""
    #     SELECT organisation_name, updated_at, created_at
    #     FROM dms_after_org.global_organisations
    #     WHERE id = %s;
    # """, (parent_organisation_id,))
    # parent_details = cursor_read.fetchone()

    # if not parent_details:
    #     return None  

    # name, updated_at, created_at = parent_details

    # cursor_read.execute("""
    #     SELECT entity_id FROM entities_final.entity
    #     WHERE name = %s AND updated_at = %s AND created_at = %s;
    # """, (name, updated_at, created_at))

    # result = cursor_read.fetchone()
    # return result[0] if result else None

    dms_entity_id = str(int(parent_organisation_id))  # Convert to string for key lookup

    file_paths = [
        'records_entity_mapping_for_global_organisations_NULL.json'
    ]
    for file_path in file_paths:
        if not os.path.exists(file_path):
            continue
        with open(file_path, 'r') as f:
            data = json.load(f)
        if dms_entity_id in data:
            return data.get(dms_entity_id)

    return None

def process_record(record_id):
    """Processes a single organisation record and inserts data into entity and entity_property."""
    conn_db1 = connect_db("dms_after_org")
    conn_db2 = connect_db("entities_for_demo_2")

    # Use buffered cursors to avoid "Unread result found" error
    cursor_read_db1 = conn_db1.cursor(buffered=True)
    cursor_read_db2 = conn_db2.cursor(buffered=True)
    cursor_write_db2 = conn_db2.cursor(buffered=True)

    # Fetch organisation details
    cursor_read_db1.execute("""
        SELECT creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, 
               organisation_name, parent_organisation_id, trade_name 
        FROM dms_after_org.global_organisations
        WHERE id = %s AND parent_organisation_id IS NOT NULL;
    """, (record_id,))
    record = cursor_read_db1.fetchone()

    if not record:
        cursor_read_db1.close()
        cursor_read_db2.close()
        cursor_write_db2.close()
        conn_db1.close()
        conn_db2.close()
        return  # Skip if no record found

    creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, organisation_name, parent_organisation_id, trade_name = record

    # Get parent entity ID if applicable

    # Insert into entity table
    insert_entity_query = """
        INSERT INTO entities_for_demo_2.entity 
        (name, creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, type)
        VALUES (%s, %s, %s, %s, %s, %s,%s,%s);
    """
    cursor_write_db2.execute(insert_entity_query, (organisation_name, creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at,1))
    entity_last = cursor_write_db2.lastrowid  # Get inserted entity ID

    # Commit entity insertion
    conn_db2.commit()

    json_file_path = "records_entity_mapping_for_global_organisations_NOT_NULL.json"
    if os.path.exists(json_file_path):
        with open(json_file_path, "r") as f:
            data = json.load(f)
    else:
        data = {}

    data[str(record_id)] = entity_last

    with open(json_file_path, "w") as f:
        json.dump(data, f, indent=4)

    # Define properties to insert
    properties = [
        "fy_start_month", "fy_end_month", "website", "registration_number", 
        "vat_number", "registered_country_id",
        "logo_url", "currency_id", "description", "nace_section_id", 
        "revenue_access_number", "hierarchy_code",
        "crm_id", "exchange_ref_id","leads_transactions_id", 
        "status_id", "industry_id", "trade_name","parent_entity_id"
    ]

    for prop in properties:
        # Check if property exists in the property table
        cursor_read_db2.execute("SELECT property_id FROM entities_for_demo_2.property WHERE property_id = %s", (prop,))
        if cursor_read_db2.fetchone():  
            # Ensures the result is fully consumed

            if prop=='parent_entity_id':
                parent_entity_id = get_parent_entity_id(cursor_read_db2, parent_organisation_id)
                if parent_entity_id:
                    insert_property_query = """
                        INSERT INTO entities_for_demo_2.entity_property (entity_id, property_id, property_value)
                        VALUES (%s, %s, %s);
                    """
                    cursor_write_db2.execute(insert_property_query, (entity_last, 'parent_entity_id', parent_entity_id))
                    print(f"Parent entity ID for record ID {record_id}: {parent_entity_id}")
                else:
                    print('Parent entity not found for record ID:', record_id)
                    
            else:
                condition = f"{prop} IS NOT NULL"
                if prop in ["website", "registration_number", "vat_number", "description", "revenue_access_number", "crm_id", "leads_transactions_id", "trade_name"]:
                    condition += f" AND {prop} != ''"
                elif prop == "leads_transactions_id":
                    condition += " AND leads_transactions_id != 0"
                

                # Insert property value into entity_property
                insert_property_query = f"""
                    INSERT INTO entities_for_demo_2.entity_property (entity_id, property_id, property_value)
                    SELECT %s, '{prop}', {prop}
                    FROM dms_after_org.global_organisations
                    WHERE id = %s AND {condition};
                """
                cursor_write_db2.execute(insert_property_query, (entity_last, record_id))
        
    # Commit property insertions
    conn_db2.commit()

    try:
        cursor_read_db2.execute("""
            SELECT gp.created_by, gp.updated_by, gec.contact_type, gec.contact_for, gec.contact_value
            FROM dms_after_org.global_entity_contacts gec
            JOIN dms_after_org.global_people gp ON gp.id = gec.entity_id
            WHERE gec.entity_type = 2 AND gp.id = %s;
        """, (record_id,))

        contacts = cursor_read_db2.fetchall()

        for index, (created_by, updated_by, contact_type, contact_for, contact_value) in enumerate(contacts, start=1):
            # Determine property_id based on contact_type
            property_id = "phone_number" if contact_type.lower() == "phone" else "email" if contact_type.lower() == "email" else None
            if not property_id:
                continue  # Skip if it's neither phone nor email

            # Determine if it's a primary contact based on contact_for
            is_primary = "Yes" if contact_for.lower() == "primary" else "No"

            # Property title will store contact_for value
            property_title = contact_for

            insert_contact_query = """
            INSERT INTO entities_for_demo_2.entity_property (entity_id, property_id, property_title, property_value, is_primary, position, created_by, updated_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor_write_db2.execute(insert_contact_query, (entity_last, property_id, property_title, contact_value, is_primary, index, created_by, updated_by))

            conn_db2.commit()
            print(f"done for record is {record_id}")
    except mysql.connector.Error as err:
        print(f"Error inserting contact properties for record {record_id}: {err}")


    # Close connections
    cursor_read_db1.close()
    cursor_read_db2.close()
    cursor_write_db2.close()
    conn_db1.close()
    conn_db2.close()

# Fetch all records and process them
conn_db1 = connect_db("dms_after_org")
df = pd.read_sql("SELECT id FROM global_organisations", conn_db1)
conn_db1.close()

for record_id in df["id"]:
    process_record(record_id)

print("Data transferred successfully.")
