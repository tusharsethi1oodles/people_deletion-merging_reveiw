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
    
    insert_entity_query = """
    INSERT INTO entities_for_demo_2.entity (creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, type, name)
    SELECT 
        creator_ledger_id, 
        created_by, 
        updated_by, 
        created_at, 
        updated_at, 
        deleted_at, 
        1, 
        organisation_name
    FROM dms_after_org.global_organisations
    WHERE id = %s AND parent_organisation_id IS NULL;
    """
    cursor_db2.execute(insert_entity_query, (record_id,))
    entity_last = cursor_db2.lastrowid 
    conn_db2.commit()  # Commit to save changes

    json_file_path = "records_entity_mapping_for_global_organisations_NULL.json"
    if os.path.exists(json_file_path):
        with open(json_file_path, "r") as f:
            data = json.load(f)
    else:
        data = {}

    data[str(record_id)] = entity_last

    with open(json_file_path, "w") as f:
        json.dump(data, f, indent=4)

    properties = [
        "fy_start_month", "fy_end_month", "website", "registration_number", "vat_number", "registered_country_id",
        "logo_url", "currency_id", "description", "nace_section_id", "revenue_access_number", "hierarchy_code",
        "crm_id", "exchange_ref_id", "leads_transactions_id", "status_id", "industry_id", "trade_name"
    ]

    for prop in properties:
        # Check if property exists in the property table
        cursor_db2.execute("SELECT property_id FROM entities_for_demo_2.property WHERE property_id = %s", (prop,))
        if cursor_db2.fetchone():
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
            cursor_db2.execute(insert_property_query, (entity_last, record_id))

    # Commit property insertions
    conn_db2.commit()

    try:
        cursor_db2.execute("""
            SELECT go.created_by, go.updated_by, gec.contact_type, gec.contact_for, gec.contact_value
            FROM dms_after_org.global_entity_contacts gec
            JOIN dms_after_org.global_organisations go ON go.id = gec.entity_id
            WHERE gec.entity_type = 1 AND go.id = %s;
        """, (record_id,))

        contacts = cursor_db2.fetchall()

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
            cursor_db2.execute(insert_contact_query, (entity_last, property_id, property_title, contact_value, is_primary, index, created_by, updated_by))
            conn_db2.commit()
            print(f"done for record id {record_id}")
    except mysql.connector.Error as err:
        print(f"Error inserting contact properties for record {record_id}: {err}")

    
    # Close connections
    cursor_db1.close()
    cursor_db2.close()
    conn_db1.close()
    conn_db2.close()

# Fetch all records where parent_organisation_id is NULL and process them
conn_db1 = connect_db("dms_after_org")
df = pd.read_sql("SELECT id FROM global_organisations WHERE parent_organisation_id IS NULL", conn_db1)
conn_db1.close()

for record_id in df["id"]:
    process_record(record_id)

print("Data transferred successfully.")

