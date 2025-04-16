# import pandas as pd
# import numpy as np
# import mysql.connector
# import pdb  
# import json 
# import os

# cnt_not_present=0
# cnt_isNull=0

# def connect_db(database):
#     return mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="oodles",
#         database=database
#     )

# def get_entity_details(dms_entity_id, entity_type, connection):
#     cursor = connection.cursor(buffered=True)
#     print(f"my dms_entity_id is {dms_entity_id}")

#     dms_entity_id = str(int(dms_entity_id)) # Convert to string for key lookup

#     if entity_type == 2:
#         file_path = 'records_entity_mapping_for_people.json'
#         if not os.path.exists(file_path):
#             raise FileNotFoundError(f"{file_path} not found.")
#         with open(file_path, 'r') as f:
#             data = json.load(f)
#         return data.get(dms_entity_id)

#     elif entity_type == 1:
#         file_paths = [
#             'records_entity_mapping_for_global_organisations_NOT_NULL.json',
#             'records_entity_mapping_for_global_organisations_NULL.json'
#         ]
#         for file_path in file_paths:
#             if not os.path.exists(file_path):
#                 continue
#             with open(file_path, 'r') as f:
#                 data = json.load(f)
#             if dms_entity_id in data:
#                 return data.get(dms_entity_id)

#     return None



# def get_country_code(dms_country_id, db1_cursor, db2_cursor):
#     """
#     Retrieves the country code from global_countries and checks if it exists in param_country.

#     Parameters:
#     - dms_country_id: ID of the country in global_countries.
#     - db1_cursor: Cursor for the first database connection.
#     - db2_cursor: Cursor for the second database connection.

#     Returns:
#     - country_code, country_name (str) if found in param_country, otherwise None.
#     """
#     try:
#         if not dms_country_id:
#             return None  # No country ID provided

#         # Query to get the country code
#         search_country_code_query = """
#             SELECT iso_code_alpha3, country_name FROM global_countries 
#             WHERE id = %s
#         """
#         db1_cursor.execute(search_country_code_query, (dms_country_id,))
#         result = db1_cursor.fetchone()

#         if not result or not result[0]:  # Check if a valid result was found
#             return None

#         country_code, country_name = result  # Extract country code and name

#         # Query to check if country exists in param_country
#         search_code_in_param_country_query = """
#             SELECT COUNT(*) FROM param_country WHERE country_id = %s;
#         """
#         db2_cursor.execute(search_code_in_param_country_query, (country_code,))
#         count = db2_cursor.fetchone()[0]  # Extract count value

#         return (country_code, country_name) if count >= 1 else None


#     except Exception as e:
#         print(f"Error: {e}")  # Better error handling
#         return None  # Return None in case of an error

# def process_record(index,dms_row):
#     conn_db1 = connect_db("dms_after_org")
#     conn_db2 = connect_db("entities_for_demo_2")

#     dms_entity_id, entity_type = dms_row['entity_id'], dms_row['entity_type']
    
#     if dms_entity_id is None:
#         print('dms_entity_id is NULL..')
#         global cnt_isNull
#         cnt_isNull +=1
#         return

#     db1_cursor = conn_db1.cursor()
#     db2_cursor = conn_db2.cursor()

#     entities_db_entity_id=get_entity_details(dms_entity_id,entity_type,conn_db2)
    
#     dms_address_1 = dms_row.get('address_1') or ""
#     dms_address_2 = dms_row['address_2']
#     dms_address_3 = dms_row['address_3']
#     dms_city = dms_row['city']
#     dms_state_county = dms_row['state_county']
#     dms_postal_code = dms_row['postal_code']
#     dms_country_id = dms_row['country_id']
#     dms_address_type=dms_row['address_type']
#     dms_created_by=dms_row['created_by']
#     dms_updated_by=dms_row['updated_by']
#     dms_deleted_at=dms_row['deleted_at']
    
#     country_code,country_name=get_country_code(dms_country_id, db1_cursor, db2_cursor)

#     if entities_db_entity_id:
#         insert_into_entity_address = """ 
#         INSERT INTO address (entity_id, line_one, line_two,city,
#         state,zipcode,country,country_code,address_type,created_by,updated_by,deleted_at) 
#         VALUES (%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """
#         concatenated_address2_3 = (dms_address_2 or "") + (dms_address_3 or "")

#         db2_cursor.execute(insert_into_entity_address, 
#                            (entities_db_entity_id, dms_address_1,
#                             concatenated_address2_3,dms_city,dms_state_county,
#                             dms_postal_code,country_name,country_code,dms_address_type,
#                             dms_created_by,dms_updated_by,dms_deleted_at))

#         conn_db2.commit()  # Commit the transaction

#         print(f"Address added for entities_db_entity_id {entities_db_entity_id}")
#     else:
#         print(f'No entity id present for dms_entity_id {dms_entity_id}')
#         global cnt_not_present
#         cnt_not_present+=1

#     db1_cursor.close()
#     db2_cursor.close()
#     conn_db1.close()
#     conn_db2.close()


# conn_db1 = connect_db("dms_after_org")
# df = pd.read_sql("SELECT entity_id, entity_type, address_1, address_2,address_3, city, state_county,country_id, postal_code,address_type , created_by, updated_by , deleted_at FROM dms_after_org.addresses", conn_db1)
# conn_db1.close()

# # Process each record
# for index, row in df.iterrows():
#     clean_row = row.replace({np.nan: None, "": None}).to_dict()  # Convert NaN & empty strings to None
#     process_record(index,clean_row)
    
# print(f'addresses entity_id not present count is {cnt_not_present}')
# print(f'addresses entity_id NULL count is {cnt_isNull}')



#     # if entity_type == 1:  # Organisation
#     #     search_in_global_organisation = """ 
#     #         SELECT organisation_name AS full_name, created_at, updated_at
#     #         FROM dmscopy.global_organisations
#     #         WHERE id = %s
#     #     """
#     #     cursor.execute(search_in_global_organisation, (dms_entity_id,))
#     #     result = cursor.fetchone()

#     #     if result:
#     #         full_name, created_at, updated_at = result
#     #         search_in_entity = """ 
#     #             SELECT entity_id 
#     #             FROM entity 
#     #             WHERE name = %s AND created_at = %s AND updated_at = %s
#     #         """
#     #         cursor.execute(search_in_entity, (full_name, created_at, updated_at))
#     #         entity_result = cursor.fetchone()

#     #         if entity_result:
#     #             return entity_result[0]

#     #     return None

#     # elif entity_type == 2:  # People
#     #     search_in_global_people = """ 
#     #         SELECT first_name, last_name, created_by, updated_by, created_at, updated_at, creator_ledger_id,
#     #                former_last_name, notes, ppsn_document_type, photo_url, pronounced, signature_attachment,
#     #                CRM_ID, exchange_ref_id, is_delete, import_people_name, leads_transactions_id, status_id, industry_id
#     #         FROM dmscopy.global_people 
#     #         WHERE id = %s
#     #     """
#     #     cursor.execute(search_in_global_people, (dms_entity_id,))
#     #     result = cursor.fetchone()

#     #     if result:
#     #         full_name = f"{result[0]} {result[1]}"  # first_name + last_name
#     #         created_by, updated_by = result[2], result[3]
#     #         created_at, updated_at = result[4], result[5]
#     #         creator_ledger_id = result[6]

#     #         # Extract properties into a dictionary
#     #         properties = [
#     #             "former_last_name", "notes", "ppsn_document_type", "photo_url", "pronounced", "signature_attachment",
#     #             "CRM_ID", "exchange_ref_id", "is_delete", "import_people_name", "leads_transactions_id", "status_id", "industry_id"
#     #         ]
#     #         property_dict = {prop: str(result[i + 7]) for i, prop in enumerate(properties)}
#     #         print(f"property dict: {property_dict}")

#     #         # Search in entity table
#     #         search_in_entity = """ 
#     #             SELECT entity_id 
#     #             FROM entity 
#     #             WHERE name = %s AND created_by = %s AND updated_by = %s 
#     #               AND created_at = %s AND updated_at = %s AND creator_ledger_id = %s
#     #         """
#     #         cursor.execute(search_in_entity, (full_name, created_by, updated_by, created_at, updated_at, creator_ledger_id))
#     #         entity_ids = [row[0] for row in cursor.fetchall()]
            
#     #         print(f"entity_ids : {entity_ids}")
            
#     #         if not entity_ids:
#     #             return None  # No matching entity found

#     #         print(f"entity_ids are : {entity_ids}")

#     #         # Check properties in entity_property table
#     #         for entity_id in entity_ids:
#     #             cursor.execute(
#     #                 "SELECT property_id, property_value FROM entities_final.entity_property WHERE entity_id = %s", 
#     #                 (entity_id,)
#     #             )
#     #             entity_properties = {
#     #                 row[0].upper() if row[0] == "crm_id" else row[0]: row[1] 
#     #                 for row in cursor.fetchall()
#     #             }

#     #             print(f"entity_properies: {entity_properties}")
#     #             # Compare only properties that exist in both sets
#     #             matching_properties = [prop for prop in property_dict if prop in entity_properties]

#     #             print(f"matching properties:{matching_properties}")

#     #             matching = {}
#     #             non_matching = {}

#     #             for prop in matching_properties:
#     #                 entity_value = entity_properties.get(prop)
#     #                 property_value = property_dict.get(prop)

#     #                 if entity_value == property_value:
#     #                     matching[prop] = entity_value
#     #                 else:
#     #                     non_matching[prop] = {"entity_value": entity_value, "property_value": property_value}

#     #             print(f"Matching properties: {matching}")
#     #             print(f"Non-matching properties: {non_matching}")

#     #             if not non_matching:  # All properties match
#     #                 print(f"Final entity_id is {entity_id}")
#     #                 return entity_id


#     #     return None  # No exact match found

#     # else:
#     #     print("Entity type should be 1 (Organisation) or 2 (People).")
#     #     return None



import pandas as pd
import numpy as np
import mysql.connector
import json
import os

cnt_not_present = 0
cnt_isNull = 0

def connect_db(database):
    
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password="oodles",
            database=database
        )
    except mysql.connector.Error as e:
        print(f"Database connection error: {e}")
        return None

def get_entity_details(dms_entity_id, entity_type, connection):
    try:
        cursor = connection.cursor(buffered=True)
        print(f"my dms_entity_id is {dms_entity_id}")

        dms_entity_id = str(int(dms_entity_id))  # Convert to string for key lookup

        if entity_type == 2:
            file_path = 'records_entity_mapping_for_people.json'
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"{file_path} not found.")
            with open(file_path, 'r') as f:
                data = json.load(f)
            return data.get(dms_entity_id)

        elif entity_type == 1:
            file_paths = [
                'records_entity_mapping_for_global_organisations_NOT_NULL.json',
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
    except FileNotFoundError as e:
        print(f"File error in get_entity_details: {e}")
        return None
    except Exception as e:
        print(f"Error in get_entity_details: {e}")
        return None

def get_country_code(dms_country_id, db1_cursor, db2_cursor):
    try:
        if not dms_country_id:
            return None, None  # Return None for both if no country ID is provided

        # Query to get the country code
        search_country_code_query = """
            SELECT iso_code_alpha3, country_name FROM global_countries 
            WHERE id = %s
        """
        db1_cursor.execute(search_country_code_query, (dms_country_id,))
        result = db1_cursor.fetchone()

        if not result or not result[0]:  # Check if a valid result was found
            return None, None

        country_code, country_name = result  # Extract country code and name

        # Query to check if country exists in param_country
        search_code_in_param_country_query = """
            SELECT COUNT(*) FROM param_country WHERE country_id = %s;
        """
        db2_cursor.execute(search_code_in_param_country_query, (country_code,))
        count = db2_cursor.fetchone()[0]  # Extract count value

        return (country_code, country_name) if count >= 1 else (None, None)

    except mysql.connector.Error as e:
        print(f"Database error in get_country_code: {e}")
        return None, None  # Return None for both if a database error occurs
    except Exception as e:
        print(f"Error in get_country_code: {e}")
        return None, None  # Return None for both if any other error occurs

def process_record(index, dms_row):
    try:
        conn_db1 = connect_db("dms_after_org")
        conn_db2 = connect_db("entities_for_demo_2")

        if conn_db1 is None or conn_db2 is None:
            return

        dms_entity_id, entity_type = dms_row['entity_id'], dms_row['entity_type']

        if dms_entity_id is None:
            print('dms_entity_id is NULL..')
            global cnt_isNull
            cnt_isNull += 1
            return

        db1_cursor = conn_db1.cursor()
        db2_cursor = conn_db2.cursor()

        entities_db_entity_id = get_entity_details(dms_entity_id, entity_type, conn_db2)

        dms_address_1 = dms_row.get('address_1') or ""
        dms_address_2 = dms_row['address_2']
        dms_address_3 = dms_row['address_3']
        dms_city = dms_row['city']
        dms_state_county = dms_row['state_county']
        dms_postal_code = dms_row['postal_code']
        dms_country_id = dms_row['country_id']
        dms_address_type = dms_row['address_type']
        dms_created_by = dms_row['created_by']
        dms_updated_by = dms_row['updated_by']
        dms_deleted_at = dms_row['deleted_at']

        # Get country code and name, handling cases where None is returned
        country_code, country_name = get_country_code(dms_country_id, db1_cursor, db2_cursor)
        if country_code is None or country_name is None:
            print('inside')
        # If country_code or country_name is None, they will be inserted as NULL into the database
        if entities_db_entity_id:
            insert_into_entity_address = """ 
            INSERT INTO address (entity_id, line_one, line_two, city,
            state, zipcode, country, country_code, address_type, created_by, updated_by, deleted_at) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            concatenated_address2_3 = (dms_address_2 or "") + (dms_address_3 or "")

            db2_cursor.execute(insert_into_entity_address, 
                               (entities_db_entity_id, dms_address_1,
                                concatenated_address2_3, dms_city, dms_state_county,
                                dms_postal_code, country_name, country_code, dms_address_type,
                                dms_created_by, dms_updated_by, dms_deleted_at))

            conn_db2.commit()  # Commit the transaction

            print(f"Address added for entities_db_entity_id {entities_db_entity_id}")
        else:
            print(f'No entity id present for dms_entity_id {dms_entity_id}')
            global cnt_not_present
            cnt_not_present += 1

        db1_cursor.close()
        db2_cursor.close()
        conn_db1.close()
        conn_db2.close()

    except Exception as e:
        print(f"Error processing record at index {index}: {e}")

# Connect to the first database and load the dataframe
conn_db1 = connect_db("dms_after_org")
df = pd.read_sql("SELECT entity_id, entity_type, address_1, address_2, address_3, city, state_county, country_id, postal_code, address_type, created_by, updated_by, deleted_at FROM dms_after_org.addresses where is_delete=0", conn_db1)
conn_db1.close()

# Process each record
for index, row in df.iterrows():
    clean_row = row.replace({np.nan: None, "": None}).to_dict()  # Convert NaN & empty strings to None
    process_record(index, clean_row)

print(f'Addresses entity_id not present count is {cnt_not_present}')
print(f'Addresses entity_id NULL count is {cnt_isNull}')

