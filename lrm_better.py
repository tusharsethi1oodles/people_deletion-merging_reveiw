import pandas as pd
import mysql.connector
import json
import math 
import os 

def connect_db(database):
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='oodles',
        database=database
    )

def write_log(log_file, message):
    log_file.write(f"{message}\n")
    log_file.flush() 

def get_entity_details(dms_entity_id, entity_type,connection):
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
                    val=data.get(dms_entity_id)
                    print(f"val for {dms_entity_id} is {val}")
                    return data.get(dms_entity_id)

        return None
    except FileNotFoundError as e:
        print(f"File error in get_entity_details: {e}")
        return None
    except Exception as e:
        print(f"Error in get_entity_details: {e}")
        return None

    
def get_role_id_of_entities_db(dms_role_id, role_id_type, cursor1,cursor2,conn1,conn2,log_file):

    if not cursor1:
        write_log(log_file,'error..cursor1 is None, i am in get_role_id_of_entities_db!')
        return 
    if not cursor2:
        write_log(log_file,'error..cursor2 is None and i am in get_role_id_of_entities_db!')
        return
    
    """
    Retrieves the role ID from the role table based on the given dms_role_id and role_id_type.
    """
    # cursor = None
    try:
        # cursor = connection.cursor(buffered=True)

        table_name = "people_roles" if role_id_type == 2 else "organisations_roles"

        cursor1.execute(f"SELECT role_name FROM {table_name} WHERE id = %s", (dms_role_id,))
        role_name_result = cursor1.fetchone()

        if not role_name_result:
            write_log(log_file,f"No role found in {table_name} for ID {dms_role_id}")
            return None

        role_name = role_name_result[0]
        while cursor1.nextset(): pass

        # write_log(log_file,f"my role name is {role_name}")

        cursor2.execute("SELECT role_id FROM role WHERE name = %s", (role_name,))
        entity_role_result = cursor2.fetchone()

        while cursor2.nextset(): pass

        return entity_role_result[0] if entity_role_result else None

    except mysql.connector.Error as err:
        write_log(log_file,f"error.. in get_role_id_of_entities_db: {err}")
        return None


def insert_into_en_mapping(data_dict, cursor1,cursor2,conn1,conn2,log_file):

    if not cursor1:
        write_log(log_file,'error..cursor1 is None and i am in insert_into_en_roles')
        return
    if not cursor2:
        write_log(log_file,'error..cursor2 is None and i am in insert_into_en_roles')
        return
    if not conn1 or not conn1.is_connected():
        write_log(log_file,"error..conn1 is not connected!")
        return
    if not conn2 or not conn2.is_connected():
        write_log(log_file,"error..conn2 is not connected!")
        return
    
    # ---------------------------------------------------------------- # 

    """
    Inserts data into the entity_mapping table.
    - parent_id = organisation entity_id
    - entity_id = people entity_id
    """

    try:
        ledger_id = data_dict.get("ledger_id")
        entity_type = data_dict.get("entity_type")
        entity_id = data_dict.get("entity_id")
        role_id = data_dict.get("role_id")
        related_entity_type = data_dict.get("related_entity_type")
        related_entity_id = data_dict.get("related_entity_id")
        related_role_id = data_dict.get("related_role_id")
        exchange_ref_id = data_dict.get("exchange_ref_id")
        tag_ids = data_dict.get("tag_ids")
        created_by = data_dict.get("created_by")
        created_at = data_dict.get("created_at")
        updated_at = data_dict.get("updated_at")
        crm_id = data_dict.get("crm_id")
        supplier_insurance_no = data_dict.get("supplier_insurance_no")
        currency_id = data_dict.get("currency_id")
        agreed_early_payment_discount = data_dict.get("agreed_early_payment_discount")
        updated_by = data_dict.get("updated_by")
        deleted_at= data_dict.get("deleted_at")

        write_log(log_file,f"i am in insert_into_en_mapping !!")

        if entity_type == 1 and related_entity_type == 2:
            parent_id_in_mapping = get_entity_details(entity_id, entity_type, conn2)
            entity_id_in_mapping = get_entity_details(related_entity_id, related_entity_type, conn2)
        elif entity_type == 2 and related_entity_type == 1:
            parent_id_in_mapping = get_entity_details(related_entity_id, related_entity_type, conn2)
            entity_id_in_mapping = get_entity_details(entity_id, entity_type, conn2)

        if not entity_id_in_mapping:
            write_log(log_file,f"entity_id in mapping is NULL -- {entity_id_in_mapping}")
            return
        insertion_query = """
            INSERT INTO entity_mapping (
                parent_id, entity_id, title, is_primary,created_at,
                updated_at,created_by,updated_by,deleted_at
            ) 
            VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s)
        """

        write_log(log_file,f"parent id in mapping :{parent_id_in_mapping} entity_id in mapping:{entity_id_in_mapping}")

        cursor2.execute(insertion_query, (
            parent_id_in_mapping, 
            entity_id_in_mapping, 
            None,  # title column
            None,  # is_primary column
            created_at,
            updated_at,
            created_by,
            updated_by,
            deleted_at,
        ))

        write_log(log_file,f"insetion into entity_mapping done !")

        conn2.commit() 

    except mysql.connector.Error as err:
        write_log(log_file,f"error..Could not perform insertion in mapping, reason: {err}")
        conn1.rollback()
        conn2.rollback()  

def create_rel_entity_role_id(rel_en_db_id,rel_entity_type,dms_rel_role_id,cursor1,cursor2,conn1,conn2,log_file):
    if not cursor1:
        write_log(log_file,'error..cursor1 is None and i am in create_rel_entity_role_id')
        return
    if not cursor2:
        write_log(log_file,'error..cursor2 is None and i am in create_rel_entity_role_id')
        return
    
    try:
        rel_en_db_role_id=get_role_id_of_entities_db(dms_rel_role_id,rel_entity_type,cursor1,cursor2,conn1,conn2,log_file)
        if not rel_en_db_role_id:
            write_log(log_file,f"error..Could not find rel_en_db_role_id for rel_role_id {dms_rel_role_id} and rel_entity_type {rel_entity_type}")
            return None

        entity_role_id = f"{rel_entity_type}_{rel_en_db_id}_{rel_en_db_role_id}_{rel_en_db_id}"
        return entity_role_id

    except Exception as err:
        write_log(log_file,f"Error in create_rel_entity_role_id: {err}")
        return None 


def create_entity_role_id(en_db_id, entity_type, dms_role_id, rel_en_db_id,rel_entity_type,dms_rel_role_id,cursor1,cursor2,conn1,conn2,log_file):
    
    if not cursor1:
        write_log(log_file,'error..cursor1 is None and i am in create_entity_role_id')
        return
    if not cursor2:
        write_log(log_file,'error..cursor2 is None and i am in create_entity_role_id')
        return
    
    """
    Formula for creation: entity_role_id = <entity_type>_<entity_id>_<role_id>_<rel_entity_id>
    Note: < and > are to be ignored, they are just for understanding.
    """
    
    try:
        en_db_role_id = get_role_id_of_entities_db(dms_role_id, entity_type, cursor1,cursor2,conn1,conn2,log_file)

        if not en_db_role_id:
            write_log(log_file,f"error..Could not find en_db_role_id for role_id {dms_role_id} and entity_type {entity_type}")
            return None
        
        rel_en_db_role_id=get_role_id_of_entities_db(dms_rel_role_id,rel_entity_type,cursor1,cursor2,conn1,conn2,log_file)
        if not rel_en_db_role_id:
            write_log(log_file,f"error..Could not find rel_en_db_role_id for rel_role_id {dms_rel_role_id} and rel_entity_type {rel_entity_type}")
            return None

        entity_role_id = f"{entity_type}_{en_db_id}_{en_db_role_id}_{rel_en_db_id}"
        return entity_role_id

    except Exception as err:
        write_log(log_file,f"Error in create_entity_role_id: {err}")
        return None  

def insert_into_en_roles(data_dict, cursor1,cursor2,conn1,conn2,log_file):
    if not cursor1:
        write_log(log_file,'error..cursor1 is None and i am in insert_into_en_roles')
        return
    if not cursor2:
        write_log(log_file,'error..cursor2 is None and i am in insert_into_en_roles')
        return
    if not conn1 or not conn1.is_connected():
        write_log(log_file,"error..conn1 is not connected!")
        return
    if not conn2 or not conn2.is_connected():
        write_log(log_file,"error..conn2 is not connected!")
        return
    
    write_log(log_file,f"i am in insert_into_en_roles")

    """ 
    Inserts entity-role mappings into the `entity_role` table based on `ledger_role_mapping`.
    - `entity_id` and `related_entity_id` are inserted as `entity_id`.
    - Generates `entity_role_id` using a formula.
    - Maps `related_entity_id`'s `entity_role_id` to `related_role_id`.
    """

    entity_id = data_dict.get("entity_id")
    entity_type = data_dict.get("entity_type")
    role_id = data_dict.get("role_id")
    related_entity_id = data_dict.get("related_entity_id")
    related_entity_type = data_dict.get("related_entity_type")
    related_role_id = data_dict.get("related_role_id")
    ledger_id = data_dict.get("ledger_id")
    exchange_ref_id = data_dict.get("exchange_ref_id")
    tag_ids = data_dict.get("tag_ids")
    created_by = data_dict.get("created_by")
    created_at = data_dict.get("created_at")
    updated_at = data_dict.get("updated_at")
    crm_id = data_dict.get("crm_id")
    supplier_insurance_no = data_dict.get("supplier_insurance_no")
    currency_id = data_dict.get("currency_id")
    agreed_early_payment_discount = data_dict.get("agreed_early_payment_discount")
    updated_by = data_dict.get("updated_by")

    try:
        lrm_entity_id_to_en_db_id = get_entity_details(entity_id, entity_type, conn2)
        lrm_rel_entity_id_to_en_db_id = get_entity_details(related_entity_id, related_entity_type, conn2)

        en_role_id_of_lrm_entity_id = get_role_id_of_entities_db(role_id, entity_type, cursor1,cursor2,conn1,conn2,log_file)
        en_role_id_of_lrm_rel_entity_id = get_role_id_of_entities_db(related_role_id, related_entity_type, cursor1,cursor2,conn1,conn2,log_file)

        entity_role_id_of_lrm_entity_id = create_entity_role_id(lrm_entity_id_to_en_db_id, entity_type, role_id,lrm_rel_entity_id_to_en_db_id,related_entity_type,related_role_id,cursor1,cursor2,conn1,conn2,log_file)
        entity_role_id_of_lrm_rel_entity_id = create_rel_entity_role_id(lrm_rel_entity_id_to_en_db_id, related_entity_type,related_role_id,cursor1,cursor2,conn1,conn2,log_file)

        if lrm_entity_id_to_en_db_id is None or lrm_rel_entity_id_to_en_db_id is None:
            write_log(log_file, 'got lrm_id_to_en_db_ids as None!!')
            return
        
        if currency_id is not None and not (isinstance(currency_id, float) and math.isnan(currency_id)):
            try:
                currency_id_int = int(currency_id)
            except (ValueError, TypeError):
                currency_id_int = None # or some default integer value
        else:
            currency_id_int = None

        extra_data = json.dumps({
            "exchange_ref_id": exchange_ref_id, # varchar
            "tag_ids": tag_ids, # varchar
            "crm_id": crm_id, # varchar
            "supplier_insurance_no": supplier_insurance_no, #varcahr
            "currency_id": currency_id_int, #bigint
            "agreed_early_payment_discount": agreed_early_payment_discount, #varchar
        })
        

        en_roles_insertion_query_for_entity_id = """
            INSERT INTO entities_for_demo_2.entity_role(entity_role_id, entity_id, role_id,extra_data)
            VALUES (%s, %s, %s,%s)
        """
        en_roles_insertion_query_for_rel_entity_id = """
            INSERT INTO entities_for_demo_2.entity_role(entity_role_id, entity_id, role_id)
            VALUES (%s, %s, %s)
        """
        
        check_dup_for_rel="""
            select entity_role_id from entities_for_demo_2.entity_role where 
            entity_role_id=%s
        """

        update_query_for_entity_id = """
            UPDATE entities_for_demo_2.entity_role
            SET related_role_id = %s
            WHERE entity_id = %s AND entity_role_id = %s
        """

        cursor2.execute(en_roles_insertion_query_for_entity_id, 
                       (entity_role_id_of_lrm_entity_id, lrm_entity_id_to_en_db_id, en_role_id_of_lrm_entity_id,extra_data))
        write_log(log_file,f"insertion done for entity id with formula : {entity_role_id_of_lrm_entity_id}")

        cursor2.execute(check_dup_for_rel,(entity_role_id_of_lrm_rel_entity_id,))
        result_for_dup_rel=cursor2.fetchone()

        if result_for_dup_rel is not None:
            cursor2.execute(update_query_for_entity_id,(entity_role_id_of_lrm_rel_entity_id,lrm_entity_id_to_en_db_id,entity_role_id_of_lrm_entity_id))
        else:
            cursor2.execute(en_roles_insertion_query_for_rel_entity_id, 
                       (entity_role_id_of_lrm_rel_entity_id, lrm_rel_entity_id_to_en_db_id, en_role_id_of_lrm_rel_entity_id))
            cursor2.execute(update_query_for_entity_id,(entity_role_id_of_lrm_rel_entity_id,lrm_entity_id_to_en_db_id,entity_role_id_of_lrm_entity_id))

        conn2.commit()

        write_log(log_file,f"insetion into entity_roles done !")

    except mysql.connector.Error as err:
        write_log(log_file,f"error..Could not perform insertion in roles, reason: {err}")
        # write_log(log_file,f"entity_id related to en_roles table insertion error is :{entity_id}")
        conn1.rollback()
        conn2.rollback()  

# --------------------------------------------------------------------- #    
def perform_insertion(row,cursor1,cursor2,conn1,conn2,log_file):
    try:
        # print('in func perform insertion')
        if not cursor1:
            write_log(log_file,'error..cursor1 is None and i am in perform_insertion')
            return
        if not cursor2:
            write_log(log_file,'error..cursor2 is None and i am in perform_insertion')
            return
        
        if not conn1 or not conn1.is_connected():
            write_log(log_file,"error.. conn1 is not connected!")
            return
        if not conn2 or not conn2.is_connected():
            write_log(log_file,"error.. conn2 is not connected!")
            return

        """
            entity_mapping table will have : 
            (as of now)

            parent_id , entity_id, title, is_primary 
            (title and is_primary columns will be NULL)
            ledger_id ( always 1 ) 
            exchange_ref_ids.
            curreny_id
            tag_ids ( data is always NULL) 
            crm_id ( 1 row data only in my db)
            supplier_insurance_no ( 1 row data only in my db)
            agreed_early_payment_discount ( 1 row data only in my db)
            created_by 
            updated_by 
            created_at
            updated_at
        """    
        # ---------------------------------------------------------------- #
        data_dict = {
            "ledger_id": row['ledger_id'],
            "entity_type": row['entity_type'],
            "entity_id": row['entity_id'],
            "role_id": row['role_id'],
            "related_entity_type": row['related_entity_type'],
            "related_entity_id": row['related_entity_id'],
            "related_role_id": row['related_role_id'],
            "exchange_ref_id": row['exchange_ref_id'],
            "tag_ids": row['tag_ids'],
            "created_by": row['created_by'],
            "created_at": row['created_at'],
            "updated_at": row['updated_at'],
            "crm_id": row['crm_id'],
            "supplier_insurance_no": row['supplier_insurance_no'],
            "currency_id": row['currency_id'],
            "agreed_early_payment_discount": row['agreed_early_payment_discount'],
            "updated_by": row['updated_by'],
        }


        role_id= int(data_dict.get('role_id') or 0)
        related_role_id=int(data_dict.get('related_role_id') or 0)

        write_log(log_file,f"role id is: {role_id} and related_role_id is {related_role_id}")
        write_log(log_file,f"this is role id's type: {type(role_id)} and role id is{role_id}")

        if role_id==44 or related_role_id==44:
            print('good--in role id if case for mapping ')
            insert_into_en_mapping(data_dict,cursor1,cursor2,conn1,conn2,log_file)
        elif role_id==45 or related_role_id==45:
            print('inside of 45 role/related role id')
            insert_into_en_mapping(data_dict,cursor1,cursor2,conn1,conn2,log_file)
        else:
            insert_into_en_roles(data_dict,cursor1,cursor2,conn1,conn2,log_file)
    except Exception as e:
        write_log(log_file,f"in perform insertion error{e}")


def main():
    conn1 = connect_db('dms_after_org')
    conn2 = connect_db('entities_for_demo_2')
    cursor1 = conn1.cursor(buffered=True)
    cursor2 = conn2.cursor(buffered=True)

    with open("insertion_log16.txt", "w") as log_file:
        if not conn1 or not conn2:
            write_log(log_file,'could not connect to db!!')
            return
        try:
            df = pd.read_sql("SELECT * FROM ledgers_role_mapping", conn1)
            # print(df[df['role_id'] == 44])

            for _, row in df.iterrows():
                print(row['role_id'])
                role_id = row['role_id']

                try:
                    # print("role_id:", row['role_id'])  # Track it
                    perform_insertion(row, cursor1, cursor2, conn1, conn2, log_file)
                except Exception as e:
                    write_log(log_file, f"Error in row with role_id={row['role_id']}: {e}")


                # print("role_id", type(role_id), role_id)
                # if role_id == 44:
                #     print('in role id 44')
                #     write_log(log_file,"Found 44 inside loop:")
                
                # write_log(log_file,"\n")
                # perform_insertion(row, cursor1,cursor2,conn1,conn2,log_file) 

            conn1.commit()
            conn2.commit()  # Commit after processing all rows

        except Exception as err:
            write_log(log_file,f"error..processing data: {err}")
        finally:
            cursor1.close()
            cursor2.close()
            conn1.close()
            conn2.close()

if __name__ == "__main__":
    main()


    # if entity_type == 1:  # Organisation
    #     search_in_global_organisation = """ 
    #         SELECT organisation_name AS full_name, created_at, updated_at
    #         FROM dmscopy.global_organisations
    #         WHERE id = %s
    #     """
    #     cursor1.execute(search_in_global_organisation, (dms_entity_id,))
    #     result = cursor1.fetchone()

    #     if result:

    #         write_log(log_file,f"result in global_organisation {result}")
    #         full_name, created_at, updated_at = result
    #         search_in_entity = """ 
    #             SELECT entity_id 
    #             FROM entity 
    #             WHERE name = %s AND created_at = %s AND updated_at = %s
    #         """
    #         cursor2.execute(search_in_entity, (full_name, created_at, updated_at))
    #         entity_result = cursor2.fetchone()

    #         if entity_result:
    #             write_log(log_file,f"found organisation {entity_result}\n")
    #             return entity_result[0]
            
    #     write_log(log_file,f"error..could not find organisation.")
    #     return None

    # elif entity_type == 2:  # People
    #     search_in_global_people = """ 
    #         SELECT first_name, last_name, created_by, updated_by, created_at, updated_at, creator_ledger_id,
    #                former_last_name, notes, ppsn_document_type, photo_url, pronounced, signature_attachment,
    #                CRM_ID, exchange_ref_id, is_delete, import_people_name, leads_transactions_id, status_id, industry_id
    #         FROM dmscopy.global_people 
    #         WHERE id = %s
    #     """
    #     cursor1.execute(search_in_global_people, (dms_entity_id,))
    #     result = cursor1.fetchone()

    #     if result:
    #         write_log(log_file,f"in result{result}")
    #         full_name = f"{result[0] or ''} {result[1] or ''}".strip()  # first_name + last_name
    #         created_by, updated_by = result[2], result[3]
    #         created_at, updated_at = result[4], result[5]
    #         creator_ledger_id = result[6]


    #         write_log(log_file,f"full_name={full_name} , created_by={created_by}, updated_by={updated_by}, created_at={created_at}, updated_at={updated_at}, creator_ledger_id={creator_ledger_id}")

    #         # Extract properties into a dictionary
    #         properties = [
    #             "former_last_name", "notes", "ppsn_document_type", "photo_url", "pronounced", "signature_attachment",
    #             "CRM_ID", "exchange_ref_id", "is_delete", "import_people_name", "leads_transactions_id", "status_id", "industry_id"
    #         ]
    #         property_dict = {prop: str(result[i + 7]) for i, prop in enumerate(properties)}
    #         write_log(log_file,f"property dict: {property_dict}")

    #         # Search in entity table
    #         search_in_entity = """ 
    #             SELECT entity_id 
    #             FROM entity 
    #             WHERE name = %s AND created_by = %s AND updated_by = %s 
    #               AND created_at = %s AND updated_at = %s AND creator_ledger_id = %s
    #         """
    #         cursor2.execute(search_in_entity, (full_name, created_by, updated_by, created_at, updated_at, creator_ledger_id))
    #         entity_ids = [row[0] for row in cursor2.fetchall()]
            
    #         write_log(log_file,f"entity_ids : {entity_ids}")
            
    #         if not entity_ids:
    #             return None  # No matching entity found

    #         write_log(log_file,f"entity_ids are : {entity_ids}")

    #         # Check properties in entity_property table
    #         for entity_id in entity_ids:
    #             cursor2.execute(
    #                 "SELECT property_id, property_value FROM entitysample4.entity_property WHERE entity_id = %s", 
    #                 (entity_id,)
    #             )
    #             entity_properties = {
    #                 row[0].upper() if row[0] == "crm_id" else row[0]: row[1] 
    #                 for row in cursor2.fetchall()
    #             }

    #             write_log(log_file,f"entity_properies: {entity_properties}")
    #             # Compare only properties that exist in both sets
    #             matching_properties = [prop for prop in property_dict if prop in entity_properties]

    #             write_log(log_file,f"matching properties:{matching_properties}")

    #             matching = {}
    #             non_matching = {}

    #             for prop in matching_properties:
    #                 entity_value = entity_properties.get(prop)
    #                 property_value = property_dict.get(prop)

    #                 if entity_value == property_value:
    #                     matching[prop] = entity_value
    #                 else:
    #                     non_matching[prop] = {"entity_value": entity_value, "property_value": property_value}

    #             write_log(log_file,f"Matching properties: {matching}")
    #             write_log(log_file,f"Non-matching properties: {non_matching}")

    #             if not non_matching:  # All properties match
    #                 write_log(log_file,f"Final entity_id is {entity_id}")
    #                 return entity_id
        
    #     write_log(log_file,f"error..could not find people!")
    #     return None 
    # else:
    #     write_log(log_file,"Entity type should be 1 (Organisation) or 2 (People).")
    #     return None