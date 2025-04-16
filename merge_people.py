# import mysql.connector
# import pandas as pd
# from datetime import datetime

# db_config = {
#     "host": "localhost",
#     "user": "root",
#     "password": "oodles",
#     "database": "dmscopy"
# }

# def connect_db():
#     return mysql.connector.connect(**db_config)

# def merge_records(person1, person2, cursor):
    # cursor.execute("SHOW COLUMNS FROM global_people")
    # columns = [row[0] for row in cursor.fetchall() if row[0] not in ('id', 'is_delete', 'deleted_at')]
    # # print(columns)
    # cursor.execute(f"SELECT {', '.join(columns)} FROM global_people WHERE id = %s", (person1,))
    # data1 = cursor.fetchone()

    # cursor.execute(f"SELECT {', '.join(columns)} FROM global_people WHERE id = %s", (person2,))
    # data2 = cursor.fetchone()

    # update_values = [f"{col} = %s" for i, col in enumerate(columns) if data1[i] is None and data2[i] is not None]

    # print(f"person is {person1} and update values \n {update_values}")
    # if update_values:
    #     values_to_update = [data2[i] for i in range(len(columns)) if data1[i] is None and data2[i] is not None]
    #     print(values_to_update)
    #     if values_to_update:  # Ensure there are values before executing the update
    #         values_to_update.append(person1)
    #         cursor.execute(f"UPDATE global_people SET {', '.join(update_values)} WHERE id = %s", values_to_update)

    # deleted_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # cursor.execute("UPDATE global_people SET is_delete = 1, deleted_at = %s WHERE id = %s", (deleted_at, person2))

#     update_other_tables(person1, person2, cursor)

# def update_other_tables(person1, person2, cursor):
#     deleted_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#     cursor.execute("""                 # in contacts -- if email is contact type dont update -- as it person1 will have same email two times in db 
#         UPDATE global_entity_contacts  
#         SET entity_id = %s 
#         WHERE entity_id = %s AND contact_type != 'email' AND entity_type = 2
#     """, (person1, person2))

#     cursor.execute("""     # So when contact_type is email -- mark that as is_delete true
#         UPDATE global_entity_contacts  
#         SET is_delete = 1, deleted_at = %s 
#         WHERE entity_id = %s AND contact_type = 'email' AND entity_type = 2
#     """, (deleted_at, person2))

#     cursor.execute("""       # So when updating address -- if i update the entity_id then person will have two addresses 
#         UPDATE addresses       # instead mark it as is_delete True
#         SET is_delete = 1, deleted_at = %s 
#         WHERE entity_id = %s
#     """, (deleted_at, person2))

#     cursor.execute("""
#         SELECT entity_id FROM ledgers_role_mapping 
#         WHERE related_entity_id = %s AND entity_type = 1 AND related_entity_type=2
#     """, (person1,))
#     # change here in query 
#     person1_entities = {row[0] for row in cursor.fetchall()}

#     # Step 2: Fetch entity_ids for person2 (where related_entity_id = person2 and entity_type = 2)
#     cursor.execute("""
#         SELECT entity_id FROM ledgers_role_mapping 
#         WHERE related_entity_id = %s AND entity_type = 1 AND related_entity_type=2
#     """, (person2,))
#     person2_entities = {row[0] for row in cursor.fetchall()}

#     # Step 3: Find common entity_ids
#     common_entity_ids = person1_entities & person2_entities  # Intersection

#     # Step 4: Perform update where related_entity_id = person2 for common entity_ids
#     if common_entity_ids:
#         cursor.execute(f"""
#             UPDATE ledgers_role_mapping 
#             SET is_delete = 1, deleted_at = %s 
#             WHERE related_entity_id = %s 
#             AND entity_id IN ({','.join(map(str, common_entity_ids))})
#         """, (deleted_at, person2))


#     cursor.execute("""
#         UPDATE leads_connections 
#         SET entity_id = %s 
#         WHERE entity_id = %s AND entity_type = 2
#     """, (person1, person2))

#     cursor.execute("""
#         UPDATE leads_transactions 
#         SET full_name = %s 
#         WHERE full_name = %s
#     """, (person1,person2))

#     cursor.execute("""
#         UPDATE people_crm_ids 
#         SET people_id = %s 
#         WHERE people_id = %s
#     """, (person1, person2))

# def process_csv(csv_file):
#     df = pd.read_csv(csv_file)

#     connection = connect_db()
#     cursor = connection.cursor()

#     for _, row in df.iterrows():
#         person1, person2, status = int(row["Person1"]), int(row["Person2"]), row["Status"]

#         if status == "OK":
#             person1, person2 = min(person1, person2), max(person1, person2)  # Ensure person1 is the minimum ID
#             merge_records(person1, person2, cursor)

#     # connection.commit()
#     cursor.close()
#     connection.close()

# process_csv("to_merge_people.csv")


# --------------------------------------------------------------- # 
# --------------------------------------------------------------- # 
# --------------------------------------------------------------- # 

import pandas as pd
import mysql.connector
from datetime import datetime

def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database="dms_after_org"
    )

def merge_people_records(master_people_id, people_ids, cursor):

    other_people_ids = [pid for pid in people_ids if pid != master_people_id]

    if not other_people_ids:
        return  # Nothing to merge
    
    cursor.execute("SHOW COLUMNS FROM global_people")
    columns = [row[0] for row in cursor.fetchall() if row[0] not in ('id', 'is_delete', 'deleted_at')]

    cursor.execute(f"SELECT {', '.join(columns)} FROM global_people WHERE id = %s", (master_people_id,))
    master_people_data = list(cursor.fetchone())

    updates = {}
    treat_zero_as_null = ['leads_transactions_id', 'signature_attachment']

    for i, col in enumerate(columns):

        current_val = master_people_data[i]

        if (current_val is None) or (col in treat_zero_as_null and current_val == 0):
            for dup_id in reversed(other_people_ids):  # Check duplicates in reverse order
                cursor.execute(f"SELECT {col} FROM global_people WHERE id = %s", (dup_id,))
                val = cursor.fetchone()[0]
                if col in treat_zero_as_null and val==0:
                    continue 
                elif val is not None:
                    updates[col] = val
                    master_people_data[i] = val  
                    break

    if updates:
        print(f"in if updates\n {updates}")
        set_clause = ", ".join(f"{col} = %s" for col in updates)
        values = list(updates.values()) + [master_people_id]
        cursor.execute(f"UPDATE global_people SET {set_clause} WHERE id = %s", values)

    deleted_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # print(updates)
    for dup_id in other_people_ids:
        cursor.execute(
            "UPDATE global_people SET is_delete = 1, deleted_at = %s WHERE id = %s",
            (deleted_at, dup_id)
        )

    id_list_str = ",".join(map(str, other_people_ids))  # Format for SQL IN clause
    # this referes to other people -- but in format that can be used in SQL

    # Update global_entity_contacts (ignore email contact type)
    cursor.execute(f"""
        UPDATE global_entity_contacts  
        SET entity_id = %s 
        WHERE entity_id IN ({id_list_str}) AND contact_type != 'email' AND entity_type = 2
    """, (master_people_id,))

    cursor.execute(f"""
        UPDATE global_entity_contacts  
        SET is_delete = 1, deleted_at = %s 
        WHERE entity_id IN ({id_list_str}) AND contact_type = 'email' AND entity_type = 2
    """, (deleted_at,))


    # Mark addresses as deleted
    cursor.execute(f"""
        UPDATE addresses       
        SET is_delete = 1, deleted_at = %s 
        WHERE entity_id IN ({id_list_str})
    """, (deleted_at,))

    # Handle ledgers_role_mapping updates
    cursor.execute("""
        SELECT entity_id FROM ledgers_role_mapping 
        WHERE related_entity_id = %s AND entity_type = 1 AND related_entity_type = 2
    """, (master_people_id,))
    master_entities = {row[0] for row in cursor.fetchall()}

    cursor.execute(f"""
        SELECT entity_id FROM ledgers_role_mapping 
        WHERE related_entity_id IN ({id_list_str}) AND entity_type = 1 AND related_entity_type = 2
    """)
    other_entities = {row[0] for row in cursor.fetchall()}

    # Find common entity_ids
    common_entity_ids = master_entities & other_entities

    if common_entity_ids:
        cursor.execute(f"""
            UPDATE ledgers_role_mapping 
            SET is_delete = 1, deleted_at = %s 
            WHERE related_entity_id IN ({id_list_str}) 
            AND entity_id IN ({','.join(map(str, common_entity_ids))}) AND entity_type = 1 AND related_entity_type = 2
        """, (deleted_at,))

    extra_ids_in_other_entities = other_entities - master_entities
    if extra_ids_in_other_entities:
        print('in extra ids in other!!')
        cursor.execute(f"""
            UPDATE ledgers_role_mapping
            set realted_entity_id=%s
            where entity_id IN ({','.join(map(str, extra_ids_in_other_entities))}) AND entity_type=1 and related_entity_type=2
        """,(master_people_id,))

    # ------------------------------------------------------------------------------------------------------- # 
    
    # --------------------------- people should be in entity_id   ------------------------------------------ #

    cursor.execute("""
        SELECT related_entity_id FROM ledgers_role_mapping 
        WHERE entity_id = %s AND related_entity_type = 1 AND entity_type = 2
    """, (master_people_id,))
    master_entities = {row[0] for row in cursor.fetchall()}

    cursor.execute(f"""
        SELECT related_entity_id FROM ledgers_role_mapping 
        WHERE entity_id IN ({id_list_str}) AND related_entity_type = 1 AND entity_type = 2
    """)
    other_entities = {row[0] for row in cursor.fetchall()}

    common_entity_ids = master_entities & other_entities

    if common_entity_ids:
        cursor.execute(f"""
            UPDATE ledgers_role_mapping 
            SET is_delete = 1, deleted_at = %s 
            WHERE entity_id IN ({id_list_str}) 
            AND related_entity_id IN ({','.join(map(str, common_entity_ids))}) AND entity_type = 1=2 AND related_entity_type = 1
        """, (deleted_at,))

    extra_ids_in_other_entities = other_entities - master_entities
    if extra_ids_in_other_entities:
        print('in extra ids in other!!')
        cursor.execute(f"""
            UPDATE ledgers_role_mapping
            set entity_id=%s
            where related_entity_id IN ({','.join(map(str, extra_ids_in_other_entities))}) AND entity_type=2 and related_entity_type=1
        """,(master_people_id,))
    
    # --------------------------------------------------------------------------------------------------- # 




    # Update leads_transactions, people_crm_ids
    cursor.execute(f"""
        UPDATE leads_transactions 
        SET full_name = %s 
        WHERE full_name IN ({id_list_str})
    """, (master_people_id,))

    cursor.execute(f"""
        UPDATE people_crm_ids 
        SET people_id = %s 
        WHERE people_id IN ({id_list_str})
    """, (master_people_id,))

def update_leads_transaction_ids(master_people_id, other_people_ids, cursor):
    updateFields = [
        "deal_fields", "deal_fields_values", "deal_sum", "leads_connections",
        "leads_notes", "leads_sss", "leads_tags", "leads_tickets", "leads_transactions_contacts", "lock_notes"
    ]

    cursor.execute("SELECT leads_transactions_id FROM global_people WHERE id = %s", (master_people_id,))
    master_leads_transaction_id = cursor.fetchone()

    if master_leads_transaction_id:
        master_leads_transaction_id = master_leads_transaction_id[0]
        other_people_ids_str = ",".join(map(str, other_people_ids))

        for table in updateFields:

            if master_leads_transaction_id != 0:
                cursor.execute("SELECT id FROM leads_transactions WHERE id = %s", (master_leads_transaction_id,))
                if cursor.fetchone() is None:
                    print(f"[SKIP] {table}: master_leads_transaction_id {master_leads_transaction_id} does not exist in leads_transactions.")
                    continue

                update_query = f"""
                UPDATE {table} 
                SET leads_transactions_id = %s 
                WHERE leads_transactions_id IN ({other_people_ids_str})
                """
                cursor.execute(update_query, (master_leads_transaction_id,))

        key_lib_table_update_query = f"""
            UPDATE key_library 
            SET lead_transcation_id = %s 
            WHERE lead_transcation_id IN ({other_people_ids_str})
        """
        cursor.execute(key_lib_table_update_query, (master_leads_transaction_id,))

        cursor.execute(f"""
            SELECT SUM(investment_hours) FROM investment_hours 
            WHERE leads_transactions_id IN ({other_people_ids_str}, {master_leads_transaction_id})
        """)
        total_hours = cursor.fetchone()[0] or 0  

        cursor.execute("""
            UPDATE investment_hours 
            SET investment_hours = %s
            WHERE leads_transactions_id = %s
        """, (total_hours, master_leads_transaction_id))

        # Delete duplicate investment_hours records for other IDs
        cursor.execute(f"""
            DELETE FROM investment_hours 
            WHERE leads_transactions_id IN ({other_people_ids_str})
        """)

def process_csv(csv_file):
    df = pd.read_csv(csv_file)

    connection = connect_db()
    cursor = connection.cursor()

    for _, row in df.iterrows():
        try:
            email = row["email"]
            people_ids = list(map(lambda x: int(float(x)), row["people_id"].split(",")))  # Convert to float first, then int

            master_people_id = min(people_ids)  # Get smallest ID as master
            other_people_ids = [pid for pid in people_ids if pid != master_people_id]
            print(f"{master_people_id} is master and {other_people_ids} are others")
            # Merge all other people_ids into master_people_id in batch
            # if master_people_id== 52496:
            merge_people_records(master_people_id, people_ids, cursor)
                    # Update leads_transaction_id in relevant tables
            update_leads_transaction_ids(master_people_id, other_people_ids, cursor)
            connection.commit()
            
        except Exception as e:
            print(f"Error processing {row['email']}: {e}")
            # connection.rollback()

    connection.commit()  
    cursor.close()
    connection.close()

csv_file='filtered_output_merging_data.csv'
process_csv(csv_file)






