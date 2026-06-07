import psycopg2
import uuid
import time
from datetime import datetime
from db_config import LEADER_DB

from logger import log_operation

def execute_query(query, params=(), fetch_id=False):
    conn = psycopg2.connect(**LEADER_DB)
    cur = conn.cursor()
    cur.execute(query, params)
    
    returned_id = None
    if fetch_id:
        returned_id = cur.fetchone()[0]
        
    conn.commit()
    cur.close()
    conn.close()
    return returned_id

def insert_data(name, value):
    operation_id = str(uuid.uuid4())
    timestamp = datetime.now()
    version = 1
    
    query = """
        INSERT INTO data (name, value, version, last_updated, operation_id) 
        VALUES (%s, %s, %s, %s, %s) RETURNING id;
    """
    record_id = execute_query(query, (name, value, version, timestamp, operation_id), fetch_id=True)
    log_operation("INSERT", record_id)
    print(f"INSERTED: {name} (ID: {record_id}, Version: {version})")
    return record_id

def update_data(record_id, new_value):
    operation_id = str(uuid.uuid4())
    timestamp = datetime.now()
    
    query = """
        UPDATE data 
        SET value = %s, version = version + 1, last_updated = %s, operation_id = %s 
        WHERE id = %s RETURNING id;
    """
    execute_query(query, (new_value, timestamp, operation_id, record_id), fetch_id=True)
    log_operation("UPDATE", record_id)
    
    # Güncel versiyonu doğrudan veritabanından çekerek kesin kanıt sunuyoruz
    conn = psycopg2.connect(**LEADER_DB)
    cur = conn.cursor()
    cur.execute("SELECT version FROM data WHERE id = %s;", (record_id,))
    actual_version = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    print(f"UPDATED: TestValue_1 (ID: {record_id}, Version: {actual_version}, Last Updated: {timestamp}, Operation ID: {operation_id})")

def delete_data(record_id):
    query = "DELETE FROM data WHERE id = %s RETURNING id;"
    execute_query(query, (record_id,), fetch_id=True)
    log_operation("DELETE", record_id)
    print(f"DELETED: TestValue_1 (ID: {record_id})")

if __name__ == "__main__":
    print("--- ROADMAP TEST STARTING ---")
    
    new_id = insert_data("TestValue_1", "CENG1")
    input("INSERT 1 completed, to pass UPDATE, press ENTER...")

    update_data(new_id, "CENG465")
    input("UPDATE completed, to pass DELETE, press ENTER...")

    update_data(new_id, "CENG465-Updated")
    input("UPDATE 2 completed, to pass DELETE, press ENTER...")

    delete_data(new_id)
    input("DELETE completed, to exit, press ENTER...")
    
    insert_data("TestValue_4", "CENG4")
    input("INSERT 2 completed, to pass INSERT 3, press ENTER...")
    
    insert_data("TestValue_5", "CENG5")
    print("INSERT 3 completed")
    
    print("Operations completed and logged.")
