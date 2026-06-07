import psycopg2
import json
from datetime import datetime
from db_config import LEADER_DB

def log_operation(operation_type, table_name, record_id, details=None, node="Leader"):
    """
    Writes logs to both replication_log table in database and local crud.log file
    with millisecond precision.
    """
    if details is None:
        details = {}
        
    try:
        timestamp = datetime.now()
        
        # 1. Write to replication_log table in the database
        conn = psycopg2.connect(**LEADER_DB)
        cur = conn.cursor()
        
        query = """
            INSERT INTO replication_log (operation_type, table_name, record_id, details, timestamp, node) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        # Send details parameter as a JSON string (automatically converted for PostgreSQL JSONB)
        cur.execute(query, (operation_type, table_name, record_id, json.dumps(details), timestamp, node))
        
        conn.commit()
        cur.close()
        conn.close()

        # 2. Write to local crud.log file
        time_str = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        log_line = f"[{time_str}] NODE: {node} | OP: {operation_type:6s} | TABLE: {table_name:15s} | ID: {record_id:<4d} | DETAILS: {json.dumps(details)}\n"
        
        with open("crud.log", "a", encoding="utf-8") as f:
            f.write(log_line)
            
    except Exception as e:
        print(f"Log error: {e}")
        # Even if DB connection fails, write to the local log file with error status
        try:
            timestamp = datetime.now()
            time_str = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            log_line = f"[{time_str}] NODE: {node:6s} | OP: {operation_type:6s} | TABLE: {table_name:15s} | ID: {record_id:<4d} | DETAILS: {json.dumps(details)} [DB LOG FAILED: {str(e)}]\n"
            with open("crud.log", "a", encoding="utf-8") as f:
                f.write(log_line)
        except Exception as file_err:
            print(f"File log error: {file_err}")

def log_info(message, node="System"):
    """
    Appends a simple info log message directly to local crud.log file.
    Useful for showing real-time experiment progress in the UI dashboard.
    """
    try:
        timestamp = datetime.now()
        time_str = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        log_line = f"[{time_str}] NODE: {node:6s} | INFO: {message}\n"
        with open("crud.log", "a", encoding="utf-8") as f:
            f.write(log_line)
    except Exception as e:
        print(f"Log info error: {e}")