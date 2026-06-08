import os
import shutil
import psycopg2
from db_config import LEADER_DB

def reset_database_and_logs():
    print("=" * 60)
    print("COMPREHENSIVE SYSTEM RESET & INITIALIZATION")
    print("=" * 60)
    
    # 1. Reset PostgreSQL tables and load seed data
    print("Connecting to Leader database to reset tables...")
    try:
        conn = psycopg2.connect(**LEADER_DB)
        conn.autocommit = True
        cur = conn.cursor()
        
        print("Reading cinema_schema.sql...")
        with open("cinema_schema.sql", "r", encoding="utf-8") as f:
            sql_script = f.read()
            
        print("Executing schema and seeding main tables...")
        cur.execute(sql_script)
        
        # Truncate replication_log table to keep it completely empty as requested
        print("Truncating replication_log table (clearing DB logs)...")
        cur.execute("TRUNCATE TABLE replication_log RESTART IDENTITY CASCADE;")
        
        print("Database tables reset and main tables populated with clean sample data!")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"ERROR: Error resetting database: {e}")

    # 2. Clear crud.log file
    crud_log_path = "crud.log"
    print(f"Truncating {crud_log_path}...")
    try:
        with open(crud_log_path, "w", encoding="utf-8") as f:
            f.write("")  # Clear contents
        print(f"{crud_log_path} successfully cleared.")
    except Exception as e:
        print(f"ERROR: Error clearing {crud_log_path}: {e}")

    # 3. Clean results directory
    results_dir = "results"
    print(f"Clearing old results files in {results_dir}...")
    try:
        if os.path.exists(results_dir):
            for filename in os.listdir(results_dir):
                file_path = os.path.join(results_dir, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f"   WARNING: Failed to delete {file_path}: {e}")
            print(f"{results_dir} folder successfully cleared.")
        else:
            os.makedirs(results_dir)
            print(f"Created clean {results_dir} folder.")
    except Exception as e:
        print(f"ERROR: Error clearing results directory: {e}")

    print("=" * 60)
    print("SYSTEM SUCCESSFULLY RESET TO CLEAN SEEDED STATE!")
    print("=" * 60)

if __name__ == "__main__":
    reset_database_and_logs()
