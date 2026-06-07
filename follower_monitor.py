import psycopg2
import time
from datetime import datetime
from db_config import LEADER_DB, FOLLOWER_DB

# ============================================================================
# COMMON DB CONNECTION HELPERS
# ============================================================================

def execute_follower_query(query, params=(), fetch_all=False, fetch_one=False):
    """
    Executes a read-only query on the Follower DB.
    """
    conn = psycopg2.connect(**FOLLOWER_DB)
    cur = conn.cursor()
    cur.execute(query, params)
    
    result = None
    if fetch_all:
        result = cur.fetchall()
    elif fetch_one:
        result = cur.fetchone()
        
    cur.close()
    conn.close()
    return result

def execute_leader_query(query, params=(), fetch_one=True):
    """
    Executes a query on the Leader DB for comparison.
    """
    conn = psycopg2.connect(**LEADER_DB)
    cur = conn.cursor()
    cur.execute(query, params)
    result = cur.fetchone() if fetch_one else cur.fetchall()
    cur.close()
    conn.close()
    return result

# ============================================================================
# READ FUNCTIONS (Read-Only on Follower)
# ============================================================================

def read_all_movies():
    """
    Lists all movies in the Follower database.
    """
    query = "SELECT id, title, genre, duration_min, version, last_updated FROM movies ORDER BY id;"
    rows = execute_follower_query(query, fetch_all=True)
    
    print("\n🎬 [FOLLOWER] ACTIVE MOVIES LIST:")
    print("-" * 75)
    for r in rows:
        print(f"  ID: {r[0]:<2d} | Movie: {r[1]:<30s} | Genre: {r[2]:<10s} | Duration: {r[3]} min | v{r[4]} | Last Updated: {r[5]}")
    print("-" * 75)
    return rows

def read_reservations(showtime_id):
    """
    Lists reservations in the Follower for a specific showtime.
    """
    query = """
        SELECT r.id, m.title, h.name, s.row_label || s.seat_number, r.customer_name, r.status, r.version
        FROM reservations r 
        JOIN showtimes st ON r.showtime_id = st.id 
        JOIN movies m ON st.movie_id = m.id
        JOIN halls h ON st.hall_id = h.id
        JOIN seats s ON r.seat_id = s.id
        WHERE r.showtime_id = %s
        ORDER BY r.id;
    """
    rows = execute_follower_query(query, (showtime_id,), fetch_all=True)
    
    print(f"\n🎫 [FOLLOWER] SHOWTIME RESERVATIONS (Showtime ID: {showtime_id}):")
    print("-" * 75)
    if not rows:
        print("  There are no reservations for this showtime yet.")
    for r in rows:
        print(f"  ResID: {r[0]:<2d} | Movie: {r[1]:<20s} | Hall: {r[2]} | Seat: {r[3]} | Customer: {r[4]:<15s} | Status: {r[5]:<10s} | v{r[6]}")
    print("-" * 75)
    return rows

def read_record_version(table_name, record_id):
    """
    Reads the version, last_updated, and operation_id of a specific record in the Follower.
    """
    # Check table name to prevent SQL injection
    allowed_tables = ['movies', 'halls', 'seats', 'showtimes', 'reservations', 'replication_log']
    if table_name not in allowed_tables:
        raise ValueError("Invalid table name!")
        
    query = f"SELECT version, last_updated, operation_id FROM {table_name} WHERE id = %s;"
    result = execute_follower_query(query, (record_id,), fetch_one=True)
    return result

# ============================================================================
# MONITORING & DISCREPANCY ANALYSIS (Replication Lag & Verification)
# ============================================================================

def watch_replication_log(interval=2, duration=30):
    """
    Monitors the replication log table live at specified intervals.
    """
    print(f"\n📋 [FOLLOWER] REPLICATION LOG LIVE TRACKING STARTED (For {duration} seconds)...")
    print("-" * 80)
    print(f"{'Log ID':<8s} | {'Operation':<9s} | {'Table':<15s} | {'Rec ID':<8s} | {'Node':<10s} | {'Timestamp'}")
    print("-" * 80)
    
    start_time = time.time()
    seen_ids = set()
    
    # Fetch existing logs first to print only new ones
    initial_query = "SELECT log_id FROM replication_log;"
    for r in execute_follower_query(initial_query, fetch_all=True):
        seen_ids.add(r[0])
        
    while time.time() - start_time < duration:
        query = "SELECT log_id, operation_type, table_name, record_id, node, timestamp FROM replication_log ORDER BY log_id DESC LIMIT 10;"
        rows = execute_follower_query(query, fetch_all=True)
        
        # Print new logs from oldest to newest
        for r in reversed(rows):
            if r[0] not in seen_ids:
                print(f"#{r[0]:<7d} | {r[1]:<9s} | {r[2]:<15s} | RecID:{r[3]:<4d} | {r[4]:<10s} | {r[5]}")
                seen_ids.add(r[0])
                
        time.sleep(interval)
    print("-" * 80)
    print("📋 Live tracking completed.")

def compare_with_leader(table_name, record_id):
    """
    Compares a specific record on Leader and Follower to display replication status.
    """
    allowed_tables = ['movies', 'halls', 'seats', 'showtimes', 'reservations']
    if table_name not in allowed_tables:
        raise ValueError("Invalid table name!")
        
    leader_query = f"SELECT version, last_updated, operation_id FROM {table_name} WHERE id = %s;"
    
    try:
        leader_res = execute_leader_query(leader_query, (record_id,), fetch_one=True)
    except Exception as e:
        print(f"⚠️ Leader connection error: {e}")
        return None
        
    follower_res = read_record_version(table_name, record_id)
    
    print(f"\n🔍 [DATA COMPARISON] Table: {table_name} | ID: {record_id}")
    print("-" * 75)
    
    if not leader_res:
        print("  Leader: Record not found.")
    else:
        print(f"  Leader   -> Version: {leader_res[0]} | Last Updated: {leader_res[1]} | OpID: {leader_res[2]}")
        
    if not follower_res:
        print("  Follower -> Record not found. (May not have replicated yet)")
    else:
        print(f"  Follower -> Version: {follower_res[0]} | Last Updated: {follower_res[1]} | OpID: {follower_res[2]}")
        
    print("-" * 75)
    
    if leader_res and follower_res:
        in_sync = (leader_res[0] == follower_res[0]) and (leader_res[2] == follower_res[2])
        if in_sync:
            print("  ✅ STATUS: IN SYNC (Leader and Follower data are completely identical!)")
        else:
            print("  ⚠️ STATUS: SYNC LAG DETECTED (Versions or UUID do not match!)")
        return in_sync
    return False

def measure_replication_lag(table_name, record_id):
    """
    Measures the millisecond difference between a record being updated on the Leader and appearing on the Follower.
    """
    print(f"\n⏱️ [LAG MEASUREMENT] Table: {table_name} | ID: {record_id} - Monitoring replication lag...")
    
    # Fetch write timestamp from Leader
    leader_query = f"SELECT last_updated FROM {table_name} WHERE id = %s;"
    try:
        leader_time = execute_leader_query(leader_query, (record_id,), fetch_one=True)[0]
    except Exception as e:
        print(f"⚠️ Could not read timestamp from Leader: {e}")
        return None
        
    print(f"  1. Leader Write Time: {leader_time}")
    
    # Loop until the data is in sync on Follower or timeout occurs (10 seconds)
    start_poll = time.time()
    replicated = False
    follower_time = None
    
    while time.time() - start_poll < 10:
        res = read_record_version(table_name, record_id)
        if res:
            follower_time = res[1]
            if follower_time >= leader_time:
                replicated = True
                break
        time.sleep(0.01)  # 10ms wait for precision
        
    if replicated:
        lag = (follower_time - leader_time).total_seconds() * 1000.0
        print(f"  2. Follower Appearance Time: {follower_time}")
        print(f"  🎯 Replication Lag: {lag:.2f} milliseconds (ms)")
        return lag
    else:
        print("  ❌ Data did not replicate or timestamp was not updated within 10 seconds.")
        return None

# ============================================================================
# INTERACTIVE MENU (For running easily on Follower VM)
# ============================================================================
if __name__ == "__main__":
    while True:
        print("\n" + "=" * 60)
        print("🔄 CENG 465 - FOLLOWER READ AND MONITORING SCREEN")
        print("=" * 60)
        print("  1. List All Movies (Read-Only)")
        print("  2. Show Reservations for a Showtime")
        print("  3. Watch Replication Log Live (30 seconds)")
        print("  4. Compare Data with Leader (Sync Analysis)")
        print("  5. Measure Replication Lag")
        print("  0. Exit")
        print("=" * 60)
        
        choice = input("Your choice [0-5]: ")
        
        if choice == '1':
            read_all_movies()
        elif choice == '2':
            st_id = input("Please enter Showtime ID: ")
            try:
                read_reservations(int(st_id))
            except ValueError:
                print("Please enter a valid number!")
        elif choice == '3':
            watch_replication_log()
        elif choice == '4':
            table = input("Table name (movies / reservations): ")
            rec_id = input("Record ID: ")
            try:
                compare_with_leader(table, int(rec_id))
            except ValueError as e:
                print(f"Error: {e}")
        elif choice == '5':
            table = input("Table name (movies / reservations): ")
            rec_id = input("Record ID: ")
            try:
                measure_replication_lag(table, int(rec_id))
            except ValueError as e:
                print(f"Error: {e}")
        elif choice == '0':
            print("Exiting...")
            break
        else:
            print("Invalid choice!")
            
        input("\nPress ENTER to continue...")
