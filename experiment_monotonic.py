import psycopg2
import uuid
import time
import threading
from datetime import datetime
from db_config import LEADER_DB, FOLLOWER_DB
from logger import log_operation, log_info
from utils import save_to_csv, save_to_json, print_table

def run_monotonic_reads_experiment():
    """
    Monotonic Reads Experiment:
    1. Single Node Read (Follower Only): Verifies that the version never goes back when sequential reads are made from a single follower.
    2. Multi-Node Read (Leader -> Follower): Demonstrates how a client that first reads from the up-to-date Leader and immediately after
       from a lagged Follower violates the 'Monotonic Reads' rule by seeing an older version of the data.
    """
    log_info("START: Monotonic Reads Experiment initiated", node="Exp2")
    print("\n" + "=" * 60)
    print("🧪 EXPERIMENT 2: MONOTONIC READS (Single Node vs Multi-Node Reads)")
    print("=" * 60)

    # Let's add a new movie and showtime for the experiment
    movie_title = f"Exp2_Monotonic_Movie_{int(time.time())}"
    
    # 1. Preparation: Add movie and showtime
    conn_l = psycopg2.connect(**LEADER_DB)
    cur_l = conn_l.cursor()
    
    cur_l.execute("INSERT INTO movies (title, genre, duration_min) VALUES (%s, 'Sci-Fi', 150) RETURNING id;", (movie_title,))
    movie_id = cur_l.fetchone()[0]
    
    cur_l.execute("INSERT INTO showtimes (movie_id, hall_id, show_date, show_time) VALUES (%s, 1, '2026-06-05', '20:00') RETURNING id;", (movie_id,))
    showtime_id = cur_l.fetchone()[0]
    
    # Create reservation (Seat B1 = ID: 6)
    cur_l.execute("INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version) VALUES (%s, 6, 'Client V0', 'reserved', 1) RETURNING id;", (showtime_id,))
    res_id = cur_l.fetchone()[0]
    
    conn_l.commit()
    cur_l.close()
    conn_l.close()
    
    print(f"Preparation complete. Reservation ID: {res_id} (Seat B1) created.")
    print("Starting 10 rapid updates on the Leader. Concurrent reads will be performed on the Follower...")
    log_info(f"Writer thread initialized. Starting rapid updates on reservation ID: {res_id}...", node="Exp2")
    
    follower_reads_only = []  # Reads from Follower only
    cross_node_reads = []      # Cross-node reads (Leader -> Follower)
    
    # Thread synchronization lock
    stop_event = threading.Event()
    
    # 2. Background Writer Thread: Performs 10 updates on the Leader
    def writer_thread():
        conn = psycopg2.connect(**LEADER_DB)
        cur = conn.cursor()
        for version in range(2, 12):
            op_id = str(uuid.uuid4())
            ts = datetime.now()
            query = """
                UPDATE reservations 
                SET customer_name = %s, version = %s, last_updated = %s, operation_id = %s 
                WHERE id = %s;
            """
            cur.execute(query, (f"Client V{version}", version, ts, op_id, res_id))
            conn.commit()
            
            # Log record
            log_operation("UPDATE", "reservations", res_id, {
                "customer_name": f"Client V{version}",
                "version": version
            })
            log_info(f"Writer: Updated reservation {res_id} version to v{version} on Leader", node="Exp2")
            
            # Rapid sequential updates (100ms interval)
            time.sleep(0.1)
            
        cur.close()
        conn.close()
        stop_event.set()

    # Start the background writer thread
    w_t = threading.Thread(target=writer_thread)
    w_t.start()
    
    # 3. Client Read Simulation
    conn_f = psycopg2.connect(**FOLLOWER_DB)
    cur_f = conn_f.cursor()
    
    conn_l_read = psycopg2.connect(**LEADER_DB)
    cur_l_read = conn_l_read.cursor()
    
    read_count = 0
    last_seen_follower_version = 0
    
    while not stop_event.is_set() or read_count < 30:
        read_count += 1
        t_now = datetime.now()
        
        # A. Read only from the Follower (Single Node Read)
        cur_f.execute("SELECT version, customer_name FROM reservations WHERE id = %s;", (res_id,))
        row_f = cur_f.fetchone()
        f_version = row_f[0] if row_f else 0
        f_name = row_f[1] if row_f else "None"
        
        # Monotonic violation check: Is it smaller than the previously seen version?
        violation_follower = "VIOLATION" if f_version < last_seen_follower_version else "NORMAL"
        
        follower_reads_only.append([
            read_count, 
            t_now.strftime('%H:%M:%S.%f')[:-3], 
            f_version, 
            f_name, 
            last_seen_follower_version, 
            violation_follower
        ])
        
        last_seen_follower_version = max(last_seen_follower_version, f_version)
        
        # B. Leader -> Follower Cross-Node Read Simulation
        # First read from the Leader
        cur_l_read.execute("SELECT version FROM reservations WHERE id = %s;", (res_id,))
        row_l = cur_l_read.fetchone()
        l_version = row_l[0] if row_l else 0
        
        # Immediately after, read from the Follower
        cur_f.execute("SELECT version FROM reservations WHERE id = %s;", (res_id,))
        row_f_immediate = cur_f.fetchone()
        f_immediate_version = row_f_immediate[0] if row_f_immediate else 0
        
        # Violation check: Is the version read from Leader greater than that of the Follower?
        # (Meaning, did the client read stale data from a lagged server after seeing up-to-date data?)
        violation_cross = "VIOLATION (Monotonic Reads Violation!)" if f_immediate_version < l_version else "NORMAL"
        if f_immediate_version < l_version:
            log_info(f"Violation Detected: Leader v{l_version} but Follower immediate read returned v{f_immediate_version} (stale!)", node="Exp2")
        
        cross_node_reads.append([
            read_count, 
            t_now.strftime('%H:%M:%S.%f')[:-3], 
            l_version, 
            f_immediate_version, 
            violation_cross
        ])
        
        # Very small wait between reads (50ms)
        time.sleep(0.05)
        
    w_t.join()
    
    cur_f.close()
    conn_f.close()
    cur_l_read.close()
    conn_l_read.close()
    
    # 4. Printing Results in Table Format
    print("\n📊 TEST A: SEQUENTIAL READS ONLY FROM FOLLOWER (Single Node)")
    print("Expected: Versions should continuously increase or stay constant. No backward reads (violations) should occur.")
    headers_a = ["Read No", "Time", "Read Version (F)", "Data Content", "Previous Version", "Status"]
    print_table(headers_a, follower_reads_only[:15]) # Print first 15 records
    
    print("\n" + "📊 TEST B: SEQUENTIAL CROSS-NODE READS (LEADER -> FOLLOWER) (Multi-Node)")
    print("Expected: Immediately after reading the latest version from Leader, an older version may be read from the lagged Follower (VIOLATION).")
    headers_b = ["Read No", "Time", "Leader Version", "Follower Version", "Status"]
    print_table(headers_b, cross_node_reads[:15]) # Print first 15 records
    
    # Statistics
    follower_violations = sum(1 for r in follower_reads_only if r[5] == "VIOLATION")
    cross_violations = sum(1 for r in cross_node_reads if "VIOLATION" in r[4])
    
    log_info(f"SUMMARY: Single-node violations: {follower_violations}, Multi-node violations: {cross_violations}", node="Exp2")
    print("=" * 60)
    print("📊 SUMMARY STATISTICS:")
    print(f"  Single Node (Follower Only) Violation Count  : {follower_violations} (Expected: 0)")
    print(f"  Multi-Node (Leader -> Follower) Violation Count: {cross_violations} (Expected: > 0)")
    print("=" * 60)
    
    # Save Results
    save_to_csv("monotonic_results_single.csv", headers_a, follower_reads_only)
    save_to_csv("monotonic_results_cross.csv", headers_b, cross_node_reads)
    
    json_data = {
        "experiment": "Monotonic Reads",
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "single_node_violations": follower_violations,
            "cross_node_violations": cross_violations
        },

        "details_single": [
            {
                "read_no": r[0],
                "time": r[1],
                "version": r[2],
                "content": r[3],
                "prev_version": r[4],
                "status": r[5]
            } for r in follower_reads_only
        ],
        "details_cross": [
            {
                "read_no": r[0],
                "time": r[1],
                "leader_version": r[2],
                "follower_version": r[3],
                "status": r[4]
            } for r in cross_node_reads
        ]
    }
    save_to_json("monotonic_results.json", json_data)

if __name__ == "__main__":
    run_monotonic_reads_experiment()
