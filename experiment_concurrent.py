import psycopg2
import uuid
import time
import threading
from datetime import datetime
from db_config import LEADER_DB, FOLLOWER_DB
from logger import log_operation
from utils import save_to_csv, save_to_json, print_table

def run_concurrent_writes_experiment():
    """
    Concurrent Writes Experiment (Ordering and Conflict Analysis):
    Part 1 - Ordering Preservation (Global Ordering): 20 parallel threads send rapid booking requests to the Leader.
             The commit sequence of each write on the Leader is compared with its replication sequence (WAL sequence)
             on the Follower to analyze if any out-of-order replication occurs.
    
    Part 2 - Race Condition (Race Condition & Double Booking): Two parallel threads attempt to reserve the same seat
             in the same showtime (Hall A, Seat B5 = SeatID: 10) at the exact same millisecond. Demonstrates how
             application-level checks (select-then-insert) without locking or database-level constraints lead to Double Booking.
    """
    print("\n" + "=" * 60)
    print("🧪 EXPERIMENT 4: CONCURRENT WRITES & RACE CONDITION")
    print("=" * 60)

    # ============================================================================
    # PART 1: ORDERING PRESERVATION (GLOBAL ORDERING)
    # ============================================================================
    print("\n[PART 1] Ordering Preservation Test (20 Parallel Threads) Started...")
    showtime_id = 1
    
    # Ensure showtime with ID 1 exists dynamically (as showtimes table is empty initially)
    conn_prep = psycopg2.connect(**LEADER_DB)
    cur_prep = conn_prep.cursor()
    cur_prep.execute("SELECT id FROM showtimes WHERE id = %s;", (showtime_id,))
    if not cur_prep.fetchone():
        print(f"Creating showtime {showtime_id} dynamically for the concurrent writes experiment...")
        cur_prep.execute("INSERT INTO showtimes (id, movie_id, hall_id, show_date, show_time) VALUES (1, 1, 1, '2026-06-01', '14:00') ON CONFLICT (id) DO NOTHING;")
        conn_prep.commit()
    cur_prep.close()
    conn_prep.close()
    # Use Seat IDs 13 to 32 (other seats in Hall A)
    seats_pool = list(range(13, 33))
    
    leader_commits = []
    leader_commits_lock = threading.Lock()
    
    barrier = threading.Barrier(20)  # Barrier to start all threads simultaneously
    
    def reservation_worker(thread_idx, seat_id):
        customer = f"Concurrent_Cust_{thread_idx}"
        op_id = str(uuid.uuid4())
        
        # Hold threads at the barrier so they start simultaneously
        barrier.wait()
        
        try:
            conn = psycopg2.connect(**LEADER_DB)
            cur = conn.cursor()
            
            # Write start time
            t_start = datetime.now()
            
            query = """
                INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version, last_updated, operation_id) 
                VALUES (%s, %s, %s, 'reserved', 1, %s, %s) RETURNING id;
            """
            cur.execute(query, (showtime_id, seat_id, customer, t_start, op_id))
            res_id = cur.fetchone()[0]
            conn.commit()
            
            # Write completion (commit) time
            t_commit = datetime.now()
            
            log_operation("INSERT", "reservations", res_id, {
                "customer_name": customer,
                "operation_id": op_id
            })
            
            cur.close()
            conn.close()
            
            with leader_commits_lock:
                leader_commits.append({
                    "thread": thread_idx,
                    "res_id": res_id,
                    "seat_id": seat_id,
                    "customer": customer,
                    "start_time": t_start,
                    "commit_time": t_commit,
                    "op_id": op_id
                })
        except Exception as e:
            print(f"   ❌ Thread {thread_idx} Error: {e}")

    # Create and start worker threads
    threads = []
    for idx, s_id in enumerate(seats_pool):
        t = threading.Thread(target=reservation_worker, args=(idx+1, s_id))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    # Sort Leader commits by commit timestamp
    leader_commits.sort(key=lambda x: x["commit_time"])
    
    # Wait 3 seconds for replication
    print("Operations completed on the Leader. Waiting 3 seconds for replication...")
    time.sleep(3)
    
    # Fetch log sequence from replication_log on Follower
    conn_f = psycopg2.connect(**FOLLOWER_DB)
    cur_f = conn_f.cursor()
    
    # Fetch logs only for records we added
    query_f = """
        SELECT record_id, timestamp, details->>'customer_name' 
        FROM replication_log 
        WHERE table_name = 'reservations' AND details->>'customer_name' LIKE 'Concurrent_Cust_%%'
        ORDER BY log_id ASC;
    """
    cur_f.execute(query_f)
    follower_logs = cur_f.fetchall()
    
    cur_f.close()
    conn_f.close()
    
    # Ordering Comparison Analysis
    order_preservation_results = []
    out_of_order_count = 0
    
    print("\n📊 LEADER COMMIT ORDER vs FOLLOWER REPLICATION LOG ORDER:")
    print("-" * 105)
    print(f"{'Order':<5s} | {'Write ID':<8s} | {'Customer Name':<22s} | {'Leader Commit':<22s} | {'Follower Log':<22s} | {'Status'}")
    print("-" * 105)
    
    for seq, l_item in enumerate(leader_commits):
        # Find this record_id in the Follower logs and get its sequence
        f_seq = -1
        f_time = None
        for fs, f_item in enumerate(follower_logs):
            if f_item[0] == l_item["res_id"]:
                f_seq = fs
                f_time = f_item[1]
                break
                
        status = "IN ORDER"
        if f_seq != seq:
            status = f"DIFFERENT (F_Order: {f_seq})"
            out_of_order_count += 1
            
        l_time_str = l_item["commit_time"].strftime('%H:%M:%S.%f')[:-3]
        f_time_str = f_time.strftime('%H:%M:%S.%f')[:-3] if f_time else "NOT FOUND"
        
        print(f"#{seq+1:<4d} | ID:{l_item['res_id']:<4d} | {l_item['customer']:<22s} | {l_time_str:<22s} | {f_time_str:<22s} | {status}")
        order_preservation_results.append([seq+1, l_item['res_id'], l_item['customer'], l_time_str, f_time_str, status])

    print("-" * 105)
    print(f"Ordering Violation Count (Out-of-Order Replication): {out_of_order_count} (Expected: 0 - Because WAL is applied sequentially in a single thread!)")
    
    # ============================================================================
    # PART 2: RACE CONDITION (RACE CONDITION & DOUBLE BOOKING)
    # ============================================================================
    print("\n[PART 2] Conflict (Double Booking / Race Condition) Test Started...")
    print("Target: Two threads will attempt to reserve the exact same seat (Seat B5 = ID: 10) simultaneously.")
    
    race_results = []
    race_results_lock = threading.Lock()
    race_barrier = threading.Barrier(2)
    
    # Cleanup: Delete any existing active reservation for Seat B5
    conn_clean = psycopg2.connect(**LEADER_DB)
    cur_clean = conn_clean.cursor()
    cur_clean.execute("DELETE FROM reservations WHERE showtime_id = 1 AND seat_id = 10;")
    conn_clean.commit()
    cur_clean.close()
    conn_clean.close()

    def booking_racer(racer_id):
        customer = f"Racer_Client_{racer_id}"
        op_id = str(uuid.uuid4())
        
        # Barrier to start exactly simultaneously
        race_barrier.wait()
        t_start = datetime.now()
        
        try:
            conn = psycopg2.connect(**LEADER_DB)
            cur = conn.cursor()
            
            # Application-level conflict check (Select-then-Insert)
            # This query can return "empty" for both threads simultaneously!
            cur.execute("""
                SELECT id FROM reservations 
                WHERE showtime_id = 1 AND seat_id = 10 AND status = 'reserved';
            """)
            existing = cur.fetchone()
            
            # Adding a simulated delay here to trigger the race condition (50ms)
            time.sleep(0.05)
            
            status_text = ""
            res_id = None
            
            if existing is None:
                # Seat appeared empty, record the reservation
                cur.execute("""
                    INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version, last_updated, operation_id) 
                    VALUES (1, 10, %s, 'reserved', 1, %s, %s) RETURNING id;
                """, (customer, t_start, op_id))
                res_id = cur.fetchone()[0]
                conn.commit()
                status_text = "SUCCESS (Reservation Made)"
                
                log_operation("INSERT", "reservations", res_id, {
                    "customer_name": customer,
                    "seat_id": 10,
                    "racer_id": racer_id
                })
            else:
                status_text = "REJECTED (Seat Full)"
                
            cur.close()
            conn.close()
            
            with race_results_lock:
                race_results.append({
                    "racer_id": racer_id,
                    "customer": customer,
                    "time": t_start.strftime('%H:%M:%S.%f')[:-3],
                    "status": status_text,
                    "res_id": res_id
                })
                
        except Exception as e:
            with race_results_lock:
                race_results.append({
                    "racer_id": racer_id,
                    "customer": customer,
                    "time": t_start.strftime('%H:%M:%S.%f')[:-3],
                    "status": f"ERROR ({str(e)})",
                    "res_id": None
                })
 
    # Run the two racing threads
    t1 = threading.Thread(target=booking_racer, args=(1,))
    t2 = threading.Thread(target=booking_racer, args=(2,))
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    
    print("\n📊 RACE RESULTS:")
    print("-" * 75)
    for r in race_results:
        print(f"  Racer #{r['racer_id']} ({r['customer']}) | Time: {r['time']} | Result: {r['status']} | Res.ID: {r['res_id']}")
    print("-" * 75)
    
    double_booked = all(r["status"].startswith("SUCCESS") for r in race_results)
    if double_booked:
        print("  ⚠️ ANALYSIS: RACE CONDITION OCCURRED!")
        print("  The same seat was reserved for two different users simultaneously (Double Booking!).")
        print("  Because the SELECT check in the application layer was done concurrently, and no")
        print("  unique constraint or SELECT FOR UPDATE lock was used at the database level.")
    else:
        print("  ✅ ANALYSIS: Race condition did not occur (one thread blocked the other).")
        
    print("=" * 60)
    
    # Save Results
    headers_order = ["Order", "Write ID", "Customer Name", "Leader Commit", "Follower Log", "Status"]
    save_to_csv("concurrent_order_results.csv", headers_order, order_preservation_results)
    
    headers_race = ["Racer ID", "Customer", "Time", "Result", "Reservation ID"]
    save_to_csv("concurrent_race_results.csv", headers_race, [[r['racer_id'], r['customer'], r['time'], r['status'], r['res_id']] for r in race_results])
    
    json_data = {
        "experiment": "Concurrent Writes & Race Condition",
        "timestamp": datetime.now().isoformat(),
        "ordering_summary": {
            "total_writes": len(leader_commits),
            "out_of_order_count": out_of_order_count
        },
        "details_ordering": [
            {
                "sequence": r[0],
                "write_id": r[1],
                "customer": r[2],
                "leader_time": r[3],
                "follower_time": r[4],
                "status": r[5]
            } for r in order_preservation_results
        ],
        "race_condition_summary": {
            "double_booking_occurred": double_booked,
            "racers": race_results
        }
    }
    save_to_json("concurrent_results.json", json_data)

if __name__ == "__main__":
    run_concurrent_writes_experiment()
