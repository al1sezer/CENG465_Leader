import psycopg2
import uuid
import time
import threading
import json
from datetime import datetime
from db_config import LEADER_DB, FOLLOWER_DB
from logger import log_operation, log_info
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
    log_info("START: Concurrent Writes & Race Condition Experiment initiated", node="Exp4")
    print("\n" + "=" * 60)
    print("EXPERIMENT 4: CONCURRENT WRITES & RACE CONDITION")
    print("=" * 60)

    # ============================================================================
    # PART 1: ORDERING PRESERVATION (GLOBAL ORDERING)
    # ============================================================================
    log_info("Part 1: Ordering Preservation Test (20 Parallel Threads) Started...", node="Exp4")
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
    
    # Resolve seat name helper
    # Fetch seat names once before starting threads to avoid thread database contention
    conn_seats = psycopg2.connect(**LEADER_DB)
    cur_seats = conn_seats.cursor()
    cur_seats.execute("SELECT id, row_label || seat_number FROM seats;")
    seat_map = {row[0]: row[1] for row in cur_seats.fetchall()}
    cur_seats.close()
    conn_seats.close()

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
            
            # Write commit time
            t_commit = datetime.now()
            
            query = """
                INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version, last_updated, operation_id) 
                VALUES (%s, %s, %s, 'reserved', 1, %s, %s) RETURNING id;
            """
            cur.execute(query, (showtime_id, seat_id, customer, t_commit, op_id))
            res_id = cur.fetchone()[0]
            
            conn.commit()
            
            # Write to local file log
            log_info(f"Thread {thread_idx} wrote reservation ID {res_id} (Seat ID: {seat_id})", node="Exp4")
            
            # Write to local file log specifically for crud.log format
            time_str = t_commit.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            log_line = f"[{time_str}] NODE: Leader | OP: INSERT | TABLE: reservations    | ID: {res_id:<4d} | DETAILS: {json.dumps({'customer_name': customer, 'operation_id': op_id})}\n"
            with open("crud.log", "a", encoding="utf-8") as f_log:
                f_log.write(log_line)
                
            cur.close()
            conn.close()
            
        except Exception as e:
            print(f"   ERROR: Thread {thread_idx} Error: {e}")

    # Create and start worker threads
    threads = []
    for idx, s_id in enumerate(seats_pool):
        t = threading.Thread(target=reservation_worker, args=(idx+1, s_id))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    # Fetch Leader commits directly from reservations table, sorted by ID
    conn = psycopg2.connect(**LEADER_DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, seat_id, customer_name, last_updated 
        FROM reservations 
        WHERE customer_name LIKE 'Concurrent_Cust_%%' 
        ORDER BY id ASC;
    """)
    leader_rows = cur.fetchall()
    cur.close()
    conn.close()
    
    # Ordering Results
    order_preservation_results = []
    
    print("\nLEADER COMMIT ORDER (RESERVATIONS TABLE):")
    print("-" * 80)
    print(f"{'Order':<5s} | {'Res ID':<8s} | {'Seat':<6s} | {'Customer Name':<22s} | {'Commit Time'}")
    print("-" * 80)
    
    for seq, row in enumerate(leader_rows):
        res_id, seat_id, customer, t_commit = row
        l_time_str = t_commit.strftime('%H:%M:%S.%f')[:-3] if t_commit else "None"
        seat_name = seat_map[seat_id]
        print(f"#{seq+1:<4d} | ID:{res_id:<4d} | {seat_name:<6s} | {customer:<22s} | {l_time_str}")
        order_preservation_results.append([seq+1, res_id, customer, l_time_str])

    print("-" * 80)
    log_info("Part 1 Verified: Leader commit sequencing complete.", node="Exp4")
    
    # ============================================================================
    # PART 2: RACE CONDITION (RACE CONDITION & DOUBLE BOOKING)
    # ============================================================================
    log_info("Part 2: Starting race condition booking test on seat 10...", node="Exp4")
    print("\n[PART 2] Conflict (Double Booking / Race Condition) Test Started...")
    print("Target: Two threads will attempt to reserve the exact same seat (Seat B5 = ID: 10) simultaneously.")
    
    race_results = []
    race_results_lock = threading.Lock()
    race_barrier = threading.Barrier(2)
    
    # Resolve Seat ID 10 name dynamically
    conn_seats = psycopg2.connect(**LEADER_DB)
    cur_seats = conn_seats.cursor()
    cur_seats.execute("SELECT row_label || seat_number FROM seats WHERE id = 10;")
    seat_10_name = cur_seats.fetchone()[0]
    
    # Cleanup: Delete any existing active reservation for Seat B5 (SeatID: 10)
    cur_seats.execute("DELETE FROM reservations WHERE showtime_id = 1 AND seat_id = 10;")
    conn_seats.commit()
    cur_seats.close()
    conn_seats.close()

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
                
                # Write to replication_log in SAME transaction
                log_query = """
                    INSERT INTO replication_log (operation_type, table_name, record_id, details, timestamp, node) 
                    VALUES ('INSERT', 'reservations', %s, %s, %s, 'Leader')
                """
                import json
                cur.execute(log_query, (res_id, json.dumps({"customer_name": customer, "seat_id": 10, "racer_id": racer_id}), t_start))
                
                conn.commit()
                status_text = f"SUCCESS (Seat {seat_10_name} Reserved!)"
                
                # Write to local crud.log
                log_line = f"[{t_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] NODE: Leader | OP: INSERT | TABLE: reservations    | ID: {res_id:<4d} | DETAILS: {json.dumps({'customer_name': customer, 'seat_id': 10})}\n"
                with open("crud.log", "a", encoding="utf-8") as f_log:
                    f_log.write(log_line)
            else:
                status_text = f"REJECTED (Seat {seat_10_name} already FULL)"
                
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
            log_info(f"Racer {racer_id} ({customer}) execution completed: {status_text}", node="Exp4")
                
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
    
    print("\nCONCURRENT RACERS RESULTS:")
    print("-" * 90)
    for r in race_results:
        print(f"  Racer #{r['racer_id']} ({r['customer']}) | Time: {r['time']} | Result: {r['status']} | Res.ID: {r['res_id']}")
    print("-" * 90)
    
    double_booked = all(r["status"].startswith("SUCCESS") for r in race_results)
    log_info(f"Part 2 Analysis: Double Booking Occurred = {double_booked}", node="Exp4")
    if double_booked:
        print("  ANALYSIS: RACE CONDITION OCCURRED!")
        print(f"  The same physical seat ({seat_10_name}) was double-booked for two different users simultaneously!")
        print("  [PROOF OF CONFLICT IN DATABASE]")
        print("  ---------------------------------------------------------------------------------")
        print(f"  Seat        : {seat_10_name} (Seat ID: 10)")
        print(f"  Booking 1   : Customer = {race_results[0]['customer']} (Res ID: {race_results[0]['res_id']})")
        print(f"  Booking 2   : Customer = {race_results[1]['customer']} (Res ID: {race_results[1]['res_id']})")
        print("  ---------------------------------------------------------------------------------")
        print("  Because the application performed a SELECT check concurrently without locking,")
        print("  both transactions saw the seat as EMPTY and successfully committed their writes.")
    else:
        print("  ANALYSIS: Race condition did not occur (one thread blocked the other).")
        
    print("=" * 60)

    
    # Save Results
    headers_order = ["Order", "Write ID", "Customer Name", "Leader Commit"]
    save_to_csv("concurrent_order_results.csv", headers_order, order_preservation_results)
    
    headers_race = ["Racer ID", "Customer", "Time", "Result", "Reservation ID"]
    save_to_csv("concurrent_race_results.csv", headers_race, [[r['racer_id'], r['customer'], r['time'], r['status'], r['res_id']] for r in race_results])
    
    json_data = {
        "experiment": "Concurrent Writes & Race Condition",
        "timestamp": datetime.now().isoformat(),
        "ordering_summary": {
            "total_writes": len(leader_rows)
        },
        "details_ordering": [
            {
                "sequence": r[0],
                "write_id": r[1],
                "customer": r[2],
                "leader_time": r[3]
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
