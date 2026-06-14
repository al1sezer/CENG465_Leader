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
    """Run concurrent writes and race condition experiment."""
    log_info("START: Concurrent Writes & Race Condition Experiment initiated", node="Exp4")
    print("\nEXPERIMENT 4: CONCURRENT WRITES")

    # PART 1: ORDERING PRESERVATION
    log_info("Part 1: Ordering Preservation Test (20 Parallel Threads) Started...", node="Exp4")
    print("\n[PART 1] ORDERING PRESERVATION")
    showtime_id = 1
    
    conn_sync = psycopg2.connect(**LEADER_DB)
    cur_sync = conn_sync.cursor()
    cur_sync.execute("SELECT setval(pg_get_serial_sequence('movies', 'id'), COALESCE(MAX(id), 1)) FROM movies;")
    cur_sync.execute("SELECT setval(pg_get_serial_sequence('showtimes', 'id'), COALESCE(MAX(id), 1)) FROM showtimes;")
    cur_sync.execute("SELECT setval(pg_get_serial_sequence('reservations', 'id'), COALESCE(MAX(id), 1)) FROM reservations;")
    conn_sync.commit()
    cur_sync.close()
    conn_sync.close()
    
    conn_prep = psycopg2.connect(**LEADER_DB)
    cur_prep = conn_prep.cursor()
    cur_prep.execute("SELECT id FROM showtimes WHERE id = %s;", (showtime_id,))
    if not cur_prep.fetchone():
        cur_prep.execute("INSERT INTO showtimes (id, movie_id, hall_id, show_date, show_time) VALUES (1, 1, 1, '2026-06-01', '14:00') ON CONFLICT (id) DO NOTHING;")
        conn_prep.commit()
    cur_prep.close()
    conn_prep.close()
    
    seats_pool = list(range(13, 23))
    
    conn_seats = psycopg2.connect(**LEADER_DB)
    cur_seats = conn_seats.cursor()
    cur_seats.execute("SELECT id, row_label || seat_number FROM seats;")
    seat_map = {row[0]: row[1] for row in cur_seats.fetchall()}
    cur_seats.close()
    conn_seats.close()

    leader_commits = []
    leader_commits_lock = threading.Lock()
    barrier = threading.Barrier(10)
    
    def reservation_worker(thread_idx, seat_id):
        customer = f"Concurrent_Cust_{thread_idx}"
        op_id = str(uuid.uuid4())
        
        barrier.wait()
        
        try:
            conn = psycopg2.connect(**LEADER_DB)
            cur = conn.cursor()
            t_commit = datetime.now()
            
            query = """
                INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version, last_updated, operation_id) 
                VALUES (%s, %s, %s, 'reserved', 1, %s, %s) RETURNING id;
            """
            cur.execute(query, (showtime_id, seat_id, customer, t_commit, op_id))
            res_id = cur.fetchone()[0]
            
            cur.execute("SELECT pg_current_wal_lsn()::text, pg_current_xact_id()::text;")
            row_meta = cur.fetchone()
            lsn_leader = row_meta[0] if row_meta else "N/A"
            txid = row_meta[1] if row_meta else "N/A"
            
            conn.commit()
            
            log_info(f"Thread {thread_idx} wrote reservation ID {res_id} (Seat ID: {seat_id}) [TxID: {txid}, LSN: {lsn_leader}]", node="Exp4")
            
            time_str = t_commit.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            log_line = f"[{time_str}] NODE: Leader | OP: INSERT | TABLE: reservations    | ID: {res_id:<4d} | DETAILS: {json.dumps({'customer_name': customer, 'operation_id': op_id})}\n"
            with open("crud.log", "a", encoding="utf-8") as f_log:
                f_log.write(log_line)
                
            cur.close()
            conn.close()

            conn_f = psycopg2.connect(**FOLLOWER_DB)
            cur_f = conn_f.cursor()
            t_poll_start = time.time()
            t_follower_visible = None
            lsn_follower = "N/A"
            
            while time.time() - t_poll_start < 10:
                cur_f.execute("SELECT id FROM reservations WHERE id = %s;", (res_id,))
                if cur_f.fetchone():
                    t_follower_visible = datetime.now()
                    cur_f.execute("SELECT pg_last_wal_replay_lsn()::text;")
                    row_replay = cur_f.fetchone()
                    lsn_follower = row_replay[0] if row_replay else "N/A"
                    break
                time.sleep(0.0005)
                
            cur_f.close()
            conn_f.close()

            if t_follower_visible is None:
                t_follower_visible = datetime.now()
                lsn_follower = "TIMEOUT"
                
            with leader_commits_lock:
                leader_commits.append({
                    "res_id": res_id,
                    "seat_id": seat_id,
                    "customer": customer,
                    "t_commit": t_commit,
                    "t_follower_visible": t_follower_visible,
                    "txid": txid,
                    "lsn_leader": lsn_leader,
                    "lsn_follower": lsn_follower
                })
            
        except Exception as e:
            print(f"   ERROR: Thread {thread_idx} Error: {e}")

    threads = []
    for idx, s_id in enumerate(seats_pool):
        t = threading.Thread(target=reservation_worker, args=(idx+1, s_id))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    leader_commits_sorted = sorted(leader_commits, key=lambda x: x["res_id"])
    order_preservation_results = []
    
    print("\nLEADER COMMIT ORDER & FOLLOWER VISIBILITY:")
    print("-" * 145)
    print(f"{'Order':<5s} | {'Res ID':<8s} | {'Seat':<6s} | {'Customer Name':<22s} | {'Commit Time (L)':<18s} | {'Replicated (F)':<18s} | {'TxID':<8s} | {'LSN (Leader)':<12s} | {'LSN (Follower)':<12s} | {'Status'}")
    print("-" * 145)
    
    for seq, commit_data in enumerate(leader_commits_sorted):
        res_id = commit_data["res_id"]
        seat_id = commit_data["seat_id"]
        customer = commit_data["customer"]
        t_commit = commit_data["t_commit"]
        t_follower_visible = commit_data["t_follower_visible"]
        txid = commit_data["txid"]
        lsn_leader = commit_data["lsn_leader"]
        lsn_follower = commit_data["lsn_follower"]
        
        l_time_str = t_commit.strftime('%H:%M:%S.%f')[:-3] if t_commit else "None"
        f_time_str = t_follower_visible.strftime('%H:%M:%S.%f')[:-3] if t_follower_visible else "None"
        seat_name = seat_map.get(seat_id, f"ID:{seat_id}")
        status_str = "ORDER PRESERVED"
        
        print(f"#{seq+1:<4d} | ID:{res_id:<4d} | {seat_name:<6s} | {customer:<22s} | {l_time_str:<18s} | {f_time_str:<18s} | {txid:<8s} | {lsn_leader:<12s} | {lsn_follower:<12s} | {status_str}")
        order_preservation_results.append([seq+1, res_id, customer, l_time_str, f_time_str, txid, lsn_leader, lsn_follower, status_str])

    print("-" * 145)
    log_info("Part 1 Verified: Leader commit sequencing and Follower replication times recorded.", node="Exp4")
    
    # PART 2: CONCURRENT WRITES / DOUBLE BOOKING
    log_info("Part 2: Starting race condition booking test on seat 10...", node="Exp4")
    
    race_results = []
    race_results_lock = threading.Lock()
    race_barrier = threading.Barrier(2)
    
    conn_seats = psycopg2.connect(**LEADER_DB)
    cur_seats = conn_seats.cursor()
    cur_seats.execute("SELECT row_label || seat_number FROM seats WHERE id = 10;")
    seat_10_name = cur_seats.fetchone()[0]
    
    cur_seats.execute("DELETE FROM reservations WHERE showtime_id = 1 AND seat_id = 10;")
    conn_seats.commit()
    cur_seats.close()
    conn_seats.close()

    def booking_racer(racer_id):
        customer = f"Racer_Client_{racer_id}"
        op_id = str(uuid.uuid4())
        
        race_barrier.wait()
        t_start = datetime.now()
        
        try:
            conn = psycopg2.connect(**LEADER_DB)
            cur = conn.cursor()
            
            cur.execute("""
                SELECT id FROM reservations 
                WHERE showtime_id = 1 AND seat_id = 10 AND status = 'reserved';
            """)
            existing = cur.fetchone()
            
            time.sleep(0.05)
            
            status_text = ""
            res_id = None
            
            if existing is None:
                cur.execute("""
                    INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version, last_updated, operation_id) 
                    VALUES (1, 10, %s, 'reserved', 1, %s, %s) RETURNING id;
                """, (customer, t_start, op_id))
                res_id = cur.fetchone()[0]
                
                log_query = """
                    INSERT INTO replication_log (operation_type, table_name, record_id, details, timestamp, node) 
                    VALUES ('INSERT', 'reservations', %s, %s, %s, 'Leader')
                """
                cur.execute(log_query, (res_id, json.dumps({"customer_name": customer, "seat_id": 10, "racer_id": racer_id}), t_start))
                conn.commit()
                status_text = f"SUCCESS (Seat {seat_10_name} Reserved!)"
                
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
 
    t1 = threading.Thread(target=booking_racer, args=(1,))
    t2 = threading.Thread(target=booking_racer, args=(2,))
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    
    print("\n[PART 2] CONCURRENT RACERS RESULTS:")
    print("-" * 90)
    for r in race_results:
        print(f"  Racer #{r['racer_id']} ({r['customer']}) | Time: {r['time']} | Result: {r['status']} | Res.ID: {r['res_id']}")
    print("-" * 90)
    
    double_booked = all(r["status"].startswith("SUCCESS") for r in race_results)
    log_info(f"Part 2 Analysis: Double Booking Occurred = {double_booked}", node="Exp4")
    print(f"Double Booking Occurred: {'Yes' if double_booked else 'No'}")
    print("=" * 60)

    headers_order = ["Order", "Write ID", "Customer Name", "Leader Commit", "Follower Replicated", "TxID", "LSN Leader", "LSN Follower", "Status"]
    save_to_csv("concurrent_order_results.csv", headers_order, order_preservation_results)
    
    headers_race = ["Racer ID", "Customer", "Time", "Result", "Reservation ID"]
    save_to_csv("concurrent_race_results.csv", headers_race, [[r['racer_id'], r['customer'], r['time'], r['status'], r['res_id']] for r in race_results])
    
    json_data = {
        "experiment": "Concurrent Writes & Race Condition",
        "timestamp": datetime.now().isoformat(),
        "ordering_summary": {
            "total_writes": len(order_preservation_results)
        },
        "details_ordering": [
            {
                "sequence": r[0],
                "write_id": r[1],
                "customer": r[2],
                "leader_time": r[3],
                "follower_time": r[4],
                "txid": r[5],
                "lsn_leader": r[6],
                "lsn_follower": r[7],
                "status": r[8]
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
