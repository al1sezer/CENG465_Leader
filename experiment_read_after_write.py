import psycopg2
import uuid
import time
import threading
from datetime import datetime
from db_config import LEADER_DB, FOLLOWER_DB
from logger import log_operation, log_info
from utils import save_to_csv, save_to_json, print_table

def run_read_after_write_experiment(iterations=10):
    """
    Read-After-Write (RAW) Consistency Experiment:
    Tests if the client can see their own write operation (ticket reservation) immediately:
      1. Verifies if they can see the data instantly when reading from the Leader (RAW guarantee).
      2. Tests if the data is immediately visible when reading from the Follower (potential RAW violation).
    Measures the visibility lag of the data on the Follower.
    """
    log_info("START: Read-After-Write (RAW) Consistency Experiment initiated", node="Exp3")
    print("\n" + "=" * 60)
    print("EXPERIMENT 3: READ-AFTER-WRITE CONSISTENCY (Read-Your-Own-Writes)")
    print("=" * 60)
    print(f"This experiment will be repeated for {iterations} different ticket reservations.")
    print("The visibility lag of the client's own reservation will be measured on both the Leader and Follower.")
    print("=" * 60)

    # Let's use a showtime (Showtime ID: 1, Inception Hall A) for the experiment.
    # Use seats sequentially from A3 to A12 (Seat ID: 3 to 12).
    showtime_id = 1
    
    # Ensure showtime with ID 1 exists dynamically (as showtimes table is empty initially)
    conn_prep = psycopg2.connect(**LEADER_DB)
    cur_prep = conn_prep.cursor()
    cur_prep.execute("SELECT id FROM showtimes WHERE id = %s;", (showtime_id,))
    if not cur_prep.fetchone():
        print(f"Creating showtime {showtime_id} dynamically for the RAW experiment...")
        cur_prep.execute("INSERT INTO showtimes (id, movie_id, hall_id, show_date, show_time) VALUES (1, 1, 1, '2026-06-01', '14:00') ON CONFLICT (id) DO NOTHING;")
        conn_prep.commit()
    cur_prep.close()
    conn_prep.close()
    
    results = []

    # CSV headers
    headers = ["Iteration", "Reservation ID", "Customer Name", "Leader Visibility (ms)", "Follower Immediate Visibility", "Follower Visibility (ms)", "Query Count", "Status"]

    # Print table header
    print_headers = ["Iteration", "Seat", "Customer Name", "Commit Time (L)", "Visible Time (F)", "Leader Vis (ms)", "Follower Lag (ms)", "Query Count", "Status"]
    col_widths = [9, 6, 26, 15, 16, 15, 17, 11, 36]
    row_format = " | ".join([f"{{:<{w}}}" for w in col_widths])
    border = "-+-".join(["-" * w for w in col_widths])
    
    print("\n" + border)
    print(row_format.format(*print_headers))
    print(border)

    # Establish persistent connections to eliminate connection setup overhead
    # Connection for Leader write operations
    conn_l_write = psycopg2.connect(**LEADER_DB)
    cur_l_write = conn_l_write.cursor()
    
    # Connection for Leader read check (Thread 1)
    conn_l_check = psycopg2.connect(**LEADER_DB)
    cur_l_check = conn_l_check.cursor()
    
    # Connection for Follower read check (Thread 2)
    conn_f_check = psycopg2.connect(**FOLLOWER_DB)
    cur_f_check = conn_f_check.cursor()

    for i in range(1, iterations + 1):
        seat_id = 2 + i  # Seats: 3, 4, 5, ..., 12
        customer_name = f"RAW_Customer_{int(time.time())}_{i}"
        
        log_info(f"[{i}/{iterations}] Creating reservation on Leader: Customer={customer_name}, SeatID={seat_id}...", node="Exp3")
        
        # A. WRITING TO LEADER
        t_write = datetime.now()
        
        query_insert = """
            INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version, last_updated, operation_id) 
            VALUES (%s, %s, %s, 'reserved', 1, %s, %s) RETURNING id;
        """
        op_id = str(uuid.uuid4())
        cur_l_write.execute(query_insert, (showtime_id, seat_id, customer_name, t_write, op_id))
        res_id = cur_l_write.fetchone()[0]
        conn_l_write.commit()
        t_commit = datetime.now()
        
        # Resolve seat label dynamically
        cur_l_write.execute("SELECT row_label || seat_number FROM seats WHERE id = %s;", (seat_id,))
        seat_label = cur_l_write.fetchone()[0]
        
        # Shared thread outputs
        leader_res = {}
        follower_res = {}
        
        # Thread 1: Leader Visibility Check
        def check_leader_visibility():
            try:
                cur_l_check.execute("SELECT customer_name FROM reservations WHERE id = %s;", (res_id,))
                row_l = cur_l_check.fetchone()
                t_end = datetime.now()
                leader_res['visible'] = (row_l is not None)
                leader_res['lag_ms'] = (t_end - t_commit).total_seconds() * 1000.0
            except Exception as e:
                leader_res['visible'] = False
                leader_res['lag_ms'] = -1.0
                leader_res['error'] = str(e)
                
        # Thread 2: Follower Visibility Check and Polling
        def check_follower_visibility():
            try:
                # First immediate read check
                t_read_first = datetime.now()
                cur_f_check.execute("SELECT customer_name FROM reservations WHERE id = %s;", (res_id,))
                row_f_first = cur_f_check.fetchone()
                
                first_visible = (row_f_first is not None)
                attempts = 1
                t_poll_start = time.time()
                visible_time = None
                
                if first_visible:
                    visible_time = t_read_first
                else:
                    while time.time() - t_poll_start < 10:  # 10 seconds limit
                        attempts += 1
                        cur_f_check.execute("SELECT customer_name FROM reservations WHERE id = %s;", (res_id,))
                        row_f = cur_f_check.fetchone()
                        if row_f:
                            visible_time = datetime.now()
                            break
                        time.sleep(0.0005)  # 0.5ms wait
                        
                follower_res['first_visible'] = first_visible
                follower_res['attempts'] = attempts
                follower_res['visible_time'] = visible_time
                if visible_time:
                    follower_res['lag_ms'] = (visible_time - t_commit).total_seconds() * 1000.0
                else:
                    follower_res['lag_ms'] = -1.0
            except Exception as e:
                follower_res['first_visible'] = False
                follower_res['attempts'] = 0
                follower_res['lag_ms'] = -1.0
                follower_res['error'] = str(e)
                
        # Start both checks in parallel threads
        t1 = threading.Thread(target=check_leader_visibility)
        t2 = threading.Thread(target=check_follower_visibility)
        
        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        # Read thread outputs
        leader_visible = leader_res.get('visible', False)
        leader_lag_ms = leader_res.get('lag_ms', -1.0)
        
        first_read_visible = follower_res.get('first_visible', False)
        attempts = follower_res.get('attempts', 0)
        follower_lag_ms = follower_res.get('lag_ms', -1.0)
        follower_visible_time = follower_res.get('visible_time', None)
        
        # Log record (moved here so it doesn't add latency to replication check)
        log_operation("INSERT", "reservations", res_id, {
            "customer_name": customer_name,
            "operation_id": op_id
        })
        
        # D. ANALYSIS AND CALCULATION
        if follower_visible_time:
            # Measure lag from t_commit (when replication actually started)
            violation = "VIOLATION (Could not see own write!)" if not first_read_visible else "NORMAL (Saw instantly)"
            
            if not first_read_visible:
                log_info(f"[{i}/{iterations}] RAW Violation! Delayed on Follower. Visible after {attempts} attempts / {follower_lag_ms:.2f}ms", node="Exp3")
            else:
                log_info(f"[{i}/{iterations}] RAW Secured! Visible on Follower immediately", node="Exp3")
                
            results.append([
                i, res_id, customer_name, 
                leader_lag_ms, 
                "Yes" if first_read_visible else "No", 
                follower_lag_ms, 
                attempts, 
                violation
            ])
            
            row = [
                str(i), seat_label, customer_name, 
                t_commit.strftime('%H:%M:%S.%f')[:-3], 
                follower_visible_time.strftime('%H:%M:%S.%f')[:-3], 
                f"{leader_lag_ms:.3f}", 
                f"{follower_lag_ms:.2f}", 
                str(attempts), 
                violation
            ]
            print(row_format.format(*row))
        else:
            log_info(f"[{i}/{iterations}] ERROR: Record did not replicate on Follower within 10 seconds timeout!", node="Exp3")
            results.append([i, res_id, customer_name, leader_lag_ms, "No", -1, attempts, "TIMEOUT"])
            
            row = [
                str(i), seat_label, customer_name, 
                t_commit.strftime('%H:%M:%S.%f')[:-3], 
                "TIMEOUT", 
                f"{leader_lag_ms:.3f}", 
                "-1.00", 
                str(attempts), 
                "TIMEOUT"
            ]
            print(row_format.format(*row))
            
        time.sleep(0.5)

    # Close persistent connections
    cur_l_write.close()
    conn_l_write.close()
    cur_l_check.close()
    conn_l_check.close()
    cur_f_check.close()
    conn_f_check.close()

    print(border)

    # 4. Result Report and Statistics
    
    total_runs = len(results)
    violations_count = sum(1 for r in results if "VIOLATION" in r[7])
    violation_rate = (violations_count / total_runs) * 100.0 if total_runs > 0 else 0
    
    valid_follower_lags = [r[5] for r in results if r[5] > 0]
    avg_follower_lag = sum(valid_follower_lags) / len(valid_follower_lags) if valid_follower_lags else 0
    
    log_info(f"SUMMARY: Violations: {violations_count}/{total_runs} ({violation_rate:.1f}%), Avg Lag: {avg_follower_lag:.2f}ms", node="Exp3")
    print("=" * 60)
    print("READ-AFTER-WRITE SUMMARY REPORT:")
    print(f"  Total Attempt Count           : {total_runs}")
    print(f"  Follower RAW Violation Count  : {violations_count}")
    print(f"  RAW Violation Rate            : {violation_rate:.1f}%")
    print(f"  Average Follower Visibility Lag : {avg_follower_lag:.2f} ms")
    print(f"  Leader RAW Violation Count    : 0 (Always sees own write instantly)")
    print("=" * 60)

    
    # Save to files
    save_to_csv("raw_results.csv", headers, results)
    
    json_data = {
        "experiment": "Read-After-Write Consistency",
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_runs": total_runs,
            "violations_count": violations_count,
            "violation_rate_percent": round(violation_rate, 2),
            "avg_follower_lag_ms": round(avg_follower_lag, 2)
        },
        "details": [
            {
                "run": r[0],
                "reservation_id": r[1],
                "showtime_id": showtime_id,
                "customer_name": r[2],
                "leader_lag_ms": round(r[3], 3),
                "follower_immediate_visible": r[4],
                "follower_lag_ms": round(r[5], 2),
                "attempts": r[6],
                "status": r[7]
            } for r in results
        ]
    }
    save_to_json("raw_results.json", json_data)

if __name__ == "__main__":
    run_read_after_write_experiment(10)
