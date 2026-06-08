import psycopg2
import uuid
import time
from datetime import datetime
from db_config import LEADER_DB, FOLLOWER_DB
from logger import log_operation, log_info
from utils import save_to_csv, save_to_json, print_table

def run_eventual_consistency_experiment(iterations=10):
    """
    Eventual Consistency Experiment:
    Inserts new movies to the Leader and measures the time it takes for each to become visible in the Follower
    (replication lag) with millisecond precision.
    """
    log_info("START: Eventual Consistency Experiment (Lag Test) initiated", node="Exp1")
    print("\n" + "=" * 60)
    print("EXPERIMENT 1: EVENTUAL CONSISTENCY (Replication Lag)")
    print("=" * 60)
    print(f"This experiment will be repeated for {iterations} different records.")
    print("In each iteration, the data written to the Leader VM will be monitored from the Follower VM in milliseconds.")
    print("=" * 60)

    results = []
    
    headers = ["Iteration", "Record ID", "Movie Title", "Write Time (L)", "Visible Time (F)", "Query Count", "Lag (ms)"]
    col_widths = [9, 9, 26, 26, 26, 11, 8]
    row_format = " | ".join([f"{{:<{w}}}" for w in col_widths])
    border = "-+-".join(["-" * w for w in col_widths])
    
    print("\n" + border)
    print(row_format.format(*headers))
    print(border)
    
    for i in range(1, iterations + 1):
        movie_title = f"Exp1_Movie_{int(time.time())}_{i}"
        genre = "Thriller"
        duration = 120 + i
        operation_id = str(uuid.uuid4())
        
        log_info(f"[{i}/{iterations}] Writing movie: '{movie_title}' to Leader...", node="Exp1")
        
        # 1. Write to Leader DB (INSERT)
        conn_l = psycopg2.connect(**LEADER_DB)
        cur_l = conn_l.cursor()
        
        t_write_start = datetime.now()
        
        query_insert = """
            INSERT INTO movies (title, genre, duration_min, version, last_updated, operation_id) 
            VALUES (%s, %s, %s, 1, %s, %s) RETURNING id, last_updated;
        """
        cur_l.execute(query_insert, (movie_title, genre, duration, t_write_start, operation_id))
        record_id, last_updated_leader = cur_l.fetchone()
        
        conn_l.commit()
        
        # Log record on the Leader side
        log_operation("INSERT", "movies", record_id, {
            "title": movie_title,
            "operation_id": operation_id
        })
        
        cur_l.close()
        conn_l.close()
        
        # 2. Follower DB Polling (Live Monitoring with Frequent Reads)
        conn_f = psycopg2.connect(**FOLLOWER_DB)
        cur_f = conn_f.cursor()
        
        t_poll_start = time.time()
        t_visible = None
        attempts = 0
        
        query_select = "SELECT last_updated FROM movies WHERE id = %s;"
        
        while time.time() - t_poll_start < 15:  # 15 seconds timeout
            attempts += 1
            cur_f.execute(query_select, (record_id,))
            row = cur_f.fetchone()
            
            if row:
                t_visible = datetime.now()
                last_updated_follower = row[0]
                break
            
            time.sleep(0.001)  # Very high precision with 1ms wait
            
        cur_f.close()
        conn_f.close()
        
        # 3. Calculating and Saving Results
        if t_visible:
            # Replication lag: The difference between the time data is visible in the Follower and the time it was written to the Leader
            lag_ms = (t_visible - t_write_start).total_seconds() * 1000.0
            log_info(f"[{i}/{iterations}] Replicated to Follower! (Attempts: {attempts}, Lag: {lag_ms:.2f}ms)", node="Exp1")
            results.append([i, record_id, movie_title, t_write_start, t_visible, attempts, round(lag_ms, 2)])
            
            lag_ms_str = f"{lag_ms:.2f}"
            visible_time_str = t_visible.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        else:
            log_info(f"[{i}/{iterations}] ERROR: Replication timeout on Follower after 15s!", node="Exp1")
            results.append([i, record_id, movie_title, t_write_start, "TIMEOUT", attempts, -1])
            
            lag_ms_str = "-1.00"
            visible_time_str = "TIMEOUT"
            
        write_time_str = t_write_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        row = [str(i), str(record_id), movie_title, write_time_str, visible_time_str, str(attempts), lag_ms_str]
        print(row_format.format(*row))
            
        time.sleep(0.5)  # Short wait between iterations

    print(border)

    # 4. Evaluating & Reporting Results
    valid_lags = [r[6] for r in results if r[6] > 0]
    
    if valid_lags:
        min_lag = min(valid_lags)
        max_lag = max(valid_lags)
        avg_lag = sum(valid_lags) / len(valid_lags)
    else:
        min_lag = max_lag = avg_lag = 0
        
    log_info(f"SUMMARY: Avg Lag = {avg_lag:.2f}ms (Min: {min_lag:.2f}ms, Max: {max_lag:.2f}ms)", node="Exp1")
    
    print("=" * 60)
    print("SUMMARY STATISTICS:")
    print(f"  Minimum Lag (Min Lag) : {min_lag:.2f} ms")
    print(f"  Maximum Lag (Max Lag): {max_lag:.2f} ms")
    print(f"  Average Lag (Avg Lag): {avg_lag:.2f} ms")
    print("=" * 60)

    
    # Save data to disk
    save_to_csv("eventual_results.csv", headers, results)
    
    json_data = {
        "experiment": "Eventual Consistency",
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "min_lag_ms": round(min_lag, 2),
            "max_lag_ms": round(max_lag, 2),
            "avg_lag_ms": round(avg_lag, 2),
            "successful_runs": len(valid_lags)
        },
        "details": [
            {
                "run": r[0],
                "record_id": r[1],
                "title": r[2],
                "write_time": r[3].isoformat(),
                "visible_time": r[4].isoformat() if isinstance(r[4], datetime) else r[4],
                "attempts": r[5],
                "lag_ms": r[6]
            } for r in results
        ]
    }
    save_to_json("eventual_results.json", json_data)

if __name__ == "__main__":
    run_eventual_consistency_experiment(10)
