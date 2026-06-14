import psycopg2
import uuid
import time
from datetime import datetime
from db_config import LEADER_DB, FOLLOWER_DB
from logger import log_operation, log_info
from utils import save_to_csv, save_to_json

def run_eventual_consistency_experiment(iterations=10, use_persistent=False):
    """Run eventual consistency experiment measuring replication lag."""
    log_info(f"START: Eventual Consistency Experiment (Lag Test) initiated (use_persistent={use_persistent})", node="Exp1")
    print("\nEXPERIMENT 1: EVENTUAL CONSISTENCY (Replication Lag)")

    results = []
    
    headers = ["Iteration", "Record ID", "Movie Title", "Write Time (L)", "Visible Time (F)", "Query Count", "Lag (ms)"]
    col_widths = [9, 9, 26, 26, 26, 11, 8]
    row_format = " | ".join([f"{{:<{w}}}" for w in col_widths])
    border = "-+-".join(["-" * w for w in col_widths])
    
    print("\n" + border)
    print(row_format.format(*headers))
    print(border)
    
    conn_sync = psycopg2.connect(**LEADER_DB)
    cur_sync = conn_sync.cursor()
    cur_sync.execute("SELECT setval(pg_get_serial_sequence('movies', 'id'), COALESCE(MAX(id), 1)) FROM movies;")
    cur_sync.execute("SELECT setval(pg_get_serial_sequence('showtimes', 'id'), COALESCE(MAX(id), 1)) FROM showtimes;")
    cur_sync.execute("SELECT setval(pg_get_serial_sequence('reservations', 'id'), COALESCE(MAX(id), 1)) FROM reservations;")
    conn_sync.commit()
    cur_sync.close()
    conn_sync.close()
    
    conn_l_persist = None
    cur_l_persist = None
    conn_f_persist = None
    cur_f_persist = None
    
    if use_persistent:
        conn_l_persist = psycopg2.connect(**LEADER_DB)
        cur_l_persist = conn_l_persist.cursor()
        conn_f_persist = psycopg2.connect(**FOLLOWER_DB)
        cur_f_persist = conn_f_persist.cursor()
    
    run_counter = 0
    for i in range(1, iterations + 1):
        movie_title_base = f"Exp1_Movie_{int(time.time())}_{i}"
        genre = "Thriller"
        duration = 120 + i
        operation_id = str(uuid.uuid4())
        
        # 1. INSERT
        run_counter += 1
        movie_title_insert = f"{movie_title_base}_INSERT"
        log_info(f"[{i}/{iterations}] [INSERT] Writing movie: '{movie_title_insert}' to Leader...", node="Exp1")
        
        if use_persistent:
            conn_l = conn_l_persist
            cur_l = cur_l_persist
        else:
            conn_l = psycopg2.connect(**LEADER_DB)
            cur_l = conn_l.cursor()
            
        t_insert_time = datetime.now()
        query_insert = """
            INSERT INTO movies (title, genre, duration_min, version, last_updated, operation_id) 
            VALUES (%s, %s, %s, 1, %s, %s) RETURNING id;
        """
        cur_l.execute(query_insert, (movie_title_insert, genre, duration, t_insert_time, operation_id))
        record_id = cur_l.fetchone()[0]
        conn_l.commit()
        t_write_committed = datetime.now()
        
        if not use_persistent:
            cur_l.close()
            conn_l.close()
            log_operation("INSERT", "movies", record_id, {
                "title": movie_title_insert,
                "operation_id": operation_id
            })
            
        if use_persistent:
            conn_f = conn_f_persist
            cur_f = cur_f_persist
        else:
            conn_f = psycopg2.connect(**FOLLOWER_DB)
            cur_f = conn_f.cursor()
            
        t_poll_start = time.time()
        t_visible = None
        attempts = 0
        while time.time() - t_poll_start < 15:
            attempts += 1
            cur_f.execute("SELECT last_updated FROM movies WHERE id = %s;", (record_id,))
            row = cur_f.fetchone()
            if row:
                t_visible = datetime.now()
                break
            time.sleep(0.001)
            
        if not use_persistent:
            cur_f.close()
            conn_f.close()
            
        if use_persistent:
            log_operation("INSERT", "movies", record_id, {
                "title": movie_title_insert,
                "operation_id": operation_id
            })
            
        if t_visible:
            lag_ms = (t_visible - t_write_committed).total_seconds() * 1000.0
            log_info(f"[{i}/{iterations}] [INSERT] Replicated! (Lag: {lag_ms:.2f}ms)", node="Exp1")
            results.append([run_counter, record_id, movie_title_insert, t_write_committed, t_visible, attempts, round(lag_ms, 2)])
            lag_ms_str = f"{lag_ms:.2f}"
            visible_time_str = t_visible.strftime('%H:%M:%S.%f')[:-3]
        else:
            log_info(f"[{i}/{iterations}] [INSERT] ERROR: Timeout!", node="Exp1")
            results.append([run_counter, record_id, movie_title_insert, t_write_committed, "TIMEOUT", attempts, -1])
            lag_ms_str = "-1.00"
            visible_time_str = "TIMEOUT"
            
        write_time_str = t_write_committed.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        row_disp = [str(run_counter), str(record_id), movie_title_insert, write_time_str, visible_time_str, str(attempts), lag_ms_str]
        print(row_format.format(*row_disp))
        
        time.sleep(0.2)
        
        # 2. UPDATE
        run_counter += 1
        movie_title_update = f"{movie_title_base}_UPDATE"
        log_info(f"[{i}/{iterations}] [UPDATE] Updating movie ID {record_id} to '{movie_title_update}' on Leader...", node="Exp1")
        
        if use_persistent:
            conn_l = conn_l_persist
            cur_l = cur_l_persist
        else:
            conn_l = psycopg2.connect(**LEADER_DB)
            cur_l = conn_l.cursor()
            
        t_update_time = datetime.now()
        query_update = """
            UPDATE movies SET title = %s, version = 2, last_updated = %s, operation_id = %s WHERE id = %s;
        """
        cur_l.execute(query_update, (movie_title_update, t_update_time, operation_id, record_id))
        conn_l.commit()
        t_update_committed = datetime.now()
        
        if not use_persistent:
            cur_l.close()
            conn_l.close()
            log_operation("UPDATE", "movies", record_id, {
                "title": movie_title_update,
                "operation_id": operation_id
            })
            
        if use_persistent:
            conn_f = conn_f_persist
            cur_f = cur_f_persist
        else:
            conn_f = psycopg2.connect(**FOLLOWER_DB)
            cur_f = conn_f.cursor()
            
        t_poll_start = time.time()
        t_visible_update = None
        attempts = 0
        while time.time() - t_poll_start < 15:
            attempts += 1
            cur_f.execute("SELECT version, title FROM movies WHERE id = %s;", (record_id,))
            row = cur_f.fetchone()
            if row and row[0] == 2 and row[1] == movie_title_update:
                t_visible_update = datetime.now()
                break
            time.sleep(0.001)
            
        if not use_persistent:
            cur_f.close()
            conn_f.close()
            
        if use_persistent:
            log_operation("UPDATE", "movies", record_id, {
                "title": movie_title_update,
                "operation_id": operation_id
            })
            
        if t_visible_update:
            lag_ms = (t_visible_update - t_update_committed).total_seconds() * 1000.0
            log_info(f"[{i}/{iterations}] [UPDATE] Replicated! (Lag: {lag_ms:.2f}ms)", node="Exp1")
            results.append([run_counter, record_id, movie_title_update, t_update_committed, t_visible_update, attempts, round(lag_ms, 2)])
            lag_ms_str = f"{lag_ms:.2f}"
            visible_time_str = t_visible_update.strftime('%H:%M:%S.%f')[:-3]
        else:
            log_info(f"[{i}/{iterations}] [UPDATE] ERROR: Timeout!", node="Exp1")
            results.append([run_counter, record_id, movie_title_update, t_update_committed, "TIMEOUT", attempts, -1])
            lag_ms_str = "-1.00"
            visible_time_str = "TIMEOUT"
            
        write_time_str = t_update_committed.strftime('%H:%M:%S.%f')[:-3]
        row_disp = [str(run_counter), str(record_id), movie_title_update, write_time_str, visible_time_str, str(attempts), lag_ms_str]
        print(row_format.format(*row_disp))
        
        time.sleep(0.2)
        
        # 3. DELETE
        run_counter += 1
        movie_title_delete = f"{movie_title_base}_DELETE"
        log_info(f"[{i}/{iterations}] [DELETE] Deleting movie ID {record_id} on Leader...", node="Exp1")
        
        if use_persistent:
            conn_l = conn_l_persist
            cur_l = cur_l_persist
        else:
            conn_l = psycopg2.connect(**LEADER_DB)
            cur_l = conn_l.cursor()
            
        query_delete = "DELETE FROM movies WHERE id = %s;"
        cur_l.execute(query_delete, (record_id,))
        conn_l.commit()
        t_delete_committed = datetime.now()
        
        if not use_persistent:
            cur_l.close()
            conn_l.close()
            log_operation("DELETE", "movies", record_id, {
                "title": movie_title_delete,
                "operation_id": operation_id
            })
            
        if use_persistent:
            conn_f = conn_f_persist
            cur_f = cur_f_persist
        else:
            conn_f = psycopg2.connect(**FOLLOWER_DB)
            cur_f = conn_f.cursor()
            
        t_poll_start = time.time()
        t_visible_delete = None
        attempts = 0
        while time.time() - t_poll_start < 15:
            attempts += 1
            cur_f.execute("SELECT id FROM movies WHERE id = %s;", (record_id,))
            row = cur_f.fetchone()
            if not row:
                t_visible_delete = datetime.now()
                break
            time.sleep(0.001)
            
        if not use_persistent:
            cur_f.close()
            conn_f.close()
            
        if use_persistent:
            log_operation("DELETE", "movies", record_id, {
                "title": movie_title_delete,
                "operation_id": operation_id
            })
            
        if t_visible_delete:
            lag_ms = (t_visible_delete - t_delete_committed).total_seconds() * 1000.0
            log_info(f"[{i}/{iterations}] [DELETE] Replicated! (Lag: {lag_ms:.2f}ms)", node="Exp1")
            results.append([run_counter, record_id, movie_title_delete, t_delete_committed, t_visible_delete, attempts, round(lag_ms, 2)])
            lag_ms_str = f"{lag_ms:.2f}"
            visible_time_str = t_visible_delete.strftime('%H:%M:%S.%f')[:-3]
        else:
            log_info(f"[{i}/{iterations}] [DELETE] ERROR: Timeout!", node="Exp1")
            results.append([run_counter, record_id, movie_title_delete, t_delete_committed, "TIMEOUT", attempts, -1])
            lag_ms_str = "-1.00"
            visible_time_str = "TIMEOUT"
            
        write_time_str = t_delete_committed.strftime('%H:%M:%S.%f')[:-3]
        row_disp = [str(run_counter), str(record_id), movie_title_delete, write_time_str, visible_time_str, str(attempts), lag_ms_str]
        print(row_format.format(*row_disp))
        
        time.sleep(0.5)

    if use_persistent:
        cur_l_persist.close()
        conn_l_persist.close()
        cur_f_persist.close()
        conn_f_persist.close()

    print(border)

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
    run_eventual_consistency_experiment(10, use_persistent=True)
