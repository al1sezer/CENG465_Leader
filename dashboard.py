import os
import json
import psycopg2
from flask import Flask, jsonify, render_template, send_from_directory, request
from db_config import LEADER_DB
from datetime import datetime

# Import experiment functions
try:
    from experiment_eventual import run_eventual_consistency_experiment
    from experiment_monotonic import run_monotonic_reads_experiment
    from experiment_read_after_write import run_read_after_write_experiment
    from experiment_concurrent import run_concurrent_writes_experiment
    from visualize import generate_all_plots
except ImportError as e:
    print(f"WARNING: Experiment files could not be loaded: {e}")

app = Flask(__name__, template_folder="templates")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
CRUD_LOG_PATH = os.path.join(BASE_DIR, "crud.log")

def get_db_connection():
    return psycopg2.connect(**LEADER_DB)

# ============================================================================
# WEB ROUTES
# ============================================================================

@app.route('/')
def index():
    """
    Renders the main dashboard page.
    """
    return render_template("index.html")

@app.route('/results/<path:filename>')
def serve_results(filename):
    """
    Serves chart images, visualizations, and raw CSV/JSON data.
    """
    return send_from_directory(RESULTS_DIR, filename)

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/api/db-state', methods=['GET'])
def get_db_state():
    """
    Returns the current state of tables (movies, showtimes, reservations, replication_log) from the DB.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. Movies
        cur.execute("SELECT id, title, genre, duration_min, version FROM movies ORDER BY id LIMIT 10;")
        movies = [{"id": r[0], "title": r[1], "genre": r[2], "duration": r[3], "version": r[4]} for r in cur.fetchall()]
        
        # 2. Showtimes
        cur.execute("""
            SELECT st.id, m.title, h.name, st.show_date, st.show_time 
            FROM showtimes st 
            JOIN movies m ON st.movie_id = m.id 
            JOIN halls h ON st.hall_id = h.id 
            ORDER BY st.id LIMIT 10;
        """)
        showtimes = [{"id": r[0], "movie": r[1], "hall": r[2], "date": str(r[3]), "time": str(r[4])} for r in cur.fetchall()]
        
        # 3. Last 10 Reservations
        cur.execute("""
            SELECT r.id, m.title, h.name, s.row_label || s.seat_number, r.customer_name, r.status, r.version 
            FROM reservations r 
            JOIN showtimes st ON r.showtime_id = st.id 
            JOIN movies m ON st.movie_id = m.id 
            JOIN halls h ON st.hall_id = h.id 
            JOIN seats s ON r.seat_id = s.id 
            ORDER BY r.id DESC LIMIT 10;
        """)
        reservations = [{"id": r[0], "movie": r[1], "hall": r[2], "seat": r[3], "customer": r[4], "status": r[5], "version": r[6]} for r in cur.fetchall()]
        
        # 4. Last 15 Replication Logs
        cur.execute("""
            SELECT log_id, operation_type, table_name, record_id, details, timestamp, node 
            FROM replication_log 
            ORDER BY log_id DESC LIMIT 15;
        """)
        logs = [{"log_id": r[0], "op_type": r[1], "table": r[2], "record_id": r[3], "details": r[4], "timestamp": str(r[5]), "node": r[6]} for r in cur.fetchall()]
        
        # 5. Halls
        cur.execute("SELECT id, name, capacity, version FROM halls ORDER BY id LIMIT 10;")
        halls = [{"id": r[0], "name": r[1], "capacity": r[2], "version": r[3]} for r in cur.fetchall()]
        
        # 6. Seats
        cur.execute("""
            SELECT s.id, h.name, s.row_label || s.seat_number, s.version 
            FROM seats s 
            JOIN halls h ON s.hall_id = h.id 
            ORDER BY s.id LIMIT 10;
        """)
        seats = [{"id": r[0], "hall": r[1], "seat": r[2], "version": r[3]} for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "data": {
                "movies": movies,
                "halls": halls,
                "seats": seats,
                "showtimes": showtimes,
                "reservations": reservations,
                "replication_logs": logs
            }
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/logs', methods=['GET'])
def get_crud_logs():
    """
    Reads and returns the last 20 lines of the crud.log file.
    """
    if not os.path.exists(CRUD_LOG_PATH):
        return jsonify({"status": "success", "logs": ["No log file found yet. Run an operation to start logging."]})
        
    try:
        with open(CRUD_LOG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        # Get last 20 lines
        tail = [line.strip() for line in lines[-20:]]
        return jsonify({"status": "success", "logs": tail})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/run-experiment/<name>', methods=['POST'])
def run_experiment(name):
    """
    Runs the specified experiment on the server, updates charts, and returns result data.
    """
    try:
        if name == "eventual":
            # Run Experiment 1 with 5 iterations quickly (to avoid keeping UI waiting)
            run_eventual_consistency_experiment(iterations=5)
            # Update the chart
            generate_all_plots()
            json_file = os.path.join(RESULTS_DIR, "eventual_results.json")
            
        elif name == "monotonic":
            # Run Experiment 2
            run_monotonic_reads_experiment()
            # Update the chart
            generate_all_plots()
            json_file = os.path.join(RESULTS_DIR, "monotonic_results.json")
            
        elif name == "raw":
            # Run Experiment 3 with 5 iterations
            run_read_after_write_experiment(iterations=5)
            # Update the chart
            generate_all_plots()
            json_file = os.path.join(RESULTS_DIR, "raw_results.json")
            
        elif name == "concurrent":
            # Run Experiment 4
            run_concurrent_writes_experiment()
            # Update the chart
            generate_all_plots()
            json_file = os.path.join(RESULTS_DIR, "concurrent_results.json")
            
        else:
            return jsonify({"status": "error", "message": "Unknown experiment name"}), 400
            
        # Read and return the results JSON file
        if os.path.exists(json_file):
            with open(json_file, "r", encoding="utf-8") as f:
                res_data = json.load(f)
            return jsonify({
                "status": "success",
                "experiment": name,
                "data": res_data
            })
        else:
            return jsonify({"status": "error", "message": "Experiment completed but results JSON not found"}), 500
            
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/table/<name>', methods=['GET'])
def get_full_table(name):
    """
    Returns the complete list of rows and columns for a given database table,
    allowing full inspection on the dashboard UI.
    """
    allowed_tables = ['movies', 'halls', 'seats', 'showtimes', 'reservations', 'replication_log']
    if name not in allowed_tables:
        return jsonify({"status": "error", "message": "Invalid table name"}), 400
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        if name == 'movies':
            cur.execute("SELECT id, title, genre, duration_min, version, last_updated, operation_id FROM movies ORDER BY id;")
            cols = ['id', 'title', 'genre', 'duration', 'version', 'last_updated', 'operation_id']
        elif name == 'halls':
            cur.execute("SELECT id, name, capacity, version, last_updated, operation_id FROM halls ORDER BY id;")
            cols = ['id', 'name', 'capacity', 'version', 'last_updated', 'operation_id']
        elif name == 'seats':
            cur.execute("""
                SELECT s.id, h.name, s.row_label || s.seat_number, s.version, s.last_updated, s.operation_id 
                FROM seats s 
                JOIN halls h ON s.hall_id = h.id 
                ORDER BY s.id;
            """)
            cols = ['id', 'hall', 'seat', 'version', 'last_updated', 'operation_id']
        elif name == 'showtimes':
            cur.execute("""
                SELECT st.id, m.title, h.name, st.show_date, st.show_time 
                FROM showtimes st 
                JOIN movies m ON st.movie_id = m.id 
                JOIN halls h ON st.hall_id = h.id 
                ORDER BY st.id;
            """)
            cols = ['id', 'movie', 'hall', 'date', 'time']
        elif name == 'reservations':
            cur.execute("""
                SELECT r.id, m.title, h.name, s.row_label || s.seat_number, r.customer_name, r.status, r.version, r.last_updated, r.operation_id
                FROM reservations r 
                JOIN showtimes st ON r.showtime_id = st.id 
                JOIN movies m ON st.movie_id = m.id 
                JOIN halls h ON st.hall_id = h.id 
                JOIN seats s ON r.seat_id = s.id 
                ORDER BY r.id;
            """)
            cols = ['id', 'movie', 'hall', 'seat', 'customer', 'status', 'version', 'last_updated', 'operation_id']
        elif name == 'replication_log':
            cur.execute("SELECT log_id, operation_type, table_name, record_id, details, timestamp, node FROM replication_log ORDER BY log_id DESC;")
            cols = ['log_id', 'op_type', 'table', 'record_id', 'details', 'timestamp', 'node']
            
        rows = cur.fetchall()
        
        # Serialize fields like timestamps or JSON dicts
        serialized_rows = []
        for r in rows:
            serialized_row = {}
            for idx, col in enumerate(cols):
                val = r[idx]
                if isinstance(val, datetime) or hasattr(val, 'isoformat'):
                    serialized_row[col] = str(val)
                elif col == 'details' and isinstance(val, dict):
                    serialized_row[col] = json.dumps(val)
                elif isinstance(val, dict):
                    serialized_row[col] = json.dumps(val)
                else:
                    serialized_row[col] = val
            serialized_rows.append(serialized_row)
            
        cur.close()
        conn.close()
        return jsonify({"status": "success", "columns": cols, "rows": serialized_rows})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ============================================================================
# APPLICATION BOOT
# ============================================================================

if __name__ == '__main__':
    # Verify results directory
    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)
        
    print("\n" + "=" * 60)
    print("CENG 465 - SINGLE-LEADER REPLICATION WEB DASHBOARD")
    print("=" * 60)
    print("  Server Address: http://localhost:5000")
    print("  Please connect to this address from your browser.")
    print("=" * 60 + "\n")
    
    # Run on 0.0.0.0 to allow external connections
    app.run(host='0.0.0.0', port=5000, debug=True)
