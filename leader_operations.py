import psycopg2
import uuid
from datetime import datetime
from db_config import LEADER_DB
from logger import log_operation

def execute_query(query, params=(), fetch_one=False):
    """
    Helper function: executes SQL queries, commits, and closes the connection.
    """
    conn = psycopg2.connect(**LEADER_DB)
    cur = conn.cursor()
    cur.execute(query, params)
    
    result = None
    if fetch_one:
        result = cur.fetchone()
        
    conn.commit()
    cur.close()
    conn.close()
    return result

# ============================================================================
# MOVIE CRUD OPERATIONS
# ============================================================================

def add_movie(title, genre, duration_min):
    """
    Adds a new movie and inserts an INSERT record into replication_log table.
    """
    operation_id = str(uuid.uuid4())
    timestamp = datetime.now()
    version = 1
    
    query = """
        INSERT INTO movies (title, genre, duration_min, version, last_updated, operation_id) 
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
    """
    result = execute_query(query, (title, genre, duration_min, version, timestamp, operation_id), fetch_one=True)
    record_id = result[0]
    
    details = {
        "title": title,
        "genre": genre,
        "duration_min": duration_min,
        "operation_id": operation_id,
        "version": version
    }
    
    log_operation("INSERT", "movies", record_id, details)
    print(f"[MOVIE ADDED] ID: {record_id} | Title: {title} | Genre: {genre} | Duration: {duration_min} min | Version: {version} | OpID: {operation_id}")
    return record_id

def update_movie(movie_id, new_title):
    """
    Updates the title of an existing movie, increments its version, and records an UPDATE.
    """
    operation_id = str(uuid.uuid4())
    timestamp = datetime.now()
    
    # Use UPDATE ... RETURNING to retrieve the current version and increment it by 1
    query = """
        UPDATE movies 
        SET title = %s, version = version + 1, last_updated = %s, operation_id = %s 
        WHERE id = %s RETURNING version;
    """
    result = execute_query(query, (new_title, timestamp, operation_id, movie_id), fetch_one=True)
    
    if result is None:
        print(f"WARNING: [MOVIE UPDATE FAILED] Movie ID: {movie_id} not found!")
        return None
        
    new_version = result[0]
    details = {
        "new_title": new_title,
        "operation_id": operation_id,
        "version": new_version
    }
    
    log_operation("UPDATE", "movies", movie_id, details)
    print(f"[MOVIE UPDATED] ID: {movie_id} | New Title: {new_title} | New Version: {new_version} | OpID: {operation_id}")
    return new_version

def delete_movie(movie_id):
    """
    Deletes a movie and records a DELETE in replication_log.
    """
    # Fetch the movie details first for logging purposes before deletion
    select_query = "SELECT title FROM movies WHERE id = %s;"
    select_result = execute_query(select_query, (movie_id,), fetch_one=True)
    
    if select_result is None:
        print(f"WARNING: [MOVIE DELETE FAILED] Movie ID: {movie_id} not found!")
        return False
        
    title = select_result[0]
    
    query = "DELETE FROM movies WHERE id = %s RETURNING id;"
    result = execute_query(query, (movie_id,), fetch_one=True)
    
    details = {
        "deleted_title": title,
        "movie_id": movie_id
    }
    
    log_operation("DELETE", "movies", movie_id, details)
    print(f"[MOVIE DELETED] ID: {movie_id} | Title: {title}")
    return True

# ============================================================================
# SHOWTIME CRUD OPERATIONS
# ============================================================================

def add_showtime(movie_id, hall_id, show_date, show_time):
    """
    Creates a new showtime and inserts an INSERT record into replication_log table.
    """
    operation_id = str(uuid.uuid4())
    timestamp = datetime.now()
    version = 1
    
    query = """
        INSERT INTO showtimes (movie_id, hall_id, show_date, show_time, version, last_updated, operation_id) 
        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
    """
    result = execute_query(query, (movie_id, hall_id, show_date, show_time, version, timestamp, operation_id), fetch_one=True)
    record_id = result[0]
    
    details = {
        "movie_id": movie_id,
        "hall_id": hall_id,
        "show_date": str(show_date),
        "show_time": str(show_time),
        "operation_id": operation_id,
        "version": version
    }
    
    log_operation("INSERT", "showtimes", record_id, details)
    print(f"[SHOWTIME ADDED] ID: {record_id} | MovieID: {movie_id} | HallID: {hall_id} | Date: {show_date} | Time: {show_time} | Version: {version}")
    return record_id

# ============================================================================
# RESERVATION CRUD OPERATIONS
# ============================================================================

def create_reservation(showtime_id, seat_id, customer_name):
    """
    Creates a new ticket reservation and inserts an INSERT record into replication_log table.
    """
    operation_id = str(uuid.uuid4())
    timestamp = datetime.now()
    version = 1
    
    # Check if the seat is already reserved
    check_query = """
        SELECT id FROM reservations 
        WHERE showtime_id = %s AND seat_id = %s AND status = 'reserved';
    """
    check_result = execute_query(check_query, (showtime_id, seat_id), fetch_one=True)
    if check_result is not None:
        print(f"WARNING: [RESERVATION FAILED] Seat is already reserved! (Seat ID: {seat_id}, Showtime ID: {showtime_id})")
        return None
        
    query = """
        INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version, last_updated, operation_id) 
        VALUES (%s, %s, %s, 'reserved', %s, %s, %s) RETURNING id;
    """
    result = execute_query(query, (showtime_id, seat_id, customer_name, version, timestamp, operation_id), fetch_one=True)
    record_id = result[0]
    
    details = {
        "showtime_id": showtime_id,
        "seat_id": seat_id,
        "customer_name": customer_name,
        "status": "reserved",
        "operation_id": operation_id,
        "version": version
    }
    
    log_operation("INSERT", "reservations", record_id, details)
    print(f"[RESERVATION CREATED] ID: {record_id} | Customer: {customer_name} | ShowtimeID: {showtime_id} | SeatID: {seat_id} | Version: {version}")
    return record_id

def update_reservation(reservation_id, new_status):
    """
    Updates reservation status (reserved/cancelled) and records an UPDATE.
    """
    operation_id = str(uuid.uuid4())
    timestamp = datetime.now()
    
    query = """
        UPDATE reservations 
        SET status = %s, version = version + 1, last_updated = %s, operation_id = %s 
        WHERE id = %s RETURNING version;
    """
    result = execute_query(query, (new_status, timestamp, operation_id, reservation_id), fetch_one=True)
    
    if result is None:
        print(f"WARNING: [RESERVATION UPDATE FAILED] Reservation ID: {reservation_id} not found!")
        return None
        
    new_version = result[0]
    details = {
        "new_status": new_status,
        "operation_id": operation_id,
        "version": new_version
    }
    
    log_operation("UPDATE", "reservations", reservation_id, details)
    print(f"[RESERVATION UPDATED] ID: {reservation_id} | New Status: {new_status} | New Version: {new_version} | OpID: {operation_id}")
    return new_version

def cancel_reservation(reservation_id):
    """
    Cancels a reservation (sets status to 'cancelled') and records an UPDATE.
    """
    return update_reservation(reservation_id, "cancelled")

def delete_reservation(reservation_id):
    """
    Completely deletes a reservation record and records a DELETE in replication_log.
    """
    # Fetch details of the reservation to be deleted for logging purposes
    select_query = "SELECT customer_name, showtime_id, seat_id FROM reservations WHERE id = %s;"
    select_result = execute_query(select_query, (reservation_id,), fetch_one=True)
    
    if select_result is None:
        print(f"WARNING: [RESERVATION DELETE FAILED] Reservation ID: {reservation_id} not found!")
        return False
        
    customer, showtime_id, seat_id = select_result
    
    query = "DELETE FROM reservations WHERE id = %s RETURNING id;"
    result = execute_query(query, (reservation_id,), fetch_one=True)
    
    details = {
        "deleted_customer": customer,
        "showtime_id": showtime_id,
        "seat_id": seat_id,
        "reservation_id": reservation_id
    }
    
    log_operation("DELETE", "reservations", reservation_id, details)
    print(f"[RESERVATION DELETED] ID: {reservation_id} | Customer: {customer} | ShowtimeID: {showtime_id}")
    return True

# ============================================================================
# INTERACTIVE TEST BLOCK
# ============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("CENG 465 - LEADER CRUD OPERATIONS TEST")
    print("=" * 60)
    
    # 1. MOVIE INSERT TEST
    input("\n[1/5] Press ENTER to start Movie Insertion (INSERT) test...")
    movie_id = add_movie("The Matrix Resurrections", "Sci-Fi", 148)
    
    # 2. MOVIE UPDATE TEST
    input("\n[2/5] Press ENTER to start Movie Update (UPDATE) test...")
    update_movie(movie_id, "The Matrix Resurrections (Updated)")
    
    # 3. SHOWTIME INSERT TEST
    input("\n[3/5] Press ENTER to start Showtime Insertion (INSERT) test...")
    # Add showtime using Hall A (ID: 1)
    showtime_id = add_showtime(movie_id, 1, "2026-06-10", "21:00")
    
    # 4. RESERVATION TESTS
    input("\n[4/5] Press ENTER to start Reservation Creation & Cancellation test...")
    # Reserve seat A5 in Hall A (ID: 5)
    res_id = create_reservation(showtime_id, 5, "Ahmet Yurt")
    
    # Cancel the reservation (status = cancelled, version + 1)
    input("\n   -> Press ENTER to cancel the reservation...")
    cancel_reservation(res_id)
    
    # 5. DELETE TESTS
    input("\n[5/5] Press ENTER to start Delete (DELETE) tests...")
    
    # First delete the reservation
    delete_reservation(res_id)
    
    # Then delete the movie we added (showtime will be deleted due to CASCADE)
    delete_movie(movie_id)
    
    print("\n" + "=" * 60)
    print("ALL CRUD TESTS COMPLETED SUCCESSFULLY!")
    print("Please check the local 'crud.log' file and the replication_log table.")
    print("=" * 60 + "\n")
