import psycopg2
from db_config import LEADER_DB

def view_database():
    conn = psycopg2.connect(**LEADER_DB)
    cur = conn.cursor()

    print("=" * 60)
    print("  SINEMA BILET REZERVASYON SISTEMI - VERITABANI DURUMU")
    print("=" * 60)

    # Movies
    print("\nMOVIES (Filmler)")
    print("-" * 40)
    cur.execute("SELECT id, title, genre, duration_min, version FROM movies ORDER BY id")
    for r in cur.fetchall():
        print(f"  ID:{r[0]} | {r[1]} | {r[2]} | {r[3]} dk | v{r[4]}")

    # Halls
    print("\n HALLS (Salonlar)")
    print("-" * 40)
    cur.execute("SELECT id, name, capacity, version FROM halls ORDER BY id")
    for r in cur.fetchall():
        print(f"  ID:{r[0]} | {r[1]} | Kapasite: {r[2]} | v{r[3]}")

    # Seats count
    print("\nSEATS (Koltuklar)")
    print("-" * 40)
    cur.execute("SELECT h.name, count(s.id) FROM halls h JOIN seats s ON s.hall_id = h.id GROUP BY h.name ORDER BY h.name")
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]} koltuk")

    # Showtimes
    print("\nSHOWTIMES (Seanslar)")
    print("-" * 40)
    cur.execute("""SELECT st.id, m.title, h.name, st.show_date, st.show_time 
                   FROM showtimes st JOIN movies m ON st.movie_id = m.id 
                   JOIN halls h ON st.hall_id = h.id ORDER BY st.show_date, st.show_time""")
    for r in cur.fetchall():
        print(f"  ID:{r[0]} | {r[1]} | {r[2]} | {r[3]} {r[4]}")

    # Reservations
    print("\nRESERVATIONS (Rezervasyonlar)")
    print("-" * 40)
    cur.execute("""SELECT r.id, m.title, h.name, s.row_label || s.seat_number, 
                   r.customer_name, r.status, r.version
                   FROM reservations r JOIN showtimes st ON r.showtime_id = st.id 
                   JOIN movies m ON st.movie_id = m.id JOIN halls h ON st.hall_id = h.id 
                   JOIN seats s ON r.seat_id = s.id ORDER BY r.id""")
    for r in cur.fetchall():
        print(f"  ID:{r[0]} | {r[1]} | {r[2]} | Koltuk:{r[3]} | {r[4]} | {r[5]} | v{r[6]}")

    # Replication Log
    print("\nREPLICATION LOG")
    print("-" * 40)
    cur.execute("SELECT log_id, operation_type, table_name, record_id, node FROM replication_log ORDER BY log_id")
    for r in cur.fetchall():
        print(f"  #{r[0]} | {r[1]:6s} | {r[2]:15s} | RecID:{r[3]} | {r[4]}")

    # Summary
    print("\n" + "=" * 60)
    cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")
    print(f"  Toplam tablo: {cur.fetchone()[0]}")
    cur.execute("SELECT count(*) FROM movies")
    print(f"  Film sayisi: {cur.fetchone()[0]}")
    cur.execute("SELECT count(*) FROM seats")
    print(f"  Koltuk sayisi: {cur.fetchone()[0]}")
    cur.execute("SELECT count(*) FROM showtimes")
    print(f"  Seans sayisi: {cur.fetchone()[0]}")
    cur.execute("SELECT count(*) FROM reservations")
    print(f"  Rezervasyon sayisi: {cur.fetchone()[0]}")
    print("=" * 60)

    cur.close()
    conn.close()

if __name__ == "__main__":
    view_database()
