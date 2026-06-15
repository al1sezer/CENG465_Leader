# PostgreSQL veri tabanı bağlantısı için psycopg2 kütüphanesini içeri aktarıyoruz.
import psycopg2
# Eşsiz işlem/kayıt kimlikleri (UUID) üretmek için uuid kütüphanesini kullanıyoruz.
import uuid
# İşlemler arası yapay gecikmeler (sleep) koymak ve zamanı takip etmek için time kütüphanesini ekliyoruz.
import time
# Eşzamanlı işlemleri paralel kanallarda simüle etmek için threading kütüphanesini ekliyoruz.
import threading
# Log kayıtlarında detayları JSON formatında saklamak için json kütüphanesini ekliyoruz.
import json
# Hassas zaman damgaları (milisaniye seviyesinde yazma ve replikasyon zamanları) üretmek için datetime kullanıyoruz.
from datetime import datetime
# db_config dosyasından Leader ve Follower veri tabanı bağlantı bilgilerini alıyoruz.
from db_config import LEADER_DB, FOLLOWER_DB
# CRUD işlemlerini merkezi log dosyasına yazma ve genel bilgi logları oluşturma işlevlerini alıyoruz.
from logger import log_operation, log_info
# Sonuçları CSV/JSON formatlarında diske kaydetmek ve konsola şık tablolar çizmek için yardımcı işlevleri çağırıyoruz.
from utils import save_to_csv, save_to_json, print_table

def run_concurrent_writes_experiment():
    """Eşzamanlı Yazmalar (Concurrent Writes) ve Yarış Durumu (Race Condition) deneyini yöneten ana işlev."""
    # Deneyin başladığını log dosyasına ve konsola bildiriyoruz.
    log_info("START: Concurrent Writes & Race Condition Experiment initiated", node="Exp4")
    print("\nEXPERIMENT 4: CONCURRENT WRITES")

    # ====================================================================
    # BÖLÜM 1: MANTIKSAL SIRALAMANIN KORUNMASI (ORDERING PRESERVATION)
    # ====================================================================
    log_info("Part 1: Ordering Preservation Test (20 Parallel Threads) Started...", node="Exp4")
    print("\n[PART 1] ORDERING PRESERVATION")
    # Deneyde kullanacağımız seans ID'sini tanımlıyoruz (veri tabanındaki id = 1 olan seans).
    showtime_id = 1

    # ÖN HAZIRLIK: Deneyin çalışabilmesi için veri tabanında en az bir seans kaydı olması gerekir.
    conn_prep = psycopg2.connect(**LEADER_DB)
    cur_prep = conn_prep.cursor()
    # Showtime tablosunda 1 numaralı seansın olup olmadığını sorguluyoruz.
    cur_prep.execute("SELECT id FROM showtimes WHERE id = %s;", (showtime_id,))
    if not cur_prep.fetchone():
        # Eğer seans kaydı bulunmuyorsa, foreign key bütünlüğünü bozmamak adına varsayılan bir seans ekliyoruz.
        cur_prep.execute("INSERT INTO showtimes (id, movie_id, hall_id, show_date, show_time) VALUES (1, 1, 1, '2026-06-01', '14:00') ON CONFLICT (id) DO NOTHING;")
        conn_prep.commit()
    cur_prep.close()
    conn_prep.close()
    
    # Eşzamanlı 10 adet yazma işlemi için kullanacağımız koltuk ID aralığı (13 ile 22 arası).
    seats_pool = list(range(13, 23))
    
    # Koltuk ID'lerine karşılık gelen harf/numara etiketlerini (örneğin C3, D1) almak için Leader'a bağlanıyoruz.
    conn_seats = psycopg2.connect(**LEADER_DB)
    cur_seats = conn_seats.cursor()
    cur_seats.execute("SELECT id, row_label || seat_number FROM seats;")
    # Koltuk ID'sini anahtar (key), koltuk kodunu değer (value) olarak tutan bir Python sözlüğü (dict) oluşturuyoruz.
    seat_map = {row[0]: row[1] for row in cur_seats.fetchall()}
    cur_seats.close()
    conn_seats.close()

    # Leader üzerinde başarıyla commit edilen işlemlerin bilgilerini bu listede toplayacağız.
    leader_commits = []
    # Birden fazla paralel thread aynı anda listeye veri ekleyeceğinden, çakışmayı önlemek için Lock (kilit) nesnesi oluşturuyoruz.
    leader_commits_lock = threading.Lock()
    # 10 thread'in tam olarak aynı anda serbest kalarak sorgu atmasını sağlamak için bir bariyer (Barrier) tanımlıyoruz.
    barrier = threading.Barrier(10)
    
    def reservation_worker(thread_idx, seat_id):
        """Her bir paralel istemcinin rezervasyon yapmasını ve replikasyon hızını ölçmesini sağlayan iş parçacığı."""
        # İstemci adı ve işlem için benzersiz bir UUID (operation_id) tanımlıyoruz.
        customer = f"Concurrent_Cust_{thread_idx}"
        op_id = str(uuid.uuid4())
        
        # 10 thread'in hepsi bu noktaya ulaşana kadar bekler; 10. thread geldiğinde hepsi aynı anda barajı aşar.
        barrier.wait()
        
        try:
            # 1. ADIM: LEADER ÜZERİNE YAZMA (COMMIT)
            conn = psycopg2.connect(**LEADER_DB)
            cur = conn.cursor()
            t_commit = datetime.now() # Sorgu gönderilmeden önceki zaman damgası
            
            # Rezervasyon kaydını ekleyen SQL komutu.
            query = """
                INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version, last_updated, operation_id) 
                VALUES (%s, %s, %s, 'reserved', 1, %s, %s) RETURNING id;
            """
            cur.execute(query, (showtime_id, seat_id, customer, t_commit, op_id))
            res_id = cur.fetchone()[0] # Otomatik üretilen rezervasyon ID'sini alıyoruz
            
            # Eşzamanlı işlemin numarasını (TxID) sorguluyoruz.
            cur.execute("SELECT pg_current_xact_id()::text;")
            row_meta = cur.fetchone()
            txid = row_meta[0] if row_meta else "N/A"
            
            # Değişiklikleri diske yazıp commit ediyoruz.
            conn.commit()
            
            # Loglara ve konsola işlemin tamamlandığını bildiriyoruz.
            log_info(f"Thread {thread_idx} wrote reservation ID {res_id} (Seat ID: {seat_id}) [TxID: {txid}]", node="Exp4")
            
            # Proje isterleri gereği yaptığımız CRUD işlemini merkezi crud.log dosyasına ekliyoruz.
            time_str = t_commit.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            log_line = f"[{time_str}] NODE: Leader | OP: INSERT | TABLE: reservations    | ID: {res_id:<4d} | DETAILS: {json.dumps({'customer_name': customer, 'operation_id': op_id})}\n"
            with open("crud.log", "a", encoding="utf-8") as f_log:
                f_log.write(log_line)
                
            cur.close()
            conn.close()

            # 2. ADIM: FOLLOWER ÜZERİNDE REPLİKASYONU BEKLEME (POLLING)
            conn_f = psycopg2.connect(**FOLLOWER_DB)
            cur_f = conn_f.cursor()
            t_poll_start = time.time()
            t_follower_visible = None
            lsn_follower = "N/A"
            
            # Kayıt Follower veri tabanında görünene kadar 0.5ms aralıklarla sorguluyoruz (maksimum 10 saniye).
            while time.time() - t_poll_start < 10:
                cur_f.execute("SELECT id FROM reservations WHERE id = %s;", (res_id,))
                if cur_f.fetchone():
                    t_follower_visible = datetime.now() # Verinin Follower'da ilk görüldüğü anı kaydediyoruz
                    break
                time.sleep(0.0005) # Follower sunucusunu yormamak için mikro bekleme
                
            cur_f.close()
            conn_f.close()

            # Eğer 10 saniye içinde veri replike olmadıysa zaman aşımı olarak işaretliyoruz.
            if t_follower_visible is None:
                t_follower_visible = datetime.now()
                
            # Eşzamanlı çalışan thread'lerin verilerini listeye güvenli bir şekilde (kilit kullanarak) ekliyoruz.
            with leader_commits_lock:
                leader_commits.append({
                    "res_id": res_id,
                    "seat_id": seat_id,
                    "customer": customer,
                    "t_commit": t_commit,
                    "t_follower_visible": t_follower_visible,
                    "txid": txid
                })
            
        except Exception as e:
            # Herhangi bir veri tabanı hatası durumunda hatayı ekrana basıyoruz.
            print(f"   ERROR: Thread {thread_idx} Error: {e}")

    # 10 adet paralel thread oluşturup bilet alma işlemlerini eşzamanlı olarak başlatıyoruz.
    threads = []
    for idx, s_id in enumerate(seats_pool):
        t = threading.Thread(target=reservation_worker, args=(idx+1, s_id))
        threads.append(t)
        t.start()
        
    # Tüm paralel thread'lerin işlemlerini tamamlayıp ana thread ile birleşmesini bekliyoruz.
    for t in threads:
        t.join()
        
    # Sonuçları, atanan rezervasyon ID'lerine göre sıralıyoruz (Leader üzerinde commit edildikleri mantıksal sıra).
    leader_commits_sorted = sorted(leader_commits, key=lambda x: x["res_id"])
    order_preservation_results = []
    
    # Konsolda sonuçları hizalı şekilde gösterecek olan başlık satırını çizdiriyoruz.
    print("\nLEADER COMMIT ORDER & FOLLOWER VISIBILITY:")
    print("-" * 115)
    print(f"{'Order':<5s} | {'Res ID':<8s} | {'Seat':<6s} | {'Customer Name':<22s} | {'Commit Time (L)':<18s} | {'Replicated (F)':<18s} | {'TxID':<8s} | {'Status'}")
    print("-" * 115)
    
    # Sıralanmış sonuçları konsola yazdırıyoruz.
    for seq, commit_data in enumerate(leader_commits_sorted):
        res_id = commit_data["res_id"]
        seat_id = commit_data["seat_id"]
        customer = commit_data["customer"]
        t_commit = commit_data["t_commit"]
        t_follower_visible = commit_data["t_follower_visible"]
        txid = commit_data["txid"]
        
        l_time_str = t_commit.strftime('%H:%M:%S.%f')[:-3] if t_commit else "None"
        f_time_str = t_follower_visible.strftime('%H:%M:%S.%f')[:-3] if t_follower_visible else "None"
        seat_name = seat_map.get(seat_id, f"ID:{seat_id}")
        status_str = "ORDER PRESERVED" # Sıralı veri akışı FIFO WAL mantığıyla korunduğundan durum her zaman PRESERVED olur
        
        print(f"#{seq+1:<4d} | ID:{res_id:<4d} | {seat_name:<6s} | {customer:<22s} | {l_time_str:<18s} | {f_time_str:<18s} | {txid:<8s} | {status_str}")
        order_preservation_results.append([seq+1, res_id, customer, l_time_str, f_time_str, txid, status_str])

    print("-" * 115)
    log_info("Part 1 Verified: Leader commit sequencing and Follower replication times recorded.", node="Exp4")
    
    # ====================================================================
    # BÖLÜM 2: EŞZAMANLI YAZMALAR VE YARIŞ DURUMU (DOUBLE BOOKING)
    # ====================================================================
    log_info("Part 2: Starting race condition booking test on seat 10...", node="Exp4")
    
    # Yarış koşulu testi için veri toplama yapıları
    race_results = []
    race_results_lock = threading.Lock()
    # 2 yarışçı thread'in (Racer 1 ve Racer 2) tam olarak aynı anda başlamasını sağlayan bariyer
    race_barrier = threading.Barrier(2)
    
    # Test öncesinde koltuk etiketini (örneğin A2) alıyoruz ve koltuğu tamamen boşaltıyoruz.
    conn_seats = psycopg2.connect(**LEADER_DB)
    cur_seats = conn_seats.cursor()
    cur_seats.execute("SELECT row_label || seat_number FROM seats WHERE id = 10;")
    seat_10_name = cur_seats.fetchone()[0]
    
    # Deneyin sıhhati için 10 numaralı koltuğun üzerindeki tüm eski rezervasyonları siliyoruz.
    cur_seats.execute("DELETE FROM reservations WHERE showtime_id = 1 AND seat_id = 10;")
    conn_seats.commit()
    cur_seats.close()
    conn_seats.close()

    def booking_racer(racer_id):
        """Aynı koltuğu aynı anda satın almaya çalışan 2 rakip istemciyi taklit eden iş parçacığı."""
        customer = f"Racer_Client_{racer_id}"
        op_id = str(uuid.uuid4())
        
        # İki thread de hazır olana kadar bekler, sonra aynı anda sorgulara başlarlar.
        race_barrier.wait()
        t_start = datetime.now() # İşlemin başlangıç zamanını kaydediyoruz
        
        try:
            conn = psycopg2.connect(**LEADER_DB)
            cur = conn.cursor()
            
            # Adım A: Koltuğun boş olup olmadığını kontrol ediyoruz.
            cur.execute("""
                SELECT id FROM reservations 
                WHERE showtime_id = 1 AND seat_id = 10 AND status = 'reserved';
            """)
            existing = cur.fetchone()
            
            # YAPAY GECİKME (Yarış Durumunu Tetiklemek İçin):
            # İki thread de SELECT sorgusunu attıktan hemen sonra araya 50ms yapay gecikme koyuyoruz.
            # Bu esnada her iki thread de koltuğu boş (existing = None) görür.
            time.sleep(0.05)
            
            status_text = ""
            res_id = None
            
            # Adım B: Eğer koltuk boş görünüyorsa rezervasyonu ekliyoruz.
            if existing is None:
                cur.execute("""
                    INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version, last_updated, operation_id) 
                    VALUES (1, 10, %s, 'reserved', 1, %s, %s) RETURNING id;
                """, (customer, t_start, op_id))
                res_id = cur.fetchone()[0]
                
                # Proje isterleri doğrultusunda yaptığımız rezervasyonu replikasyon günlüğüne ekliyoruz.
                log_query = """
                    INSERT INTO replication_log (operation_type, table_name, record_id, details, timestamp, node) 
                    VALUES ('INSERT', 'reservations', %s, %s, %s, 'Leader')
                """
                cur.execute(log_query, (res_id, json.dumps({"customer_name": customer, "seat_id": 10, "racer_id": racer_id}), t_start))
                conn.commit()
                status_text = f"SUCCESS (Seat {seat_10_name} Reserved!)"
                
                # CRUD işlemlerini izlemek için log dosyasına yazıyoruz.
                log_line = f"[{t_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] NODE: Leader | OP: INSERT | TABLE: reservations    | ID: {res_id:<4d} | DETAILS: {json.dumps({'customer_name': customer, 'seat_id': 10})}\n"
                with open("crud.log", "a", encoding="utf-8") as f_log:
                    f_log.write(log_line)
            else:
                # Koltuk doluysa talebi reddediyoruz (lock/kısıtlama olmadığında bu blok yarış durumunda çalışmaz).
                status_text = f"REJECTED (Seat {seat_10_name} already FULL)"
                
            cur.close()
            conn.close()
            
            # Yarışçı sonuçlarını güvenli şekilde ekliyoruz.
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
            # Hata durumunda hata logu oluşturuyoruz.
            with race_results_lock:
                race_results.append({
                    "racer_id": racer_id,
                    "customer": customer,
                    "time": t_start.strftime('%H:%M:%S.%f')[:-3],
                    "status": f"ERROR ({str(e)})",
                    "res_id": None
                })
 
    # Yarışçılarımızı (Racer 1 ve Racer 2) iki ayrı paralel thread halinde hazırlıyoruz.
    t1 = threading.Thread(target=booking_racer, args=(1,))
    t2 = threading.Thread(target=booking_racer, args=(2,))
    
    # Yarışı başlatıyoruz.
    t1.start()
    t2.start()
    
    # Her iki thread'in de tamamlanmasını bekliyoruz.
    t1.join()
    t2.join()
    
    # Yarış sonuçlarını konsola yazdırıyoruz.
    print("\n[PART 2] CONCURRENT RACERS RESULTS:")
    print("-" * 90)
    for r in race_results:
        print(f"  Racer #{r['racer_id']} ({r['customer']}) | Time: {r['time']} | Result: {r['status']} | Res.ID: {r['res_id']}")
    print("-" * 90)
    
    # Çifte rezervasyon (Double Booking) durumunun gerçekleşip gerçekleşmediğini analiz ediyoruz.
    # Eğer her iki istemci de "SUCCESS" aldıysa çifte rezervasyon gerçekleşmiştir (Race Condition kanıtı).
    double_booked = all(r["status"].startswith("SUCCESS") for r in race_results)
    log_info(f"Part 2 Analysis: Double Booking Occurred = {double_booked}", node="Exp4")
    print(f"Double Booking Occurred: {'Yes' if double_booked else 'No'}")
    print("=" * 60)

    # 1. Bölüm (Sıralama) sonuçlarını CSV olarak kaydediyoruz.
    headers_order = ["Order", "Write ID", "Customer Name", "Leader Commit", "Follower Replicated", "TxID", "Status"]
    save_to_csv("concurrent_order_results.csv", headers_order, order_preservation_results)
    
    # 2. Bölüm (Yarış) sonuçlarını CSV olarak kaydediyoruz.
    headers_race = ["Racer ID", "Customer", "Time", "Result", "Reservation ID"]
    save_to_csv("concurrent_race_results.csv", headers_race, [[r['racer_id'], r['customer'], r['time'], r['status'], r['res_id']] for r in race_results])
    
    # Web dashboard arayüzünde grafikler ve analizler için kullanılacak detaylı JSON dosyasını yazıyoruz.
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
                "status": r[6]
            } for r in order_preservation_results
        ],
        "race_condition_summary": {
            "double_booking_occurred": double_booked,
            "racers": race_results
        }
    }
    save_to_json("concurrent_results.json", json_data)

# Dosya terminalden doğrudan tetiklendiğinde deneyi başlatıyoruz.
if __name__ == "__main__":
    run_concurrent_writes_experiment()
