# CENG 465 - Dağıtık Sistemler Replikasyon Deneyleri Kod Açıklamaları

Bu döküman, projenizde yer alan dört tutarlılık deneyinin python kodlarını (`experiment_eventual.py`, `experiment_monotonic.py`, `experiment_read_after_write.py`, `experiment_concurrent.py`) ve bu deneylerdeki kritik mantıksal blokları satır bazlı olarak açıklamaktadır.

---

## 1. Deney 1: Eventual Consistency (Replikasyon Gecikmesi)
**Dosya:** `experiment_eventual.py`

Bu deneyin amacı, Leader veri tabanına yazılan (INSERT), güncellenen (UPDATE) ve silinen (DELETE) kayıtların asenkron replikasyon akışıyla Follower veri tabanına ne kadar sürede yansıdığını milisaniye seviyesinde hassas bir şekilde ölçmektir. Kod, hem tek kullanımlık (bağlantı aç/kapa) hem de kalıcı (persistent) veri tabanı bağlantı modlarını (`use_persistent`) destekler.

### A. Film Kaydı Ekleme (INSERT) Gecikmesi Ölçümü
```python
# 54-72. satırlar: Kaydın Leader veri tabanına yazılması
movie_title_insert = f"{movie_title_base}_INSERT"
t_insert_time = datetime.now()
query_insert = """
    INSERT INTO movies (title, genre, duration_min, version, last_updated, operation_id) 
    VALUES (%s, %s, %s, 1, %s, %s) RETURNING id;
"""
cur_l.execute(query_insert, (movie_title_insert, genre, duration, t_insert_time, operation_id))
record_id = cur_l.fetchone()[0]
conn_l.commit()
t_write_committed = datetime.now()
```
*   **Açıklama:** Leader düğümünde SQL `INSERT` sorgusu çalıştırılır. Replikasyon takibini sağlayan `version=1`, o anki zaman damgası `last_updated=t_insert_time` ve benzersiz `operation_id` parametreleri gönderilir. İşlem `commit` edildikten hemen sonra Leader tarafındaki kesin yazma zaman damgası `t_write_committed` değişkenine atanır.

```python
# 89-100. satırlar: Follower veri tabanı polling (tarama) döngüsü
t_poll_start = time.time()
t_visible = None
attempts = 0
while time.time() - t_poll_start < 15:  # 15 saniyelik zaman aşımı süresi
    attempts += 1
    cur_f.execute("SELECT last_updated FROM movies WHERE id = %s;", (record_id,))
    row = cur_f.fetchone()
    if row:
        t_visible = datetime.now()
        break
    time.sleep(0.001)  # 1 milisaniye aralıklarla tarama
```
*   **Açıklama:** Yazma işlemi bittiği an Follower üzerinde tarama döngüsü başlatılır. Follower DB'ye `SELECT` sorgusu atılarak kaydın gelip gelmediği sorgulanır. `time.sleep(0.001)` ile bekleme süresi 1ms tutularak replikasyon hızı en yüksek doğrulukla yakalanır. Kayıt Follower'da görüldüğü an `t_visible` kaydedilir.

```python
# 112. satır: INSERT replikasyon gecikmesinin (lag) hesaplanması
lag_ms = (t_visible - t_write_committed).total_seconds() * 1000.0
```
*   **Açıklama:** Follower'da görünme zamanı (`t_visible`) ile Leader commit zamanı (`t_write_committed`) arasındaki fark saniye cinsinden bulunup milisaniyeye (ms) dönüştürülür.

### B. Film Kaydı Güncelleme (UPDATE) Gecikmesi Ölçümü
```python
# 141-147. satırlar: Kaydın Leader üzerinde güncellenmesi
t_update_time = datetime.now()
query_update = """
    UPDATE movies SET title = %s, version = 2, last_updated = %s, operation_id = %s WHERE id = %s;
"""
cur_l.execute(query_update, (movie_title_update, t_update_time, operation_id, record_id))
conn_l.commit()
t_update_committed = datetime.now()
```
*   **Açıklama:** Mevcut film kaydı `version=2` yapılarak güncellenir ve Leader üzerindeki commit zamanı `t_update_committed` olarak kaydedilir.

```python
# 164-174. satırlar: Güncellemenin Follower'a yansımasının izlenmesi
while time.time() - t_poll_start < 15:
    attempts += 1
    cur_f.execute("SELECT version, title FROM movies WHERE id = %s;", (record_id,))
    row = cur_f.fetchone()
    if row and row[0] == 2 and row[1] == movie_title_update:
        t_visible_update = datetime.now()
        break
    time.sleep(0.001)
```
*   **Açıklama:** Follower taranırken sadece kaydın varlığı değil, `version` alanının `2` olması ve film başlığının yeni güncellenen değer olması aranır. Bu şartlar sağlandığında güncellemenin Follower'a ulaştığı anlaşılır ve yansıma anı `t_visible_update` olarak kaydedilir. Gecikme hesaplaması `(t_visible_update - t_update_committed)` farkı alınarak yapılır.

### C. Film Kaydı Silme (DELETE) Gecikmesi Ölçümü
```python
# 216-219. satırlar: Kaydın Leader üzerinde silinmesi
query_delete = "DELETE FROM movies WHERE id = %s;"
cur_l.execute(query_delete, (record_id,))
conn_l.commit()
t_delete_committed = datetime.now()
```
*   **Açıklama:** İlgili film kaydı Leader üzerinden silinir ve silme onay anı `t_delete_committed` olarak saklanır.

```python
# 236-246. satırlar: Silme işleminin Follower'a yansımasının izlenmesi
while time.time() - t_poll_start < 15:
    attempts += 1
    cur_f.execute("SELECT id FROM movies WHERE id = %s;", (record_id,))
    row = cur_f.fetchone()
    if not row:  # Kayıt veri tabanından silindiğinde sorgu boş döner
        t_visible_delete = datetime.now()
        break
    time.sleep(0.001)
```
*   **Açıklama:** Follower veri tabanı taranarak kaydın silinip silinmediği denetlenir. `cur_f.fetchone()` sonucu `None` döndüğü an silme işleminin Follower'a yansıdığı kanıtlanır ve yansıma zamanı `t_visible_delete` olarak kaydedilir. Gecikme `(t_visible_delete - t_delete_committed)` formülü ile hesaplanır.

---

## 2. Deney 2: Monotonic Reads (Monotonik Okumalar)
**Dosya:** `experiment_monotonic.py`

Bu deney, bir istemcinin önce güncel olan Leader düğümünden okuma yapıp, hemen ardından replikasyonu asenkron olarak geriden takip eden Follower düğümünden okuma yaptığında verinin versiyonunun geriye gitmesi (stale read / zaman yolculuğu anomalisi) durumunu kanıtlamaktadır.

### A. Arka Plan Yazıcı İşlemi (Background Writer)
```python
# 58-87. satırlar: Eşzamanlı güncellemeler üreten thread
def writer_thread():
    conn = psycopg2.connect(**LEADER_DB)
    cur = conn.cursor()
    for version in range(2, 12):  # v2'den v11'e kadar 10 adet güncelleme yapar
        op_id = str(uuid.uuid4())
        ts = datetime.now()
        query = """
            UPDATE reservations 
            SET customer_name = %s, version = %s, last_updated = %s, operation_id = %s 
            WHERE id = %s;
        """
        cur.execute(query, (f"Client V{version}", version, ts, op_id, res_id))
        conn.commit()
        time.sleep(0.15)  # Her güncelleme arasında 150ms yapay uyku süresi
```
*   **Açıklama:** Ayrı bir thread içerisinde çalıştırılan bu işlev, Leader veri tabanındaki rezervasyon kaydını 150ms aralıklarla günceller. Her adımda `version` değerini bir artırarak `v2`'den `v11`'e kadar ilerletir.

### B. Tek Bir Follower Düğümünden Ardışık Okuma (TEST A)
```python
# 105-123. satırlar: Single Node Monotonik Okuma Kontrolü
cur_f.execute("SELECT version, customer_name FROM reservations WHERE id = %s;", (res_id,))
row_f = cur_f.fetchone()
f_version = row_f[0] if row_f else 0
violation_follower = "VIOLATION" if f_version < last_seen_follower_version else "NORMAL"
last_seen_follower_version = max(last_seen_follower_version, f_version)
```
*   **Açıklama:** İstemci sadece Follower düğümüne bağlanarak okuma döngüsü çalıştırır. Follower veri tabanı replikasyon akışını sıralı (FIFO) işlediği için versiyon numarası her okumada ya aynı kalır ya da artar; asla geriye düşmez. Bu nedenle Test A'da hiçbir monotonik okuma ihlali (0 violation) gerçekleşmez.

### C. Leader -> Follower Çapraz Düğüm Okuması (TEST B)
```python
# 125-147. satırlar: Cross-Node Replikasyon Gecikmesi İhlal Simülasyonu
# 1. Okuma: Önce en güncel veriyi almak için Leader veri tabanına sorgu atılır
cur_l_read.execute("SELECT version FROM reservations WHERE id = %s;", (res_id,))
row_l = cur_l_read.fetchone()
l_version = row_l[0] if row_l else 0

# 2. Okuma: Hemen ardından Follower veri tabanından sorgulanır
cur_f.execute("SELECT version FROM reservations WHERE id = %s;", (res_id,))
row_f_immediate = cur_f.fetchone()
f_immediate_version = row_f_immediate[0] if row_f_immediate else 0

# İhlal Kontrolü: Follower versiyonu Leader'dan okunan versiyondan küçükse ihlal vardır!
violation_cross = "VIOLATION (Monotonic Reads Violation!)" if f_immediate_version < l_version else "NORMAL"
```
*   **Açıklama:** İstemci önce Leader'dan okuyarak güncel veriyi (`l_version`) çeker, hemen ardından Follower'dan okur (`f_immediate_version`). Replikasyon henüz Follower düğümüne ulaşmadığı gecikme pencerelerinde, Follower istemciye eski versiyonu döner. İstemci güncel veriyi gördükten sonra eski veriyi gördüğü için **Monotonic Reads İhlali** gerçekleşir. Okumalar `time.sleep(0.01)` ile 10ms aralıklarla tekrarlanır.

---

## 3. Deney 3: Read-After-Write Consistency (Yazdığını Hemen Okuma)
**Dosya:** `experiment_read_after_write.py`

Bu deney, bir kullanıcının veri tabanına bir rezervasyon kaydettiğinde, bunu yazma yaptığı sunucuda (Leader) anında görebildiğini (RAW garantisi), ancak okuma yaptığı sunucu (Follower) replikasyon gerisinde kaldığında kendi yazdığı kaydı ilk okumada göremediğini (RAW ihlali) kronolojik bir sorgu izleme (trace) mekanizmasıyla kanıtlar.

### A. Leader Üzerine Yazma ve Zaman Kaydı
```python
# 53-62. satırlar: Rezervasyonun Leader'a yazılması
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
```
*   **Açıklama:** Koltuk rezervasyonu Leader veri tabanına kaydedilir. İşlemin commit edildiği an `t_commit` olarak milisaniye hassasiyetinde tutulur.

### B. Eşzamanlı Leader ve Follower Okuma Thread'leri
Sorgulama gecikmesini ve iş parçacığı senkronizasyonunu hatasız ölçmek amacıyla paralel çalışan iki thread başlatılır:

```python
# 72-102. satırlar: Leader Görünürlük Kontrolü (Thread 1)
def check_leader_visibility():
    t_start = datetime.now()
    cur_l_check.execute("SELECT customer_name FROM reservations WHERE id = %s;", (res_id,))
    row_l = cur_l_check.fetchone()
    t_end = datetime.now()
    leader_res['visible'] = (row_l is not None)
    leader_res['lag_ms'] = (t_end - t_commit).total_seconds() * 1000.0
```
*   **Açıklama:** İstemci, kaydı gönderdiği Leader sunucusuna anında okuma sorgusu gönderir. Leader veriyi kendi diskinden anında getirdiği için `visible` her zaman True döner ve Leader üzerindeki gecikme sıfıra yakın (genelde <1ms) çıkar.

```python
# 104-167. satırlar: Follower Görünürlük ve Polling Kontrolü (Thread 2)
def check_follower_visibility():
    t_read_first = datetime.now()
    cur_f_check.execute("SELECT customer_name FROM reservations WHERE id = %s;", (res_id,))
    row_f_first = cur_f_check.fetchone()
    t_read_first_end = datetime.now()
    first_visible = (row_f_first is not None)
    
    # Eger ilk okumada veri bulunamazsa, Follower taranmaya baslanir (Polling)
    attempts = 1
    t_poll_start = time.time()
    visible_time = None
    if first_visible:
        visible_time = t_read_first_end
    else:
        while time.time() - t_poll_start < 10:
            attempts += 1
            cur_f_check.execute("SELECT customer_name FROM reservations WHERE id = %s;", (res_id,))
            row_f = cur_f_check.fetchone()
            if row_f:
                visible_time = datetime.now()
                break
            time.sleep(0.0005)  # 0.5ms frekansta arama
```
*   **Açıklama:** Eşzamanlı çalışan ikinci thread, Follower düğümünü okur. Eğer ilk okuma anında veri Follower'da yoksa (`first_visible = False`), bu anında bir **RAW İhlali** olarak kaydedilir. Veri görünür olana kadar Follower 0.5 milisaniye aralıklarla sorgulanarak asenkron replikasyon gecikmesinin kesin süresi (`follower_res['lag_ms']`) saptanır.

---

## 4. Deney 4: Concurrent Writes & Conflict (Eşzamanlı Yazmalar ve Çakışma)
**Dosya:** `experiment_concurrent.py`

Bu deney iki bölümden oluşur: Global Sıralamanın replikasyon sırasında aynen korunduğunun doğrulanması (Part 1) ve uygulama düzeyinde yetersiz kilit kullanımı sonucu ortaya çıkan Çifte Rezervasyon (Double Booking) çakışması (Part 2).

### Part 1: Global Sıralama Doğrulaması (Global Ordering Verification)
```python
# 45-59. satırlar: 10 paralel thread ve baslangic bariyeri
seats_pool = list(range(13, 23))  # 10 adet farkli koltuk ID'si havuzu
barrier = threading.Barrier(10)   # 10 thread'i aynı anda baslatacak senkronizasyon bariyeri
```
*   **Açıklama:** 10 istemcinin aynı anda istek atmasını sağlamak amacıyla thread'ler bir bariyerde kilitlenir. Bariyer açıldığında tüm thread'ler veri tabanına paralel olarak istek gönderir.

```python
# 61-120. satırlar: Rezervasyon işçi fonksiyonu ve LSN / TxID takibi
def reservation_worker(thread_idx, seat_id):
    barrier.wait()  # 10 thread'in birlesme noktası
    conn = psycopg2.connect(**LEADER_DB)
    cur = conn.cursor()
    t_commit = datetime.now()
    
    # Leader'a rezervasyon insert edilir
    cur.execute(query_insert, (showtime_id, seat_id, customer, t_commit, op_id))
    res_id = cur.fetchone()[0]
    
    # Leader'daki anlık Log Sequence Number (LSN) ve Transaction ID (TxID) bilgisi alınır
    cur.execute("SELECT pg_current_wal_lsn(), txid_current();")
    lsn_leader, txid = cur.fetchone()
    conn.commit()
    
    # Follower veri tabanı 0.5ms aralıklarla taranarak verinin yansıdıgı LSN ve zaman yakalanır
    conn_f = psycopg2.connect(**FOLLOWER_DB)
    cur_f = conn_f.cursor()
    t_poll_start = time.time()
    t_follower_visible = None
    lsn_follower = None
    while time.time() - t_poll_start < 10:
        cur_f.execute("SELECT last_updated, pg_last_wal_replay_lsn() FROM reservations WHERE id = %s;", (res_id,))
        row_f = cur_f.fetchone()
        if row_f:
            t_follower_visible = datetime.now()
            lsn_follower = row_f[1]
            break
        time.sleep(0.0005)
```
*   **Açıklama:** Her thread Leader üzerinde rezervasyon kaydını oluşturup, Leader'ın o an ürettiği WAL Sıra Numarasını (`pg_current_wal_lsn()`) ve işlem numarasını (`txid_current()`) alır. Ardından hemen Follower düğümünü sorgulayarak replikasyonun tamamlandığı andaki Follower LSN bilgisini (`pg_last_wal_replay_lsn()`) kaydeder.
*   **Sonuç:** Veri tabanından dönen sıralı birincil anahtarlar (`id` sırası), Leader commit sırası ve Follower'a yansıma sıraları karşılaştırılır. PostgreSQL replikasyon akışı WAL stream üzerinden FIFO (İlk Giren İlk Çıkar) çalıştığı için **Global Replikasyon Sıralaması** kusursuz şekilde korunur.

### Part 2: Yarış Durumu ve Çifte Rezervasyon (Double Booking)
```python
# 163-184. satırlar: Yarış durumunu tetikleyen Select-then-Insert mantığı
def booking_racer(racer_id):
    # Eşzamanlı baslatmak için 2'lik bariyerde beklenir
    race_barrier.wait()
    t_start = datetime.now()
    
    conn = psycopg2.connect(**LEADER_DB)
    cur = conn.cursor()
    
    # Uygulama düzeyinde yapılan güvensiz doluluk kontrolü
    cur.execute("""
        SELECT id FROM reservations 
        WHERE showtime_id = 1 AND seat_id = 10 AND status = 'reserved';
    """)
    existing = cur.fetchone()
    
    # Yarış koşulunu (race condition) simüle etmek için 50ms yapay uyku
    time.sleep(0.05)
```
*   **Açıklama:** İki farklı istemci thread'i aynı koltuğu (Koltuk B5, ID 10) rezerve etmek için aynı anda çalışır. İki işlem de önce `SELECT` sorgusu atarak koltuğun durumunu kontrol eder. Araya konan 50ms uyku süresi nedeniyle her iki thread de sorguyu eşzamanlı çalıştırır ve koltuğu boş (`existing is None`) görür.

```python
# 202-214. satırlar: İki thread'in de koltuğu rezerve etmesi (Double Booking)
if existing is None:
    cur.execute("""
        INSERT INTO reservations (showtime_id, seat_id, customer_name, status, version, last_updated, operation_id) 
        VALUES (1, 10, %s, 'reserved', 1, %s, %s) RETURNING id;
    """, (customer, t_start, op_id))
    conn.commit()
```
*   **Açıklama:** Koltuk durumunu boş gören iki işlem de `INSERT` sorgusunu çalıştırarak commit eder. Veri tabanında kilit (`SELECT FOR UPDATE`) veya eşsizlik kısıtı (`UNIQUE constraint`) kullanılmadığından, veri tabanı iki kaydı da kabul eder. Sonuç olarak aynı koltuk için iki farklı müşteriye ait iki aktif rezervasyon oluşur (Çifte Rezervasyon Anomalisi).
