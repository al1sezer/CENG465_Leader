# CENG 465 - Dağıtık Sistemler Single-Leader Replikasyon Deneyleri Kılavuzu

Bu kılavuz; projedeki 4 ana tutarlılık deneyinin matematiksel hesaplama yöntemlerini, kullanılan formülleri, proje isterlerine nasıl uyum sağlandığını ve hocanıza sunum yaparken kullanabileceğiniz kanıtlama adımlarını detaylıca açıklamaktadır.

---

## Genel Altyapı ve Mimari Özet

Tüm deneyler, asenkron streaming replication ile birbirine bağlı iki sanal makine üzerinde gerçekleştirilmiştir:
*   **Leader VM (`10.0.0.4`):** Tüm yazma (yazma ağırlıklı) işlemlerinin ve birincil okumaların yapıldığı ana sunucu.
*   **Follower VM (`10.0.0.5`):** Leader üzerindeki güncellemelerin WAL (Write-Ahead Log) üzerinden asenkron olarak yansıtıldığı salt-okunur kopya sunucu.

Deneylerde kullanılan veri tabanı şeması:
*   `movies` (Filmler) - Deney 1 için CRUD işlemlerini içerir.
*   `reservations` (Koltuk Rezervasyonları) - Deney 2, 3 ve 4 için sürüm takibi ve eşzamanlılığı test eder.
*   `seats` & `showtimes` - Koltuk ve seans bilgilerini tutan yardımcı tablolar.

---

## 1. Deney 1: Eventual Consistency (Nihai Tutarlılık & Replikasyon Gecikmesi)

### A. Deneyin Amacı ve Mantığı
Bu deneyin amacı, Leader veri tabanına yapılan bir yazma işleminin (CRUD: Ekleme, Güncelleme, Silme) asenkron replikasyon akışıyla Follower veri tabanına ne kadar sürede ulaştığını ölçmektir.

### B. Nasıl Hesaplandı? (Matematiksel Formül)
Replikasyon gecikmesi ($Lag$), istemci tarafında milisaniye düzeyinde ölçülen zaman damgalarının farkı alınarak hesaplanır:

$$\text{Replikasyon Gecikmesi (ms)} = (t_{\text{görünme}} - t_{\text{commit}}) \times 1000$$

*   **$t_{\text{commit}}$ (Commit Zamanı):** Leader üzerinde işlemin (INSERT, UPDATE veya DELETE) onaylandığı ve diske yazıldığı an.
*   **$t_{\text{görünme}}$ (Görünme Zamanı):** Follower üzerinde yapılan yüksek frekanslı sorgularda (1ms aralıklarla polling), söz konusu değişikliğin ilk kez başarıyla tespit edildiği an.

### C. Proje Şartlarına Nasıl Uyuldu?
*   **Tam CRUD Kapsamı:** Proje şartı gereği sadece `INSERT` değil, `UPDATE` (film süresi/türü güncelleme) ve `DELETE` (filmi silme) işlemleri için de ayrı replikasyon gecikmeleri ölçülmüş ve sonuçlar `results/eventual_lag_plot.png` üzerinde renk kodlu bar grafiğiyle gösterilmiştir.
*   **Önbellek (Caching) Önleme:** Her döngüde benzersiz Unix zaman damgaları içeren veriler (örneğin benzersiz film adları: `Exp1_Movie_178145...`) üretilerek veri tabanı veya işletim sistemi düzeyindeki önbellekleme mekanizmaları devre dışı bırakılmıştır.

---

## 2. Deney 2: Monotonic Reads (Monotonik Okumalar)

### A. Deneyin Amacı ve Mantığı
İstemcinin bir kaydı okurken zamanda geriye gitmesini (yani daha güncel bir versiyonu gördükten sonra replikasyon gecikmesi nedeniyle Follower'dan eski bir versiyonu okumasını) simüle ederek Monotonik Okuma tutarlılık modelinin ihlalini ve korunmasını göstermektir.

### B. Nasıl Hesaplandı?
Arka planda çalışan bir thread, Leader üzerinde tek bir rezervasyon kaydının versiyonunu her 150ms'de bir artırır ($v_1 \rightarrow v_2 \rightarrow \dots \rightarrow v_{11}$).
*   **Test A (Single Node - Koruma):** İstemci sadece salt Follower düğümünden ardışık okumalar yapar. Okunan versiyonların monotonik artışı kontrol edilir:
    $$\text{İhlal Koşulu} = V_{\text{read}}(t_n) < V_{\text{read}}(t_{n-1})$$
    Follower'ın replikasyon günlüğü sıralı işlendiği için burada ihlal **%0** çıkar (Monotonik Okuma korunur).
*   **Test B (Cross-Node - İhlal):** İstemci önce Leader'dan güncel versiyonu ($V_L$) okur. Hemen ardından Follower'dan okuma ($V_F$) yapar:
    $$\text{İhlal Koşulu} = V_F < V_L$$
    Replikasyon asenkron olduğundan Follower henüz güncellenmediyse $V_F < V_L$ durumu oluşur ve **Monotonik Okuma İhlali** kanıtlanır.

### C. Proje Şartlarına Nasıl Uyuldu?
*   **Threaded Simülasyon:** Gerçek hayattaki eşzamanlı istemci yükü, arka planda çalışan bir `writer_thread` ve bağımsız okuyucu istemci mantığıyla modellenmiştir.

---

## 3. Deney 3: Read-After-Write Consistency (Yazdığını Hemen Okuma)

### A. Deneyin Amacı ve Mantığı
Bir kullanıcının kendi yaptığı bir değişikliği (örneğin bilet alımını) hemen ardından okumak istediğinde bunu Leader'dan anında okuyabildiğini (RAW garantisi), ancak Follower'dan sorguladığında replikasyon gecikmesinden ötürü geçici olarak göremediğini (RAW ihlali) milisaniye bazında kanıtlamaktır.

### B. Nasıl Hesaplandı? (Matematiksel Hizalama)
Çelişkili sonuçları önlemek için zaman damgaları ve lag hesabı **işlem tamamlanma anlarına** göre hizalanmıştır:
*   **Leader Commit Zamanı ($t_{\text{commit}}$):** Leader üzerindeki `INSERT` sorgusu tamamlanıp commit onayının istemciye döndüğü an.
*   **Leader Okuma Bitiş Zamanı ($t_{\text{end\_L}}$):** Leader üzerinde anında yapılan `SELECT` sorgusunun başarıyla tamamlandığı an.
    $$\text{Leader Görünürlük Lag} = (t_{\text{end\_L}} - t_{\text{commit}}) \times 1000 \approx 0.5\,\text{ms}$$
*   **Follower İlk Okuma Zamanı ($t_{\text{read\_F}}$):** Follower üzerinde yapılan ilk immediate `SELECT` sorgusunun tamamlandığı an.
    $$\text{Eğer dönen sonuç NULL ise} \rightarrow \text{RAW İhlali}$$
*   **Follower Polling Zamanı ($t_{\text{attempt\_end\_F}}$):** Polling esnasında verinin nihayet göründüğü an.
    $$\text{Follower Replikasyon Lag} = (t_{\text{attempt\_end\_F}} - t_{\text{commit}}) \times 1000$$

### C. Proje Şartlarına Nasıl Uyuldu?
*   **Uyumlu Zaman Damgaları:** `[Leader - Write Reservation]` olayının zamanı sorgu başlangıcı yerine commit anına (`t_commit`) çekilerek arayüzdeki zaman farkları ile matematiksel lag değerlerinin (örneğin $5\,\text{ms}$) kusursuz şekilde uyuşması sağlandı.
*   **Adım Adım Trace:** SQL komutlarını ve dönen sonuçları (`Found` / `NULL`) gösteren detaylı bir günlük (trace) hem terminale hem de Web arayüzündeki verifikasyon modalına entegre edildi.

---

## 4. Deney 4: Concurrent Writes & Conflict (Eşzamanlı Yazmalar ve Çakışma)

### A. Deneyin Amacı ve Mantığı
*   **Part A (Global Sıralama):** Eşzamanlı yapılan yazmaların Leader üzerindeki commit sıralamasının, Follower veri tabanında da birebir aynı sırada uygulanıp uygulanmadığını doğrulamak (WAL loglarının sıralı doğası).
*   **Part B (Çifte Rezervasyon):** Veri tabanı düzeyinde lock (kilit) veya benzersizlik kısıtlaması (unique constraint) kullanılmadığında, uygulama düzeyindeki boş koltuk kontrollerinin yarış durumu (Race Condition) nedeniyle başarısız olacağını ve aynı koltuğa iki kişinin kaydedileceğini (Double Booking) göstermek.

### B. Nasıl Hesaplandı?
*   **Part A:** 10 thread bir bariyer (`threading.Barrier`) ile aynı anda serbest bırakılır.
    *   Sorguların fiziksel transaction ID'leri (`pg_current_xact_id()`) ve WAL konumları (LSN: Log Sequence Number) sorgulanarak Leader'daki mantıksal sıralama çıkarılır.
    *   Follower'daki görünme anları kaydedilir. Grafikte (`results/concurrent_ordering_plot.png`) yatay çizgiler halinde zaman damgalarıyla sıralamanın korunduğu gösterilir.
*   **Part B (Yarış Durumu):** 
    *   Thread 1 ve Thread 2 koltuğun boş olduğunu sorgular (`SELECT`).
    *   Araya **50ms yapay gecikme** konur. Bu esnada her iki thread de koltuğu boş görür.
    *   İki thread de `INSERT` atar ve commit eder. Sonuçta tek koltuk için 2 aktif rezervasyon oluşur (Çakışma kanıtlanır).

### C. Proje Şartlarına Nasıl Uyuldu?
*   **Fiziksel WAL LSN ve XID Takibi:** Replikasyonun derinliklerini kanıtlamak amacıyla sadece istemci zaman damgaları kullanılmamış; PostgreSQL'in fiziksel transaction ID ve WAL LSN değerleri sorgulanarak akademik seviyede bir sıralama analizi yapılmıştır.
*   **Görsel Paralellik:** Eşzamanlı yazma işlemlerinin zaman içindeki paralel ilerleyişi, çakışmayan yatay çizgiler ve kırmızı zaman etiketleri ile görselleştirilmiştir.

---

## Hocanıza Kanıtlama Adımları (Sunum Tüyoları)

1.  **Eventual Consistency Kanıtı:**
    *   `results/eventual_lag_plot.png` grafiğini açın. INSERT, UPDATE ve DELETE işlemlerinin Follower'a ortalama yansıma sürelerini (örneğin 10ms - 40ms arası) gösterin.
2.  **Monotonic Reads İhlal Kanıtı:**
    *   Web arayüzünde Monotonic Reads sekmesine gelin. Test B sonuçlarında kırmızı renkli **VIOLATION** etiketlerini ve zaman damgasıyla versiyonun geriye gittiği durumları (örneğin Leader'da v5 okunmuşken Follower'dan hemen ardından v3 okunması) gösterin.
3.  **Read-After-Write (RAW) Kanıtı:**
    *   Terminal çıktısındaki detaylı logları veya Web arayüzünde bir satıra tıklayarak açılan modalı gösterin.
    *   **Zaman Damgaları:** `Leader - Write Reservation` (`17:25:25.054`) ile `Follower - Immediate Read Check` (`17:25:25.056`) zamanlarını gösterin. Arada 2ms olmasına rağmen Follower'ın `NULL / Stale Read` döndüğünü, yani istemcinin kendi yazdığı veriyi Follower'dan okuyamadığını kanıtlayın.
4.  **Double Booking Kanıtı:**
    *   Veri tabanını doğrudan sorgulayarak (`SELECT * FROM reservations WHERE seat_id = 10;`) aynı koltuk için iki farklı müşterinin rezervasyon kaydının yan yana oluştuğunu gösterin.
