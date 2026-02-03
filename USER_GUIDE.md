# Genesys Cloud Raporlama Aracı - Kullanım Kılavuzu

Bu kılavuz, Genesys Cloud Reporting API uygulamasının özellikleri, yetkilendirme yapısı ve rapor metriklerinin anlamları hakkında detaylı bilgi sunar.

---

## 🔐 1. Giriş ve Yetkilendirme

Uygulamaya erişim, rol tabanlı bir yetkilendirme sistemi (RBAC) ile korunmaktadır. Her kullanıcının görebileceği sayfalar ve rapor metrikleri Admin tarafından belirlenir.

### Kullanıcı Rolleri
| Rol | Açıklama |
| :--- | :--- |
| **Admin** | Uygulamanın tam yetkili sahibidir. Kullanıcı ekleyebilir, silebilir, metrik kısıtlaması yapabilir ve Genesys API bağlantı ayarlarını yönetebilir. |
| **Manager** | Hem Canlı Dashboard hem de Raporlar sayfasına erişebilir. Ancak Admin ayarlarını veya kullanıcı yönetimini göremez. |
| **Reports User** | Sadece Raporlar sayfasına erişebilir. |
| **Dashboard User** | Sadece Canlı Dashboard sayfasına erişebilir. |

> [!TIP]
> **İlk Giriş:** Varsayılan admin bilgileri: Kullanıcı: `admin` / Şifre: `admin123`. Giriş yaptıktan sonra mutlaka şifrenizi değiştiriniz.

---

## 📊 2. Raporlar Sayfası

Raporlar sayfası, geçmişe dönük verileri analiz etmek için kullanılır.

### Rapor Türleri
- **Agent Raporu:** Belirli agent'ların performansını özetler.
- **Kuyruk Raporu:** Belirli kuyrukların (workgroup) genel performansını gösterir.
- **Detaylı Rapor:** Agent ve Kuyruk kırılımında en detaylı veriyi sunar.
- **Etkileşim Arama (Detay):** Tarih bazlı olarak gerçekleşmiş tüm çağrı, chat ve etkileşimlerin ham kayıtlarını listeler. Kim, kimi, ne zaman aramış, kaç saniye sürmüş gibi tekil kayıtları incelemek için kullanılır. **Raporu almadan önce istediğiniz sütunları çoklu seçim menüsünden filtreleyebilirsiniz.**
- **Kaçan Çağrılar Raporu:** Belirtilen tarih aralığındaki CEVAPLANMAYAN (Kaçan, Cevapsız, Ulaşılamayan) tüm sesli ve yazılı etkileşimleri listeler. Inbound ve Outbound yönündeki başarısız çağrıları tek raporda toplamayı sağlar.

### Özellikler
- **Periyot (Granularity):** Veriyi "Toplam", "Saatlik" veya "30 Dakikalık" dilimlerde görebilirsiniz.
- **Boşlukları Doldur:** Veri olmayan zaman dilimlerini (0 değerleriyle) tabloya ekleyerek grafiklerde kopukluk olmasını engeller.
- **Süre Formatı:** Etkileşim raporundaki tüm süreler **SAAT:DAKİKA:SANİYE (HH:MM:SS)** formatında gösterilir.
- **Görünüm Kaydetme (Presets):** Sık kullandığınız filtreleri (kuyruklar, metrikler, rapor türü) bir isimle kaydedebilir, daha sonra tek tıkla yükleyebilirsiniz.
- **Excel İndirme:** Oluşturulan raporları Excel formatında bilgisayarınıza indirebilirsiniz.

---

## 📺 3. Dashboard (Canlı)

Canlı Dashboard, çağrı merkezinizin o anki durumunu takip etmenizi sağlar.

- **Gruplar:** Kuyrukları mantıksal gruplara ayırabilirsiniz (Örn: Satış Grubu, Destek Grubu).
- **Modlar:** 
    - **Live:** O andaki bekleyen çağrı ve aktif görüşme sayılarını gösterir.
    - **Yesterday / Date:** Seçilen günün toplam performans verilerini (Gelen, Cevaplanan, Servis Seviyesi) gösterir.
- **Otomatik Yenileme:** "Live" modunda veriler her 10 saniyede bir otomatik olarak güncellenir.

---

## 📖 4. Metrik Sözlüğü (Rapor Metrikleri)

Raporlarda kullanılan temel metriklerin teknik açıklamaları aşağıdadır:

### Çağrı Adetleri
- **nOffered (Gelen):** Kuyruğa giren toplam etkileşim (çağrı, chat vb.) sayısı.
- **nAnswered (Cevaplanan)::** Bir agent tarafından başarıyla cevaplanan ACD etkileşim sayısı.
- **nAbandon (Kaçan):** Müşterinin bir agent'a bağlanmadan önce kuyrukta beklerken kapattığı çağrı sayısı.
- **nConnected (Bağlanan):** Sisteme başarıyla bağlanan (cevaplanan veya IVR/Flow aşamasındaki) tüm etkileşimler.
- **nTransferred (Transfer):** Bir agent'tan başka bir agent'a veya harici bir numaraya yapılan toplam transfer sayısı.
- **nBlindTransferred (Yönlenen):** Agent'ın görüşmeyi karşı tarafın açmasını beklemeden yaptığı transferler.
- **nConsult (Danışma):** Agent'ın görüşme sırasında başka bir agent'a veya süpervizöre danıştığı arama sayısı.
- **nConsultConnected (Bağlanan Danışma):** Başlatılan danışma aramalarından karşı tarafın cevap verdiği (bağlandığı) adet.
- **nConsultAnswered (Cevaplanan Danışma):** Danışma aramasının karşı tarafça başarıyla cevaplandığı adet.
- **nConsultTransferred (Danışma Transferi):** Danışma araması yapıldıktan sonra tamamlanan transfer sayısı.
- **nOutbound (Dış Arama):** Agent'ın manuel veya kampanya üzerinden başlattığı giden arama sayısı.
- **nNotResponding (Cevapsız):** Çağrı agent'a sunulduğu (çaldığı) halde agent'ın kabul etmediği veya süresinin dolduğu durumlar.
- **nOverSla (SLA Aşan):** Kuyruk için belirlenen servis seviyesi (SLA) hedef süresini aşan çağrı sayısı.
- **nHandled (Agent Kapatma):** Agent'ın etkileşimi sonlandırma (kapatma) sayısı.
- **nAlert (Çalma Adedi):** Agent'ın ekranında etkileşimin kaç kez çaldığı veya uyarı verdiği.

### Zaman Metrikleri (Saniye cinsinden)
- **tAnswered (Cevaplanma Süresi):** Çağrının kuyruğa girişi ile bir agent'ın cevaplaması arasında geçen toplam süre.
- **tTalk (Konuşma Süresi):** Agent ile müşteri arasındaki toplam aktif sesli/yazılı görüşme süresi.
- **tTalkComplete (Tamamlanan Konuşma):** Sadece tamamlanmış (bitmiş) görüşme segmentlerinin toplam süresi.
- **tHeld (Bekletme Süresi):** Görüşme sırasında müşterinin bekleme (hold) moduna alındığı toplam süre.
- **tHeldComplete (Tamamlanan Bekletme):** Sadece tamamlanmış bekletme segmentlerinin toplam süresi.
- **tAcw (Çağrı Sonrası İşlem):** Görüşme bittikten sonra agent'ın yaptığı not alma veya kayıt kapatma süresi.
- **tHandle (Toplam İşlem):** (Konuşma + Bekletme + ACW) sürelerinin toplamı. Bir etkileşimin agent'ı ne kadar meşgul ettiğini gösterir.
- **tAlert (Çalma Süresi):** Etkileşimin agent ekranında çalarak beklediği süre.
- **tAcd (Kuyruk Süresi):** Etkileşimin kuyrukta (flow sonrası) agent bekleyerek geçirdiği süre.
- **tAcdWait (ACD Bekleme):** Agent atanana kadar kuyrukta geçen süre (cevaplanan veya kaçan fark etmeksizin).
- **tWait (Bekleme Süresi):** Flow dahil, müşterinin agent'a bağlanana kadar beklediği tüm süre.
- **tFlowOut (Flow Çıkış):** Etkileşimin flow (IVR) içinde geçirdiği ve sonrasında başka bir yere aktarıldığı/sonlandığı süre.
- **tVoicemail (Sesli Mesaj):** Müşterinin sesli mesaj bırakırken geçirdiği süre.
- **tOrganizationResponse (Org. Cevap):** Çağrının organizasyona girişi ile sonlanması arasında geçen toplam süre (uçtan uca).
- **tContacting (Arama/Bağlanma):** Dış aramalarda karşı tarafa ulaşılana kadar geçen süre.

### Durum ve Performans Metrikleri
- **oServiceLevel (Servis Seviyesi):** Belirlenen hedef sürede cevaplanan çağrıların oranı.
- **AvgHandle (Ort. İşlem Süresi):** Toplam işlem süresinin çağrı sayısına bölünmüş hali.
- **col_login / col_logout:** Agent'ın sistemdeki ilk login ve son logout saatleri.
- **col_staffed_time:** Agent'ın sistemde toplam login kaldığı süre.

### Agent Durum Süreleri
- **tAvailable (Hazır):** Agent'ın çağrı beklediği "Uygun" süresi.
- **tBusy (Meşgul):** Agent'ın mola harici meşgul olduğu süre.
- **tMeal (Yemek):** Yemek molasında geçen süre.
- **tMeeting (Toplantı):** Toplantıda geçen süre.
- **tAway (Uzakta):** Diğer mola türlerinde geçen süre.

---

## ⚙️ 5. Admin Ayarları

Sadece **Admin** rolündeki kullanıcılar erişebilir.

- **Genesys API Credentials:** Genesys Cloud bağlantısı için gerekli olan Client ID, Secret ve Region ayarlarının yapıldığı yerdir.
- **Kullanıcı Yönetimi:**
    - Yeni kullanıcı oluşturma.
    - Şifre atama.
    - Rol belirleme.
    - **Metrik Yetkilendirme:** Belirli bir kullanıcının raporlarda sadece belirli metrikleri görmesini sağlayabilirsiniz.
- **Dışa/İçe Aktar:** Uygulama ayarlarını ve kayıtlı rapor görünümlerini yedekleyebilir veya başka bir tarayıcıya taşıyabilirsiniz.

---

## 🔍 6. Etkileşim Detay Raporu Sütunları

Bu rapor türü, çağrıların ve diğer etkileşimlerin en ince detayına inmenizi sağlar.

- **Yön (In/Out):** Çağrının yönü (Gelen/Giden).
- **Cevap Durumu:** Çağrının agent veya müşteri tarafından cevaplanıp cevaplanmadığı (Örn: "Cevaplandı", "Ulaşılamadı", "Kaçan").
- **Agent Adı:** Görüşmeyi yapan agent'ın Adı Soyadı.
- **Kullanıcı Adı:** Agent'ın sistemdeki kullanıcı adı (domain hariç, örn: `a.tuzun`).
- **Kapanış Nedeni:** Çağrıyı kimin sonlandırdığı (Örn: `Müşteri`, `Sistem`, `Transfer`, `Agent`).
- **Çalma Süresi:** Agent'ın ekranında çağrının çaldığı süre (Alert/Ring).
- **Bekletme Sayısı:** Görüşme boyunca müşterinin kaç kez beklemeye (Hold) alındığı.
