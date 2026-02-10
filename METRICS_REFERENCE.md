# Metrics Reference

Bu doküman, uygulamadaki rapor metriklerinin **ne anlama geldiğini** ve uygulamada **nasıl hesaplandığını/dönüştürüldüğünü** açıklar.

## 1) Veri Kaynağı

- Ana kaynak: Genesys Cloud `POST /api/v2/analytics/conversations/aggregates/query`
- Kullanıcı durum metrikleri (login/presence):  
  - `POST /api/v2/analytics/users/aggregates/query`  
  - `POST /api/v2/analytics/users/details/query`

## 2) Uygulama Hesaplama Kuralları

- `t*` ile başlayan metrikler: Genesys `stats.sum` milisaniye gelir, uygulamada **saniyeye çevrilir** (`/1000`).
- `n*` ile başlayan metrikler: Genesys `stats.count` olarak alınır.
- `oServiceLevel`: `stats.numerator / stats.denominator * 100` olarak hesaplanır.
- `AvgHandle`: `tHandle / CountHandle` (CountHandle = `tHandle` count).
- Süre metrikleri gösterimde `HH:MM:SS` formatına çevrilir.

## 3) UI -> API Metric Dönüşümleri

| UI metriği | API metriği | Uygulama notu |
|---|---|---|
| `nAnswered` | `tAnswered` | Sayı olarak `tAnswered.count` kullanılır |
| `nAbandon` | `tAbandon` | Sayı olarak `tAbandon.count` kullanılır |
| `nOffered` (Agent) | `tAlert` | Agent tarafında offered için alert count kullanılır |
| `nOffered` (Queue) | `nOffered` | Doğrudan |
| `nWrapup` | `tAcw` | Sayı olarak `tAcw.count` kullanılır |
| `nHandled` | `tHandle` | Sayı olarak `tHandle.count` kullanılır |
| `nOutbound` | `nOutbound` + `tTalk` | Süre alias’ı için `tTalk` da çağrılır |
| `tOutbound` | `tTalk` | Uygulama alias’ı |
| `nNotResponding` | `tNotResponding` | Sayı olarak `tNotResponding.count` |
| `nAlert` | `tAlert` | Sayı olarak `tAlert.count` |
| `nConsultTransferred` | `nConsultTransferred` | Doğrudan |
| `AvgHandle` | `tHandle` | Sonradan `AvgHandle` hesaplanır |

## 4) Raporlarda Kullanılan Metrikler (Seçilebilir)

### 4.1 Konuşma Aggregate Metrikleri

`nOffered`, `nAnswered`, `nAbandon`, `nTransferred`, `oServiceLevel`,  
`tAnswered`, `tAbandon`, `tTalk`, `tAcw`, `tHandle`, `tHeld`, `tWait`, `tAcd`, `tAlert`,  
`nOutbound`, `tOutbound`, `nNotResponding`, `nConsult`, `nConsultTransferred`,  
`nBlindTransferred`, `nConnected`, `nOverSla`, `nError`, `nWrapup`, `nAlert`, `nHandled`,  
`tTalkComplete`, `tHeldComplete`, `tFlowOut`, `tVoicemail`, `tContacting`,  
`nBotInteractions`, `nCobrowseSessions`, `nConversations`, `nOutboundAbandoned`,  
`nOutboundAttempted`, `nOutboundConnected`, `nStateTransitionError`,  
`oAudioMessageCount`, `oExternalAudioMessageCount`, `oExternalMediaCount`, `oMediaCount`,  
`oMessageCount`, `oMessageSegmentCount`, `oMessageTurn`, `oServiceTarget`,  
`tActiveCallback`, `tActiveCallbackComplete`, `tAgentResponseTime`, `tAgentVideoConnected`,  
`tAverageAgentResponseTime`, `tAverageCustomerResponseTime`, `tBarging`, `tCoaching`,  
`tCoachingComplete`, `tConnected`, `tDialing`, `tFirstConnect`, `tFirstDial`,  
`tFirstEngagement`, `tFirstResponse`, `tIvr`, `tMonitoring`, `tMonitoringComplete`,  
`tNotResponding`, `tPark`, `tParkComplete`, `tScreenMonitoring`, `tShortAbandon`,  
`tSnippetRecord`, `tUserResponseTime`

### 4.2 Kullanıcı Durum/Operasyon Metrikleri

- `tMeal`, `tMeeting`, `tAvailable`, `tBusy`, `tAway`, `tTraining`, `tOnQueue`
- `col_login`, `col_logout`, `col_staffed_time`
- `AvgHandle`

## 5) Seçim Listesinden Kaldırılan Metrikler

Aşağıdaki metrikler aggregate endpoint ile tutarlı üretilemediği için seçim listesinden çıkarılmıştır:

- `tOrganizationResponse`
- `tAcdWait`
- `nConsultConnected`
- `nConsultAnswered`

## 6) Anlam Notları (Kısa)

- `n*`: adet/sayı metrikleri  
- `t*`: süre metrikleri  
- `o*`: oran/observation tipi metrikler (ör. `oServiceLevel`)  
- Bazı metrikler Genesys tarafında doğrudan aynı isimde gelir; bazıları yukarıdaki dönüşüm tablosuna göre türetilir.

## 7) Örnek Hesaplama Senaryoları

### 7.1 `oServiceLevel` (Oran)

Genesys response (örnek):

- `metric = oServiceLevel`
- `stats.numerator = 42`
- `stats.denominator = 50`

Uygulama hesabı:

- `oServiceLevel = 42 / 50 * 100 = 84.0`

Eğer birden fazla satır group sonrası birleşiyorsa:

- Toplam numerator = satırların numerator toplamı
- Toplam denominator = satırların denominator toplamı
- Final `oServiceLevel = toplam_numerator / toplam_denominator * 100`

### 7.2 `AvgHandle`

Genesys response (örnek):

- `metric = tHandle`
- `stats.sum = 1_800_000` (ms)
- `stats.count = 120`

Uygulama hesabı:

- `tHandle = 1_800_000 / 1000 = 1800` saniye
- `CountHandle = 120`
- `AvgHandle = 1800 / 120 = 15` saniye
- UI’da süre formatı: `00:00:15`

### 7.3 `nAnswered` (UI seçimi)

UI’de `nAnswered` seçilince API’ye `tAnswered` metric’i gönderilir.

Genesys response (örnek):

- `metric = tAnswered`
- `stats.count = 73`

Uygulama çıktısı:

- `nAnswered = 73`

Not: `tAnswered` süresi ayrıca `stats.sum/1000` olarak saniye bazında da tutulur.

### 7.4 `nWrapup` (UI seçimi)

UI’de `nWrapup` seçilince API’ye `tAcw` metric’i gönderilir.

Genesys response (örnek):

- `metric = tAcw`
- `stats.count = 55`

Uygulama çıktısı:

- `nWrapup = 55`

### 7.5 `tOutbound` (UI seçimi)

UI’de `tOutbound` seçilince API’de `tTalk` istenir (uygulama alias’ı).

Genesys response (örnek):

- `metric = tTalk`
- `stats.sum = 540_000` (ms)

Uygulama çıktısı:

- `tTalk = 540` saniye
- `tOutbound = 540` saniye (alias)

## 8) Metrik Sözlüğü (Her Metriğin Açıklaması)

### 8.1 Temel çağrı/konuşma metrikleri

| Metrik | Açıklama |
|---|---|
| `nOffered` | Kuyruğa sunulan toplam etkileşim adedi. |
| `nAnswered` | Cevaplanan etkileşim adedi (`tAnswered.count` türevi). |
| `nAbandon` | Agent’a bağlanmadan düşen/terk edilen etkileşim adedi (`tAbandon.count` türevi). |
| `nTransferred` | Transfer edilen etkileşim adedi. |
| `nConsult` | Danışma (consult) başlatılan etkileşim adedi. |
| `nConsultTransferred` | Danışma sonrası transferle sonuçlanan etkileşim adedi. |
| `nBlindTransferred` | Kör transfer adedi. |
| `nConnected` | Bağlantı kurulan etkileşim adedi. |
| `nOverSla` | SLA hedefini aşan etkileşim adedi. |
| `nError` | Hata ile sonuçlanan etkileşim adedi. |
| `nWrapup` | Wrap-up/ACW tamamlanan etkileşim adedi (`tAcw.count` türevi). |
| `nAlert` | Çalma/alert aşamasına düşen etkileşim adedi (`tAlert.count` türevi). |
| `nHandled` | İşlenen (handle edilen) etkileşim adedi (`tHandle.count` türevi). |
| `nOutbound` | Dış arama (outbound) adedi. |
| `nConversations` | Toplam conversation adedi. |
| `nOutboundAbandoned` | Outbound denemede terk edilen çağrı adedi. |
| `nOutboundAttempted` | Outbound deneme adedi. |
| `nOutboundConnected` | Outbound başarılı bağlantı adedi. |
| `nStateTransitionError` | Durum geçişi hatası adedi. |

### 8.2 Süre metrikleri (t*)

| Metrik | Açıklama |
|---|---|
| `tAnswered` | Cevaplanmaya kadar geçen toplam süre (sum). |
| `tAbandon` | Terk edilen etkileşimlerin toplam bekleme süresi. |
| `tTalk` | Konuşma süresi toplamı. |
| `tOutbound` | Uygulama alias’ı; `tTalk` üzerinden türetilir. |
| `tAcw` | After Call Work (çağrı sonrası işlem) toplam süresi. |
| `tHandle` | Toplam handle süresi (talk+hold+acw vb.). |
| `tHeld` | Bekletme (hold) toplam süresi. |
| `tWait` | Bekleme süresi toplamı. |
| `tAcd` | ACD/kuyruk bekleme toplam süresi. |
| `tAlert` | Çalma/alert toplam süresi. |
| `tTalkComplete` | Tamamlanan konuşma segmentleri toplam süresi. |
| `tHeldComplete` | Tamamlanan hold segmentleri toplam süresi. |
| `tFlowOut` | Akıştan (flow) çıkış toplam süresi. |
| `tVoicemail` | Sesli mesaj (voicemail) toplam süresi. |
| `tContacting` | Arama/bağlantı kurma (contacting) süresi. |
| `tConnected` | Bağlı durumda kalınan toplam süre. |
| `tDialing` | Çevirme (dialing) toplam süresi. |
| `tFirstConnect` | İlk bağlantıya kadar geçen süre. |
| `tFirstDial` | İlk çevirme aşamasına kadar geçen süre. |
| `tFirstEngagement` | İlk etkileşime kadar geçen süre. |
| `tFirstResponse` | İlk yanıta kadar geçen süre. |
| `tIvr` | IVR aşamasında geçen toplam süre. |
| `tNotResponding` | Cevapsız/yanıt vermeme durumlarında geçen toplam süre. |
| `tShortAbandon` | Kısa abandon kapsamındaki süre toplamı. |
| `tSnippetRecord` | Snippet kayıt süresi toplamı. |
| `tUserResponseTime` | Kullanıcı yanıt süresi toplamı. |
| `tActiveCallback` | Aktif callback süresi toplamı. |
| `tActiveCallbackComplete` | Tamamlanan aktif callback süresi toplamı. |
| `tAgentResponseTime` | Agent yanıt süresi toplamı. |
| `tAgentVideoConnected` | Agent video bağlantı süresi toplamı. |
| `tAverageAgentResponseTime` | Agent ortalama yanıt süresi (platform metriği). |
| `tAverageCustomerResponseTime` | Müşteri ortalama yanıt süresi (platform metriği). |
| `tBarging` | Barge-in süresi toplamı. |
| `tCoaching` | Koçluk süresi toplamı. |
| `tCoachingComplete` | Tamamlanan koçluk süresi toplamı. |
| `tMonitoring` | İzleme süresi toplamı. |
| `tMonitoringComplete` | Tamamlanan izleme süresi toplamı. |
| `tPark` | Park süresi toplamı. |
| `tParkComplete` | Tamamlanan park süresi toplamı. |
| `tScreenMonitoring` | Ekran izleme süresi toplamı. |

### 8.3 Oran/observation tipi metrikler (o*)

| Metrik | Açıklama |
|---|---|
| `oServiceLevel` | Servis seviyesi oranı (`numerator/denominator*100`). |
| `oServiceTarget` | Servis seviyesi hedef değeri. |
| `oAudioMessageCount` | Sesli mesaj içerik sayısı. |
| `oExternalAudioMessageCount` | Harici sesli mesaj içerik sayısı. |
| `oExternalMediaCount` | Harici medya içerik sayısı. |
| `oMediaCount` | Toplam medya içerik sayısı. |
| `oMessageCount` | Mesaj adedi/yoğunluğu metriği. |
| `oMessageSegmentCount` | Mesaj segment adedi metriği. |
| `oMessageTurn` | Mesaj turn/adım metriği. |

### 8.4 Dijital/özel etkileşim metrikleri

| Metrik | Açıklama |
|---|---|
| `nBotInteractions` | Bot ile gerçekleşen etkileşim adedi. |
| `nCobrowseSessions` | Cobrowse oturum adedi. |

### 8.5 Kullanıcı durum/operasyon metrikleri

| Metrik | Açıklama |
|---|---|
| `tMeal` | Yemek durumunda geçirilen süre. |
| `tMeeting` | Toplantı durumunda geçirilen süre. |
| `tAvailable` | Uygun/hazır durumda geçirilen süre. |
| `tBusy` | Meşgul durumda geçirilen süre. |
| `tAway` | Uzakta/away durumunda geçirilen süre. |
| `tTraining` | Eğitim durumunda geçirilen süre. |
| `tOnQueue` | On-queue durumda geçirilen süre. |
| `col_login` | Seçili periyottaki ilk login zamanı. |
| `col_logout` | Seçili periyottaki son logout zamanı. |
| `col_staffed_time` | Staffed time (kullanıcı aggregate’dan türetilen süre). |
| `AvgHandle` | Ortalama handle süresi (`tHandle / CountHandle`). |

### 8.6 Seçimden kaldırılan metrikler (bilgi)

| Metrik | Durum |
|---|---|
| `tOrganizationResponse` | Aggregate rapor seçiminden kaldırıldı (tutarlı üretim yok). |
| `tAcdWait` | Aggregate rapor seçiminden kaldırıldı (tutarlı üretim yok). |
| `nConsultConnected` | Aggregate rapor seçiminden kaldırıldı (doğrudan karşılık yok). |
| `nConsultAnswered` | Aggregate rapor seçiminden kaldırıldı (doğrudan karşılık yok). |
