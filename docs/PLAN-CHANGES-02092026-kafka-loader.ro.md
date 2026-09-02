# Propunere — mutarea Loader-ului de pe HTTP POST pe Kafka

> **Traducere în română a [`PLAN-CHANGES-02092026-kafka-loader.md`](PLAN-CHANGES-02092026-kafka-loader.md).** Versiunea
> engleză rămâne cea de referință; dacă cele două diferă, engleza câștigă.
>
> **Stare:** propunere pentru discuție. Neimplementată, necuplată la `make verify`.
> **Dată:** 2026-09-02. Bazată pe arborele de lucru de la acel commit.
> **Public:** alinierea din 2026-09-03, apoi grupul de backend devs.
> Document complementar la [`alternative-implementations.md`](alternative-implementations.md),
> care propune *motoare* alternative; acesta propune o *muchie de Load* alternativă.

---

## 1. Ce muchie schimbăm, de fapt

Loader App are două muchii, iar „să facem Loader-ul pe Kafka" poate însemna oricare dintre
ele. Sunt independente, iar propunerea de față se referă doar la a doua.

```
        (muchia de intrare)              LoaderApp              (muchia de ieșire)
  GCS  json/<runId>/*.jsonl   ───────►  citește, validează, ──────►  POST /v1/accounts
  scris de json_producer                trimite, numără             Target System (mock)
```

| Muchie | Azi | Varianta Kafka | În această propunere |
|---|---|---|---|
| **Ieșire** — cum ajung documentele la Target System | `POST /v1/accounts` + `X-Idempotency-Key`, retry și backoff scrise de mână | publicare pe `target-system-target`, Target System consumă | **da — secțiunea 3** |
| **Intrare** — de unde ia Loader-ul documentele | `store.list(jsonBucket, "json/<runId>/")` | consumă `target-system-target`, pe care `json_producer` **deja publică** (`--sinks both`) | opțional, secțiunea 6 |

Atenție la consecința incomodă dacă le-am face pe amândouă: `json_producer` scrie deja pe
*același* topic pe care ar publica Loader-ul. Ambele muchii înseamnă fie două topicuri
(`…-target-staged` → Loader → `…-target`), fie scoaterea Loader-ului cu totul din mijloc.
Este o bifurcație reală de design și își are locul pe agenda de mâine (secțiunea 7, Î1).

---

## 2. Ce ne oferă azi calea HTTP (lucrul pe care nu avem voie să-l pierdem)

Aici e miezul. `LoaderApp` nu este o țeavă proastă — **codul de răspuns sincron este
verdictul**, iar din el se construiesc trei artefacte:

| Rezultat HTTP | Loader îl tratează ca | Ajunge în |
|---|---|---|
| `201` | `CREATED` → `accepted++` | `.RPT` `accepted` |
| `200` | `DUPLICATE` (reluare idempotentă) → `duplicates++` | `.RPT` `duplicatesIgnored` |
| `429` / `5xx` | tranzitoriu → backoff+jitter, reîncearcă, `retries++` | `.RPT` `retriesPerformed` |
| alt `4xx` | **permanent** → rând în `.ERR` | `.ERR` + `.RPT` `errors` |
| lipsă `accountId` / `dedupKey` | permanent, nu se trimite niciodată | `.ERR` |

Apoi `recon-service` citește `documentsRead` și `errors` din acel `.RPT`
(`ReconService.java:141-143`), iar `tests/acceptance.py:288` citește `.CHS`-ul de load.

**Un ack de produce Kafka nu înseamnă nimic din toate acestea.** `acks=all` înseamnă că
*broker-ul ține octeții durabil*. Nu înseamnă că Target System a parsat, a acceptat sau a
persistat înregistrarea. Înlocuind naiv POST-ul cu produce, transformăm un verdict
per-document într-un „am pus-o la poștă" per-document — iar `.RPT` devine un raport despre
propriul nostru outbox.

**Așadar verdictul trebuie relocat, nu șters.** Ăsta e întregul design al propunerii și
singurul punct care merită 10 minute din ședință.

---

## 3. Forma propusă

Verdictul se mută pe două topicuri de retur. Unul dintre ele **există deja și funcționează
deja**.

```
LoaderApp  ──produce──►  target-system-target        ──►  Target System
                                                            │
recon/loader ◄──consumă── target-system-confirmations ◄─────┤  aplicat
             ◄──consumă── target-system-rejections   ◄──────┘  refuzat (topic nou)
```

### 3.1 Faza de publicare (înlocuiește `Loader.send`)

* Configurația producer-ului: `enable.idempotence=true`, `acks=all`, `retries=5`,
  `linger.ms=20` — **identică cu `pipelines/common/sinks.py:KafkaTargetWriter`**, ca cele
  două producer-e din proiect să fie de acord.
* `key = migration.dedupKey`. `dedupKey` este `account_key` — un sha256 peste câmpurile de
  cheie de cont din mapping (`mapping.py:313`) — deci **cheia partiționează pe cont și
  păstrează ordinea per cont**. Aceeași cheie pe care `KafkaTargetWriter` o folosește deja.
* Header-e: `run-id`, `idempotency-key`, `batch-id` — aceleași trei, cu aceleași nume.
* Verificarea prezenței `accountId` / `dedupKey` **rămâne exact cum e** și duce în
  continuare la `.ERR` fără să trimită. Este un defect de lot și este anterior Kafka.
* Se verifică valoarea returnată de `flush()`. Nelivrat înseamnă eșec, nu succes lent —
  proiectul a pierdut deja 400 de înregistrări ignorând asta o dată (`sinks.py:296-303`).

### 3.2 Faza de decontare (înlocuiește ramura 200/201/4xx)

După publicarea tuturor documentelor, Loader-ul face o **citire mărginită a celor două
topicuri de retur pentru acest `runId`**, apoi își scrie artefactele:

* Se refolosește exact tiparul din `ReconService.readConfirmations` — grup de consumatori
  proaspăt pe rulare, `loader-<runId>`, `assign` pe toate partițiile, `seekToBeginning`,
  poll până când fiecare partiție atinge `endOffsets`-ul din instantaneu, cu un termen
  limită de 30s ca plasă de siguranță, filtrat pe `runId`. E scris, revizuit și funcționează
  împotriva Managed Kafka.
* `accepted` = confirmările potrivite cu cheile publicate.
* `errors` = evenimente de respingere → rânduri `.ERR`, purtând motivul propriu al Target
  System în loc de un cod de stare HTTP.
* `sent - accepted - errors` = **`unsettled`** — un al treilea rezultat, nou, fără echivalent
  HTTP, și numele onest pentru „am publicat și nu ne-a spus nimeni nimic". Un `unsettled`
  nenul trebuie să pice rularea.

### 3.3 Modificări de câmpuri în `.RPT`

| Câmp | Azi | După |
|---|---|---|
| `documentsRead` | neschimbat | neschimbat |
| `accepted` | număr de HTTP 201 | număr confirmat de Target System |
| `duplicatesIgnored` | număr de HTTP 200 | **dispare** (vezi Î2) |
| `errors` | 4xx + malformate | evenimente de respingere + malformate |
| `retriesPerformed` | bucla noastră de backoff | **dispare** — intern producer-ului |
| `published` | — | **nou**: trimiteri confirmate de broker |
| `unsettled` | — | **nou**: publicate, niciodată decontate |

⚠️ `accepted` își păstrează numele, dar **își schimbă înțelesul**. Proiectul a păstrat
deliberat numele `written`/`rejected` „ca rapoartele de evidență arhivate să rămână
comparabile" (`ReconService.java`, `Balance`). Fie redenumim în `confirmed`, fie adăugăm un
câmp `reportVersion`. Î3 mai jos.

### 3.4 Reconcilierea trece din consultativă în primară

Azi criteriul 9 este protejat: `TARGET_SYSTEM_CONFIRMATION_BOOTSTRAP` gol → recon sare peste
citirea confirmărilor și raportează `enabled=false`, „ceea ce menține verde o rulare fără
Kafka".

Sub această propunere **nu mai există rulare fără Kafka**, iar fluxul de confirmări este
singura dovadă că încărcarea a avut loc. Flag-ul `enabled` încetează să fie un comutator de
cost și devine o cale de a trece în tăcere o rulare care n-a dovedit nimic. Ar trebui scos
sau inversat ca să pice închis. (`docs/production-readiness.md` semnalează deja acest flag
ca deviație.)

---

## 4. Ce se simplifică, ce se complică

**Se simplifică**
- Dispar ~90 de linii: bucla de retry, `backoff()` cu jitter, clasificarea
  tranzitoriu/permanent, `LoadFailure.status`. Idempotența producer-ului plus `retries` le
  înlocuiesc.
- Fără token de identitate Cloud Run (`GcpToken.identityToken`), fără întrebarea de
  accesibilitate din VPC pentru un endpoint HTTPS.
- Target System absoarbe încărcarea în ritmul propriu. Limitarea prin 429 dispare ca noțiune.
- Reluarea e gratuită: topicul reține 7 zile (`terraform/modules/kafka/main.tf`), deci un val
  eșuat se poate reconsuma fără a rerula pipeline-ul.

**Se complică**
- **Pierderea adevărului sincron** — secțiunea 2. Ăsta e tot costul.
- **Contrapresiunea se inversează.** Azi un Target System lent împinge înapoi cu 429 și
  Loader-ul își reglează ritmul. Cu Kafka publicăm la viteză maximă, iar singurul semnal este
  *lag-ul consumatorului* — pe care nimic din acest repo nu îl urmărește și nu îl alarmează.
- **O dependență nouă pentru `.ERR`.** Respingerile trebuie să devină un contract pe care
  echipa de Loader și-l asumă. Fără topic de respingeri, un document greșit e imposibil de
  distins de unul lent, iar totul cade în `unsettled`.
- **Două moduri de eșec fără analog HTTP:** o partiție al cărei consumator e mort
  (înregistrări în coadă durabilă, niciodată aplicate) și un mesaj otrăvit care blochează
  consumatorul Target System pe un offset.
- **Faza de decontare are nevoie de o politică reală de timeout.** 30s e bine pentru 400 de
  documente. Pentru milioane nu e, iar „cât așteptăm confirmările înainte să picăm rularea"
  este o decizie operațională nouă, pe care HTTP nu ne-a cerut-o niciodată.

---

## 5. Lista concretă de modificări

| # | Fișier | Modificare |
|---|---|---|
| 1 | `apps/loader-app/pom.xml` | adaugă `kafka-clients` + `slf4j-simple` (copiate identic din `recon-service/pom.xml:30-36` — fără binder, un eșec SASL se citește doar ca „Topic not present in metadata after 60000 ms") |
| 2 | `apps/common/.../KafkaClients.java` *(nou)* | extrage constructorul de proprietăți pentru producer/consumer plus blocul `KAFKA_SECURITY_PROTOCOL` → SASL_SSL/OAUTHBEARER, **azi copiat-lipit în `TargetSystemMock:96-121` și `ReconService:437-462`**. A treia copie e momentul extragerii. |
| 3 | `apps/loader-app/.../LoaderApp.java` | `Loader.send` → `publish` + `settle`; se șterge `backoff`; se păstrează `requireField` |
| 4 | `apps/loader-app/.../LoaderApp.java` | noi: `--kafka-bootstrap`, `--kafka-topic`, `--rejection-topic`, `--settle-timeout-seconds`; se păstrează `--target-system-url` în spatele unui flag `--sink http\|kafka` pentru o versiune |
| 5 | `apps/target-system-mock/.../TargetSystemMock.java` | buclă de consum pe `target-system-target` care aplică aceeași hartă de idempotență ca handler-ul de POST și publică pe topicul de respingeri la 400. Injectorul de 429/503 devine injector de lag. |
| 6 | `terraform/modules/kafka/main.tf` | adaugă `rejections` în harta `topics` (1 partiție, aceeași formă ca `confirmations`) |
| 7 | `terraform/modules/iam` | `roles/managedkafka.client` pentru SA-ul `loader-app` (acordarea există deja pentru celelalte trei principaluri) |
| 8 | `composer/dags/mig_000001_1.py` | task-ul de loader primește argumentele `--kafka-bootstrap`/`--kafka-topic`. `KAFKA_SECURITY_PROTOCOL` **este deja în env_vars-ul lui `java_app`** — acel bug e deja plătit. |
| 9 | `apps/recon-service` | flag-ul `enabled` pică închis (secțiunea 3.4) |
| 10 | `tests/` | test pur, în stilul `ReconciliationMatcherTest`, pentru diferența de mulțimi din decontare; criteriu de acceptanță `unsettled == 0` |
| 11 | `docs/`, `apps/README.md`, `README.md` slide 2 | săgeata de pe banda de Load încetează să fie „REST + retries" |

Stiva locală nu necesită modificări: redpanda e deja pornit, `make init-infra` creează deja
topicurile, iar mock-ul e deja pe rețea.

---

## 6. Companion opțional — muchia de intrare

`json_producer --sinks both` publică deja fiecare document TARGET pe
`target-system-target`. Dacă Loader-ul ar consuma de acolo în loc să listeze GCS, sink-ul de
JSON pe GCS devine doar evidență, nu predare, iar contractul de semafor `.FLG` pe GCS pentru
banda de Load slăbește. **Recomand amânarea**: este o schimbare mai mare a contractului de
predare cu echipa de Loader decât este muchia de ieșire, și nu este ce s-a cerut.

---

## 7. Întrebări deschise pentru mâine

| Î | Întrebare | De ce blochează |
|---|---|---|
| **Î1** | Dacă `json_producer` publică deja pe `target-system-target`, **mai există Loader App deloc**, sau Target System consumă pur și simplu topicul pipeline-ului? | Este bifurcația strategică. Răspunsul Kafka poate fi *ștergem Loader-ul*, nu *îl rescriem*. Se răspunde prima. |
| **Î2** | Target System deduplică pe cheia mesajului, sau păstrăm o noțiune de idempotență în payload? | Decide dacă `duplicates` supraviețuiește ca număr pe care îl mai putem raporta. |
| **Î3** | Redenumim `accepted` → `confirmed`, sau versionăm raportul? | Comparabilitatea evidenței arhivate este o valoare declarată a proiectului. |
| **Î4** | Își asumă echipa de Loader un **topic de respingeri**? | Fără el nu există `.ERR`, iar orice document greșit e `unsettled`. Dependență nenegociabilă. |
| **Î5** | Cât așteaptă Loader-ul confirmările înainte să pice rularea? | Parametru operațional nou, fără analog HTTP. |
| **Î6** | Cine deține alertarea pe lag-ul consumatorului? | Este înlocuitorul contrapresiunii prin 429. Azi nu îl urmărește nimic. |

## 8. Recomandare

Facem muchia de ieșire (secțiunea 3), păstrăm `--sink http|kafka` pentru o versiune ca ambele
căi să fie rulabile în paralel împotriva aceleiași suite de acceptanță, și **rezolvăm Î1 și Î4
înainte de a scrie cod** — Î1 poate face toată schimbarea inutilă, iar Î4 o poate face
nelivrabilă.
