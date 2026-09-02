# Tipare reutilizabile extrase din acest repo

> **Traducere în română a [`reusable-patterns.md`](reusable-patterns.md).** Versiunea engleză
> rămâne cea de referință; dacă cele două diferă, engleza câștigă.
>
> **Ce este:** părțile din `test-gcp-dataflow-dataform-cloud-composer` care merită duse mai
> departe în alte proiecte — tipare, plus corecturi de bug-uri câștigate greu, care altfel
> vor fi redescoperite pe calea scumpă. Fiecare intrare numește fișierul, deci codul este
> referința.
>
> **Dată:** 2026-09-02. **Pentru cine:** dezvoltatorii backend și inginerii devops de pe proiectul următor.
>
> Ordonat după cât timp îi economisește următorului om, nu pe subsisteme.

---

## Partea 1 — Pentru dezvoltatori

### 1.1 Sursa efectivă în acest repo: date sintetice, generate local

`harness/generate.py`, `apps/extractor-app/src/main/java/ro/mig/extractor/ExtractorApp.java`

Pentru că datele reale lipsesc, repo-ul are doi generatori/mock-uri care simulează acea sursă:

`apps/extractor-app/.../ExtractorApp.java` — javadoc-ul e explicit: „Owned by the other team
in reality; here it is a faithful mock that honours the stated contract”. Nu generează el
datele — citește un dump deja existent (`readSource(a.input)`, stil table2table) și doar îl
împachetează în artefactele `.DAT`/`.CHS`/`.ERR`/`.RPT`/`.FLG` conform contractului.

`harness/generate.py` — acesta e generatorul propriu-zis. Produce un extract sintetic de
conturi Db2 (`python -m harness.generate --accounts 2000 --format copybook`), plus un
`Manifest` JSON cu numărătorile exacte așteptate (câte înregistrări valide, câte respinse, și
pe ce motiv). Seedează deliberat 5 tipuri de înregistrări malformate (`short_record`,
`bad_numeric`, `bad_date`, `unmapped_status`, `schema_violation`) ca să poată verifica ulterior
că pipeline-ul le clasifică corect — manifestul e „oracolul” pe care `make verify` îl compară
cu rezultatul real al motorului.

Fluxul complet, cu restul pipeline-ului real (partea testată de această echipă) și cu bucla de
confirmare de la capătul de Load:

```
harness/generate.py     →  dump sintetic (CSV cu separator |) + manifest.json 
        ↓
ExtractorApp (mock)     →  împachetează în .DAT/.CHS/.ERR/.RPT/.FLG, criptează PGP,
                            urcă bundle-ul în File Storage (GCS)
                            (`apps/extractor-app/src/main/java/ro/mig/extractor/ExtractorApp.java`)
        ↓
Dataflow File Processor →  citește după semaforul .FLG (1.2), scrie în BigQuery „Extraction”
                            (`pipelines/file_processor/pipeline.py`)
        ↓
Dataform                →  transformare SQL (`dataform/definitions/account_curated.sqlx`),
                            scrie în BigQuery „Transformation”
        ↓
Dataflow Data Enrichment →  îmbogățește înregistrările (`pipelines/data_enrichment/pipeline.py`)
        ↓
Dataflow JSON Producer  →  produce mesajele țintă, le scrie în File Storage (FS2)
                            (`pipelines/json_producer/pipeline.py`)
        ↓
LoaderApp               →  citește din File Storage, trimite fiecare înregistrare cu
                            `X-Idempotency-Key` (1.8) către Target System
                            (`apps/loader-app/src/main/java/ro/mig/loader/LoaderApp.java`)
        ↓
TargetSystemMock        →  simulează Target System: răspunde 201/200 la idempotență,
                            injectează 429/503 (rate limited/unavailable) la o rată configurabilă (1.9), trimite înapoi
                            confirmări + respingeri
                            (`apps/target-system-mock/src/main/java/ro/mig/vault/TargetSystemMock.java`)
        ↓
ReconService             →  citește confirmările/respingerile, calculează ecuația de
                            echilibru `src_read == migrated + not_migrated` (1.3) și
                            produce rapoartele de migrabilitate/reconciliabilitate
                            (`apps/recon-service/src/main/java/ro/mig/recon/ReconService.java`)
        ↓
Cloud Composer            orchestrează toate etapele de mai sus (FP, DF, EN, JP) și pică
                           DAG-ul dacă recon-ul iese cu cod nenul
                           (`composer/dags/mig_000001_1.py`)
```

Deci: sursa „de business” e mainframe-ul (indisponibil acum), iar sursa efectivă folosită azi
e `harness/generate.py`, cu `ExtractorApp` ca strat de mock care imită fidel formatul/contractul
pe care l-ar produce echipa cealaltă, iar la celălalt capăt `TargetSystemMock` joacă același rol
pentru Target System — tot lanțul, de la generare până la reconciliere, rulează fără nicio
dependință de sistemele reale ale celor două echipe.

Această abstractizare are un beneficiu suplimentar: pentru că `contracts/` separă motorul de
runtime-ul care îl execută, iar criteriile de acceptanță verifică rezultate (tabele BigQuery,
artefacte GCS, cifrele din manifest, echilibrul), nu mecanica internă a Beam, a fost posibil să
se genereze 4 propuneri alternative de runtime pentru același C1 (Spark pe Dataproc Serverless,
dbt + BigQuery, Cloud Run Jobs + Workflows, o variantă streaming/CDC — plus o sub-variantă
bazată pe Flink pentru cea din urmă), documentate în
[`docs/alternative-implementations.md`](alternative-implementations.md).

Chiar modelul C4 — de la C1 System Context până la C4 Code — a fost generat ca surse PlantUML
randate în SVG, câte un fișier per nivel; perechile de fișiere sunt enumerate în
[anexă](#anexă--diagramele-c4-ca-fișiere).

### 1.2 Predarea prin semaforul `.FLG` ⭐ *cea mai portabilă idee de aici*

`apps/README.md`, `composer/dags/mig_000001_1.py` (`GCSObjectExistenceSensor`)

Producătorul scrie `.DAT` `.CHS` `.ERR` `.RPT`, apoi — **ultimul, doar după ce tot restul e
durabil** — un fișier `.FLG`. Nimic din aval nu citește niciun octet până nu apare `.FLG`.

Un extras scris parțial nu poate fi niciodată procesat pe jumătate. Fără lacăte, fără
coordonare, fără o bază de date comună între două echipe care nu se apelează niciodată.
Funcționează pe orice object store. **Folosiți acest tipar pentru orice predare de fișiere între
echipe.**

### 1.3 Ecuația de echilibru ca poartă de build

`ReconService.Balance`, `assert_run_balanced` din DAG

`src_read == migrated + not_migrated`, unde **fiecare termen este citit de acolo de unde a
fost efectiv înregistrat, nu re-derivat**: `src_read` din `.RPT`-ul *propriu al amontelui*,
`written` din tabela țintă, `rejected` din jurnalul de respingeri. O discrepanță oriunde apare
ca dezechilibru, în loc să fie definită ca inexistentă de o derivare comună.

Apoi: **recon iese cu cod nenul și face DAG-ul să eșueze.** „O rulare care nu se echilibrează este o
rulare eșuată” devine operațional, nu aspirațional. Fără codul de ieșire e o linie într-un
raport pe care nu-l citește nimeni.

### 1.4 Contracte ca date, impuse la build

`contracts/artefacts.json`, `apps/common/.../Artefacts.java`, `pipelines/common/artefacts.py`

Un singur fișier JSON ține convenția de denumire și numele coloanelor comune. Maven îl copiază
pe classpath-ul Java, iar o **regulă `maven-enforcer` face build-ul să eșueze dacă lipsește**, deci un
jar nu poate fi livrat vreodată fără el. Python citește același fișier. `ArtefactsTest`
folosește manifestul de pe disc drept referință, deci rescrierea unui nume direct în Java face
build-ul să eșueze.

Așa împiedici două limbaje să divergă. (Avertisment: `ARCHITECTURE.md`, slăbiciunea #1, notează
că aceasta acoperă *numele*, nu *tipurile* — un IDL adevărat ar fi mai bun. Luați tiparul,
cunoscându-i plafonul.)

### 1.5 Nu puneți niciodată o valoare implicită pe un câmp de identitate

`LoaderApp.requireField`

Punerea implicită a unei chei de idempotență lipsă pe `""` a făcut serverul să accepte `""` ca
o cheie *validă*: primul document fără cheie a fost creat, iar fiecare următor s-a ciocnit de
el și a fost numărat ca duplicat. **N-1 conturi au dispărut cu un cod de ieșire zero și un
`.FLG` care pretindea succes.**

Regulă: un câmp de identitate lipsă este un defect al intrării, nu o valoare de substituit.
Respingeți-l, duceți-l în fișierul de erori, nu-l trimiteți niciodată.

### 1.6 O scriere asincronă nelivrată este un eșec, nu un succes lent

`pipelines/common/sinks.py:_check_delivered`

`producer.flush(30)` returnează **numărul de mesaje rămase în coadă când a renunțat**.
Ignorarea acelei valori este modul în care un broker mort trece drept succes: cu nimeni în
ascultare, fiecare mesaj stă în coada locală până la timeout, callback-ul de livrare nu este
invocat deloc, iar lista de erori rămâne goală.

Prima rulare pe Composer a pierdut 400 de înregistrări așa — două loturi verzi de 30 de
secunde, zero mesaje în Kafka, nimeni anunțat. **Verificați valoarea returnată de fiecare
flush/close/drain.**

### 1.7 Citire mărginită dintr-un flux nemărginit

`ReconService.readConfirmations`

Cum citești „tot ce e acum în topic” și te oprești: grup de consumatori proaspăt pe rulare,
`assign` pe toate partițiile, `seekToBeginning`, instantaneu al `endOffsets` luat o singură
dată, poll până când fiecare partiție își atinge instantaneul, cu un termen limită pe ceas ca
plasă de siguranță.

Trei lucruri o fac corectă: **grupul proaspăt pe rulare** (o rerulare nu sare peste ce a
comis un grup anterior), **instantaneul de end-offset** (un topic în regim permanent nu e
niciodată gol, deci marginea e cea care o face să se termine) și **termenul limită** (un
broker care nu mai avansează offset-urile nu poate bloca job-ul).

### 1.8 Idempotență reală, nu sperată

`LoaderApp` + `TargetSystemMock`

Clientul trimite `X-Idempotency-Key`; serverul *și-o amintește* și returnează `200` în loc de
`201` la reluare. Acest lucru — și numai el — face sigură livrarea at-least-once. Backoff
exponențial **cu jitter**, ca un lot limitat să nu reîncerce în pas cadențat.

### 1.9 Un mock care se poartă urât intenționat

`TargetSystemMock` — injectează 429/503 la o rată configurabilă, cu sămânță pentru
reproductibilitate

Căile de retry și backoff sunt *executate la fiecare rulare*, nu doar scrise. Un mock care
returnează mereu 200 nu testează nimic din ce te îngrijora.

Plus endpoint-uri de administrare pentru testabilitate: `/__admin/stats`, `/__admin/reset` și
un `/__admin/suppress-next-confirmation` cu un singur foc, care fabrică o breșă „trimis dar
nepersistat” **determinist, niciodată pe o cale reală** — deci criteriul de acceptanță pe
calea negativă chiar poate fi dovedit.

### 1.10 Separați nucleul decidabil de I/O

`ReconciliationMatcherTest` testează logica de diferență de mulțimi fără Kafka și fără
BigQuery.

Orice lucru care are forma „ia din două locuri, compară, decide” ar trebui să aibă jumătatea de
*comparat și decis* apelabilă cu două colecții în memorie.

### 1.11 REST scris de mână în loc de SDK-uri de furnizor — dar cu o cusătură

`apps/common/`: `HttpObjectStore`, `BigQueryRest`, `GcpToken`

~200 de linii de HTTP simplu în loc de arborele de clienți Google, pentru aplicații de
mock/suport unde costul dependenței depășește economia. **Valoarea stă în raționamentul
consemnat plus interfața `ObjectStore` ca cusătură** — trecerea la `google-cloud-storage` este
o schimbare de o clasă dacă acest compromis încetează să aibă sens. Faceți-o conștient,
documentați plafonul.

### 1.12 Comentarii care consemnează incidentul, nu codul

Peste tot — `sinks.py`, `LoaderApp`, DAG-ul

Comentariile din acest repo spun *ce a mers prost, când, și cum arăta mesajul de eroare*:
„2026-08-23, json_producer on the DAG”, „an error that names the symptom and not the
signature”. Cine dă peste aceeași linie de log o poate căuta cu grep.

Este convenția cu cel mai mare efect de pârghie din repo și nu costă nimic.


---

## Partea 2 — Pentru devops / platformă

### 2.1 GCP Managed Kafka OAUTHBEARER — cele două părți neevidente ⭐

`GcpTokenOauthCallbackHandler.java`, `sinks.py:_kafka_token`, `_oauth_token_cb`

Două eșecuri care au costat câte o zi fiecare:

1. **Broker-ul nu acceptă un access token brut.** Vrea o valoare în formă de JWT, unită cu
   puncte, base64url: `b64(header).b64(claims).b64(accessToken)` cu `alg=GOOG_OAUTH2_TOKEN`,
   `scope=kafka`, `sub` = adresa contului de serviciu. Trimiterea token-ului brut eșuează cu
   *„invalid credentials with SASL mechanism OAUTHBEARER”* — care numește mecanismul, nu
   codificarea.
2. **Modulul de login JAAS este obligatoriu chiar și cu un callback handler propriu**, altfel
   Kafka eșuează cu *„No login module found for OAUTHBEARER”*.

Iar pe partea de Python: `librdkafka` apelează `oauth_cb` cu șirul `sasl.oauthbearer.config`,
deci un **apelabil fără argumente aruncă TypeError în firul de serviciu al clientului**,
token-ul nu e setat niciodată, iar handshake-ul moare cu *„OAuth token not set within 10
seconds timeout”*. În plus, coada aceea e servită abia la primul produce/poll — deci
**amorsați handshake-ul cu o buclă de `poll()`** înainte să dați producer-ului un lot.

### 2.2 O singură variabilă de mediu comută transportul local vs cloud

`KAFKA_SECURITY_PROTOCOL` — `PLAINTEXT` (redpanda, implicit) vs `SASL_SSL` (Managed Kafka)

Același binar rulează în ambele lumi. **Omiterea ei nu este un no-op tăcut**: un client
PLAINTEXT împotriva unui broker doar-SASL_SSL se blochează până la termenul de poll și
raportează zero înregistrări. Trebuie să ajungă la *pod-uri* — o variabilă de mediu Composer
este invizibilă pentru un `KubernetesPodOperator`, care transmite doar ce numește `env_vars`.

> Contra-lecția din același repo: `ARCHITECTURE.md`, slăbiciunea #2, semnalează **trei
> comutatoare independente de tip „în ce lume sunt” care pot să nu fie de acord**. Un comutator
> e bine; trei comutatoare sunt bug-ul următor.

### 2.3 Nu livrați niciodată cu un tag de imagine flotant

`composer/dags/mig_000001_1.py`

Etichetați imaginile cu **SHA-ul de git** (`-dirty` adăugat pentru un arbore murdar) și setați
`image_pull_policy="Always"`. Un tag flotant înseamnă că o reconstruire schimbă în tăcere ce
rulează DAG-ul și că o rulare trecută nu poate fi reprodusă. Kubernetes folosește implicit
`IfNotPresent` pentru orice tag în afară de `:latest`, deci cu un tag SHA mutabil un nod
continuă să servească **stratul vechi**, iar o corectură reconstruită nu intră în vigoare, în
tăcere.

### 2.4 Nu înghețați o credențială într-un pod spec

Același fișier

Un access token trăiește ~1 h; un DAG rulează timp îndelungat. Înghețarea unuia în pod spec înseamnă o
credențială care expiră în mijlocul migrării. Aplicațiile care vorbesc HTTP brut nu preiau
automat identitatea de tip Workload Identity — deci **luați token-ul de la metadata server-ul GKE și reîmprospătați-l**,
cu o suprascriere prin variabilă de mediu drept cusătura prin care injectează orchestratorul
local.

### 2.5 Pod-urile Composer trebuie să aterizeze în namespace-ul propriu al Composer

Același fișier, `_pod_namespace()`

Worker-ul Airflow rulează ca `system:serviceaccount:<composer-ns>:default`, iar RBAC-ul
Composer îl restrânge la propriul namespace. Cererea către `default` eșuează cu *„pods is
forbidden … cannot list resource pods in the namespace default”*.

Citiți namespace-ul **din fișierul pe care îl poartă fiecare pod**, nu dintr-o variabilă de
mediu — `--update-env-variables` al Composer scapă în tăcere unele nume și înlocuiește tot
setul.

### 2.6 Timeout-urile de pornire la rece se citesc ca eșecuri de aplicație

`startup_timeout_seconds=900`, nu 600

Pe un mediu Composer rece, pod-ul așteaptă ca Autopilot să provizioneze un nod *și* trage o
imagine de câțiva GB cu `image_pull_policy=Always`. Acest lucru a depășit 600s și a eșuat cu *„Pod
took longer than 600 seconds to start”* — un timeout de infrastructură deghizat în eșec al
pipeline-ului. Clusterele calde pornesc în sub un minut, așa că pragul mai mare costă timp doar la
prima rulare după o reconstruire.

### 2.7 Dați fiecărei aplicații identitatea ei

`terraform/modules/iam`, `service_account_name` per pod

`dataflow-worker`, `loader-app`, `recon-service`, `target-system-mock` sunt SA-uri separate cu
roluri deliberat înguste (recon are voie doar să *citească* BigQuery). Definirea unor roluri
înguste este inutilă dacă fiecare pod rulează tot pe contul comun — **`service_account_name` la
nivel de pod este ce le pune în vigoare.**

### 2.8 Comutatoare de cost ca variabile Terraform de prim rang

`enable_kafka`, `enable_composer`; `count = var.enabled ? 1 : 0`; output-uri care returnează
`""` când e dezactivat, ca apelanții să le poată transmite necondiționat

Managed Kafka se facturează pe oră-vCPU. Tot lanțul dependent — cluster, topicuri, conector
VPC, atribuiri de roluri IAM — atârnă de un singur flag, iar un apply dezactivat nu produce nimic și nu
costă nimic.

> ⚠️ **Și capcana:** același tipar de flag s-a scurs în *comportament*. Un bootstrap Kafka gol
> face recon să sară peste verificarea confirmărilor și să raporteze `enabled=false` — „ceea ce
> menține verde o rulare fără Kafka”. Un comutator de cost care **degradează în tăcere și o
> poartă de corectitudine** este modul în care o rulare trece fără să fi dovedit nimic. Țineți
> comutatoarele de cost în afara căilor de aserțiune, sau faceți-le să pice închis.

### 2.9 Serverless→VPC nu e gratis

`terraform/modules/vpc_connector`

Managed Kafka este accesibil doar din interiorul VPC-ului. Cloud Run nu îl poate accesa fără un conector Serverless VPC
Access. Lucrul acesta s-a descoperit *după* ce mock-ul fusese deja livrat și a blocat un criteriu de
acceptanță. **Verificați accesibilitatea încă din faza de proiectare, nu la testare.**

### 2.10 Evidența ca director în repo

`docs/evidence/<scenariu>-<dată>/`, `docs/evidence-map.md`

Log-uri de terraform apply, log-uri de pod-uri, ieșiri de verificare, log-uri de teardown —
comise, datate și indexate după ce criteriu de acceptanță dovedește fiecare. Când cineva
întreabă „a funcționat vreodată pe infrastructură reală?”, răspunsul e o cale, nu o
amintire.

### 2.11 Intrările Terraform critice pentru producție n-ar trebui să fie opționale

`ARCHITECTURE.md`, slăbiciunea #6 — o constatare deschisă, listată aici ca avertisment

Variabilele opționale cu valori implicite plauzibile transformă o omisiune într-un eșec la
**runtime** în loc de unul la **plan**. Dacă o valoare nu are nicio valoare implicită care ar
putea fi vreodată corectă, nu-i dați niciuna.

### 2.12 Containerizați smoke-testul

`Dockerfile.toolbox`, `make smoke-gcp`

O singură rulare cap-coadă, minusculă, împotriva GCP-ului real, dintr-un container care
fixează Python, Beam și JRE — ca „merge pe laptopul meu” să nu facă parte din rezultat.

---

## Partea 3 — Moduri de lucru care merită "furate"

| Practică | Unde | De ce se merită |
|---|---|---|
| **`make help`** cu comentarii `##` pe fiecare țintă | `Makefile` | un singur punct de intrare descoperibil per activitate; fără derivă de README |
| **Documente de schimbare datate** (`PLAN-CHANGES-<dată>.md`) care înlocuiesc pe loc | `docs/` | documentele ulterioare adnotează în loc să contrazică în tăcere |
| **Numele câmpurilor din rapoarte sunt înghețate intenționat** | `ReconService.Balance` | evidența arhivată rămâne comparabilă între versiuni; o redenumire e o schimbare distructivă |
| **O listă de slăbiciuni ierarhizată, cu marcaje de stare** | `ARCHITECTURE.md` | „cunoscut și ierarhizat” bate „necunoscut”; marcajele ✅/⚠️ arată mișcarea |
| **Stiva locală oglindește topologia de producție, nu scara** | `local/docker-compose.yml` | fake-gcs + emulator BQ + redpanda + un mock care se poartă urât — *forma* e corectă |
| **Spuneți din start ce este mock** | `docs/production-readiness.md` §0 | nimeni nu confundă prototipul cu produsul |
| **Diagrame C4 cu fiecare muchie etichetată *ce curge* și *peste ce tehnologie*** | `README.md`, `ARCHITECTURE.md` | săgețile neetichetate ascund exact deciziile care contează |

---

## Top 5, dacă e timp doar pentru cinci

1. **Semaforul `.FLG` scris ultimul** (1.2) — portabil la orice echipă, orice stocare, azi.
2. **Verificați valoarea returnată de fiecare flush asincron** (1.6) — aici s-au pierdut date în tăcere.
3. **Nu puneți valoare implicită pe un câmp de identitate** (1.5) — și aici s-au pierdut date în tăcere.
4. **Ecuația de echilibru cu un cod de ieșire** (1.3) — transformă un raport într-o poartă.
5. **Tag-uri de imagine cu SHA de git + `pull_policy: Always`** (2.3) — reproductibilitate gratuită.

---

## Anexă — diagramele C4, ca fișiere

Diagramele C4 — de la C1 System Context până la C4 Code — au fost generate ca surse
PlantUML și randate în SVG, câte o pereche de fișiere `.puml` + `.svg` pentru fiecare
nivel, în [`docs/plantuml/`](plantuml), indexate în
[`docs/plantuml/README.md`](plantuml/README.md):

| Nivel | Sursă PlantUML | SVG randat |
|---|---|---|
| C1 System Context | [`readme-03-c1-system-context.puml`](plantuml/readme-03-c1-system-context.puml) | [`readme-03-c1-system-context.svg`](plantuml/readme-03-c1-system-context.svg) |
| C2 Containers | [`readme-04-c2-containers.puml`](plantuml/readme-04-c2-containers.puml) | [`readme-04-c2-containers.svg`](plantuml/readme-04-c2-containers.svg) |
| C3 Components — File Processor | [`architecture-01-c3-file-processor.puml`](plantuml/architecture-01-c3-file-processor.puml) | [`architecture-01-c3-file-processor.svg`](plantuml/architecture-01-c3-file-processor.svg) |
| C3 Components — Recon Service | [`architecture-02-c3-recon-service.puml`](plantuml/architecture-02-c3-recon-service.puml) | [`architecture-02-c3-recon-service.svg`](plantuml/architecture-02-c3-recon-service.svg) |
| C4 Code — two-door engine | [`architecture-03-c4-code-two-door-engine.puml`](plantuml/architecture-03-c4-code-two-door-engine.puml) | [`architecture-03-c4-code-two-door-engine.svg`](plantuml/architecture-03-c4-code-two-door-engine.svg) |

Le regenerați pe toate cu `./render.sh` — are nevoie doar de un JRE și `plantuml.jar`;
diagramele C4 folosesc stdlib-ul C4-PlantUML cu `layout smetana`, deci nu au nevoie de
Graphviz.
