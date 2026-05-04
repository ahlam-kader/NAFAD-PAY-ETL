# G3 - Data Engineering Team : Medallion Architecture

## Quick-start (ce dépôt)

```bash
cp .env.example .env          # ajuste les ports si besoin
make up                        # lance Postgres + MinIO
make pipeline                  # init DDL + bronze + silver + gold
make idempotence               # rejoue le pipeline et compare les snapshots
make test                      # pytest (10 tests : idempotence + anomalies)
make anomalies                 # affiche les compteurs d'anomalies
make psql                      # shell psql vers la warehouse
make minio-ui                  # URL console MinIO (visualiser le Bronze)
make clean                     # arrête tout et supprime les volumes
```

### Livrables dans ce dossier

| Fichier | Contenu |
|---|---|
| `pipeline/`                       | Code Python (Bronze / Silver / Gold + helpers) |
| `sql/init_warehouse.sql`          | DDL Silver + Gold (idempotente) |
| `tests/`                          | Tests pytest : idempotence + anomalies |
| `docker-compose.yml` + `Makefile` | Orchestration locale |
| `ANOMALIES_REPORT.md`             | Rapport d'anomalies + réponses aux questions d'investigation |
| `ARCHITECTURE_EARLY_STAGE.md`     | Doc d'architecture MVP livrable maintenant |
| `ARCHITECTURE_AT_SCALE.md`        | Doc d'architecture cible 5 M tx/mois |
| `DEPLOYMENT_AWS.md`               | Runbook copy-paste pour déployer sur le compte AWS sandbox |
| `Dockerfile.aws`                  | Image self-contained pour ECS (inclut `daily_dumps/`) |
| `aws/`                            | Templates IAM + task definition |



  ## Déploiement AWS (sandbox)
                                                  
  Pipeline déployé et exécuté avec succès sur AWS 
  `eu-west-3` :                                   
  - Image Docker poussée sur ECR                  
  - Tâche ECS Fargate exécutée (exit code 0)      
  - Données chargées en RDS PostgreSQL conformes
  aux comptages locaux                            
  - Logs CloudWatch capturés
                                                  
  Preuves dans `docs/screenshots/` (01 ECR, 02    
  ECS, 03 CloudWatch, 04 psql).                   
  Runbook reproductible dans `DEPLOYMENT_AWS.md`. 



  

## Rôle dans l'entreprise

Vous êtes la **Data Engineering Team**. Vous faites circuler et nettoyer les données entre les systèmes OLTP (G1) et analytiques (G4). Un doublon, une ligne perdue, et la compta ne tombe plus juste. Vous êtes l'épine dorsale de la data.

## Approche imposée : Medallion Architecture

Vous allez construire un pipeline en **3 couches** - c'est le standard moderne (Databricks, Snowflake, AWS Lake Formation, dbt) :

| Couche | Contenu | Objectif |
|---|---|---|
| **Bronze** | Données brutes, immuables, au format source | Traçabilité, replay, audit |
| **Silver** | Données nettoyées, typées, dédupliquées, schéma unifié | Source de vérité technique |
| **Gold** | Agrégats métier prêts pour consommation | Alimente G4 (DWH) et dashboards |

**Règle d'or :** chaque couche est reproductible à partir de la précédente. Rejouer Bronze→Silver→Gold deux fois doit donner un état **bit-exact identique**. C'est ça, l'idempotence.

## Objectif en 10 jours

1. Analyser les 7 jours de dumps (structure full vs delta, anomalies)
2. Implémenter le pipeline Bronze / Silver / Gold en Python + SQL
3. Tests d'idempotence + rapport d'anomalies détectées
4. Deux documents d'architecture **AWS** : Early Stage et At Scale
5. Déploiement test sur AWS ECS Task + S3 (compte sandbox fourni)

Référez-vous à `PROJET_NAFAD_PAY.html` à la racine pour le planning, la grille d'évaluation, et le template d'archi.

## Données fournies

7 dossiers datés dans `daily_dumps/` :

| Date | Type | users | accounts | transactions | fees |
|---|---|---|---|---|---|
| 2024-01-15 | **FULL** | 1 429 (full) | 1 575 (full) | 14 286 | 5 913 |
| 2024-01-16 | Delta | 1 429 | 1 575 | 14 589 | 5 913 |
| 2024-01-17 | Delta | 1 459 | 1 575 | 14 286 | 5 913 |
| 2024-01-18 | **FULL** | 5 716 (full) | 6 300 (full) | 14 286 | 5 913 |
| 2024-01-19 | Delta | 1 444 (dont **58 CLOSED/SUSPENDED**) | 1 590 | 14 586 | 5 913 |
| 2024-01-20 | Delta | 1 442 | 1 575 | 14 286 | 5 913 |
| 2024-01-21 | Delta | 1 426 | 1 569 | 14 284 | 5 913 |

**Total : 100 603 transactions cumulées** sur les 7 jours.

### Anomalies mesurées

| Anomalie | Volume | Couche à traiter |
|---|---|---|
| Clock skew (`completed_at < created_at`) | **2 575 (2,56 %)** | Flaggée en Silver avec `_has_clock_skew=true` |
| Soft-deletes (status CLOSED/SUSPENDED) le 19 | **58 users** | Propager en Silver avec `_is_deleted=true` |
| Transactions cross-datacenter (`is_cross_dc=true`) | À mesurer | Flagger en Silver |
| `fees_daily.csv` volumétrie stable (5 913 lignes/jour), contenu variable selon la date | 7 × 5 913 | Déduplication contrôlée en Bronze→Silver (clés métier + date) |

### Schéma `transactions_daily.csv` (24 colonnes)

```
id, reference, transaction_type, amount, fee, total_amount,
source_account_id, source_user_id, destination_account_id, destination_user_id,
merchant_id, agency_id,
status, failure_reason,
source_node, processing_node, source_datacenter, processing_datacenter, is_cross_dc,
transaction_date, transaction_time, created_at, completed_at
```

**À noter** : ce schéma est **spécifique à G3**. Il contient `source_node/processing_node/source_datacenter/processing_datacenter/is_cross_dc` - colonnes absentes des autres groupes. **Pas de `idempotency_key`** : votre dedup doit s'appuyer sur `(id, reference)`.

## Livrables attendus

1. **Pipeline Python** en 3 étapes (Bronze / Silver / Gold) + orchestration simple (Makefile ou script principal)
2. **Test d'idempotence** : script qui lance le pipeline 2 fois et vérifie l'égalité bit-à-bit
3. **Rapport d'anomalies** (1-2 pages MD) : clock skew par nœud/DC, soft-deletes, cohérence full du 18 vs accumulation des jours précédents
4. **Document d'architecture Early Stage** (1-2 pages)
5. **Document d'architecture At Scale** (2-3 pages, focus streaming CDC)
6. `docker-compose.yml` + `Makefile` pour rejouer tout

## Guidelines techniques

### Structure de projet recommandée

```
G3_ETL/
  daily_dumps/          # fourni
  pipeline/
    bronze.py           # copie dumps → bronze/ (S3 ou MinIO)
    silver.py           # clean + dedupe + normalise
    gold.py             # agrégats métier
    common/
      schemas.py        # définitions Pandera / Pydantic
      io.py             # lecture/écriture CSV/Parquet
  tests/
    test_idempotence.py
    test_anomalies.py
  docker-compose.yml
  Makefile
```

### Docker Compose

```yaml
services:
  warehouse:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: warehouse
      POSTGRES_PASSWORD: ${DB_PASSWORD}
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: ${MINIO_PASSWORD}
    ports: ["9000:9000", "9001:9001"]
  runner:
    build: ./pipeline
    depends_on: [warehouse, minio]
```

### Couche Bronze (ingestion brute)

- Copie `daily_dumps/YYYY-MM-DD/*.csv` vers MinIO sous `s3://bronze/dt=YYYY-MM-DD/<fichier>.csv`
- Ajoute des métadonnées : `_ingested_at`, `_source_file`, `_batch_id` (UUID)
- **Immuable** : on n'écrase jamais un fichier Bronze
- Table `bronze_runs` (batch_id, date, file, lines_read, ingested_at, checksum_sha256)

### Couche Silver (nettoyage)

- Lit Bronze, valide schéma (`pandera` ou `great_expectations`)
- Déduplique par `(reference, id)` via fenêtre de 30 jours glissants
- Réconcilie full dumps (15, 18) vs deltas : le full du 18 sert de **point de contrôle**
- Applique les soft-deletes du 19 janvier (58 users → `silver_users._is_deleted=true`)
- Flagge les anomalies : `_has_clock_skew`, `_is_cross_dc`
- Charge dans Postgres : `silver_users`, `silver_accounts`, `silver_transactions`, `silver_fees`

### Couche Gold (agrégats)

Tables prêtes pour G4 :

- `gold_daily_volume (transaction_date, tx_count, success_count, failed_count, total_amount)`
- `gold_user_activity (user_id, tx_count_30d, last_tx_date, active_flag)`
- `gold_agency_perf (agency_id, tx_count, total_fees_collected, float_usage_pct)`

**Rafraîchissement incrémental** (pas de full refresh) via clé `(transaction_date, entity_id)` avec `INSERT ... ON CONFLICT DO UPDATE`.

### Test d'idempotence

```bash
make pipeline   # run 1
SHA1=$(md5sum state.snapshot)
make pipeline   # run 2
SHA2=$(md5sum state.snapshot)
test "$SHA1" = "$SHA2" || exit 1
```

## Questions d'investigation obligatoires

1. Parmi les 2 575 `completed_at < created_at`, quel % a `is_cross_dc=true` ? Le clock skew est-il plus fréquent sur les tx cross-DC ?
2. Le full du 18 contient 5 716 users. Additionnez les users_delta des jours 16, 17 : l'accumulation colle-t-elle avec le full du 18 ?
3. Pour les 58 CLOSED/SUSPENDED du 19 : votre pipeline supprime-t-il ou soft-delete (flag) ? Argumentez.
4. Comment garantissez-vous qu'un rejeu du dump du 20 (après l'avoir déjà chargé) ne crée pas de doublons ?
5. Réconciliation multi-nœuds : si une tx apparaît sur `source_node=NKC-NODE-1` puis `processing_node=NDB-NODE-1`, laquelle est la "vraie" ? Last-write-wins ? Source of truth par nœud ?

## Architecture AWS - points obligatoires

### Cible At Scale - diagramme attendu

```
Partner/OLTP ──► S3 (Bronze, Object Lock, SSE-KMS)
                         │
                         ▼ EventBridge (new object)
                  Step Functions
                    ├─ ECS Task (Silver transform)
                    └─ ECS Task (Gold aggregate)
                         │
                         ▼
                  RDS Postgres (Gold) + Glue Catalog (Silver Parquet)
                         │
                         ▼
                    Consumption G4 (Athena/Redshift)
```

### Hébergement

| Couche | Stockage | Compute |
|---|---|---|
| Bronze | S3 (partitionnement Hive `dt=YYYY-MM-DD/`), Object Lock | ECS Task (Fargate) |
| Silver | S3 Parquet partitionné + Glue Data Catalog | ECS Task (Fargate) |
| Gold | RDS PostgreSQL séparé du DWH de G4 (ou Athena) | ECS Task (Fargate) |
| Orchestration | EventBridge + Step Functions | - |

### Sécurité

- IAM role dédié par étape Step Functions (moindre privilège)
- Bronze task : lecture bucket source + écriture bucket Bronze uniquement
- Silver task : lecture Bronze + écriture Silver uniquement
- Gold task : lecture Silver + écriture RDS Gold uniquement
- Chiffrement S3 SSE-KMS avec CMK par couche (rotation annuelle)
- Secrets DB dans Secrets Manager, injection via task role
- Validation en amont : taille max dump 10 GB, schéma via `pandera`, quarantaine `s3://nafad-quarantine/`

### Threat model (top 3)

1. **Dump altéré en transit** → checksum SHA-256 stocké en Bronze + MFA Delete sur bucket
2. **Zip bomb / CSV énorme** → limite taille + extraction streaming + timeout ECS task
3. **Credentials DB en logs** → filtres CloudWatch + redaction + audit trail

### Scale - passage au streaming

Remplacer EventBridge + batch par CDC (AWS DMS ou Debezium) depuis RDS OLTP (G1) → **Kinesis Data Streams** → ECS consumer → Silver temps réel. **Coexistence batch + streaming = Lambda Architecture** (batch garde le backfill, streaming alimente le temps réel).

## Correspondance des datacenters fictifs

| Donnée | AWS (implémentation) | GCP (comparaison) | Hetzner (comparaison bare-metal) |
|---|---|---|---|
| `DC-NKC-PRIMARY` | `eu-west-3a` | `europe-west9-a` | `fsn1` (Falkenstein) |
| `DC-NKC-SECONDARY` | `eu-west-3b` | `europe-west9-b` | `nbg1` (Nuremberg) |
| `DC-NDB` | `eu-west-3c` | `europe-west9-c` | `hel1` (Helsinki, DR éloigné) |

Implémentation cible : **AWS**. GCP et Hetzner servent de référence comparative pour votre doc d'archi.
