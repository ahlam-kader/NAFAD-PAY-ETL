# Architecture — Group G3 (Data Engineering) — **At Scale**

> Cible haute charge : 5 M tx/mois (~170 k/jour), 500 k utilisateurs actifs,
> ingestion streaming en complément du batch pour les KPI temps réel.
> Région principale : `eu-west-3` (Paris), 3 AZ, alignée avec G1/G2/G4.

## 1. Contexte & contraintes

| Dimension | Cible At Scale |
|---|---|
| **Charge** | 170 k tx/jour batch + ~500 tx/s en pic streaming |
| **Volume Silver cumulé** | 60 M lignes/an (transactions), partitionnement obligatoire |
| **Latence ETL** | Batch J+1 pour le DWH historique, **< 5 min** pour le near-real-time |
| **SLO disponibilité** | 99,9 % (8 h 45 min downtime/an) |
| **RPO / RTO** | RPO ≤ 1 min (CDC) sur Silver streaming, RTO 30 min (multi-AZ) |
| **Budget** | < 4 000 USD / mois AWS, justifié par les 5 M tx |
| **Compliance** | BCM Mauritanie + secret bancaire + RGPD (PII utilisateurs réels) |

### Hypothèses
- G1 (OLTP RDS) tourne sur `eu-west-3` Multi-AZ avec WAL accessible — prérequis pour CDC via DMS.
- G4 consomme à la fois le Silver Parquet (Athena/Redshift) et le Gold Postgres.
- Les pics de trafic sont *prévisibles* (fin de mois, fêtes) → autoscaling planifié.

## 2. Diagramme — niveau Containers

```
   +----------------+       +-----------------+
   |  G1 OLTP (RDS  |       | Partner uploads |
   |  Multi-AZ)     |       | (S3 Pre-signed) |
   +--------+-------+       +--------+--------+
            |                        |
            | Logical replication    | S3 PUT
            v                        v
     +------+------+         +-------+-------+
     |  AWS DMS    |         | s3://nafad-   |
     | (CDC stream)|         |   bronze-ew3/ |
     +------+------+         +-------+-------+
            |                        |
            v                        | EventBridge "ObjectCreated"
   +--------+--------+                v
   |  Kinesis Data   |       +--------+---------+
   |   Streams       |       |  Step Functions  |
   |  (8 shards)     |       |  Bronze→Silver→  |
   +--------+--------+       |  Gold (batch)    |
            |                +--------+---------+
            v                         |
   +--------+----------+              v
   | ECS Fargate       |     +--------+----------+
   | streaming worker  |     | ECS Fargate tasks |
   | (Silver near-real |     | (Silver/Gold      |
   |  time + Quarantine|     |  batch transform) |
   +--------+----------+     +--------+----------+
            |                         |
            v                         v
   +--------+--------+        +-------+--------+      +-----------------+
   | ElastiCache     |        | s3://nafad-    |<---->| Glue Data       |
   | Redis (live KPI)|        |  silver-ew3/   |      | Catalog         |
   +-----------------+        |  Parquet by    |      +--------+--------+
                              |  dt + node     |               |
                              +-------+--------+               v
                                      |                 +------+-------+
                                      v                 |  Athena +    |
                              +-------+--------+        |  Redshift    |
                              | RDS Postgres   |<------>|  Spectrum    |
                              | (Gold, M-AZ)   |        | (G4 reads)   |
                              +----------------+        +--------------+
```

## 3. Choix techniques (ADR-lite)

### ADR-1 — Bronze = **S3 Parquet partitionné Hive**, Object Lock activé
- **Décision :** `s3://nafad-bronze-ew3/dt=YYYY-MM-DD/source=<file>/` en Parquet snappy ; Object Lock mode `GOVERNANCE` rétention 1 an ; SSE-KMS avec CMK dédiée Bronze (rotation annuelle).
- **Alternatives considérées :** S3 CSV (Early Stage), HDFS/EMR FS.
- **Pourquoi :** Parquet × Athena = scan 30× moins cher que CSV ; Object Lock = bouclier ransomware/erreur humaine ; CMK par couche pour blast-radius minimal.

### ADR-2 — Silver = **S3 Parquet + Glue Catalog**, partitionnement `(dt, source_datacenter)`
- **Décision :** Silver écrit sur `s3://nafad-silver-ew3/<entity>/dt=…/source_datacenter=…/`, registered dans Glue Data Catalog, queryable via Athena.
- **Alternatives considérées :** Iceberg/Delta tables, RDS Postgres scaled up.
- **Pourquoi :** Postgres ne tient pas la lecture analytique des 60 M tx/an. Iceberg serait préférable à terme (DELETE/UPDATE atomiques) — on garde la porte ouverte en n'utilisant pas de format propriétaire en écriture. Le partitionnement sur `source_datacenter` colle aux questions métier les plus fréquentes et accélère les filtres.

### ADR-3 — Gold = **RDS Postgres Multi-AZ db.r7g.large** + read replica
- **Décision :** Gold reste sur Postgres pour la lecture transactionnelle des dashboards G4 (jointures complexes, latence < 100 ms). Multi-AZ sur 2 zones de Paris, 1 read-replica `eu-west-3c` pour les requêtes ad-hoc.
- **Alternatives considérées :** Aurora PostgreSQL, Redshift dédié, DynamoDB.
- **Pourquoi :** Aurora coûterait +60 % pour des KPI dont le volume reste sous le TB ; Redshift est déjà chez G4. Postgres = jointures SQL pures + connectivité native pour BI.

### ADR-4 — Streaming = **AWS DMS (CDC) → Kinesis Data Streams → ECS consumer**
- **Décision :** DMS lit le WAL Postgres de G1 (`replication_slot`), pousse les changements ligne-par-ligne dans un Kinesis stream à 8 shards, un service ECS Fargate consomme et alimente Silver (Parquet + Redis).
- **Alternatives considérées :** Debezium / MSK, Kafka Connect autogéré, RDS Streams.
- **Pourquoi :** DMS est managé donc 0 ops ; Kinesis 8 shards = 8 MB/s = 50× la cible 500 tx/s ; Debezium nécessiterait MSK + opérations Kafka, hors-budget équipe.

### ADR-5 — Orchestration batch = **EventBridge + Step Functions**
- **Décision :** EventBridge déclenche le DAG quand un nouveau dump arrive sur S3 ; Step Functions enchaîne `Bronze → Validate → Silver → Gold` avec retry+backoff, DLQ SQS et alarmes CloudWatch.
- **Alternatives considérées :** MWAA (Airflow managé), Prefect Cloud.
- **Pourquoi :** 4 tâches linéaires + branchement quarantine ne justifie pas Airflow. Step Functions = state machine visuelle, gratuit jusqu'à 4 000 transitions/mois.

### ADR-6 — Lambda Architecture explicite
- **Batch path** : EventBridge → Step Functions → ECS tasks → Silver/Gold (J+1).
- **Speed path** : DMS → Kinesis → ECS streaming worker → Silver Parquet incrémental + Redis live counters.
- **Réconciliation** : la table `silver.transactions` Parquet est *upsertée* par la version batch toutes les 24 h, écrasant les éventuelles approximations du streaming.

## 4. Flux de données — happy path

### Batch (J+1)
1. G1 ou un partenaire dépose un nouveau dump sur `s3://nafad-bronze-ew3/inbox/dt=YYYY-MM-DD/`.
2. EventBridge capte `ObjectCreated`, déclenche la Step Function.
3. **Validate task** (Fargate) — pandera + checksum SHA-256, taille < 10 GB, sinon → `s3://nafad-quarantine/`.
4. **Silver task** (Fargate, Spot OK) — lit Bronze, dedupe, flags, écrit Parquet partitionné, met à jour Glue partitions.
5. **Gold task** (Fargate) — `INSERT … ON CONFLICT DO UPDATE` sur les 3 tables Gold via SQL.
6. CloudWatch métriques : `g3.bronze.lines`, `g3.silver.duped`, `g3.gold.upserts`, `g3.lag_seconds`.

### Streaming (continu, p99 < 5 min)
1. DMS consomme le WAL G1 → publie sur Kinesis (1 enregistrement par row change).
2. ECS streaming worker (KCL en Python) :
   - dedup par `(id, reference)` sur fenêtre 1 h Redis ;
   - écrit en append sur `s3://nafad-silver-ew3/transactions/dt=…/` (micro-batch 60 s) ;
   - publie agrégats live dans ElastiCache Redis (`tx_per_second`, `cross_dc_count`).
3. G4 lit Redis pour les dashboards live, Athena/Redshift pour l'historique.

### Idempotence — au niveau cluster
- Bronze : `batch_id = uuid5(NS, "{date}/{file}")`, S3 Object Lock empêche l'écrasement avec rétention.
- Silver Parquet : convention `<entity>/dt=<date>/run_id=<uuid5>/part-*.parquet` ; un re-run remplace le sous-dossier `run_id=` correspondant via `aws s3 sync --delete`.
- Streaming : `(id, reference, dump_kind)` dans Redis avec TTL 1 h sert de clef de dédoublonnage.
- Gold : `INSERT … ON CONFLICT DO UPDATE` sur clés `transaction_date`, `user_id`, `agency_id`.

## 5. Points de rupture identifiés & seuils de bascule

| Métrique | Seuil de bascule | Action |
|---|---|---|
| Kinesis IteratorAge p99 | > 60 s | scale shards (×2) ou ECS tasks (×2) |
| Athena scan coût | > 100 USD/jour | re-partitionner Silver, créer projection Glue |
| Postgres Gold connexions | > 80 % max | RDS Proxy + augmenter `max_connections` |
| Step Function durée | > 90 min | paralléliser entités (Map state), passer Silver à EMR Serverless |
| Lag CDC vs OLTP | > 5 min sur 3 fenêtres consécutives | alarme PagerDuty, basculer en mode batch-only le temps de fixer |

## 6. Plan de migration — Early Stage → At Scale

### Étape 0 (J0) — pré-requis
- Activer Object Lock sur le bucket Bronze.
- Créer les CMK KMS dédiées (3 × `nafad-bronze`, `nafad-silver`, `nafad-gold`).
- Provisionner un VPC dédié G3 ou utiliser le VPC partagé Network Account si l'org est en place.

### Étape 1 (J+5) — Step Functions sans casser la prod
- Containeriser Silver / Gold en images ECR (déjà fait — `nafad_g3_runner`).
- Définir la state machine, faire tourner en parallèle de cron pendant 1 semaine, comparer les snapshots.
- *Sans downtime.*

### Étape 2 (J+10) — Silver bascule en S3 Parquet
- Réécrire `silver.py` pour écrire en Parquet (pyarrow) en plus de Postgres.
- Enregistrer les partitions dans Glue Data Catalog.
- *Sans downtime.* Postgres Silver reste maintenu pendant 30 j en lecture pour rollback.

### Étape 3 (J+20) — RDS Multi-AZ + read replica
- `aws rds modify-db-instance --multi-az` (failover < 60 s).
- Créer la read-replica sur `eu-west-3c`.
- *Mini downtime de quelques secondes au switch.*

### Étape 4 (J+40) — CDC streaming
- Activer `wal_level=logical` sur G1 RDS (downtime ~30 s pour redémarrage paramétré par G1 — coordonner).
- Provisionner DMS replication instance + endpoint source/cible Kinesis.
- ECS streaming worker en mode shadow pendant 2 semaines (pas d'écriture Silver).
- Switcher après validation des écarts < 0,1 %.

### Étape 5 (J+60) — décom Silver Postgres
- Une fois Silver Parquet stable et Athena/Redshift en prod chez G4, retirer les tables `silver.*` de Postgres.
- Garder Gold uniquement.

### Effort
~25 personne-jours étalés sur 2 mois (1 senior + 1 mid).

## 7. Risques & mitigations (top 3)

1. **CDC bloque le WAL G1** (slot replication non consommé) → alarme PagerDuty si lag > 5 min, run-book "drop & recreate slot" documenté, DMS auto-restart configuré.
2. **Coût Athena explose** (analystes lancent des `SELECT *`) → workgroup avec quota 100 GB scannés/jour/user, partitionnement strict imposé (toute requête sans filtre `dt=` est rejetée par WLM Redshift), tableau de bord coût quotidien dans CloudWatch.
3. **Drift schéma WAL** (G1 ajoute une colonne sans prévenir) → contrat Avro versionné dans AWS Glue Schema Registry, le streaming worker rejette en quarantaine + alerte Slack avant même de toucher à Silver.

## 8. Sécurité — focus At Scale

| Couche | Contrôle |
|---|---|
| Réseau | VPC privé, **VPC Endpoint Gateway S3** (jamais Internet pour Bronze/Silver), SG `runner` autorise sortant uniquement vers RDS port 5432 et Kinesis Endpoint |
| Identité | IAM role par étape Step Functions ; Bronze task = `s3:GetObject` source-bucket + `s3:PutObject` bronze-bucket, rien d'autre |
| Chiffrement | KMS CMK par couche (Bronze/Silver/Gold) ; rotation annuelle automatique ; envelope encryption pour les CSV de partenaire |
| Secrets | Secrets Manager + IAM database authentication pour RDS Gold (pas de mot de passe en clair) |
| Audit | CloudTrail + S3 Access Logs sur tous les buckets ; pgaudit Postgres → CloudWatch ; rétention 1 an |
| Threat | (1) dump altéré → SHA-256 + signature MFA Delete ; (2) injection CSV → pandera + sanity bounds ; (3) credentials log fuite → Secrets Manager + filtres CloudWatch redaction `*` sur patterns sensibles |

## 9. Observabilité

- **Logs** : stdout JSON depuis ECS → CloudWatch Logs (rétention 90 j chaud, 1 an Glacier).
- **Métriques** : `g3.lag_seconds`, `g3.bronze.bytes`, `g3.silver.dup_rate`, `g3.gold.row_count` poussées par EMF (Embedded Metric Format).
- **Traces** : AWS X-Ray sur les Step Functions + ECS tasks.
- **Alertes** : 5 seuils CloudWatch → SNS → Slack `#data-alerts` + PagerDuty pour les niveaux critiques (lag > 5 min, run échoué 2× consécutivement).

## 10. Coût mensuel estimé

| Poste | Estimation USD |
|---|---:|
| RDS db.r7g.large Multi-AZ + storage 200 GB | 290 |
| 1 read replica db.r7g.large `eu-west-3c` | 145 |
| ECS Fargate (Silver/Gold tasks ~2 h/j) | 35 |
| ECS Fargate streaming worker 24/7 (0,5 vCPU, 1 GB) | 25 |
| DMS replication instance dms.t3.medium | 75 |
| Kinesis Data Streams 8 shards | 100 |
| S3 Bronze + Silver (~500 GB Parquet) | 12 |
| Athena (5 TB scannés/mois) | 25 |
| ElastiCache Redis cache.t4g.small Multi-AZ | 50 |
| KMS + Secrets Manager + CloudWatch Logs | 30 |
| NAT Gateway + Data Transfer | 60 |
| **Total** | **~850** |

Avec ce plafond on reste largement sous le budget cible 4 000 USD/mois. Les Reserved Instances (1 an, no upfront) sur RDS rabotent 30 % supplémentaires si la charge se confirme.
