# Architecture — Group G3 (Data Engineering) — **Early Stage**

> Version MVP livrable *immédiatement* avec l'équipe et les moyens actuels.
> Toutes les briques mentionnées ici sont déjà implémentées dans `pipeline/`,
> testables avec `make pipeline && make test`.

## 1. Contexte & contraintes

| Dimension | Valeur cible (Early Stage) |
|---|---|
| **Charge** | ≤ 5 dumps / jour, < 1 GB total / jour, ~100 k transactions cumulées / mois |
| **Volume Silver** | ~100 k tx, 10 k users, 11 k accounts, 41 k fees |
| **Latence ETL** | Batch quotidien, fenêtre nocturne 22 h → 04 h (6 h max) |
| **SLO disponibilité** | 99 % — un run raté = re-tirable le matin sans casser G4 |
| **RPO / RTO** | RPO 24 h (un dump perdu = re-réceptionnable), RTO 1 h |
| **Équipe** | 5 data engineers, 0,3 ETP DBA partagé avec G1 |
| **Budget** | < 200 USD / mois AWS *all-in* (idéal pour valider le modèle) |
| **Compliance** | BCM Mauritanie — données 100 % synthétiques en MVP, donc pas de PII réelle ; chiffrement at-rest préparé pour l'arrivée des vraies données |

### Hypothèses explicites
- Les dumps arrivent **fiables** (pas de partials), uploadés par G1 dans un bucket S3 unique.
- Un opérateur humain peut intervenir le matin si un run échoue (pas d'astreinte).
- Les analystes de G4 lisent depuis Postgres directement (pas encore de Redshift).

## 2. Diagramme C4 — niveaux 1 & 2

### Niveau 1 — System Context

```
                       +-------------------+
                       |  G1 OLTP (RDS)    |
                       +---------+---------+
                                 |  pg_dump nocturne
                                 v
   +------------+      +------------------+      +------------+
   | Operator   |      |   G3 ETL MVP     |      |  G4 BI/DWH |
   |  (CLI/SSH) +----->+ (Bronze→Silver→  +----->+  (Postgres |
   |            |      |  Gold)           |      |   Gold)    |
   +------------+      +------------------+      +------------+
                                 |
                                 v
                       +-------------------+
                       | Slack #data-alerts|
                       +-------------------+
```

### Niveau 2 — Containers

```
                     S3 (raw dumps inbox)
                            |
                            v
+------+   docker compose run runner --all
|      |  +---------------------------------------+
| EC2  |  |  python -m pipeline.main all          |
| t3.  |  |   ├─ init_db   (DDL)                  |
| medium  |   ├─ bronze    (S3 PUT + lineage)     |
| 1 AZ |  |   ├─ silver    (pandas dedup + flags) |
|      |  |   └─ gold      (SQL aggregates)       |
+--+---+  +---------+----------------+------------+
   |                |                |
   v                v                v
 MinIO container   PG 16 container   CloudWatch Logs (stdout)
 (single AZ)       (single AZ)
```

## 3. Choix techniques (ADR-lite)

### ADR-1 — Compute = **EC2 t3.medium** (single AZ)
- **Décision :** une instance EC2 t3.medium (2 vCPU, 4 GB) en `eu-west-3a`.
- **Alternatives considérées :** ECS Fargate, Lambda, MWAA.
- **Pourquoi :** un cron + script python n'a pas besoin de service managé ; t3.medium tient les pics RAM (pandas charge ~700 MB en pic) ; 30 USD/mois ; debug = `ssh + tail -f`.

### ADR-2 — Storage Bronze = **S3 standard** (1 bucket)
- **Décision :** `s3://nafad-bronze-ew3/` avec partition Hive `dt=YYYY-MM-DD/`.
- **Alternatives considérées :** EFS, EBS partagée, RDS BLOB.
- **Pourquoi :** 0,023 USD/GB·mois, durabilité 11×9, déjà mounté par boto3 dans le code, aucun ops à faire. Object Lock désactivé en MVP (l'activera At-Scale).

### ADR-3 — Silver / Gold = **un seul Postgres 16 RDS** (Single-AZ)
- **Décision :** `db.t4g.small` Multi-AZ désactivé, 50 GB gp3.
- **Alternatives considérées :** Redshift Serverless, Athena, MariaDB.
- **Pourquoi :** Silver < 200 MB tables — surdimensionner Redshift coûte 5× plus, pour aucune analyse au-dessus du seuil. Postgres = même moteur que G1 → courbe d'apprentissage zéro pour les data analysts. Multi-AZ désactivé tant que SLO < 99,5 %.

### ADR-4 — Orchestration = **cron + Makefile**
- **Décision :** cron sur l'EC2, `make pipeline` à 02 h UTC.
- **Alternatives considérées :** Airflow, Step Functions, Prefect.
- **Pourquoi :** un seul DAG linéaire de 4 étapes. Airflow pour 4 tasks = sur-ingénierie. À At-Scale, on passera à Step Functions.

### ADR-5 — Langage = **Python**
- **Décision :** Python 3.12 + pandas + SQLAlchemy + boto3 + pandera.
- **Alternatives considérées :** Go, Scala/Spark, dbt-only.
- **Pourquoi :** écosystème data Python imbattable pour la transformation, pandera valide les schémas en amont, équipe ETL = 5 dev déjà familiers Python (cf. README §). Le write-path critique resterait sur Go (cf. §"Choix de stack par chemin critique"), mais G3 = chemin data, donc Python est le choix orthodoxe.

## 4. Flux de données critiques

### Happy path — un dump nocturne
1. **02:00 UTC** — cron lance `make pipeline` sur l'EC2.
2. **Bronze** — pour chaque CSV de `daily_dumps/<date>/*` : calcule `batch_id = uuid5(NS, "{date}/{file}")`, PUT sur S3 sous `s3://nafad-bronze-ew3/dt=<date>/<file>.csv`, upsert dans `bronze_meta.bronze_runs`.
3. **Silver** — lecture exhaustive du préfixe `dt=`, dedup par PK, flags (`_has_clock_skew`, `_is_cross_dc`, `_is_deleted`), `TRUNCATE silver.* + INSERT` dans une seule transaction.
4. **Gold** — `TRUNCATE gold.* + INSERT` agrégats SQL pure (3 vues métier).
5. **Notification** — succès → log CloudWatch ; échec → Lambda → webhook Slack `#data-alerts`.

### Échec / retry
- Bronze échoue (S3 timeout) → cron relance à 03:00 ; les `batch_id` étant déterministes, c'est trivialement idempotent.
- Silver échoue (validation schéma) → dump rejeté → copie vers `s3://nafad-quarantine/`, alerte Slack, pas de Gold.
- Gold échoue → l'état Silver précédent reste en place ; G4 voit la veille jusqu'à ce qu'on retire la blocage.

### Idempotence — comment c'est garanti
| Couche | Mécanisme |
|---|---|
| Bronze | `batch_id = uuid5(NS, key)` + S3 PUT (overwrite same bytes) + `INSERT … ON CONFLICT DO UPDATE` |
| Silver | `TRUNCATE silver.* ; INSERT …` dans une seule transaction |
| Gold | idem Silver |

## 5. Points de rupture & seuils de bascule

| Métrique | Seuil | Action |
|---|---|---|
| Volume cumulé tx Silver | > 5 M | passer Silver à S3 Parquet + Athena (cf. doc At Scale) |
| Durée run total | > 4 h | passer Silver à Spark on EMR ou DuckDB partitionné |
| Pic RAM pandas | > 3 GB | refactor en streaming par fichier ou Polars |
| Connexions PG simultanées | > 50 | RDS Proxy + read replica |
| Latence batch acceptable | > 24 h SLA | passer en CDC (cf. doc At Scale) |

## 6. Risques & mitigations (top 3)

1. **Le dump du jour n'arrive pas** → cron monitor ; alerte si bucket source vide à 02:30 ; le run skip silencieusement la date manquante (déjà codé dans `bronze.py`).
2. **Pic RAM pandas sur transactions** (~700 MB observés) → instance t3.medium (4 GB) laisse 3 GB de marge ; en cas de doublement du volume, passer t3.large (15 USD/mois supplémentaires) suffit avant d'avoir à refactorer.
3. **Drift schéma OLTP côté G1** (nouvelle colonne, type changé) → pandera valide en amont chaque dump et bloque le run avant Silver. Le dump fautif part en `s3://nafad-quarantine/` pour reproduction et le réveil de l'équipe.

## 7. Réseau, sécurité, protocoles (rappel)

- **Réseau** : VPC unique, EC2 + RDS dans subnets privés ; sortie Internet via NAT Gateway ; aucune IP publique sur RDS.
- **Sécurité** : RDS chiffré KMS (CMK gérée), `rds.force_ssl=1`, S3 SSE-KMS, IAM role minimal sur EC2 (S3:GetObject + S3:PutObject sur 2 buckets, RDS:Connect via IAM Auth), Secrets Manager pour le mot de passe Postgres avec rotation 90 j.
- **Protocole** : S3 API HTTPS, Postgres TLS 1.3, COPY FROM `STDIN` plutôt qu'INSERT pour les bulk-loads (déjà via SQLAlchemy `method="multi"`).
