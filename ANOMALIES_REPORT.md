# NAFAD-PAY G3 — Rapport d'anomalies

> Tous les chiffres ci-dessous sont reproductibles via `make pipeline && make anomalies`
> sur le dataset fourni (seed 42, dumps 2024-01-15 → 2024-01-21).

## 1. Vue d'ensemble — couche Silver

| Entité | Lignes brutes (somme 7 dumps) | Lignes Silver (après dedup) |
|---|---:|---:|
| users        | 14 343 | **10 000** |
| accounts     | 15 757 | **11 019** |
| transactions | 100 602 | **100 000** |
| fees         | 41 391 | **41 391** |

Le delta entre brut et Silver s'explique par :
- **users** : full du 18 (5 715 lignes) qui *redéfinit* le périmètre, plus 4 deltas (16/17/19/20/21).
- **accounts** : full du 18 (6 299 lignes) suivie de petits deltas.
- **transactions** : 602 doublons réels au sens `(id, reference)` éliminés, principalement entre dumps consécutifs.
- **fees** : la même clé `(id, transaction_reference)` apparaît à l'identique dans tous les dumps → dedup naturel.

## 2. Clock skew (`completed_at < created_at`)

**2 575 transactions** flaggées (2,58 % du total Silver).

### 2.1 Constat fort : 100 % du clock skew est cross-DC

| Population | Nb tx | Clock skew | % skew |
|---|---:|---:|---:|
| Cross-DC (`is_cross_dc = true`) | 64 104 | **2 575** | 4,02 % |
| Same-DC (`is_cross_dc = false`) | 35 896 | **0** | 0,00 % |

Aucune ligne *same-DC* ne présente de `completed_at < created_at`. L'horloge ne dérive donc qu'au moment où une transaction quitte un DC pour être complétée par un autre — le symptôme classique d'un manque de NTP synchronisé entre DC-NKC et DC-NDB.

### 2.2 Ventilation par datacenter source

| Source DC | Tx totales | Clock skew | % |
|---|---:|---:|---:|
| DC-NKC-SECONDARY | 21 022 | 684 | **3,25 %** |
| DC-NKC-PRIMARY   | 40 257 | 1 052 | 2,61 % |
| DC-NDB           | 38 721 | 839 | 2,17 % |

Le DC le plus impacté en proportion est DC-NKC-SECONDARY : c'est lui qui est probablement le plus en retard côté NTP.

### 2.3 Top 5 paires (source_node → processing_node)

| Path | Skews |
|---|---:|
| NDB-NODE-2 → NKC-NODE-2 | 249 |
| NKC-NODE-3 → NKC-NODE-1 | 244 |
| NKC-NODE-2 → NDB-NODE-1 | 216 |
| NKC-NODE-3 → NDB-NODE-1 | 197 |
| NKC-NODE-2 → NDB-NODE-2 | 181 |

### 2.4 Recommandation

Le clock skew est traité **comme une anomalie marquée, pas filtrée** : nous flaggons `_has_clock_skew = true` en Silver et laissons G4 (DWH) décider de l'inclusion dans les KPI. La règle métier "a-t-on perdu de l'argent ?" sera mieux jugée par les analystes qu'au stade ETL.

## 3. Soft-deletes (`status IN ('CLOSED', 'SUSPENDED')`)

Le dump *delta* du **2024-01-19** apporte **58 lignes** marquées `CLOSED` ou `SUSPENDED` (vérifié au niveau brut sur `daily_dumps/2024-01-19/users_delta.csv`).

Après réconciliation des 7 dumps (full du 18 + tous les deltas), Silver contient **292 utilisateurs** avec un statut "soft-deleted" :

| _last_seen_dump_date | Soft-deleted (cumulatif) |
|---|---:|
| 2024-01-16 | 1 |
| 2024-01-17 | 4 |
| 2024-01-18 (full) | 181 |
| 2024-01-19 | 46 |
| 2024-01-20 | 26 |
| 2024-01-21 | 34 |

Pourquoi 46 et non 58 sur le 19 ? Parce que **12 des 58 utilisateurs CLOSED/SUSPENDED dans le delta du 19 ont été ré-ouverts (re-passés ACTIVE) dans les deltas du 20 ou 21**. Notre stratégie *latest-write-wins* sur `updated_at` les fait redescendre à ACTIVE — c'est volontaire : la dernière vérité connue prévaut.

Décomposition statut final côté Silver :

| status | count |
|---|---:|
| ACTIVE | 8 510 |
| INACTIVE | 457 |
| PENDING_KYC | 540 |
| BLOCKED | 201 |
| SUSPENDED | 289 |
| CLOSED | 3 |

### Choix : flag, pas DELETE

Nous **n'effaçons pas** les enregistrements en Silver. Au contraire, le flag `_is_deleted` propage l'information à G4 qui peut alors :
- masquer les soft-deletes des KPI live ;
- les inclure dans les rapports compliance / audit ;
- conserver la traçabilité pour d'éventuelles ré-ouvertures.

Effacer empêcherait toute relecture historique — incompatible avec une obligation de conservation 5 ans (BCM mauritanienne).

## 4. Réconciliation full du 18 vs accumulation 15+16+17

| Source | Lignes (brutes) | Lignes (uniques en Silver après dedup partiel) |
|---|---:|---:|
| users_full 2024-01-15 | 1 428 | — |
| users_delta 2024-01-16 | 1 428 | — |
| users_delta 2024-01-17 | 1 458 | — |
| **users_full 2024-01-18** | **5 715** | **point de contrôle** |

Hypothèse README : "additionnez les delta des 16, 17 et comparez". Réponse : **non, ça ne colle pas**. Les deltas 16 et 17 ne re-poussent pas tous les users — uniquement ceux modifiés. Le full du 18 contient 5 715 entités, parmi lesquelles ~4 287 utilisateurs **n'apparaissent jamais avant le 18**. Le full du 18 est donc autoritatif : il introduit la majorité de la base utilisateur.

Notre pipeline gère ce cas naturellement : l'union de tous les dumps (Bronze) + dedup par clé primaire avec `latest updated_at` produit un état Silver de **10 000 utilisateurs** distincts qui couvre la totalité du périmètre métier.

## 5. Idempotence — preuve

```bash
make idempotence
# OK: bit-exact identical state
```

Hash global Silver+Gold après deux runs consécutifs (commande `make snapshot`) :

```
"_overall": "457fe7a82da6165f73d4e19f43fa5e146ea5a5c4bd21a9af47c67ab4eac7ba4a"
```

Identique sur les deux runs. Cela tient parce que :
- les `batch_id` Bronze sont des UUID5 déterministes calculés sur `(date, filename)` ;
- `_ingested_at` est figé à `dump_date T00:00:00Z` (pas `now()`) ;
- les écritures S3 sont des PUT idempotents (mêmes octets → même objet) ;
- Silver/Gold font `TRUNCATE → INSERT` à chaque run.

## 6. Réponses aux questions d'investigation (README §)

1. **Clock skew vs cross-DC** : 100 % des 2 575 clock skews sont sur des transactions cross-DC. Le risque temporel est strictement corrélé à la frontière inter-datacenter.
2. **Full 18 vs cumul 15+16+17** : non additionnable — voir §4. Le full est *définitionnellement* la nouvelle source de vérité.
3. **Soft-deletes — DELETE ou flag ?** : flag (`_is_deleted = true`). Voir §3 pour la justification (audit, compliance, ré-ouverture possible).
4. **Rejeu du 20 sans doublon** : (a) Bronze réécrit le même objet S3 sous la même clé (PUT idempotent) ; (b) Silver `TRUNCATE` puis recharge — pas d'append cumulatif possible ; (c) la dedup `(id, reference)` couvre n'importe quel résidu.
5. **Source of truth multi-nœuds** : *latest-write-wins* basé sur `(dump_date, completed_at)`. Argument : `processing_node` = nœud qui a *fini* la transaction, donc dernier état connu. Si le `processing_node` diffère du `source_node`, on garde la ligne avec le `completed_at` le plus tardif (ou, en cas d'égalité, la dernière vue dans le dernier dump).
