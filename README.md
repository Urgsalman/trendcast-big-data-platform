# TrendCast

TrendCast collecte des signaux d’intérêt issus de Wikimedia, Hacker News et Google Trends. Il les archive sur HDFS, calcule des indicateurs historiques et expérimente la prévision des vues Wikipédia du lendemain sur un panel de 20 pages technologiques.

**État de l’intégration · 26 novembre 2026.** La branche rassemble les travaux de Soulaimane, Zakaria et Hala. La V2 a été rechargée depuis HDFS et a produit 20 prévisions en replay historique sur Spark distribué. La voie chaude Kafka, la base de restitution et le dashboard restent à intégrer.

## Architecture et fichiers

```text
API et historique → Python/Pydantic → Kafka → Spark Structured Streaming
  → Bronze Parquet HDFS → Silver par source → Gold PySpark
  → préparation temporelle → Spark ML → PipelineModel et manifeste
```

| Emplacement | Rôle |
|---|---|
| `ingestion/`, `config/schemas/` | Collecte manuelle et APScheduler, normalisation, validation et publication. Le JSON Schema Wikimedia plat est historique, le contrat en service est défini par Pydantic. |
| `docker-compose.yml`, `infra/` | Kafka 4.1.0, Spark 3.5.7 et Hadoop 3.4.2. Un Master, deux Workers, un NameNode, deux DataNodes, volumes distincts et réplication HDFS configurée à 2. |
| `storage/` | Bronze. Archive les événements bruts et les coordonnées Kafka en Parquet sous `/trendcast/raw/events`. Le checkpoint est stocké séparément. |
| `processing/` | Silver. Type et valide les mesures par source sous `/trendcast/processed`. |
| `analytics/` | Gold. Produit `daily_metrics`, `ml_dataset`, `hackernews_metrics` et `google_trends_metrics` sous `/trendcast/analytics`. |
| `analytics/ml/` | Profilage, préparation supervisée, comparaisons Spark ML, modèles, replay et passation. |
| `scripts/` | Téléchargement, nettoyage, import historique Kafka et diagnostics. |

## Installation et reconstruction

Prérequis : Git, Docker Desktop en mode conteneurs Linux, Compose v2, PowerShell et Python avec un environnement virtuel. Prévoir les ressources du cluster et l’accès au registre d’images et à Maven pour le connecteur Spark–Kafka.

```powershell
py -3.10 -m venv .venv
& ./.venv/Scripts/python.exe -m pip install -r ingestion/requirements-automation.txt -r requirements.txt
if (!(Test-Path .env)) { Copy-Item .env.example .env }
docker compose config -q
docker compose build namenode
docker compose --profile jobs build bronze
```

Les étapes de démarrage et les vérifications des montages sont décrites dans le [guide Spark/HDFS](storage/README.md). Sur un environnement neuf, le premier démarrage initialise HDFS. Sur l’environnement partagé, confirmer d’abord les volumes et le projet Compose, puis démarrer les services de données sans démarrer le streaming Bronze avant d’avoir vérifié son topic, son archive et son checkpoint.

```powershell
docker compose up -d --no-build kafka namenode datanode-1 datanode-2 hdfs-init spark-master spark-worker-1 spark-worker-2
docker compose wait hdfs-init
```

### Chaîne complète

1. Vérifier et consigner la révision de [l’historique vtasca](https://huggingface.co/datasets/vtasca/wikipedia-pageviews), l’empreinte, la période et les conditions de redistribution. Le téléchargeur courant suit le split `train` sans épingler sa révision.
2. Sur un environnement isolé, exécuter `& ./.venv/Scripts/python.exe scripts/download_historical_dataset.py` puis `& ./.venv/Scripts/python.exe scripts/clean_historical_dataset.py`. Ces scripts écrivent sous `data/historical/`.
3. Créer un topic dédié et utiliser un SQLite dédié, des chemins Bronze, checkpoint, Silver et Gold distincts. Le seuil `WIKIMEDIA_DEDUP_MIN_IDS=30971` protège le registre existant. Pour cette destination neuve et vide vérifiée, définir KAFKA_TOPIC, INGESTION_STATE_PATH et WIKIMEDIA_DEDUP_MIN_IDS=0 dans le processus de test. Initialiser ensuite le registre séparé avec `ingestion.run_ingestion.initialize_state()`, puis simuler l’import avec `& ./.venv/Scripts/python.exe scripts/import_historical_to_kafka.py --limit 10`. Vérifier destination et compteurs avant toute publication `--publish`.
4. Soumettre le streaming `kafka_to_bronze.py` via le service `bronze` dans le profil `jobs`, avec `--available-now`, `--topic`, `--output` et `--checkpoint` correspondant au test. Les jobs Silver et Gold simulent par défaut. Leurs écritures demandent des destinations dédiées et une vérification préalable des chemins, en particulier pour Gold dont les chemins de sortie sont définis dans le job.
5. Voir le [guide d’ingestion](ingestion/README_AUTOMATION.md) pour la collecte manuelle et le scheduler. Le démarrage automatique déclenche les collectes à la première échéance configurée.

### Restauration et artefacts ML

La [procédure ML](analytics/ml/README.md) explique la restauration du Gold depuis `TrendCast_Gold_Data.zip`. Vérifier son manifeste et restaurer sur une destination prévue à cet effet. Cette démonstration porte sur la lecture d’un snapshot. La démonstration de l’archivage Kafka utilise le parcours précédent.

- **V2 active** : obtenir `TrendCast_Hala_ML_Model_hala-20261125-v2.zip`, vérifier les empreintes et suivre la [passation V2](analytics/ml/PASSATION_NADIR.md). La reproduction expérimentale exacte utilise également les prédictions V1.
- **Candidat J−1** : obtenir `TrendCast_Hala_ML_Candidate_hala-j1-20261126-v1.zip` et le fichier compagnon `.zip.sha256`. Le paquet contient les deux modèles, le manifeste et les références du replay. Voir la [passation candidate](analytics/ml/PASSATION_CANDIDAT_J1.md) et le [protocole d’activation](analytics/ml/VALIDATION_CANDIDAT_J1.md).

## Démonstration sur un cluster démarré

Lancer depuis le projet Compose de démonstration. Les commandes suivantes inspectent ou simulent les traitements. L’étape ML suppose la V2 importée selon son guide.

```powershell
docker compose ps -a
(Invoke-RestMethod 'http://localhost:8080/json/').workers | Select-Object id,state,cores,memory
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic trendcast.events.validated
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T namenode hdfs dfsadmin -safemode get
docker compose exec -T namenode hdfs getconf -confKey dfs.replication
docker compose exec -T namenode hdfs dfs -ls /trendcast/analytics
docker compose exec -T namenode hdfs fsck /trendcast/analytics/ml_dataset -files -blocks -locations
docker compose --profile jobs run --rm --no-deps -T -e TRENDCAST_JOB=audit_bronze.py bronze --require-cluster
docker compose --profile jobs run --rm --no-deps -T -e TRENDCAST_JOB=bronze_to_silver.py bronze
docker compose --profile jobs run --rm --no-deps -T -e TRENDCAST_JOB=silver_quality_checks.py bronze
docker compose --profile jobs run --rm --no-deps -T -e TRENDCAST_JOB=build_gold.py bronze
$model = 'hdfs://namenode:9000/trendcast/models/wikimedia_next_day_views/hala-20261125-v2'
docker compose --profile jobs run --rm --no-deps -T -e TRENDCAST_JOB=predict_sample.py bronze --model $model --load-only
docker compose --profile jobs run --rm --no-deps -T -e TRENDCAST_JOB=predict_sample.py bronze --model $model --replay-target 2026-11-23
```

Le marqueur `WIKIMEDIA_REPLAY_SIMULATED` affiche les prévisions du replay avec des dates de réception simulées. Le test de parité se lance séparément avec `--gold` et `--reference-predictions`, sans `--load-only` ni `--replay-target`. Les prédictions de référence V2 restent à obtenir. Pour le candidat, suivre la commande distribuée de la section [Reproduction du candidat J1](analytics/ml/README.md#reproduction-du-candidat-j1) après réception du ZIP binaire.

## Preuves et limites

- Les tests unitaires couvrent les collecteurs avec réponses simulées, la validation, les échecs de publication simulés, la reprise du scheduler et les garde-fous Bronze. L’audit a compté 56 tests réussis. Les validations distribuées rapportées par Hala sont décrites dans les [résultats V2](analytics/ml/RESULTATS.md).
- Le Gold local contient 462 056 lignes ML, 3 977 métriques Wikimedia et deux lignes pour chacune des autres sources. Les données combinent un top quotidien Wikipedia et un panel API. Les sources et leurs règles de mesure diffèrent.
- Sur 1 100 clés rétrospectives, les baselines recalculées donnent 2 823,81 / 12 762,44 pour la persistance et 2 141,94 / 10 636,73 pour l’hebdomadaire, en MAE/RMSE. La V2 rapporte 2 084,00 / 9 310,54 sur cet ensemble. Les [résultats J−1](analytics/ml/RESULTATS_CANDIDAT_J1.md) supposent des réceptions simulées. Le Gold n’expose pas la première disponibilité durable `available_at`.
- Le chargement V2 a alloué un exécuteur sur chacun des deux Workers. Les blocs du modèle et du Gold ML contrôlés par HDFS avaient deux répliques. La reprise Kafka–Bronze et la réexécution des jobs Silver/Gold restent à vérifier sur le cluster de présentation.
- La reconstruction vierge dépend d’une initialisation explicite du registre et de la provenance de l’historique. Les reçus SQLite et le checkpoint Spark n’établissent aucune garantie exactly-once de bout en bout.

## Contributions

- **Soulaimane** a développé l’ingestion, la validation et la publication Kafka (`440e48c`, `87c4c40`), intégré le travail de Zakaria (`ab77ed5`) et ajouté l’import, Silver et Gold (`345c5da`).
- **Zakaria** a développé l’automatisation et l’état SQLite (`c70cf1a)), puis le cluster Spark/HDFS et le streaming Bronze (`ae31e5a`).
- **Hala** a développé et évalué le pipeline Spark ML (`bfaa930`, `71bbb33`, `1b52843`), optimisé V2 (`a2036d6`) et publié la politique candidate (`7cb024b`).
- **Nadir** doit intégrer la consommation Kafka pour la prévision, l’historique durable et horodaté, le branchement `foreachBatch` et la restitution. La [passation](analytics/ml/PASSATION_NADIR.md) définit les interfaces.
