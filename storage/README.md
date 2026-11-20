# Stockage Bronze — contribution de Zakaria

Le flux est : **Kafka → Spark Structured Streaming → HDFS Parquet**.
Le cluster local comporte un master Spark, deux workers (un cœur chacun), un
NameNode et deux DataNodes. Les versions fixées sont Spark 3.5.7, Hadoop 3.4.2
et Kafka 4.1.0. Tous les jobs fournis sont soumis à `spark://spark-master:7077`.

## Démarrer le cluster

Prérequis : Docker Desktop en mode conteneurs Linux, environ 8 Go de mémoire
attribués à Docker, et de l’espace disque pour les images et les données.
Depuis la racine du dépôt :

```powershell
docker build -t trendcast-hadoop:3.4.2 -f infra/hadoop/Dockerfile .
docker build -t trendcast-spark:3.5.7 -f infra/spark/Dockerfile .
docker compose up -d
```

Le premier démarrage télécharge les images officielles. `hdfs-init` crée les
répertoires puis se termine normalement avec le code 0. Les DataNodes ont des
volumes persistants distincts et HDFS demande une réplication de 2.
Le formatage du NameNode n’a lieu que sur un volume vide ; un volume non vide
sans métadonnées valides est refusé pour éviter un formatage accidentel.

Vérifier les services :

```powershell
docker compose ps -a
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T namenode hdfs dfsadmin -safemode wait
```

Attendre **2 DataNodes vivants** et **2 workers actifs** avant la première soumission.
Interfaces accessibles sur cet ordinateur :

- Spark : [http://localhost:8080](http://localhost:8080)
- HDFS : [http://localhost:9870](http://localhost:9870)

Ces services constituent un cluster pédagogique local, sans authentification.
Les deux interfaces web sont publiées sur l’adresse locale de l’ordinateur.
Le port Kafka 9092 conserve la publication réseau du Compose initial pour la compatibilité Windows.
Les jobs Spark utilisent les adresses internes des conteneurs.

## Alimenter Kafka

L’ingestion Python reste accessible depuis Windows via `localhost:9092`.
Les conteneurs utilisent `kafka:29092`. Le changement de listeners dans le Compose
utilise `KAFKA_LOG_DIRS=/var/lib/kafka/data`, correspondant au volume Kafka.
Une ancienne configuration sans ce paramètre peut avoir écrit dans le système
éphémère du conteneur : sauvegarder ces messages avant toute recréation de ce
conteneur. Les confirmations locales doivent être renouvelées si le broker perd ses données.

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic trendcast.events.validated --partitions 3 --replication-factor 1
.\.venv\Scripts\python.exe -m ingestion.run_automation --once
```

Les paramètres des sources proviennent du `.env` local : voir le
[guide d’automatisation](../ingestion/README_AUTOMATION.md).

## Archiver les événements

Traiter tous les événements actuellement disponibles, puis terminer :

```powershell
docker compose run --rm --no-deps bronze --available-now
```

Laisser le job consommer les nouveaux événements toutes les 30 secondes :

```powershell
docker compose run --rm --no-deps bronze
```

Le cluster doit déjà être démarré. Les dépendances du connecteur Kafka sont
résolues au premier lancement depuis Maven Central ; elles sont conservées dans
le volume `spark_ivy`. Le pilote et les workers utilisent la même version Spark.
Arrêter le job avec Ctrl+C. Ne lancer qu’un seul job par paire archive/checkpoint.

Chemins par défaut :

```text
hdfs://namenode:9000/trendcast/raw/events/
  source=wikimedia_pageviews/ingest_date=YYYY-MM-DD/*.parquet
  source=hackernews/ingest_date=YYYY-MM-DD/*.parquet
  source=google_trends/ingest_date=YYYY-MM-DD/*.parquet
  source=__invalid__/ingest_date=YYYY-MM-DD/*.parquet
  _spark_metadata/

hdfs://namenode:9000/trendcast/checkpoints/streaming/bronze-events/
```

Les partitions sont basées sur la source et la date UTC du timestamp Kafka, qui
correspond à la réception de la publication, pas à la date de l’observation métier.
Les événements anciens et les relevés successifs restent ainsi conservés.
`processed/` et `analytics/` sont initialisés pour les prochaines étapes ; ce job
ne réalise pas encore les traitements Silver et Gold.

Pour un topic d’essai, utiliser **une autre archive et un autre checkpoint** :

```powershell
docker compose run --rm --no-deps bronze --available-now --topic trendcast.events.automation-smoke --output hdfs://namenode:9000/trendcast/raw/automation-smoke --checkpoint hdfs://namenode:9000/trendcast/checkpoints/streaming/automation-smoke
```

Les arguments `--bootstrap-servers`, `--topic`, `--output`, `--checkpoint` et
`--trigger` permettent d’adapter le job. Les destinations doivent être deux chemins
HDFS distincts sous `/trendcast/`. Conserver la même configuration entre deux
reprises. Ne pas réutiliser un checkpoint pour un autre topic ou une autre archive.

## Contenu de l’archive et contrat pour Hala

| Colonne | Type / rôle |
|---|---|
| `raw_key`, `raw_value` | Binaires Kafka originaux, y compris valeur nulle |
| `raw_json` | Représentation texte du message |
| `kafka_topic`, `kafka_partition`, `kafka_offset` | Position unique du message dans Kafka |
| `kafka_timestamp`, `kafka_timestamp_type` | Timestamp et type fournis par Kafka |
| `event_id`, `source_timestamp`, `schema_version` | Champs de l’enveloppe, si lisibles |
| `event_timestamp` | Timestamp métier interprété en UTC |
| `payload_json` | Contenu métier JSON, sans imposer un schéma unique aux trois sources |
| `record_status` | `valid_envelope` ou `invalid_envelope` |
| `source`, `ingest_date` | Colonnes de partitionnement |

Le contrôle Bronze vérifie la lisibilité de l’enveloppe, sa version, sa source,
son identifiant, son timestamp et la présence d’un objet payload. Il ne remplace
pas une validation métier détaillée des payloads. Un message invalide ou une
valeur nulle n’est pas abandonné : ses octets et ses coordonnées Kafka restent
archivés dans la partition `__invalid__`.

La lecture batch doit viser **la racine** du dataset Parquet pour exploiter le
journal `_spark_metadata` du sink. Ne pas lire un ensemble arbitraire de fichiers
avec un glob en contournant ce journal.

```python
historique = spark.read.parquet("hdfs://namenode:9000/trendcast/raw/events")
historique.groupBy("source").count().show()
```

Une même valeur `event_id` peut représenter plusieurs relevés Hacker News ou
Google Trends. La clé de traçabilité Bronze est la position Kafka, pas uniquement
`event_id`. Les règles Silver seront définies avec Hala pour dédupliquer les
reprises de publication tout en conservant les observations successives.

## Reprise et persistance

`startingOffsets=earliest` s’applique à un nouveau checkpoint. Ensuite, Spark
reprend les offsets du checkpoint HDFS. Le sink Parquet natif et son journal
permettent de reprendre sans ajouter une deuxième fois les mêmes offsets.
`failOnDataLoss=true` empêche de continuer silencieusement si des offsets attendus
ont disparu de Kafka ; la rétention Kafka doit couvrir les interruptions.

Les doublons déjà présents sur des offsets Kafka différents restent conservés.
Ne supprimer ni les checkpoints ni `_spark_metadata` pour redémarrer un job.
Après un redémarrage HDFS, attendre la disponibilité du NameNode puis la fin
du mode de sûreté (`hdfs dfsadmin -safemode wait`) avant de soumettre le job.
Une soumission pendant ce mode peut être refusée sans perdre le checkpoint.

Les volumes Kafka, NameNode et DataNodes persistent après `docker compose down` ;
`docker compose down -v` les supprimerait et ne fait pas partie du lancement normal.
La réplication sur deux conteneurs du même ordinateur ne protège pas d’une panne
du disque physique de cet ordinateur.

## Vérifier les fichiers et les traitements

```powershell
docker compose exec -T namenode hdfs dfs -ls -R /trendcast/raw
docker compose exec -T namenode hdfs fsck /trendcast/raw/events -files -blocks -locations
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
```

Pour contrôler une archive sur le cluster, remplacer le nombre attendu par le
nombre réellement publié :

```powershell
docker compose run --rm --no-deps -e TRENDCAST_JOB=verify_bronze.py bronze --path hdfs://namenode:9000/trendcast/raw/events --expected-count 1910 --check-transform
```

Le vérificateur contrôle le nombre de lignes et l’unicité des positions Kafka.
`--check-transform` vérifie aussi les messages mal formés, les valeurs nulles,
les timestamps invalides et la conservation de deux relevés portant le même identifiant.
Les résultats réellement obtenus sont consignés dans `storage/VALIDATION.md`.

Références : [Spark/Kafka](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html),
[Spark Structured Streaming](https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html),
[HDFS](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html).
