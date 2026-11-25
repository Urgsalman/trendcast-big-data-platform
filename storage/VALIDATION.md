# Validation du stockage Bronze — 20 novembre 2026

## Résultat obtenu

Le pipeline Kafka → Spark Structured Streaming → HDFS Parquet a été exécuté
avec les données des trois API réelles. La lecture batch de contrôle a été
réalisée sur le cluster, et non avec un master local.

| Source | Lignes archivées |
|---|---:|
| Wikimedia Pageviews | 1 800 |
| Hacker News | 100 |
| Google Trends, Maroc | 10 |
| Total | **1 910** |

Archive : `hdfs://namenode:9000/trendcast/raw/events`.
Checkpoint : `hdfs://namenode:9000/trendcast/checkpoints/streaming/bronze-events`.

La vérification indépendante a obtenu **1 910 positions Kafka distinctes** pour
1 910 lignes. Toutes les enveloppes de cette collecte sont lisibles et classées
`valid_envelope`. Les données Wikimedia couvrent 90 journées pour 20 pages.

## Cluster vérifié

- Spark 3.5.7 : un master et deux workers ALIVE, avec un cœur par worker.
- Les logs du job confirment deux executors RUNNING, un sur chacun des workers.
- Hadoop 3.4.2 : un NameNode et deux DataNodes vivants, avec trois volumes distincts.
- Kafka 4.1.0 : accès Windows `localhost:9092` et accès Docker `kafka:29092`.
- `hdfs getconf -confKey dfs.replication` : 2.
- `hdfs fsck /trendcast/raw/events -files -blocks -locations` : HEALTHY,
  réplication moyenne 2.0, zéro bloc manquant, zéro bloc corrompu et zéro réplique manquante.

## Reprise et persistance réellement testées

1. Collecte des trois sources dans `trendcast.events.validated`.
2. Archivage continu en plusieurs micro-lots avec offsets et checkpoints HDFS.
3. Arrêt forcé du conteneur du job (`stop -t 0`, code 137 attendu).
4. Recréation du conteneur Kafka en conservant son volume.
5. Contrôle des offsets Kafka : **1 910 messages toujours présents**.
6. Redémarrage du NameNode et des deux DataNodes.
7. Relecture des Parquet et contrôle d’unicité : **1 910 lignes, 1 910 positions**.
8. Reprise du même job, avec la même archive et le même checkpoint, après la fin
   du mode de sûreté HDFS : succès, `batchId=4`, `numInputRows=0`.
   Les offsets repris sont partition 0 : 633, partition 1 : 669, partition 2 : 608.
   Aucun événement déjà archivé n’a été réécrit par cette reprise.

Un premier essai de reprise lancé trop tôt pendant le mode de sûreté HDFS a été
refusé. Après attente de `Safe mode is OFF`, le même checkpoint a été repris
avec succès. Cette attente figure dans le guide de lancement.

## Contrôles de qualité

- **56 tests Python réussis** : tests de l’ingestion, de l’automatisation,
  des chemins HDFS et de l’arrêt des tentatives répétées lors d’une panne Kafka.
- `ruff check .` : réussi.
- `docker compose config --quiet` : réussi.
- Vérification distribuée de la transformation sur six messages de test :
  deux événements portant le même identifiant conservés, texte mal formé,
  valeur nulle, source inconnue et timestamp invalide conservés et classés.
- Contrôle effectué aussi avec le mode SQL ANSI activé : les timestamps invalides
  ne font pas échouer tout le lot.
- Les octets Kafka et le payload JSON imbriqué sont préservés.

## Correctifs nécessaires et limites

La configuration Kafka initiale montait un volume sans définir `log.dirs` vers
ce volume. La première recréation a donc effacé les données éphémères de cet essai,
y compris le précédent topic de test. La collecte a été relancée depuis les API
avec un nouveau fichier de confirmations local. Les anciens relevés éphémères
ne sont pas présentés comme récupérés à l’identique.

Le Compose fixe maintenant `KAFKA_LOG_DIRS=/var/lib/kafka/data` et sa persistance
a été vérifiée par une nouvelle recréation du conteneur. Le mapping réseau
Windows initial de Kafka a été conservé pour permettre aussi l’accès IPv6 de
`localhost`. Les scripts Linux sont normalisés en LF via `.gitattributes`.

Ces contrôles portent sur ce volume académique et les reprises décrites.
Les traitements Silver/Gold, les métriques métier et les modèles ML ne sont
pas encore développés dans cette étape. La réplication locale ne remplace pas
une sauvegarde sur un autre ordinateur.

## État laissé après les essais

Kafka, le master Spark, les deux workers et les trois nœuds HDFS restent démarrés.
`hdfs-init` est terminé avec le code 0. Le job continu est arrêté volontairement
après le test d’interruption ; les jobs ponctuels de reprise et de vérification
sont terminés. Aucun scheduler de collecte n’est laissé actif.
Les logs, l’environnement Python, le `.env` et les états locaux sont ignorés par Git.
