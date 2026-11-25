# Résultats expérimentaux · Wikimedia

L’expérience `hala-20261125-v1` utilise l’export Gold transmis par Soulaimane. Le profilage utilise le corpus complet. L’entraînement et l’évaluation portent sur les 20 pages technologiques approuvées, du 23 novembre 2025 au 23 novembre 2026 inclus. Les commandes, hypothèses et types d’entrée figurent dans le [guide de reproduction et d’inférence](README.md).

## Vérification de l’export et du cluster

L’archive a passé le contrôle CRC et les cinq Parquet locaux lui correspondent octet pour octet. Après transfert, leurs empreintes SHA-256 correspondent dans HDFS. Les cinq blocs ont deux répliques vivantes, une par DataNode. Les quatre chemins sont `HEALTHY`. Le cluster comporte le Master Spark, les deux Workers distincts et le NameNode HDFS demandés. Les trois services HDFS persistent leurs données dans trois volumes distincts. Les journaux Spark confirment l’allocation d’un exécuteur à chacun des deux workers.

| Dataset Gold | Fichiers Parquet lus | Lignes comptées par Spark |
|---|---:|---:|
| ml_dataset | 2 | 462 056 |
| daily_metrics | 1 | 3 977 |
| hackernews_metrics | 1 | 2 |
| google_trends_metrics | 1 | 2 |

HDFS utilise Hadoop 3.4.2 et Java 8u212. Spark utilise la version 3.5.7, Scala 2.12.18, Java 17.0.16 et Python 3.10.12 dans les conteneurs. NumPy 1.26.4 fournit la dépendance Python de Spark ML. Le broker et les traitements d’ingestion ne sont pas requis pour relire les exports remis par l’équipe.

## Profilage Spark SQL

| Indicateur | Gold Wikimedia | 20 titres configurés, toutes dates |
|---|---:|---:|
| Lignes | 462 056 | 9 178 |
| Projets | 1 | 1 |
| Pages | 56 758 | 20 |
| Dates distinctes | 3 977 | 1 983 |
| Première et dernière dates | 2016-01-01 à 2026-11-23 | 2016-01-01 à 2026-11-23 |
| Minimum / maximum des vues | 0 / 17 110 916 | 10 / 1 467 444 |
| Moyenne | 75 550,73 | 23 461,65 |
| P25 / médiane / P75 | 38 076 / 51 084 / 79 645 | 934 / 3 002 / 24 544 |
| P95 / P99 | 202 197 / 483 944 | 110 184 / 231 480 |
| Vues nulles / négatives | 0 / 0 | 0 / 0 |
| Vues égales à zéro | 638 (0,138 %) | 0 |
| Clés de vue dupliquées, identiques ou contradictoires | 0 / 0 | 0 / 0 |
| Séries discontinues | 21 161 | 12 |
| Séries avec moins de 28 observations | 54 171 | 0 |
| Paires de jours consécutifs | 310 234 | 8 894 |
| Intervalles de plus d’un jour | 95 064 | 264 |
| Jours manquants cumulés à l’intérieur des séries | 25 509 545 | 28 884 |
| Délai médian date d’événement → date Kafka | 1 670 jours | 229 jours |

Les quantiles sont des estimations Spark. Dans Gold, la série médiane contient **deux jours observés**. La population quotidienne varie de **99 à 283 pages** et vaut exactement **100** pendant **3 601 jours sur 3 977**. Ce motif est compatible avec une composante de classement quotidien. Le schéma Gold ne permet pas d’attribuer une ligne à un import Top 100, à un autre backfill ou à une collecte récente. Sur l’ensemble, **361 192 événements** ont un Kafka timestamp de plus d’un an après la mesure. Ce décalage motive l’usage de réceptions horodatées en production et d’une hypothèse explicite de disponibilité dans le backtest.

L’API Wikimedia Analytics fournit un accès public aux mesures. La licence et la provenance détaillée ligne par ligne ne figurent pas dans l’export de l’équipe. Les données sources et les fichiers de prédictions restent locaux. [Documentation de l’API Wikimedia](https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/documentation/getting-started.html)

### Indicateurs descriptifs indépendants

| Date du snapshot | Histoires distinctes Hacker News | Score moyen Hacker News | Commentaires cumulés | Observations Google Trends Maroc | Tendances distinctes |
|---|---:|---:|---:|---:|---:|
| 2026-11-20 | 120 | 224,00 | 24 171 | 20 | 13 |
| 2026-11-24 | 1 108 | 108,89 | 72 877 | 20 | 10 |

Ces deux dates décrivent des snapshots de taille différente. Le nombre de commentaires correspond à l’agrégat Gold des lignes observées. La couverture et le plan de collecte limitent toute comparaison temporelle.

## Préparation du panel

| Étape | Lignes |
|---|---:|
| Gold complet | 462 056 |
| Panel et période, 20 pages × 366 jours | 7 320 |
| Observations valides, après vérification et déduplication | 7 320 |
| Références calendaires entre le premier jour et la veille du dernier label | 7 300 |
| Références sans label exact dans la grille | 0 |
| Références labellisées sans observation historique éligible | 40 |
| **Dataset supervisé** | **7 260** |

Le dernier jour observé fournit des labels à la veille. Il ne peut pas devenir une référence supplémentaire sans un label extérieur à la période approuvée. Les deux premiers jours de référence ont une cible, mais aucune observation compatible avec le délai de deux jours dans la fenêtre du panel.
