
# Automatisation de l’ingestion — TrendCast

Ce composant complète les collecteurs Wikimedia, Hacker News et Google Trends.
Il publie les événements validés dans Kafka, en conservant les identifiants et
le schéma des événements.

## 1. Installation

Depuis la racine du projet :

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r ingestion/requirements-automation.txt
```

Créer le fichier `.env` seulement s'il n'existe pas déjà :

```powershell
Copy-Item .env.example .env
```

Ne jamais publier le fichier `.env` sur Git.

## 2. Configuration Wikimedia

Configuration recommandée pour l'expérimentation :

```env
WIKIMEDIA_PROJECT=en.wikipedia
WIKIMEDIA_GRANULARITY=daily
WIKIMEDIA_DAYS_BACK=90
```

La collecte utilise les 20 pages configurées dans `WIKIMEDIA_PAGES`.

La période se termine à la veille du jour courant en UTC.

Les demandes historiques sont découpées en fenêtres de 90 jours maximum.

Le programme utilise le fichier SQLite :

```text
.trendcast/ingestion.sqlite3
```

Il conserve les confirmations des publications et les dates de départ
historiques par page.

La date de départ effective respecte également la période configurée dans
`WIKIMEDIA_DAYS_BACK`. Une ancienne date enregistrée dans SQLite ne force
donc pas une collecte au-delà de la période actuellement configurée.

## 3. Gestion des erreurs Wikimedia

Les erreurs sont distinguées :

- HTTP 404 : la page ou la période demandée ne fournit pas de données.
  La plage concernée est mémorisée comme sans données.
- Erreurs réseau ou HTTP autres que 404 : elles restent des erreurs de collecte.
- Erreurs de validation ou de publication Kafka : elles restent signalées.

Les journées sans données sont enregistrées séparément des confirmations
de publication Kafka.

Les réponses partielles restent contrôlées : les journées sans événement
ni confirmation restent en attente.

Une réponse HTTP 404 sur une plage entière est considérée comme une plage
sans données. Cette interprétation doit être vérifiée si une page attendue
renvoie systématiquement 404.

## 4. Lancement manuel

Démarrer Kafka :

```powershell
docker compose up -d kafka
```

Créer le topic s'il n'existe pas :

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic trendcast.events.validated --partitions 3 --replication-factor 1
```

Lancer uniquement Wikimedia :

```powershell
.\.venv\Scripts\python.exe -m ingestion.run_automation --once --source wikimedia_pageviews
```

Lancer toutes les sources :

```powershell
.\.venv\Scripts\python.exe -m ingestion.run_automation --once
```

Lancer l'automatisation périodique :

```powershell
.\.venv\Scripts\python.exe -m ingestion.run_automation
```

Arrêter avec Ctrl+C.

## 5. Fréquences par défaut

| Source | Fréquence |
|---|---:|
| Wikimedia | 24 heures |
| Hacker News | 30 minutes |
| Google Trends | 1 heure |

Un seul processus de planification doit utiliser le même fichier SQLite.

## 6. État et reprise

Le fichier `.trendcast/ingestion.sqlite3` conserve les confirmations.

Ne pas supprimer ce fichier pour relancer une collecte.

Une publication Kafka est confirmée dans SQLite uniquement après l'accusé
de réception du broker.

L'écriture Kafka et l'enregistrement SQLite ne sont pas une transaction
commune. Un arrêt entre ces deux opérations peut produire un doublon.

Pour Hacker News et Google Trends, les observations sont organisées par
fenêtre temporelle UTC. Un même identifiant peut donc être publié à nouveau
dans une fenêtre ultérieure.

Pour les couches Spark et HDFS, conserver les coordonnées Kafka
`topic`, `partition` et `offset` afin de préserver les observations.

## 7. Vérifications

Exécuter les tests :

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Vérifier la qualité du code :

```powershell
.\.venv\Scripts\python.exe -m ruff check .
```

Vérifier les dépendances :

```powershell
.\.venv\Scripts\python.exe -m pip check
```

## 8. Consommation Kafka

Pour observer les messages :

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trendcast.events.validated --from-beginning --max-messages 5
```

## 9. Précautions

- Ne pas supprimer `.trendcast/ingestion.sqlite3`.
- Ne pas supprimer les volumes Kafka ou HDFS.
- Ne pas dédupliquer les observations Hacker News et Google Trends uniquement
  par `event_id`.
- Ne pas confondre une réponse HTTP 404 avec une erreur Kafka.
- Vérifier les pages Wikimedia qui retournent régulièrement 404.
