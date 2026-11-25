# Automatisation de l’ingestion — contribution de Zakaria

Ce composant complète les collecteurs de Soulaimane sans modifier leur schéma,
leurs identifiants ni leur point d’entrée manuel. Il publie sur
`trendcast.events.validated` par défaut. Le stockage Spark/HDFS sera l’étape suivante.

## Installation et premier lancement (PowerShell, depuis la racine du dépôt)

Utiliser Python 3.10 ou plus récent, avec pip et venv, et Docker Desktop démarré.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r ingestion/requirements-automation.txt
Copy-Item .env.example .env
```

Créer `.env` seulement s’il n’existe pas déjà. Il contient les paramètres des sources
et de Kafka ; conserver les secrets en dehors de Git. Les paramètres optionnels de
l’automatisation sont décrits dans `ingestion/automation.env.example` : les ajouter
au `.env` si nécessaire. Le mode automatisé demande `WIKIMEDIA_GRANULARITY=daily`
et refuse `WIKIMEDIA_START` / `WIKIMEDIA_END`, car il gère lui-même les dates.

```powershell
docker compose up -d kafka
```

Attendre que Kafka soit prêt, puis initialiser le topic (sans le supprimer s’il existe) :

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic trendcast.events.validated --partitions 3 --replication-factor 1
.\.venv\Scripts\python.exe -m ingestion.run_automation --once
```

Le mode `--once` exécute les trois sources, puis se termine. Il renvoie un code 1
si une publication échoue, si une collecte échoue au niveau du job ou si une journée
Wikimedia demandée reste manquante. Pour cibler une seule source :

```powershell
.\.venv\Scripts\python.exe -m ingestion.run_automation --once --source wikimedia_pageviews
```

Après cette vérification, lancer la collecte périodique :

```powershell
.\.venv\Scripts\python.exe -m ingestion.run_automation
```

Ce processus reste actif jusqu’à Ctrl+C ; il n’installe aucun service Windows.
Au démarrage, chaque source est déclenchée une fois. Les déclenchements suivants
utilisent les intervalles ci-dessous, à partir du démarrage du processus.
Un seul job s’exécute à la fois ; une source lente peut retarder les autres.
APScheduler regroupe les déclenchements manqués pour éviter leur accumulation.
Utiliser un seul processus de planification par fichier d’état.

| Source | Intervalle par défaut | Fonctionnement |
|---|---|---|
| Wikimedia | 24 heures | Historique initial puis journées non confirmées |
| Hacker News | 30 minutes | Relevé des articles configurés |
| Google Trends | 1 heure | Relevé du flux RSS configuré |

## Historique, confirmations et reprise

- Au premier passage, Wikimedia demande les `WIKIMEDIA_DAYS_BACK` journées
  précédentes, jusqu’à hier en UTC (90 par défaut dans l’automatisation).
- Le début de l’historique est conservé séparément pour chaque page et projet.
  Une interruption prolongée ne fait pas glisser ce début vers une date récente.
- Seules les journées sans confirmation sont redemandées, en plages consécutives.
  Une réponse partielle, une journée absente ou un échec de publication laisse
  cette journée à reprendre lors du prochain passage. Les autres pages continuent.
- Le fichier SQLite `.trendcast/ingestion.sqlite3` conserve les confirmations après
  l’accusé de réception Kafka. Une autre destination Kafka a des confirmations distinctes.
  Ce fichier local est ignoré par Git ; le conserver entre les redémarrages.
- Les publications réalisées avec l’ancien point d’entrée manuel ne figurent pas
  dans cet état. Un premier passage automatisé peut donc reprendre leur historique.
- Les erreurs sont journalisées avec leur source ou leur identifiant. Un échec ne
  marque pas l’événement comme publié. Le producteur est fermé après chaque job.

## Relevés Hacker News et Google Trends

Un relevé est défini par une fenêtre UTC de 30 ou 60 minutes par défaut.
Un même identifiant n’est publié qu’une fois dans cette fenêtre. Lors de la fenêtre
suivante, il peut être publié à nouveau, même sans changement : c’est une nouvelle
observation, utile pour suivre l’évolution des scores et du trafic.

Les `event_id` et `timestamp` des collecteurs restent inchangés. Le timestamp
source est la date de publication, et non la date de collecte. Pour historiser
les observations dans Bronze, conserver aussi le timestamp Kafka et les coordonnées
`topic`, `partition`, `offset`. Ne pas supprimer tous les relevés en dédupliquant
uniquement sur `event_id` : coordonner la règle Silver avec Hala.
Une période d’arrêt ne permet pas de reconstituer les relevés manqués : ces API
fournissent leur état courant. Une reprise au sein de la même fenêtre ignore les
identifiants déjà confirmés jusqu’à la fenêtre suivante.

## Limites de cette première version

L’écriture Kafka et l’enregistrement SQLite ne forment pas une transaction commune.
Si le processus s’arrête après l’accusé Kafka mais avant l’enregistrement local,
un doublon reste possible au redémarrage (livraison au moins une fois).
La couche Silver devra donc gérer les doublons de reprise sans perdre les relevés.
Changer le broker de données derrière une adresse inchangée nécessite un nouvel
état ; supprimer l’état existant provoque une nouvelle collecte initiale.

Les collecteurs existants peuvent journaliser et ignorer certains articles ou items
invalides. Le succès d’un job ne garantit donc pas que tous les items de la source
ont été collectés. Les versions historiques déjà confirmées ne sont pas relues
pour détecter d’éventuelles corrections de la source Wikimedia.

## Vérifications

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
```

Les tests automatisés fonctionnent sans API externe ni broker. Ils couvrent la
reprise depuis SQLite, les échecs de publication, les dates manquantes, les sources
indépendantes, les relevés successifs et le déclenchement réel du planificateur.
Ils ne remplacent pas l’essai d’intégration avec les API et Kafka.

Pour observer le topic pendant un essai :

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trendcast.events.validated --from-beginning --max-messages 5
```

Documentation de référence : [APScheduler 3 — guide utilisateur](https://apscheduler.readthedocs.io/en/3.x/userguide.html).

## Résultat de vérification du 19 novembre 2026

- 48 tests réussis, dont 32 tests existants et 16 nouveaux cas d’automatisation.
- `ruff check .` et `pip check` réussis avec les dépendances du projet.
- Essai réel vers `trendcast.events.automation-smoke`, sur Kafka 4.1.0 :
  2 journées pour une page Wikimedia, 3 articles Hacker News et 10 tendances MA.
- Premier passage : 15 publications confirmées ; second passage : 0 nouvelle
  publication. Les relevés de cet essai utilisaient une fenêtre de 24 heures
  pour que les deux passages restent dans la même fenêtre de vérification.
- Lecture indépendante du topic : 15 messages valides au total, répartis 2 / 3 / 10.
- L’état et les logs de cet essai sont locaux et ignorés par Git. Aucun scheduler
  n’est laissé actif en permanence après cet essai.

Ces résultats vérifient ce petit volume et la reprise immédiate. La continuité
sur plusieurs jours et la couche Spark/HDFS restent à vérifier lors des prochaines étapes.
