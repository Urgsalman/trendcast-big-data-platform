# Passation à Nadir · Prévision candidate Wikimedia

La politique `hala-j1-20261126-v1` calcule les vues de J+1 sur les 20 pages `en.wikipedia`. À J−1, elle utilise la forêt logarithmique. À J−2, elle utilise une copie de l’arbre V2. Chaque route déduit la direction de sa propre prévision de vues. Le classement est une **baseline statistique de persistance**, normalisée par une médiane commune aux routes. La validation en parallèle est décrite dans [VALIDATION_CANDIDAT_J1.md](VALIDATION_CANDIDAT_J1.md).

`candidate.py` est le point d’entrée pour la [démonstration](DEMONSTRATION.md) et le module à importer pour la future intégration.

## Installation des modèles

Le ZIP `TrendCast_Hala_ML_Final_hala-j1-20261126-r2.zip` contient le manifeste, les deux pipelines Spark et les références de replay et d’évaluation. La politique reste `hala-j1-20261126-v1`. Le driver utilise Spark 3.5.7, Scala 2.12, Java 17, Python 3.10 et NumPy 1.26.4 dans l’image TrendCast. Exécuter le script depuis un clone de `hala`, sous PowerShell 7 avec le NameNode Docker démarré.

Placer le ZIP et son fichier compagnon `.zip.sha256` dans le même dossier. Depuis la racine du clone, utiliser le chemin local.

```powershell
& './analytics/ml/install_final_model.ps1' -ArchivePath './shared_data/delivery/TrendCast_Hala_ML_Final_hala-j1-20261126-r2.zip'
```

Une fois la [release `hala-ml-j1-20261126-r2`](https://github.com/Urgsalman/trendcast-big-data-platform/releases/tag/hala-ml-j1-20261126-r2) publiée, les membres authentifiés avec `gh auth login` peuvent passer `-FromRelease` à la place de `-ArchivePath`. L’accès à un dépôt privé dépend des permissions GitHub du membre.

Le script vérifie les empreintes et la réplication effective, puis installe le contenu au chemin `/trendcast/models/wikimedia_next_day_views/candidates/hala-j1-20261126-v1`. Si ce contenu est déjà identique, il affiche `reused`. Un contenu différent arrête l’installation avec le chemin concerné. Le transfert temporaire utilise un renommage WebHDFS sans écrasement.

| Route | Répertoire sous le paquet | Règle de restitution |
| --- | --- | --- |
| J−1 | `pipelines/j1_log` | `max(0, expm1(z))` |
| J−2 | `pipelines/j2_v2` | Cible brute avec plancher à 0, recette V2 du manifeste |
| Les deux | `manifest/part-00000` | Panel, ordre des variables, transformations, poids et politique |

`replay/receipts.csv` et `replay/expected.csv` servent au scénario secondaire et au test Spark. Le scénario principal lit le Gold officiel du cluster.

Le vecteur est `latest_views`, `latest_age`, `weekday_sin`, `weekday_cos`, dans cet ordre. L’âge est relatif à J en UTC et le jour de semaine est celui de la **cible**. Le candidat réutilise le calcul de features de la V2. Les deux pipelines se chargent une fois à l’initialisation du driver.

## Entrées et disponibilité

| Colonne | Type Spark | Signification |
| --- | --- | --- |
| `project` | STRING | Projet, aligné sur le manifeste. |
| `page_title` | STRING | Titre canonique de la page. |
| `event_date` | DATE | Jour UTC du total, extrait de l’événement Wikimedia. |
| `views` | BIGINT | Total journalier non négatif. Une mesure avec zéro vue est valide. |
| `available_at` | TIMESTAMP | Premier instant UTC où cette version du total est validée et durablement lisible par la prévision. |

L’historique doit donner les différentes versions par clé projet, page, date. Conserver le premier instant de disponibilité de chaque version, puis relier la provenance aux coordonnées Kafka et à l’horodatage de persistance. Le jour d’événement et le timestamp Kafka d’un rattrapage ne fournissent pas cet instant. Le candidat exige la clôture UTC du jour de l’observation et `available_at < prediction_timestamp`. Il lit les dates [J−28, J−1] à la décision située en J et prévoit q = J+1. Conserver l’instant de décision d’origine lors d’un rejeu.

Deux adaptations peuvent établir une provenance. Avec un flux instrumenté, journaliser le premier commit durable de chaque version lisible par le service d’inférence. Avec un export par lot, figer un instantané immuable **avant** la décision, valider son schéma et son contenu, puis journaliser son identifiant, son empreinte et l’heure UTC où le service l’a lu et a confirmé sa disponibilité. Cette heure établit une borne prudente pour les seules versions présentes dans l’instantané. Elle permet de renseigner leur première disponibilité prouvée sans reconstituer un reçu historique. Conserver les premières preuves lorsqu’une version figure dans plusieurs instantanés. Un instantané confirmé à la coupure ou après ne sert qu’aux décisions suivantes. Le Gold historique actuel reste une source de valeurs du replay avec réceptions simulées.

Filtrer les versions à la coupure avant déduplication. Les doublons de même total se réduisent à une valeur et à son premier reçu. Les versions déjà reçues avec des totaux divergents excluent la clé. Une correction reçue à la coupure ou après pourra apparaître à une décision ultérieure. Le dernier jour exact admissible dirige la route, J−1 puis J−2. Au-delà, le journal indique `no_admissible_j1_or_j2`, `no_admissible_history` ou `conflicting_recent_receipts`. Une sortie du modèle non finie entraîne `invalid_model_prediction`. Les abstentions sont présentes dans le journal et absentes des sorties publiables.

## Sorties

Le schéma existant reste `project STRING`, `page_title STRING`, `prediction_date DATE`, `predicted_views DOUBLE`, `model_version STRING` et `prediction_timestamp TIMESTAMP`. `model_version` vaut `hala-j1-log-20261126` ou `hala-20261125-v2` selon la route effectivement utilisée. Le journal comporte en plus `policy_version`, `route`, `abstention_reason`, `latest_available_at`, `observation_date_j1`, `j1_available_at`, `observation_date_j2` et `j2_available_at`. Les deux colonnes de disponibilité J−1 et J−2 décrivent des clés canoniques observables à la coupure.

| Extension | Type | Règle |
| --- | --- | --- |
| `historical_reference` | DOUBLE | Médiane exacte des observations admissibles sur [J−28, J−2]. |
| `trend_score` | DOUBLE | (dernières vues − médiane) / max(1, médiane). Baseline de persistance. |
| `trend_direction` | STRING | Même formule appliquée au **volume prévu**. Baisse sous −10 %, hausse au-dessus de +10 %, stable entre les deux, bornes incluses. |
| `trend_rank` | INT | Rang dense décroissant par projet, cible et décision. Titre pour l’ordre d’affichage secondaire des ex æquo. |
| `latest_observation_date` | DATE | Dernier jour admissible. Le journal peut le renseigner aussi en cas d’abstention. |
| `availability_days_used` | INT | 1 ou 2 selon la route de prévision. |
| `fallback_used` | BOOLEAN | Vrai si une prévision a utilisé J−2. |
| `eligible_pages_count` | BIGINT | Nombre de scores calculables pour cette décision. |

Les deux valeurs relatives sont arrondies à 12 décimales avant l’application du seuil ou le rang. La direction est cohérente avec le volume prédit par construction. Le signe du score de persistance peut différer de la direction prévue. Si l’historique ne donne aucune médiane, une prévision de vues peut être calculée à partir de J−1 seul. La direction, le score et le rang sont alors nuls au sens SQL. Aucune absence d’observation n’est imputée à zéro.

## Chargement et exemple Gold

```python
from datetime import datetime, timezone

from analytics.ml.candidate import WikimediaCandidate, forecasts

spark.conf.set("spark.sql.session.timeZone", "UTC")
policy = WikimediaCandidate.load(
    spark,
    "hdfs://namenode:9000/trendcast/models/wikimedia_next_day_views/candidates/hala-j1-20261126-v1",
)
cutoff = datetime(2026, 11, 22, 23, 59, 59, tzinfo=timezone.utc)
history = (
    spark.read.option("header", True)
    .schema(
        "project string, page_title string, event_date date, views long, available_at timestamp"
    )
    .csv(
        "hdfs://namenode:9000/trendcast/models/wikimedia_next_day_views/candidates/"
        "hala-j1-20261126-v1/replay/receipts.csv"
    )
)
journal = policy.evaluate(history, cutoff)
forecasts(journal, extensions=True).where("page_title = 'Amazon_(company)'").show(
    truncate=False
)
```

Dans ce replay local, les vues sources sont réelles et les dates de réception sont simulées. Pour `Amazon_(company)`, la cible est le 23 novembre 2026, le dernier jour admissible le 20 novembre, la prévision V2 vaut 6 399,07 vues et la médiane 5 972. Le volume prévu est supérieur de 7,15 % à la médiane, ce qui donne la direction stable. Le score de persistance utilise le dernier total observé et place la page au rang 2 sur les 14 pages classables de ce replay.

Le dossier `replay/` du ZIP contient l’instantané de test d’une décision et ses valeurs de référence. Les commandes de vérification Spark et de recalcul des métriques figurent dans la section « Reproduction du candidat J1 » du README ML et dans `documentation/REPRODUCTION.md` dans le ZIP.

## Structured Streaming

Charger `policy` avant `writeStream.start()`. Une file de décisions peut fournir l’instant UTC figé, par exemple une chaîne ISO terminée par `Z`. Les microlots mettent à jour la table d’historique ou déclenchent les décisions. Chaque décision lit **un instantané complet, durable et reproductible** du panel. Un lot Kafka avec quelques pages ne contient pas l’historique nécessaire. Utiliser un checkpoint et un répertoire de sortie propres à la validation parallèle.

```python
def process_decisions(batch, batch_id):
    for marker in batch.select("decision_utc").distinct().toLocalIterator():
        cutoff = datetime.fromisoformat(marker.decision_utc.replace("Z", "+00:00"))
        history = read_durable_history(cutoff)
        audit = policy.evaluate(history, cutoff)
        write_shadow_once(audit, cutoff=cutoff, batch_id=batch_id)


query = (
    decisions.writeStream.foreachBatch(process_decisions)
    .option("checkpointLocation", shadow_checkpoint_uri)
    .start()
)
```

`read_durable_history` et `write_shadow_once` représentent les interfaces du job streaming. Épingler la version de l’instantané dans le journal. Pour le sink, utiliser une clé idempotente par politique, cible, décision, projet et page et un commit atomique du panel entier. Un rejeu avec le même instant conserve la première sortie. Utiliser `forecasts(audit)` pour la projection historique à six colonnes. Le dossier `verification/` du paquet décrit le test de parité réalisé sur Spark.
