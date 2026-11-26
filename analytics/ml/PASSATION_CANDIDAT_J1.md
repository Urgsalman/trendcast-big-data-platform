# Passation à Nadir · Prévision candidate Wikimedia

La politique `hala-j1-20261126-v1` calcule les vues de J+1 sur les 20 pages `en.wikipedia`. À J−1, elle utilise la forêt logarithmique. À J−2, elle utilise une copie de l’arbre V2. Chaque route déduit la direction de sa propre prévision de vues. Le classement est une **baseline statistique de persistance**, normalisée par une médiane commune aux routes. La validation en parallèle est décrite dans [VALIDATION_CANDIDAT_J1.md](VALIDATION_CANDIDAT_J1.md).

## Fichiers et importation HDFS

Le ZIP `TrendCast_Hala_ML_Candidate_hala-j1-20261126-v1.zip` contient le manifeste d’inférence et deux pipelines Spark complets sous `hala-j1-20261126-v1/`. Il cible Spark 3.5.7, Scala 2.12, Java 17 et l’image TrendCast Python 3.10 avec NumPy 1.26.4. Le commit de base des modules partagés est `a2036d69e65d739e1a05bc1813c86271420e5fa3`. La branche `hala` contient le module candidat et son test. Le ZIP en contient aussi une copie sous `code/analytics/ml/`. Le montage du dossier `analytics/` en lecture seule permet au driver d’utiliser le code de la branche. Les workers utilisent l’image Spark existante.

Vérifier d’abord le SHA-256 avec le fichier compagnon `.zip.sha256`. Exécuter depuis la racine Docker Compose sous PowerShell. Adapter `$zip` au chemin du fichier reçu.

```powershell
$zip = (Resolve-Path './TrendCast_Hala_ML_Candidate_hala-j1-20261126-v1.zip').Path
$expected = ((Get-Content -LiteralPath "$zip.sha256" -Raw).Trim() -split '\s+')[0]
if ((Get-FileHash -LiteralPath $zip -Algorithm SHA256).Hash -ne $expected) {
    throw 'Empreinte de l’archive invalide'
}
$id = [guid]::NewGuid().ToString('N')
$extract = Join-Path (Get-Location) "trendcast-candidat-$id"
Expand-Archive -LiteralPath $zip -DestinationPath $extract
$bundle = Join-Path $extract 'hala-j1-20261126-v1'
Get-Content -LiteralPath (Join-Path $bundle 'SHA256SUMS.txt') | ForEach-Object {
    $checksum, $file = $_ -split '  ', 2
    if ((Get-FileHash -LiteralPath (Join-Path $bundle $file) -Algorithm SHA256).Hash -ne $checksum) {
        throw "Empreinte invalide pour $file"
    }
}

$stage = "/tmp/trendcast-candidat-$id"
$target = '/trendcast/models/wikimedia_next_day_views/candidates/hala-j1-20261126-v1'
docker compose exec -T namenode mkdir "$stage"
if ($LASTEXITCODE -ne 0) { throw 'Création du dossier temporaire en échec' }
docker compose cp "${bundle}/." "namenode:$stage"
if ($LASTEXITCODE -ne 0) { throw 'Copie Docker en échec' }
docker compose exec -T namenode bash -c 'if hdfs dfs -test -e "$2"; then exit 3; fi; hdfs dfs -mkdir -p "$(dirname "$2")" && hdfs dfs -put "$1" "$2"' release "$stage" "$target"
if ($LASTEXITCODE -ne 0) { throw 'Import HDFS en échec ou destination déjà présente' }
docker compose exec -T namenode hdfs dfs -ls "$target/pipelines"
```

| Route | Répertoire sous `$target` | Règle de restitution |
| --- | --- | --- |
| J−1 | `pipelines/j1_log` | `max(0, expm1(z))` |
| J−2 | `pipelines/j2_v2` | Cible brute avec plancher à 0, recette V2 du manifeste |
| Les deux | `manifest/part-00000` | Panel, ordre des variables, transformations, poids et politique |

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
