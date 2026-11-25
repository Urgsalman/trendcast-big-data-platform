# Passation à Nadir · Prévision Wikimedia

## Modèle et résultats

`hala-20261125-v1` est un `PipelineModel` Spark ML composé d’un `VectorAssembler` et d’un arbre de décision. Il prédit le nombre de vues du jour calendaire suivant pour les 20 titres technologiques de `en.wikipedia` enregistrés dans le manifeste. L’arbre retenu a une profondeur maximale de 7, un minimum de 5 observations par nœud et 32 bins.

| Partition | Dates cibles incluses | Lignes | MAE de l’arbre réglé | RMSE de l’arbre réglé |
|---|---|---:|---:|---:|
| Entraînement | 2025-11-26 à 2026-08-06 | 5 080 | — | — |
| Validation | 2026-08-07 à 2026-09-29 | 1 080 | 2 360,07 | 12 944,70 |
| Test indépendant | 2026-09-30 à 2026-11-23 | 1 100 | 2 170,20 | 9 557,23 |

Sur le test, la baseline de persistance obtient une MAE de 2 823,81 et une RMSE de 12 762,44. La baseline hebdomadaire conserve la meilleure MAE, à 2 141,94. L’arbre réglé obtient la meilleure RMSE, à 9 557,23. Les pics imprévus concentrent les erreurs. Les pages hors panel et celles sans historique éligible ne produisent aucun score. La latence de réception réelle n’est pas mesurée dans Gold.

[Rapport expérimental et graphiques sur GitHub](https://github.com/Urgsalman/trendcast-big-data-platform/blob/hala/analytics/ml/RESULTATS.md) · [Guide de reproduction sur GitHub](https://github.com/Urgsalman/trendcast-big-data-platform/blob/hala/analytics/ml/README.md)

## Fichiers et dépendances

- **Modèle HDFS chez Hala** sous `/trendcast/models/wikimedia_next_day_views/hala-20261125-v1`.
- **Archive à recevoir par un canal privé** nommée `TrendCast_Hala_ML_Model_hala-20261125-v1.zip`. Elle contient `hala-20261125-v1/pipeline/`, avec toutes les métadonnées et données internes Spark ML, `hala-20261125-v1/manifest/`, ce document et `SHA256SUMS.txt`. Le manifeste conserve les mesures d’évaluation et les références de provenance de l’expérience. Le Gold et les prédictions détaillées de test restent dans le HDFS de Hala.
- **Code d’inférence** dans `analytics/ml/predict_sample.py` et `analytics/ml/features.py` sur la branche `hala`. Garder le paquet `analytics.ml` dans le chemin Python du job Spark.
- **Environnement vérifié** avec Spark/PySpark 3.5.7, Scala 2.12.18, Java 17.0.16, Python 3.10.12 et NumPy 1.26.4 dans l’image Spark. Le cluster local utilise HDFS 3.4.2. Reconstruire l’image Spark de la branche pour disposer du code et de NumPy.

## Vérifier et importer l’archive

Depuis la racine du dépôt sur Windows, avec le NameNode de Nadir déjà actif, placer le ZIP dans `shared_data/delivery/`. Comparer son empreinte SHA-256 à celle transmise par Hala avant extraction. Utiliser un numéro de répertoire libre, par exemple un suffixe de vérification si la version existe déjà. Le nom du répertoire peut changer. Le `model_version` des sorties vient du manifeste.

```powershell
$ErrorActionPreference = 'Stop'
$version = 'hala-20261125-v1'
$zip = "./shared_data/delivery/TrendCast_Hala_ML_Model_$version.zip"
$id = [guid]::NewGuid().ToString('N')
$unpack = Join-Path $env:TEMP "trendcast-model-$id"

Get-FileHash -Algorithm SHA256 -LiteralPath $zip
python -c "import sys, zipfile; z=zipfile.ZipFile(sys.argv[1]); assert z.testzip() is None" $zip
if ($LASTEXITCODE -ne 0) { throw 'Archive ZIP illisible' }
Expand-Archive -LiteralPath $zip -DestinationPath $unpack
foreach ($entry in Get-Content -LiteralPath (Join-Path $unpack 'SHA256SUMS.txt')) {
    $expected, $relative = $entry -split '  ', 2
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $unpack $relative)).Hash
    if ($actual -ne $expected) { throw "Empreinte incorrecte pour $relative" }
}

function Invoke-CheckedDocker {
    docker @args
    if ($LASTEXITCODE -ne 0) { throw "Échec de docker $($args -join ' ')" }
}

$nn = Invoke-CheckedDocker compose ps -q namenode
if (-not $nn) { throw 'NameNode absent' }
$stage = "/tmp/trendcast-model-$id"
$parent = '/trendcast/models/wikimedia_next_day_views'
$destination = "$parent/$version"
Invoke-CheckedDocker compose exec -T namenode mkdir $stage
Invoke-CheckedDocker cp (Join-Path $unpack $version) "${nn}:${stage}/"
Invoke-CheckedDocker compose exec -T namenode hdfs dfs -mkdir -p $parent
Invoke-CheckedDocker compose exec -T namenode hdfs dfs -mkdir $destination
Invoke-CheckedDocker compose exec -T namenode hdfs dfs -put "$stage/$version/pipeline" "$stage/$version/manifest" "$destination/"
Invoke-CheckedDocker compose exec -T namenode hdfs dfs -ls $destination
Invoke-CheckedDocker compose exec -T namenode hdfs fsck $destination

Invoke-CheckedDocker compose --profile jobs build bronze
$model = "hdfs://namenode:9000$destination"
Invoke-CheckedDocker compose --profile jobs run --rm --no-deps -T -e TRENDCAST_JOB=predict_sample.py bronze --model "$model" --load-only
```

Le résultat attendu contient `WIKIMEDIA_MODEL_LOADED`, les deux étapes du pipeline et les quatre variables. Ce contrôle utilise seulement le pipeline et le manifeste. Si le Gold et des prédictions de référence sont disponibles séparément, utiliser `--gold` et `--reference-predictions` sans `--load-only` pour vérifier la parité sur données réelles. Un import interrompu laisse le chemin réservé. Choisir un chemin libre pour relancer ou traiter ce chemin selon la procédure d’exploitation de l’équipe.

## Contrat entre Kafka et le modèle

Le producteur envoie une enveloppe JSON sur `trendcast.events.validated`. Lire uniquement `source == "wikimedia_pageviews"`, `schema_version == "1.0"` et `payload.granularity == "daily"`. Le fichier `config/schemas/pageview_event.schema.json` décrit une représentation à plat. Pour décoder la valeur Kafka, utiliser l’enveloppe imbriquée publiée par le validateur d’ingestion. Le collecteur interroge les vues par article avec les filtres `all-access/user`. Garder exactement le projet et le titre du message, avec leurs underscores ou parenthèses.

| Entrée de `forecast_for_reference` | Type Spark | Origine |
|---|---|---|
| `project` | `string` | `payload.project` |
| `page_title` | `string` | `payload.page_title` |
| `event_date` | `date` | Date UTC de `timestamp` dans l’enveloppe, le jour mesuré |
| `views` | `long` | `payload.views`, entier positif ou nul |
| `available_at` | `timestamp` | Horodatage UTC enregistré lorsque cette observation devient disponible dans l’historique lu par le service |

`available_at` doit être ajouté côté consommation ou stockage. Le JSON du producteur ne contient pas cette valeur. Le `timestamp` Kafka ordinaire peut être de type `CreateTime`, qui est l’horodatage du producteur par défaut. Il ne permet pas d’établir quand le service pouvait lire cette observation. Si l’équipe configure et vérifie `LogAppendTime`, il indique l’entrée dans le journal Kafka, mais il faut aussi tenir compte des délais de consommation et de mise à disposition dans l’historique d’inférence. [Configuration des timestamps Kafka](https://kafka.apache.org/41/configuration/topic-configs/#message.timestamp.type)

Enregistrer un horodatage UTC durable lors de la première mise à disposition, avec `(topic, partition, offset)` pour identifier le message lors des reprises. Garder cette valeur sur chaque rejeu. Conserver aussi l’identité métier `(project, page_title, event_date)` et les versions reçues. Pour un rattrapage ancien sans preuve de réception, enregistrer l’heure réelle de mise à disposition dans le service. Le délai simulé du backtest appartient uniquement à l’évaluation.

Pour chaque décision, lire un instantané des observations déjà mises à disposition, limité aux 28 jours précédant la référence et au panel du manifeste. Une table historique durable alimentée depuis le topic convient, y compris après redémarrage. À l’intérieur de la fenêtre, une seule observation éligible suffit pour donner un score. Les retransmissions égales sont regroupées. Si des valeurs contradictoires pour une même date ont déjà été reçues au moment du calcul, cette date est écartée. Les observations tardives peuvent servir aux décisions ultérieures tant qu’elles sont encore dans la fenêtre. Conserver la sortie déjà émise et son heure de décision pour rendre les reprises idempotentes.

## Cible et variables

Pour la référence `t`, décider avant **00:00 UTC de `t+1`**. La version livrée admet les dates `t−28` à `t−2` et exige `available_at < prediction_timestamp`. Par exemple, une décision le **2026-11-22 à 23:59:59 UTC** vise le **2026-11-23**. La mesure admissible la plus récente date du **2026-11-20** et doit déjà être arrivée. Le jour cible est futur au moment de la décision, même si les mesures utilisées remontent à plusieurs jours. Wikimedia indique qu’un lot quotidien arrive après la période qu’il mesure et qu’un retard peut dépasser 24 heures. Le délai du modèle ne garantit donc pas qu’une observation précise sera disponible. [Disponibilité des données Wikimedia](https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/documentation/troubleshooting.html)

| Position | Variable | Calcul |
|---:|---|---|
| 1 | `latest_views` | Vues de l’observation éligible la plus récente |
| 2 | `latest_age` | Nombre de jours entre cette observation et `t` |
| 3 | `weekday_sin` | Sinus de `2π × (jour_de_semaine(t+1) − 1) / 7` |
| 4 | `weekday_cos` | Cosinus du même angle |

Spark numérote les jours de semaine du dimanche 1 au samedi 7. Passer les observations historiques à `forecast_for_reference` pour réutiliser la préparation exacte de l’entraînement. Le pipeline sauvegardé reçoit le résultat de cette préparation et produit une prédiction numérique. La sortie fournit `project`, `page_title`, `prediction_date:date`, `predicted_views:double`, `model_version` et `prediction_timestamp:timestamp`.

### Exemple de prédiction avec un lot de contrôle

Ce lot utilise une observation réelle de la vérification du modèle avec un horodatage de réception illustratif. La prédiction attendue pour `Big_data` le 23 novembre est **186,7375 vues**. Dans le flux, remplacer le lot et l’instant de décision par les données et horaires réellement mesurés.

```python
from datetime import date, datetime, timezone

from analytics.ml.predict_sample import forecast_for_reference, load_forecaster

spark.conf.set("spark.sql.session.timeZone", "UTC")
model_uri = (
    "hdfs://namenode:9000/trendcast/models/wikimedia_next_day_views/hala-20261125-v1"
)
pipeline, manifest = load_forecaster(spark, model_uri)

history = spark.createDataFrame(
    [
        (
            "en.wikipedia",
            "Big_data",
            date(2026, 11, 20),
            190,
            datetime(2026, 11, 22, 18, tzinfo=timezone.utc),
        )
    ],
    "project string, page_title string, event_date date, views long, available_at timestamp",
)
predictions = forecast_for_reference(
    spark,
    pipeline,
    manifest,
    history,
    date(2026, 11, 22),
    prediction_timestamp=datetime(2026, 11, 22, 23, 59, 59, tzinfo=timezone.utc),
)
predictions.show(truncate=False)
```

## Vérifications côté streaming

1. Comparer le schéma issu du topic aux cinq colonnes attendues et vérifier les dates près de minuit UTC.
2. Vérifier les offsets et la stabilité de `available_at` après redémarrage. Tester une arrivée exactement à l’heure de décision, une arrivée après, une retransmission et un conflit sur les vues.
3. Vérifier que le service planifie la prédiction et la rend disponible avant le début de la cible. Suivre la couverture du panel et les absences d’historique.
4. Rejouer un instantané contrôlé avec la même heure de décision et comparer les lignes et les prédictions au calcul batch. Vérifier l’unicité de `(project, page_title, prediction_date, model_version)` pour une exécution quotidienne.
5. Vérifier le chargement du modèle au démarrage sur le HDFS de Nadir. Comparer des vues collectées et les valeurs Gold à page et date égales, car la provenance de chaque ligne Gold manque. Tester la persistance, la latence et les reprises de son propre flux.
