# Soutenance TrendCast · Analytics et prévision J+1

Lancer `candidate.py` sur `hala` avec la politique `hala-j1-20261126-v1`. Le scénario principal rejoue la décision du **22 novembre 2026 à 23:59:59 UTC** pour le **23 novembre 2026**. Les vues proviennent du Gold Wikimedia. La disponibilité des reçus est simulée.

## 1. Préparer le clone et le cluster

Utiliser Git, PowerShell 7 et Docker Desktop avec les conteneurs Linux. Spark 3.5.7 et ses dépendances Python sont dans l’image TrendCast.

```powershell
git clone --branch hala --single-branch https://github.com/Urgsalman/trendcast-big-data-platform.git
Set-Location trendcast-big-data-platform
```

Dans un clone existant, partir d’un arbre de travail sans modifications locales. Exécuter `git switch hala` puis `git pull --ff-only origin hala`. Les commandes suivantes supposent la racine du clone.

```powershell
docker compose build namenode
docker compose --profile jobs build bronze
docker compose up -d --no-build namenode datanode-1 datanode-2 hdfs-init spark-master spark-worker-1 spark-worker-2
docker compose wait hdfs-init
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T namenode hdfs getconf -confKey dfs.replication
(Invoke-RestMethod 'http://localhost:8080/json/').workers | Select-Object id,state,cores,memory
docker compose exec -T namenode hdfs dfs -ls /trendcast/analytics/ml_dataset /trendcast/analytics/daily_metrics
```

Si le cluster tourne déjà avec les deux images, commencer par `dfsadmin -report`. Vérifier deux DataNodes vivants, deux Workers `ALIVE` et une réplication de 2. Initialiser HDFS sur un volume neuf et autorisé lors du premier démarrage.

Si le Gold est présent, réutiliser son chemin HDFS. S’il est absent, obtenir l’export officiel de Soulaimane, puis l’importer dans HDFS. L’archive Gold complète se transmet séparément des modèles.

```powershell
$goldDir = Join-Path 'shared_data' ("gold-officiel-" + [guid]::NewGuid().ToString('N'))
Expand-Archive -LiteralPath './shared_data/TrendCast_Gold_Data.zip' -DestinationPath $goldDir
& './analytics/ml/import_gold.ps1' -Source $goldDir
```

## 2. Installer le paquet des modèles

Placer dans `shared_data/delivery/` le ZIP r2 et son fichier `.zip.sha256`. Si la release est publiée, l’option `-FromRelease` télécharge les fichiers avec GitHub CLI authentifié. [Installation et erreurs de chemin](PASSATION_CANDIDAT_J1.md#installation-des-modèles)

```powershell
& './analytics/ml/install_final_model.ps1' -ArchivePath './shared_data/delivery/TrendCast_Hala_ML_Final_hala-j1-20261126-r2.zip'
```

L’installateur doit afficher `installed` ou `reused`, puis l’URI HDFS. Il vérifie les empreintes et la réplication de chaque bloc.

## 3. Exécuter la démonstration principale

```powershell
$analytics = (Resolve-Path 'analytics').Path
$package = 'hdfs://namenode:9000/trendcast/models/wikimedia_next_day_views/candidates/hala-j1-20261126-v1'
$bash = 'export PYTHONDONTWRITEBYTECODE=1 HADOOP_USER_NAME=hadoop PYTHONPATH=/opt/trendcast; ip="$(hostname -i)"; exec /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 768m --executor-memory 512m --conf "spark.driver.host=$ip" --conf spark.driver.bindAddress=0.0.0.0 --conf spark.executor.cores=1 --conf spark.cores.max=2 --conf spark.sql.shuffle.partitions=3 /opt/trendcast/analytics/ml/candidate.py --package "$1" --decision-utc 2026-11-22T23:59:59Z --scenario "$2"'
docker compose --profile jobs run --pull never --rm --no-deps -T -v "${analytics}:/opt/trendcast/analytics:ro" --entrypoint /bin/bash bronze -c $bash replay "$package" full-j1
if ($LASTEXITCODE -ne 0) { throw 'Échec du replay principal' }
```

`TRENDCAST_MODEL` affiche les paramètres rechargés. Spark affiche les variables et les résultats sur deux tableaux à relier par `page_title`. `TRENDCAST_PREDICTIONS` contient les 16 champs structurés. Les vues du 23 novembre rejoignent les prévisions **après** leur matérialisation.

Résultat attendu de `TRENDCAST_ROUTES` sur le Gold officiel contrôlé, **20 demandées, 20 J−1, 0 J−2, 0 abstention et 100 % de couverture**. `TRENDCAST_DAILY_METRICS` affiche, sur cette date, MAE **9 505,37**, RMSE **38 834,64**, R² **−0,0041**, WAPE **66,14 %** et biais **−8 173,01** vues. Le pic de Linux, **177 023 vues observées** pour **3 521,13 prévues**, contribue fortement à l’erreur.

Les paramètres exacts des pipelines sont résumés ici. Les deux utilisent, dans l’ordre, `latest_views`, `latest_age`, `weekday_sin`, `weekday_cos`.

| Paramètre | Forêt J−1 | Arbre V2 J−2 |
| --- | --- | --- |
| Version | `hala-j1-log-20261126` | `hala-20261125-v2` |
| Cible ou transformation | `log1p(target_next_day_views)` | `target_next_day_views` brute |
| Profondeur maximale, bins | 7, 64 | 9, 64 |
| Minimum par nœud, gain minimal | 8, 0,0001 | 8, 25,0 |
| Fraction pondérée minimale par nœud, impureté | 0,0, `variance` | 0,0, `variance` |
| Arbres, variables candidates, sous-échantillonnage, bootstrap | 16, `all`, 0,9, `true` | Arbre unique |
| Graine | 345 | 345 |

La direction vient du volume prédit. Le classement utilise la persistance normalisée par la médiane propre à chaque page et commune aux deux routes.

## 4. Vérifier la robustesse si le professeur le demande

Le replay du ZIP impose volontairement des retards et des trous sur les mêmes pages. Produire **10 J−1, 5 replis J−2, 5 abstentions**. Le test compare le volume aux références sauvegardées et vérifie l’égalité du journal entre batch et deux microlots `foreachBatch`.

```powershell
docker compose --profile jobs run --pull never --rm --no-deps -T -v "${analytics}:/opt/trendcast/analytics:ro" --entrypoint /bin/bash bronze -c $bash replay "$package" robustness
if ($LASTEXITCODE -ne 0) { throw 'Échec du replay de robustesse' }
$run = [guid]::NewGuid().ToString('N')
$check = "hdfs://namenode:9000/trendcast/analytics/wikimedia_ml_checks/final-r2-$run"
docker compose exec -T namenode hdfs dfs -mkdir -p /trendcast/analytics/wikimedia_ml_checks
$test = $bash.Replace('/analytics/ml/candidate.py', '/analytics/ml/tests/check_candidate.py').Replace('--decision-utc 2026-11-22T23:59:59Z --scenario "$2"', '--output "$2"')
docker compose --profile jobs run --pull never --rm --no-deps -T -v "${analytics}:/opt/trendcast/analytics:ro" --entrypoint /bin/bash bronze -c $test validation "$package" "$check"
```

## 5. Présenter les résultats scientifiques séparément

Spark SQL a préparé le Gold. Sur **462 056 observations**, le filtre retient **20 pages sur 366 jours**, soit **7 320 lignes**, dont **7 260 clés supervisées**. Sur les **126 jours et 2 520 observations** des trois fenêtres historiques avec J−1 simulé, la forêt atteint une MAE de **1 776,20**, une RMSE de **9 791,86**, un R² de **0,774**, une WAPE de **23,97 %** et un MASE de **0,963**. Le gain de MAE face à l’hebdomadaire est de **27,84 %**. Le F1 macro de la direction vaut **0,592** et la Precision@5 du rang par persistance **49,52 %**.

Le Gold ne contient pas de premier `available_at` vérifiable. L’activation en direct dépendra des reçus réellement journalisés lors de l’intégration avec Nadir. [Rapport scientifique](RESULTATS_CANDIDAT_J1.md) · [Validation en parallèle](VALIDATION_CANDIDAT_J1.md)
