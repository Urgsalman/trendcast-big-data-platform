# trendcast-big-data-platform
Une plateforme Big Data dédiée à l’ingestion des données de consultation des pages Wikimedia, au traitement distribué et à la prévision des tendances.

## Guides de lancement

- [Installer les deux modèles et exécuter la démonstration J+1 sur hala](analytics/ml/DEMONSTRATION.md)
- [Architecture finale, résultats et contrat Analytics & ML](analytics/ml/README.md)
- [Automatisation des collectes](ingestion/README_AUTOMATION.md)
- [Cluster Spark/HDFS et stockage Bronze](storage/README.md)

La politique finale `hala-j1-20261126-v1` utilise une forêt logarithmique à J−1, l’arbre V2 en secours à J−2 et la persistance normalisée pour classer les tendances. La démonstration s’appuie sur le Gold Wikimedia avec une disponibilité simulée. [Installation du paquet](analytics/ml/PASSATION_CANDIDAT_J1.md#installation-des-modèles)
