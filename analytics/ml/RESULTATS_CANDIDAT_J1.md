# Résultats scientifiques · Candidat TrendCast J−1 / J−2

## Décision

**Retenir la forêt logarithmique pour les vues et la direction lorsque J−1 est admissible, et la V2 dans les deux fonctions lorsque seul J−2 est admissible.** L’un ou l’autre jour manque parfois. Sans mesure admissible à J−1 ni J−2, la page passe en abstention. La persistance relative est la baseline statistique retenue pour classer les pages sur les deux routes.

La forêt utilise quatre variables et les paramètres des expériences sauvegardées. Son score composite de volume sur les trois plis est de **0,74513**, à 0,32 % du meilleur mélange Brut–log (0,74276). La tolérance de 1 % préenregistrée privilégie ici l’estimateur unique. L’arbre relatif A a une MAE de 1 722 vues et un MASE de 0,791, avec une RMSE 10,4 % supérieure à la forêt. L’arbre V2 adapté à J−1 a la plus basse RMSE (9 721 vues) parmi les candidats J−1. Le classement appris C atteint 48,73 % de Precision@5 et 0,567 de NDCG@5. La persistance atteint 49,52 % et 0,570, et C ne la dépasse que sur un pli. La logistique directionnelle B ne dépasse pas les deux meilleures règles dérivées du volume en F1 macro.

## Arbitrage directionnel avant la livraison

| Période cible (2026) | n | Persistance J−1 | Hebdomadaire | Forêt log | Brut–log | Gain brut–log, points F1 | Classes différentes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 25 mai–5 juillet | 840 | 0,432 | 0,509 | 0,560 | 0,561 | +0,12 | 55 |
| 6 juillet–16 août | 840 | 0,399 | 0,525 | 0,558 | 0,559 | +0,13 | 37 |
| 17 août–27 septembre | 840 | 0,368 | 0,584 | 0,615 | 0,621 | +0,61 | 41 |
| Trois fenêtres réunies | 2 520 | 0,425 | 0,556 | **0,592** | 0,594 | **+0,20** | **133** |
| 30 septembre–23 novembre, rétrospectif | 1 100 | 0,400 | 0,552 | 0,576 | 0,565 | −1,04 | 67 |

Les cellules de modèles et baselines sont des F1 macro. La forêt seule gagne face à l’hebdomadaire sur les trois fenêtres et de 3,59 points sur l’ensemble. Brut–log apporte 0,20 point de plus sur l’ensemble, avec des gains de 0,12 à 0,61 point par fenêtre. Sur les **133 divergences de classe (5,28 %)**, le mélange est seul correct 61 fois, la forêt 57 fois, et aucun des deux 15 fois. Les divergences traversent le seuil stable/hausse ou stable/baisse. Aucun cas ne passe directement de baisse à hausse. Le gain est trop faible pour imposer un troisième estimateur et afficher parfois une direction différente de la classe du volume livré. La direction candidate correspond à la croissance du volume qu’elle publie. La ligne rétrospective documente le comportement sur l’ancien test, avec les mêmes définitions. L’arbitrage repose sur le gain apparié des trois fenêtres et la cohérence de l’interface.

## Populations et calculs

Les trois fenêtres chronologiques totalisent **126 jours** et **2 520 observations** sur 20 pages. Les quatre familles Spark ML comparées étaient la régression linéaire, l’arbre de décision, la forêt aléatoire et les arbres à gradient boosté. Le protocole a considéré **40 variables candidates**, soit les quatre variables de base et les 36 variables des cinq groupes d’enrichissement. Les pipelines finaux utilisent dans l’ordre `latest_views`, `latest_age`, `weekday_sin`, `weekday_cos`. La cible scientifique est `target_next_day_views`.

La consolidation réutilise les prévisions Spark sauvegardées, sans nouvel ajustement. Le Gold de 462 056 lignes est filtré sur 20 pages `en.wikipedia` et 366 jours, du 23 novembre 2025 au 23 novembre 2026. On obtient 7 320 observations (20 × 366) et 7 260 cibles supervisées. Quarante références des deux premiers jours manquent d’historique à J−2 et vingt références du dernier jour manquent de cible. Les trois fenêtres externes ont 840 clés chacune. Les 1 100 clés rétrospectives ont déjà été examinées. Ces périodes n’apportent pas de nouveau test indépendant. Le label q est utilisable à partir de q+2 à 00:00 UTC, avec un cutoff train strict.

Pour les évaluations historiques, la décision est J à 23:59:59 UTC et q est J+1. La médiane de référence est calculée sur les reçus admissibles de [J−28, J−2]. Le score est (valeur − médiane)/max(1, médiane). La valeur est le futur volume réalisé pour les classes de référence, le volume prédit pour la direction et les dernières vues admissibles pour le classement de persistance. Les classes ont une bande stable fermée de ±10 %. Precision@5, NDCG@5 et Spearman sont des moyennes par date éligible avec gestion des ex æquo. NDCG pondère la croissance positive réalisée. La couverture est la proportion de vues prévues sur le panel demandé. Le MASE utilise le dénominateur saisonnier à sept jours propre à chaque page, calculé sur son train dans chaque pli.

Les routes J−2 des fenêtres externes utilisent les réajustements historiques V2 déjà archivés, chaque fois avec le train admissible. La ligne rétrospective et le paquet livré utilisent les paramètres de la V2 finale ajustée au 30 septembre. Le test de rechargement contrôle l’égalité de la copie livrée avec les prévisions archivées sur une date Gold. Les deux fenêtres de réception, J−1 et J−2, partagent le même dénominateur historique de référence par page et date.

## Volume, direction et classement

Valeurs agrégées sur les 2 520 clés des fenêtres externes, avec couverture de 100 % pour chaque ligne de ce tableau.

| Prévision des vues et source du rang | MAE | RMSE | R² | WAPE | MASE | P95 erreur absolue | Biais | Exactitude dir. | Balanced acc. | F1 macro | Spearman | P@5 | NDCG@5 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| J−1, forêt pour vues et direction, persistance pour rang | 1 776,20 | 9 791,86 | 0,774 | 23,97 % | 0,963 | 5 491,36 | −404,96 | 0,594 | 0,596 | 0,592 | 0,427 | 49,52 % | 0,570 |
| J−2, V2 pour vues et direction, persistance pour rang | 2 030,78 | 10 960,75 | 0,717 | 27,41 % | 1,175 | 6 534,23 | −130,81 | 0,523 | 0,534 | 0,527 | 0,312 | 42,30 % | 0,484 |

Avec toutes les J−1 simulées, la MAE de la forêt baisse de **24,46 % face à la persistance** et de **27,84 % face à l’hebdomadaire** sur les mêmes 2 520 clés. Le MASE baisse respectivement de 23,74 % et 8,10 %. Le rang par persistance gagne 11,27 points de Precision@5 sur la baseline hebdomadaire. Le ML améliore ici les vues et la direction. La persistance donne le meilleur classement J−1 agrégé.

Les tableaux exhaustifs dont `mesures/comparaison-fenetres.csv` et `mesures/comparaison-retrospective.csv` regroupent les métriques demandées par fenêtre, âge et modèle, avec les six configurations de volume reprises à chaque âge et les expériences A/B/C à J−1. Les P95 des erreurs absolues et les biais y sont exprimés en vues. Les volumes de B proviennent de la forêt, tandis que sa direction et son rang proviennent de la logistique. Les valeurs de C correspondent au score relatif reconverti en vues.

## Disponibilité simulée

La [démonstration du 23 novembre 2026](DEMONSTRATION.md) exécute une seule journée réelle du Gold avec disponibilité J−1 simulée sur les 20 pages. Les métriques journalières sont évaluées après matérialisation des prévisions et décrites séparément dans la procédure de démonstration.

| Réceptions au moment de la décision | n prévu / demandé | MAE | RMSE | MASE | F1 macro | P@5 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Toutes J−1 | 2 520 / 2 520 | 1 776,20 | 9 791,86 | 0,963 | 0,592 | 49,52 % |
| J−1 absente pour cinq pages par jour | 2 520 / 2 520 | 1 877,42 | 10 440,85 | 1,025 | 0,582 | 46,83 % |
| Aucune J−1 | 2 520 / 2 520 | 2 030,78 | 10 960,75 | 1,175 | 0,527 | 42,30 % |
| Deux jours absents pour cinq pages par jour | 1 890 / 2 520 | 1 783,10 | 9 597,01 | 0,971 | 0,593 | 53,81 % |
| J−1 reçue à la coupure ou après pour cinq pages par jour | 2 520 / 2 520 | 1 877,42 | 10 440,85 | 1,025 | 0,582 | 46,83 % |

Le scénario à 75 % de couverture change la population classée. Ses métriques décrivent 15 pages couvertes par jour. Les deux scénarios partiels produisent les mêmes prévisions à la coupure. La forêt mixte à deux âges dépasse les limites préenregistrées face aux deux routes dans trois scénarios sur cinq. Le Gold ne conserve pas de premier reçu durable. Sur les 2 520 clés externes, zéro a un proxy Kafka historique favorable à J−1. La rétrospective en compte 20 sur 1 100. Ces chiffres ne mesurent pas le taux du pipeline en direct.

Pour les 1 100 clés rétrospectives, la route J−1 donne MAE 1 759,43, RMSE 8 773,90, F1 macro 0,576 et P@5 49,45 %. La route J−2 donne MAE 2 084,00, RMSE 9 310,54, F1 macro 0,520 et P@5 45,45 %. Le détail par période figure dans `mesures/repli-disponibilite.csv`.

## Pics de trafic, pages et segments

Les seuils de pics P95 et P99 du ratio réalisé vues/médiane sont estimés sur le train de chaque pli. Les pics restent dans les métriques globales. Les alertes utilisent des ratios seuilés. PR-AUC est calculée comme average precision. Les essais historiques de risque probabiliste n’ont pas démontré de stabilité suffisante pour produire `spike_probability`.

| Candidat ou baseline, fenêtres externes | Seuil | Pics détectés | Fausses alertes | Pics manqués | Précision | Rappel | PR-AUC |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Forêt J−1 | P95 | 48 | 84 | 90 | 0,364 | 0,348 | 0,253 |
| Persistance J−1 | P95 | 42 | 89 | 96 | 0,321 | 0,304 | 0,203 |
| Hebdomadaire J−1 | P95 | 31 | 76 | 107 | 0,290 | 0,225 | 0,160 |
| V2 J−2 | P95 | 54 | 193 | 84 | 0,219 | 0,391 | 0,170 |
| Forêt J−1 | P99 | 9 | 26 | 33 | 0,257 | 0,214 | 0,145 |
| Persistance J−1 | P99 | 10 | 31 | 32 | 0,244 | 0,238 | 0,122 |
| Hebdomadaire J−1 | P99 | 5 | 32 | 37 | 0,135 | 0,119 | 0,055 |
| V2 J−2 | P99 | 8 | 52 | 34 | 0,133 | 0,190 | 0,073 |

Sur les fenêtres externes, la MAE J−1 de 1 776,20 tombe à 1 106,03 hors P95 (2 382 clés), à 1 295,59 hors P99 (2 478 clés) et à 1 345,43 en retirant exactement les 25 ratios les plus élevés. Sur la rétrospective, elle tombe à 924,52 hors P95 (1 004 clés), à 1 080,56 hors P99 (1 071 clés) et à 1 265,81 hors 11 ratios extrêmes. Ces résultats décrivent des cohortes plus faciles.

Sur les fenêtres externes, le segment au-dessus du P90 de popularité a une MAE de 14 318 vues et un MASE de 2,31. Les deux plus fortes MAE par page sont ChatGPT (16 684) et Google (12 745). Le MASE de Google atteint 3,68. Les segments utilisent la moyenne passée de 28 jours, avec seuils propres au train de chaque pli. Les métriques de tous les candidats par page et segment, les cohortes de pics et leurs tailles, et les diagnostics d’alerte sont fournis dans `mesures/pages-et-segments.csv`, `mesures/avec-et-sans-pics.csv` et `mesures/alertes-pics.csv`. Dans ces tableaux, le rang requiert au moins cinq pages éligibles par jour.

Le panel fixe, les périodes déjà utilisées et les erreurs sur les pics limitent la généralisation. Une extension aux pages inconnues ou une activation opérationnelle nécessite une évaluation dédiée. Le corpus mesure des consultations Wikipédia, qui représentent un indicateur d’intérêt. La procédure sur données inédites figure dans [VALIDATION_CANDIDAT_J1.md](VALIDATION_CANDIDAT_J1.md).

Le ZIP permet de recalculer les métriques publiées à partir des prévisions et références figées avec `evaluation/recalculer.py`. Les instructions figurent dans la section « Reproduction du candidat J1 » du README ML et dans `documentation/REPRODUCTION.md` dans le ZIP.
