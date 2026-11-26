# Validation en parallèle avant activation de J−1

La disponibilité simulée sur Gold mesure l’effet possible de la fraîcheur. Le premier reçu durable côté inférence doit être observé en direct. Les timestamps Kafka du backfill reflètent des imports groupés. Les prévisions officielles continuent d’être servies par la V2 pendant la validation candidate.

## Préparation

1. Épingler le ZIP, les versions des deux pipelines, la politique, la version du code candidat, les 20 pages et l’instant de décision UTC réel. Utiliser une heure avec une marge suffisante pour figer les sorties avant le début de J+1. Conserver l’heure choisie sur toute la fenêtre. Toute modification de la coupure ouvre une nouvelle série de validation.
2. Utiliser un historique as-of immuable. Pour chaque version d’une mesure, capturer la date UTC observée, le statut HTTP, le début et la fin de collecte si disponibles, l’heure d’ACK Kafka, les offsets, le premier `available_at` UTC durable côté inférence et la provenance `live` ou `backfill`. Associer les corrections à leur propre instant. En cas de doublon identique, conserver le premier reçu. Inclure dans le dénominateur les pages absentes, les conflits et les erreurs. Les rattrapages sont journalisés séparément de la disponibilité en direct.
3. Prévoir trois sorties distinctes, le journal de décision candidat complet, les prévisions V2 du même instant et le journal d’évaluation après maturité. Donner au candidat son propre checkpoint et son propre sink. Figer le résultat candidat et V2 avant le début de J+1 en enregistrant le commit du panel. Les labels ne rejoignent les prédictions qu’à partir de J+1 + 2 jours à 00:00 UTC, et après réception effective puis contrôles qualité. Les sorties et les modèles restent gelés pendant la validation.
4. Faire un essai opérationnel court de deux semaines pour vérifier la réception, les conflits, l’idempotence, le redémarrage et l’intégrité des journaux. Signaler séparément les données de cette démonstration. Prédéclarer ensuite le début d’une fenêtre de confirmation de **84 jours consécutifs** avec, pour la décision finale, tous les labels matures disponibles ou leurs motifs d’absence vérifiés.

## Journal des décisions

Conserver une ligne par page même en cas d’abstention. La clé durable contient la version de politique, la cible, l’instant de décision, le projet et la page. Enregistrer les dates attendues de J−1 et J−2 et leur premier reçu canonique disponible à la coupure, la date de dernière observation admissible et son `available_at` UTC, l’identifiant d’instantané et de run, la route, le modèle, les vues prévues à J+1, la médiane historique, le Trend Score, la direction, le rang, le nombre de pages classables et le motif d’abstention. Dans le journal d’observations, conserver aussi les réceptions après la décision pour mesurer la latence réelle sans modifier la prévision figée.

## Mesures et critères d’activation proposés

Figer les seuils ci-dessous et l’heure de coupure avant le premier jour de confirmation puis les faire valider par Hala et Nadir. Une réussite de la démonstration courte vérifie le fonctionnement du flux. La décision d’activation tient aussi compte de la qualité prospective et de la dispersion des résultats par semaine, page et segment.

| Critère | Seuil proposé |
| --- | --- |
| Traçabilité et causalité | 100 % des décisions figées avant q à 00:00 UTC. Tous les reçus utilisés sont disponibles strictement avant la décision et après la clôture de leur journée source. Le rejeu avec le même instant produit la même décision et aucune écriture officielle. |
| Disponibilité réelle de J−1 | Au moins 80 % des 20 × 84 clés page-décision ont un total J−1 canonique reçu à la coupure, et au moins 70 % pour chaque page. |
| Couverture des vues | Au moins 95 % des clés du panel ont une prévision candidate. La couverture candidate se situe au plus un point de pourcentage sous celle de la V2. |
| Volume, sur les clés communes avec label | MAE réduite d’au moins 5 % face à la V2. RMSE au plus égale à 105 % de la V2 et MASE au plus égale à 103 %. |
| Direction et rang, sur les mêmes clés et médianes | F1 macro au plus inférieur de 0,02 à celui des vues V2 classées avec la même bande. P@5 du classement publié au plus inférieure de deux points à celle de la persistance J−2, sur les journées avec au moins cinq pages communes. |
| Validation humaine | Examiner les pics, les cas tardifs, les pages à faible couverture et les semaines défavorables. Hala valide la politique et Nadir valide la procédure de bascule. |

Publier à part la couverture des vues, de la direction et du classement sur toutes les demandes. Pour les métriques appariées, prendre l’intersection des pages et jours avec deux prévisions et un label certifié. Garder la même référence de tendance et le même dénominateur de MASE. Rapporter les pertes au fil des filtres, les percentiles de délai par étape et les taux J−1, J−2 et abstention. Compter les jours évaluables pour le rang. Comparer aussi aux baselines persistance et hebdomadaire sur les mêmes reçus et dates. Calculer les métriques avec les pics et en diagnostic hors pics selon des seuils fixés sur l’historique antérieur.

Vérifier les écarts par semaine et par segment, puis produire des intervalles descriptifs par rééchantillonnage de semaines entières si le nombre de semaines le permet. Les pics rares, le panel fixe et les variations de disponibilité limitent la généralisation. Les seuils sont des conditions opérationnelles décidées à l’avance. Ils ne constituent pas une démonstration statistique définitive.
