# Réponse à la direction d'Hudson Cab Partners

## La question

Où et quand la demande de taxis jaunes est-elle la plus forte à New York, et combien rapporte un trajet selon la zone, l'heure et le mode de paiement ?

## La requête

```sql
SELECT
    z.zone_name AS zone_prise_en_charge,
    f.pickup_hour AS heure,
    p.payment_type_label AS mode_paiement,
    COUNT(*) AS nb_trajets,
    ROUND(AVG(f.total_amount), 2) AS revenu_moyen_par_trajet
FROM NYC_TAXI.MARTS.FCT_TRIPS f
JOIN NYC_TAXI.MARTS.DIM_ZONE z ON f.pickup_zone_key = z.zone_key
JOIN NYC_TAXI.MARTS.DIM_PAYMENT_TYPE p ON f.payment_type_key = p.payment_type_key
GROUP BY 1, 2, 3
ORDER BY nb_trajets DESC
LIMIT 10;
```

## Le résultat : les 10 premières lignes

_(À remplir après avoir exécuté la requête ci-dessus dans Snowflake !)_

| ZONE_PRISE_EN_CHARGE  | HEURE | MODE_PAIEMENT | NB_TRAJETS | REVENU_MOYEN_PAR_TRAJET |
| --------------------- | ----- | ------------- | ---------- | ----------------------- |
| Midtown Center        | 18    | Credit card   | 38263      | 25.06                   |
| Midtown Center        | 17    | Credit card   | 38027      | 26.35                   |
| Midtown Center        | 19    | Credit card   | 31661      | 24.25                   |
| Midtown Center        | 16    | Credit card   | 30696      | 26.53                   |
| Upper East Side South | 15    | Credit card   | 29985      | 20.34                   |
| Upper East Side South | 14    | Credit card   | 29854      | 20.41                   |
| Upper East Side North | 15    | Credit card   | 29757      | 20.64                   |
| Upper East Side South | 17    | Credit card   | 29751      | 22                      |
| Upper East Side South | 18    | Credit card   | 29553      | 21.54                   |
| Upper East Side South | 16    | Credit card   | 28712      | 22.06                   |
|                       |       |               |            |                         |

## Ce qu'il faut en retenir

_(À rédiger après avoir vu les résultats. Exemples d'idées :)_

1. La majorité des trajets sont concentrés sur quelques zones clés de Manhattan (ex: Upper East Side, Midtown) pendant les heures de pointe du soir.
2. La carte de crédit est de loin le mode de paiement dominant sur ces trajets très demandés.
3. Le revenu moyen par trajet varie fortement, avec des pics potentiellement liés aux forfaits aéroportuaires ou à la congestion.

## Les limites

Ce que cette réponse ne dit pas :

- Elle ne couvre que les mois chargés (janvier à mars 2025).
- Les trajets anormaux (erreurs de GPS, dates aberrantes, montants négatifs) ont été écartés et n'apparaissent pas.
- L'analyse est biaisée si certaines zones ont mal été renseignées (Unknown Zone).
