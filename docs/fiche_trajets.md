# Fiche source — NYC Yellow Taxi Trip Records (Janvier 2025)

Une fiche par source de données. Copier ce fichier, le renommer (`fiche_trajets.md`, `fiche_zones.md`...) et le compléter. Tout chiffre doit avoir été mesuré, pas estimé.

## Identité

| Rubrique | Réponse |
|---|---|
| Nom de la source | TLC Yellow Taxi Trip Records |
| Producteur des données | NYC Taxi & Limousine Commission (TLC) |
| Adresse (URL) | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| Accès (public, authentifié) | Public |
| Format du fichier | Parquet |
| Fréquence de publication | Mensuelle |
| Délai entre la période couverte et la publication | Environ 2 mois |

## Volume mesuré

| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outil et commande utilisés |
|---|---|---|---|---|
| `yellow_tripdata_2025-01.parquet` | ~59.1 Mo | 3 475 226 | 20 | `curl -I` (taille) et `duckdb` (lignes/colonnes) via un script Python |

## Colonnes

| Colonne | Type dans le fichier | Signification | Exemple de valeur |
|---|---|---|---|
| `VendorID` | INTEGER | Identifiant du fournisseur du terminal | `1` ou `2` |
| `tpep_pickup_datetime` | TIMESTAMP | Date et heure de prise en charge | `2025-01-01 00:05:00` |
| `tpep_dropoff_datetime`| TIMESTAMP | Date et heure de fin de course | `2025-01-01 00:20:00` |
| `passenger_count` | BIGINT | Nombre de passagers saisis par le chauffeur | `1` |
| `trip_distance` | DOUBLE | Distance de la course en miles | `2.5` |
| `RatecodeID` | BIGINT | Code du type de tarif appliqué | `1` |
| `store_and_fwd_flag` | VARCHAR | Données gardées en mémoire avant l'envoi ("Y" ou "N") | `N` |
| `PULocationID` | INTEGER | Identifiant de la zone de prise en charge | `236` |
| `DOLocationID` | INTEGER | Identifiant de la zone de dépose | `237` |
| `payment_type` | BIGINT | Code du mode de paiement | `1` |
| `fare_amount` | DOUBLE | Montant de la course mesuré par le compteur | `12.5` |
| `extra` | DOUBLE | Frais supplémentaires divers (heure de pointe, nuit...) | `1.0` |
| `mta_tax` | DOUBLE | Taxe de la MTA (Metropolitan Transportation Authority) | `0.5` |
| `tip_amount` | DOUBLE | Montant du pourboire (uniquement par carte) | `2.5` |
| `tolls_amount` | DOUBLE | Montant total des péages | `0.0` |
| `improvement_surcharge`| DOUBLE | Taxe d'amélioration des infrastructures | `0.3` |
| `total_amount` | DOUBLE | Montant total payé par le client | `16.8` |
| `congestion_surcharge` | DOUBLE | Surcharge liée à la congestion à Manhattan | `2.5` |
| `Airport_fee` | DOUBLE | Frais pour prise en charge aux aéroports (JFK/LaGuardia) | `1.25` |
| `cbd_congestion_fee` | DOUBLE | Surcharge de congestion spécifique au CBD | `0.0` |

## Codes

Pour chaque colonne qui contient un code (mode de paiement, type de tarif, fournisseur) : la liste des valeurs et leur signification, d'après le dictionnaire de données.

| Colonne | Valeur | Signification |
|---|---|---|
| `VendorID` | `1` | Creative Mobile Technologies, LLC |
| `VendorID` | `2` | VeriFone Inc. |
| `RatecodeID` | `1` | Standard rate (Tarif classique) |
| `RatecodeID` | `2` | JFK (Forfait aéroport JFK) |
| `RatecodeID` | `3` | Newark (Forfait aéroport Newark) |
| `RatecodeID` | `4` | Nassau or Westchester (Tarif banlieue) |
| `RatecodeID` | `5` | Negotiated fare (Tarif négocié) |
| `RatecodeID` | `6` | Group ride (Course partagée) |
| `payment_type` | `1` | Credit card (Carte bancaire) |
| `payment_type` | `2` | Cash (Espèces) |
| `payment_type` | `3` | No charge (Gratuit) |
| `payment_type` | `4` | Dispute (Litige) |
| `payment_type` | `5` | Unknown (Inconnu) |
| `payment_type` | `6` | Voided trip (Trajet annulé) |
| `store_and_fwd_flag` | `Y` | Yes (Trajet stocké dans le véhicule avant envoi) |
| `store_and_fwd_flag` | `N` | No (Trajet non stocké, envoyé directement) |

## Ce qui a surpris

Valeurs étonnantes repérées en regardant les données (dates hors période, montants négatifs, colonnes vides...). Deux ou trois lignes suffisent.

- **Montants négatifs :** On observe **63 037** lignes avec un montant total (`total_amount`) négatif, ce qui semble correspondre à des annulations ou des remboursements.
- **Courses immobiles mais facturées :** Il y a **75 846** trajets où la distance parcourue est de 0 mile (`trip_distance = 0`) mais dont le prix (`fare_amount`) est supérieur à 0.
- **Dates aberrantes :** Il y a **21** trajets dans le passé (avant 2025) et **1** trajet dans le futur (après janvier 2025) dans le fichier du mois de janvier.
