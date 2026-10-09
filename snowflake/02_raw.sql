-- 1. On prend le badge du robot pour qu'il soit propriétaire des tables
USE ROLE ROLE_HUDSON;
USE DATABASE NYC_TAXI;
USE SCHEMA RAW;

-- 2. Création du stage (le dossier de dépôt temporaire)
CREATE STAGE IF NOT EXISTS raw_stage;

-- 3. Création des formats pour expliquer à Snowflake comment lire les fichiers
CREATE FILE FORMAT IF NOT EXISTS format_parquet TYPE = PARQUET;
CREATE FILE FORMAT IF NOT EXISTS format_csv
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- 4. Création de la table des trajets selon le contrat
CREATE TABLE IF NOT EXISTS YELLOW_TRIPDATA (
    vendorid INT,
    tpep_pickup_datetime TIMESTAMP_NTZ,
    tpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count INT,
    trip_distance FLOAT,
    ratecodeid INT,
    store_and_fwd_flag VARCHAR,
    pulocationid INT,
    dolocationid INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    cbd_congestion_fee FLOAT,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ
);

-- 5. Création de la table des zones selon le contrat
CREATE TABLE IF NOT EXISTS TAXI_ZONE_LOOKUP (
    locationid INT UNIQUE,
    borough VARCHAR,
    zone VARCHAR,
    service_zone VARCHAR,
    _source_file VARCHAR,
    _loaded_at TIMESTAMP_NTZ
);
