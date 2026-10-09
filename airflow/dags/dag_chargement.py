import os
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup

# Le dossier temporaire d'Airflow où stocker le fichier téléchargé
DOSSIER_TEMP = "/tmp"


@dag(
    dag_id="chargement_taxi_mensuel",
    start_date=datetime(2025, 1, 1),  # On commence en janvier 2025
    end_date=datetime(2025, 3, 31),
    schedule="@monthly", # <--- AJOUT OBLIGATOIRE POUR QUE CATCHUP FONCTIONNE
    catchup=True,  # Active le rattrapage des mois passés (janv, fév, mars)
    max_active_runs=1,
    tags=["nyc_taxi", "raw"],
    template_searchpath=["/usr/local/airflow"], # <--- Permet de trouver le dossier include/
)
def pipeline_chargement():

    @task
    def telecharger_fichier(logical_date=None, **kwargs):
        """Déduit le mois de l'exécution, vérifie et télécharge le fichier."""
        mois = logical_date.strftime("%Y-%m")
        nom_fichier = f"yellow_tripdata_{mois}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{nom_fichier}"
        chemin_local = os.path.join(DOSSIER_TEMP, nom_fichier)

        print(f"Lancement pour le mois : {mois} | Fichier attendu : {nom_fichier}")

        reponse = requests.get(url, stream=True)
        if reponse.status_code == 404:
            raise Exception(f"Le fichier pour {mois} n'est pas encore publié.")
        reponse.raise_for_status()

        with open(chemin_local, "wb") as f:
            for chunk in reponse.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Fichier téléchargé avec succès : {chemin_local}")
        return nom_fichier

    @task
    def charger_sur_snowflake(nom_fichier: str):
        """Envoie le fichier sur le stage et le copie dans la table RAW."""
        chemin_local = os.path.join(DOSSIER_TEMP, nom_fichier)
        hook = SnowflakeHook(snowflake_conn_id="snowflake_nyc_taxi")

        requete_put = (
            f"PUT file://{chemin_local} @NYC_TAXI.RAW.raw_stage AUTO_COMPRESS=FALSE;"
        )
        hook.run(requete_put)
        print("Fichier envoyé sur le stage Snowflake.")
        
        # Suppression des anciennes données de ce fichier pour éviter les doublons en cas de re-run
        requete_delete = f"DELETE FROM NYC_TAXI.RAW.YELLOW_TRIPDATA WHERE _source_file = '{nom_fichier}';"
        hook.run(requete_delete)
        print("Anciennes données du mois supprimées (idempotence).")

        requete_copy = f"""
        COPY INTO NYC_TAXI.RAW.YELLOW_TRIPDATA
        FROM (
            SELECT
                $1:VendorID::INT,
                ($1:tpep_pickup_datetime::NUMBER / 1000000)::TIMESTAMP_NTZ,
                ($1:tpep_dropoff_datetime::NUMBER / 1000000)::TIMESTAMP_NTZ,
                $1:passenger_count::INT,
                $1:trip_distance::FLOAT,
                $1:RatecodeID::INT,
                $1:store_and_fwd_flag::VARCHAR,
                $1:PULocationID::INT,
                $1:DOLocationID::INT,
                $1:payment_type::INT,
                $1:fare_amount::FLOAT,
                $1:extra::FLOAT,
                $1:mta_tax::FLOAT,
                $1:tip_amount::FLOAT,
                $1:tolls_amount::FLOAT,
                $1:improvement_surcharge::FLOAT,
                $1:total_amount::FLOAT,
                $1:congestion_surcharge::FLOAT,
                $1:Airport_fee::FLOAT,
                $1:cbd_congestion_fee::FLOAT,
                METADATA$FILENAME,
                CURRENT_TIMESTAMP()
            FROM @NYC_TAXI.RAW.raw_stage/{nom_fichier}
        )
        FILE_FORMAT = NYC_TAXI.RAW.format_parquet
        FORCE = TRUE
        """
        hook.run(requete_copy)
        print("Données ingérées dans la table RAW.YELLOW_TRIPDATA.")
        os.remove(chemin_local)

    # === ATTENTION : Les opérateurs sont maintenant sortis de la fonction précédente ===

    # === 1. Initialisation des tables ===
    init_tables = SQLExecuteQueryOperator(
        task_id="init_00_tables",
        conn_id="snowflake_nyc_taxi",
        sql="include/sql/00_tables.sql",
        split_statements=True,
    )

    # === 2. Couche STAGING ===
    with TaskGroup("staging") as groupe_staging:
        stg_codes = SQLExecuteQueryOperator(
            task_id="stg_codes",
            conn_id="snowflake_nyc_taxi",
            sql="include/sql/staging/codes_tlc.sql",
            split_statements=True,
        )

        stg_zones = SQLExecuteQueryOperator(
            task_id="stg_zones",
            conn_id="snowflake_nyc_taxi",
            sql="include/sql/staging/stg_tlc__taxi_zones.sql",
        )

        stg_trips = SQLExecuteQueryOperator(
            task_id="stg_trips",
            conn_id="snowflake_nyc_taxi",
            sql="include/sql/staging/stg_tlc__yellow_trips.sql",
        )

    # === 3. Couche INTERMEDIATE ===
    with TaskGroup("intermediate") as groupe_intermediate:
        int_trips_flagged = SQLExecuteQueryOperator(
            task_id="int_trips_flagged",
            conn_id="snowflake_nyc_taxi",
            sql="include/sql/intermediate/int_trips__flagged.sql",
            params={"max_trip_distance_miles": 100, "max_trip_duration_min": 180},
            split_statements=True,
        )
        int_trips_enriched = SQLExecuteQueryOperator(
            task_id="int_trips_enriched",
            conn_id="snowflake_nyc_taxi",
            sql="include/sql/intermediate/int_trips__enriched.sql",
            split_statements=True,
        )
        int_trips_flagged >> int_trips_enriched

    # === 4. CONTRÔLES ===
    with TaskGroup("controles") as groupe_controles:
        ctrl_raw_charge = SQLCheckOperator(
            task_id="ctrl_raw_charge",
            conn_id="snowflake_nyc_taxi",
            sql="include/sql/controles/raw_mois_charge.sql",
        )
        ctrl_doublons = SQLCheckOperator(
            task_id="ctrl_doublons",
            conn_id="snowflake_nyc_taxi",
            sql="include/sql/controles/trajets_en_double.sql",
        )
        ctrl_trop_ecartes = SQLCheckOperator(
            task_id="ctrl_trop_ecartes",
            conn_id="snowflake_nyc_taxi",
            sql="include/sql/controles/trop_ecartes.sql",
        )

    # === 5. Couche MARTS ===
    with TaskGroup("marts") as groupe_marts:
        dim_date = SQLExecuteQueryOperator(
            task_id="dim_date",
            conn_id="snowflake_nyc_taxi",
            sql="include/sql/marts/dim_date.sql",
            params={"start_month": "2025-01-01", "end_month": "2025-04-01"}
        )
        dim_payment_type = SQLExecuteQueryOperator(task_id="dim_payment_type", conn_id="snowflake_nyc_taxi", sql="include/sql/marts/dim_payment_type.sql")
        dim_rate_code = SQLExecuteQueryOperator(task_id="dim_rate_code", conn_id="snowflake_nyc_taxi", sql="include/sql/marts/dim_rate_code.sql")
        dim_vendor = SQLExecuteQueryOperator(task_id="dim_vendor", conn_id="snowflake_nyc_taxi", sql="include/sql/marts/dim_vendor.sql")
        dim_zone = SQLExecuteQueryOperator(task_id="dim_zone", conn_id="snowflake_nyc_taxi", sql="include/sql/marts/dim_zone.sql")
        
        fct_trips = SQLExecuteQueryOperator(
            task_id="fct_trips",
            conn_id="snowflake_nyc_taxi",
            sql="include/sql/marts/fct_trips.sql",
            split_statements=True
        )
        
        mart_daily_revenue = SQLExecuteQueryOperator(task_id="mart_daily_revenue", conn_id="snowflake_nyc_taxi", sql="include/sql/marts/mart_daily_revenue.sql")
        mart_data_quality = SQLExecuteQueryOperator(task_id="mart_data_quality", conn_id="snowflake_nyc_taxi", sql="include/sql/marts/mart_data_quality.sql")
        mart_zone_hourly_demand = SQLExecuteQueryOperator(task_id="mart_zone_hourly_demand", conn_id="snowflake_nyc_taxi", sql="include/sql/marts/mart_zone_hourly_demand.sql")

        [dim_date, dim_payment_type, dim_rate_code, dim_vendor, dim_zone] >> fct_trips
        fct_trips >> [mart_daily_revenue, mart_data_quality, mart_zone_hourly_demand]

    # === DÉFINITION DE L'ORDRE COMPLET DU PIPELINE ===
    fichier_a_traiter = telecharger_fichier()
    tache_chargement = charger_sur_snowflake(fichier_a_traiter)

    tache_chargement >> init_tables >> groupe_staging >> groupe_intermediate >> groupe_controles >> groupe_marts


# Instanciation du DAG
pipeline_chargement()
