import os
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Le dossier temporaire d'Airflow où stocker le fichier téléchargé
DOSSIER_TEMP = "/tmp"


@dag(
    dag_id="chargement_taxi_mensuel",
    start_date=datetime(2025, 1, 1),  # On commence en janvier 2025
    schedule="@monthly",  # Une exécution par mois
    catchup=True,  # Active le rattrapage des mois passés (janv, fév, mars)
    max_active_runs=1,
    tags=["nyc_taxi", "raw"],
)
def pipeline_chargement():

    @task
    def telecharger_fichier(logical_date=None, **kwargs):
        """Déduit le mois de l'exécution, vérifie et télécharge le fichier."""
        # 1. Calcul du mois dynamique (ex: 2025-01)
        mois = logical_date.strftime("%Y-%m")
        nom_fichier = f"yellow_tripdata_{mois}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{nom_fichier}"
        chemin_local = os.path.join(DOSSIER_TEMP, nom_fichier)

        print(f"Lancement pour le mois : {mois} | Fichier attendu : {nom_fichier}")

        # 2. Téléchargement en streaming
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

        # On utilise SnowflakeHook pour récupérer la connexion configurée dans le .env
        hook = SnowflakeHook(snowflake_conn_id="snowflake_nyc_taxi")

        # 1. Envoi sur le stage (PUT)
        requete_put = (
            f"PUT file://{chemin_local} @NYC_TAXI.RAW.raw_stage AUTO_COMPRESS=FALSE;"
        )
        hook.run(requete_put)
        print("Fichier envoyé sur le stage Snowflake.")

        # 2. Copie dans la table avec conversion des microsecondes
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
        """
        hook.run(requete_copy)
        print("Données ingérées dans la table RAW.YELLOW_TRIPDATA.")

        # 3. Nettoyage local
        os.remove(chemin_local)

    # Définition des dépendances du pipeline
    fichier_a_traiter = telecharger_fichier()
    charger_sur_snowflake(fichier_a_traiter)


# Instanciation du DAG
pipeline_chargement()
