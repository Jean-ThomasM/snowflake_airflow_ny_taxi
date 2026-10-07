import sys
import os
import requests
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

# Charge les variables du fichier .env
load_dotenv()

# ==========================================
# CONFIGURATION
# ==========================================
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
CHEMIN_CLE_RSA = os.getenv("CHEMIN_CLE_RSA")

# ==========================================
# FONCTIONS (Une action = Une fonction)
# ==========================================


def telecharger_fichier_parquet(mois_cible: str) -> str:
    """Télécharge le fichier Parquet depuis le site officiel de NYC TLC."""
    nom_fichier = f"yellow_tripdata_{mois_cible}.parquet"
    url_telechargement = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{nom_fichier}"
    )

    print(
        f"[ÉTAPE 1] Téléchargement des données de {mois_cible} depuis : {url_telechargement}"
    )
    reponse_serveur = requests.get(url_telechargement)
    reponse_serveur.raise_for_status()  # Coupe le script direct si le lien est mort

    with open(nom_fichier, "wb") as fichier_local:
        fichier_local.write(reponse_serveur.content)

    print(f" -> Fichier {nom_fichier} sauvegardé localement.")
    return nom_fichier


def generer_empreinte_rsa(chemin_cle: str) -> bytes:
    """Lit le fichier .p8 et génère les octets nécessaires pour Snowflake."""
    with open(chemin_cle, "rb") as fichier_cle:
        cle_privee = serialization.load_pem_private_key(
            fichier_cle.read(), password=None, backend=default_backend()
        )

    octets_cle_privee = cle_privee.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return octets_cle_privee


def ouvrir_connexion_snowflake() -> snowflake.connector.SnowflakeConnection:
    """Ouvre une connexion sécurisée vers Snowflake."""
    print("[ÉTAPE 2] Authentification sécurisée vers Snowflake...")
    octets_cle = generer_empreinte_rsa(CHEMIN_CLE_RSA)

    connexion = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=octets_cle,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    print(" -> Connexion établie avec succès.")
    return connexion


def envoyer_fichier_sur_stage(curseur_bd, nom_fichier_local: str):
    """Envoie le fichier local vers la zone d'attente (Stage) de Snowflake."""
    print(f"[ÉTAPE 3] Envoi du fichier {nom_fichier_local} sur le Stage (PUT)...")
    commande_put = (
        f"PUT file://{nom_fichier_local} @raw_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    curseur_bd.execute(commande_put)
    print(" -> Fichier envoyé sur le stage Snowflake.")


def charger_donnees_dans_table(curseur_bd, nom_fichier_stage: str):
    """Copie les données du stage vers la table YELLOW_TRIPDATA avec le bon format."""
    print("[ÉTAPE 4] Chargement des données dans la table (COPY INTO)...")

    # Le SELECT permet de nettoyer et typer la donnée à la volée pendant le chargement
    commande_copy_into = f"""
    COPY INTO YELLOW_TRIPDATA
    FROM (
        SELECT
            $1:VendorID::INT,
            $1:tpep_pickup_datetime::TIMESTAMP_NTZ,
            $1:tpep_dropoff_datetime::TIMESTAMP_NTZ,
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
        FROM @raw_stage/{nom_fichier_stage}
    )
    FILE_FORMAT = format_parquet
    """
    curseur_bd.execute(commande_copy_into)
    print(" -> Données chargées dans la table.")


def verifier_chargement(curseur_bd, nom_fichier: str):
    """Vérifie le nombre de lignes insérées pour ce fichier."""
    curseur_bd.execute(
        f"SELECT COUNT(*) FROM YELLOW_TRIPDATA WHERE _source_file = '{nom_fichier}'"
    )
    nombre_lignes = curseur_bd.fetchone()[0]
    print(
        f"[RÉSULTAT] Succès : {nombre_lignes} lignes trouvées pour le fichier {nom_fichier}."
    )


# ==========================================
# SCRIPT PRINCIPAL (Le fil conducteur)
# ==========================================
if __name__ == "__main__":
    # Récupération du mois depuis le terminal (ex: "2025-01")
    mois_demande = sys.argv[1] if len(sys.argv) > 1 else "2025-01"

    # 1. Téléchargement
    fichier_local = telecharger_fichier_parquet(mois_demande)

    # 2. Connexion
    connexion_snowflake = ouvrir_connexion_snowflake()
    curseur_bd = connexion_snowflake.cursor()

    try:
        # 3. Traitement
        envoyer_fichier_sur_stage(curseur_bd, fichier_local)
        charger_donnees_dans_table(curseur_bd, fichier_local)

        # 4. Vérification
        verifier_chargement(curseur_bd, fichier_local)

    finally:
        # 5. Nettoyage et fermeture (Sécurité : s'exécute même si le script plante avant)
        curseur_bd.close()
        connexion_snowflake.close()

        if os.path.exists(fichier_local):
            os.remove(fichier_local)
            print(
                f"[NETTOYAGE] Fichier temporaire {fichier_local} supprimé de ton ordinateur."
            )
