import os
import requests
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

# Charge les secrets du fichier .env
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

URL_CSV = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
NOM_FICHIER = "taxi_zone_lookup.csv"

# ==========================================
# FONCTIONS
# ==========================================

def telecharger_fichier_csv() -> str:
    """Télécharge le fichier CSV des zones de taxi."""
    print(f"[ÉTAPE 1] Téléchargement depuis : {URL_CSV}")
    reponse = requests.get(URL_CSV)
    reponse.raise_for_status()
    
    with open(NOM_FICHIER, "wb") as fichier_local:
        fichier_local.write(reponse.content)
        
    print(f" -> Fichier {NOM_FICHIER} sauvegardé localement.")
    return NOM_FICHIER

def generer_empreinte_rsa(chemin_cle: str) -> bytes:
    with open(chemin_cle, "rb") as f:
        cle = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    return cle.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

def ouvrir_connexion_snowflake() -> snowflake.connector.SnowflakeConnection:
    print("[ÉTAPE 2] Authentification sécurisée vers Snowflake...")
    connexion = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=generer_empreinte_rsa(CHEMIN_CLE_RSA),
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    print(" -> Connexion établie avec succès.")
    return connexion

def envoyer_fichier_sur_stage(curseur_bd, fichier: str):
    print(f"[ÉTAPE 3] Envoi sur le Stage (PUT)...")
    curseur_bd.execute(f"PUT file://{fichier} @raw_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    print(" -> Fichier envoyé.")

def charger_donnees_dans_table(curseur_bd, fichier: str):
    """Copie les données en associant chaque colonne CSV ($1, $2...) à la table."""
    print("[ÉTAPE 4] Chargement dans la table TAXI_ZONE_LOOKUP (COPY INTO)...")
    
    commande_copy = f"""
    COPY INTO TAXI_ZONE_LOOKUP
    FROM (
        SELECT 
            $1::INT,
            $2::VARCHAR,
            $3::VARCHAR,
            $4::VARCHAR,
            METADATA$FILENAME,
            CURRENT_TIMESTAMP()
        FROM @raw_stage/{fichier}
    )
    FILE_FORMAT = format_csv
    """
    curseur_bd.execute(commande_copy)
    print(" -> Données chargées dans la table.")

def verifier_chargement(curseur_bd, fichier: str):
    curseur_bd.execute(f"SELECT COUNT(*) FROM TAXI_ZONE_LOOKUP WHERE _source_file = '{fichier}'")
    nombre_lignes = curseur_bd.fetchone()[0]
    print(f"[RÉSULTAT] Succès : {nombre_lignes} zones géographiques chargées.")

# ==========================================
# SCRIPT PRINCIPAL
# ==========================================
if __name__ == "__main__":
    fichier_local = telecharger_fichier_csv()
    connexion = ouvrir_connexion_snowflake()
    curseur = connexion.cursor()
    
    try:
        envoyer_fichier_sur_stage(curseur, fichier_local)
        charger_donnees_dans_table(curseur, fichier_local)
        verifier_chargement(curseur, fichier_local)
    finally:
        curseur.close()
        connexion.close()
        if os.path.exists(fichier_local):
            os.remove(fichier_local)
            print("[NETTOYAGE] Fichier CSV temporaire supprimé.")