import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# 1. Lecture de la clé privée locale
with open("rsa_key.p8", "rb") as key:
    p_key = serialization.load_pem_private_key(
        key.read(), password=None, backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# 2. Connexion à Snowflake sans mot de passe
ctx = snowflake.connector.connect(
    user="SRV_HUDSON",
    account="KMWLTJL-KZ14670",
    private_key=pkb,
    warehouse="HUDSON_WH",
    database="NYC_TAXI",
    schema="RAW",
)

# 3. Requête de vérification
cs = ctx.cursor()
cs.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
resultat = cs.fetchone()

print(f"✅ Connexion réussie !")
print(f"Utilisateur : {resultat[0]}")
print(f"Rôle        : {resultat[1]}")
print(f"Warehouse   : {resultat[2]}")
