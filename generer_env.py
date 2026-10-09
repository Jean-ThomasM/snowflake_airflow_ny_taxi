import json

# 1. Lecture de la clé BRUTE (on garde BEGIN, END et les retours à la ligne)
with open("rsa_key.p8", "r") as fichier_cle:
    cle_brute = fichier_cle.read()

# 2. Construction de la configuration
configuration = {
    "conn_type": "snowflake",
    "login": "SRV_HUDSON",
    "extra": {
        "account": "KMWLTJL-KZ14670",
        "database": "NYC_TAXI",
        "schema": "RAW",
        "warehouse": "HUDSON_WH",
        "role": "ROLE_HUDSON",
        "private_key_content": cle_brute,
    },
}

# 3. Écriture dans le fichier .env (json.dumps s'occupe de formater les \n)
ligne_finale = f"AIRFLOW_CONN_SNOWFLAKE_NYC_TAXI={json.dumps(configuration)}\n"

with open("airflow/.env", "w") as fichier_env:
    fichier_env.write(ligne_finale)

print("✅ Fichier airflow/.env généré avec la clé complète et les balises !")
