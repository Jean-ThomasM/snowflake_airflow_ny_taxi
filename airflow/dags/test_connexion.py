from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow import DAG

with DAG(
    dag_id="test_connexion_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    test_requete = SQLExecuteQueryOperator(
        task_id="check_current_role",
        conn_id="snowflake_nyc_taxi",
        sql="SELECT CURRENT_ROLE();",
    )
