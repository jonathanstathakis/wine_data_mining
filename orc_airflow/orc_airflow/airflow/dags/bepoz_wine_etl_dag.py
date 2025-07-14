from airflow.sdk import task, dag
from orc_airflow.definitions import RESOURCES
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import duckdb as db
import logging
import os
from pathlib import Path

# airflow doesnt give option to set path in config or rel to proj root
TEMPLATE_SEARCHPATH = (
    Path(os.environ.get("AIRFLOW_HOME")) / "include" / "bepoz_wine_etl"
)

logger = logging.getLogger(__name__)


@dag(dag_id="bepoz_wine_etl", template_searchpath=str(TEMPLATE_SEARCHPATH))
def dag_bepoz():
    duckdb_conn_id = "data_mining_db"
    hook = DuckDBHook.get_hook(duckdb_conn_id)

    @task
    def ingest_bepoz():
        # convert to df -> db, probably..
        INPUT_DATAFILE_PATH = (
            TEMPLATE_SEARCHPATH
            / "resources"
            / "Product List_ Jonathan_05Jun2025_234706.csv"
        )
        query_file_path = TEMPLATE_SEARCHPATH / "01_extract_bp_raw.sql"

        with open(query_file_path, "r") as f:
            query_str = f.read()

        query_str = query_str.replace("{INPUT_DATAFILE_PATH}", str(INPUT_DATAFILE_PATH))

        with hook.get_conn() as conn:
            conn.execute(query_str)

    @task
    def download_variety_names():
        query_file_name = "02_download_variety_names.sql"

        query_file_path = TEMPLATE_SEARCHPATH / query_file_name

        with open(query_file_path, "r") as f:
            query_str = f.read()

        with hook.get_conn() as conn:
            conn.execute(query_str)

    @task
    def extract_bepoz_fields():
        query_file_name = "03_extract_bepoz_fields.sql"

        query_file_path = TEMPLATE_SEARCHPATH / query_file_name

        with open(query_file_path, "r") as f:
            query_str = f.read()

        with hook.get_conn() as conn:
            conn.execute(query_str)

    dag = ingest_bepoz() >> download_variety_names() >> extract_bepoz_fields()


dag_bepoz()

if __name__ == "__main__":
    dag_bepoz().test()
