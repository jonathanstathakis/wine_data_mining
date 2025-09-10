from airflow.sdk import task, dag
from orc_airflow.definitions import RESOURCES
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLTableCheckOperator,
)
from textwrap import dedent
import logging
import os
from pathlib import Path

# airflow doesnt give option to set path in config or rel to proj root
TEMPLATE_SEARCHPATH = (
    Path(os.environ.get("AIRFLOW_HOME")) / "include" / "wine_list_etl_tform_col_decomp"
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="wine_list_etl_tform_col_decomp",
    template_searchpath=str(TEMPLATE_SEARCHPATH),
)
def columwise_decomp_etl():
    """ """
    duckdb_conn_id = "wine_list_etl_transform"

    (
        # creates and inserts into a collection of
        # useful fields for downstream processing
        # in the following tasks.
        SQLExecuteQueryOperator(
            task_id="create_insert_extracted_words",
            conn_id=duckdb_conn_id,
            sql="create_insert_extracted_words.sql",
            autocommit=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="decompose_base_pages",
            conn_id=duckdb_conn_id,
            sql="col_decomp_base.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="decompose_page_27_dry_sherry",
            conn_id=duckdb_conn_id,
            sql="col_decomp_page_27_dry_sherry.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="decomp_page_6_7_champagne",
            conn_id=duckdb_conn_id,
            sql="col_decomp_page_6_7_champagne.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="col_decomp_page_7_aus_spk",
            conn_id=duckdb_conn_id,
            sql="col_decomp_page_7_australian_spk.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="col_decomp_page_9_riz",
            conn_id=duckdb_conn_id,
            sql="col_decomp_page_9_riz.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="create_insert_wine",
            conn_id=duckdb_conn_id,
            sql="create_insert_wine.sql",
        )
        >> SQLTableCheckOperator(
            task_id="wine_row_count_check",
            table="wine",
            checks={"row_count_check": {"check_statement": "COUNT(*) > 0"}},
            conn_id=duckdb_conn_id,
        )
        ## cleanup
        >> SQLExecuteQueryOperator(
            task_id="cleanup",
            conn_id=duckdb_conn_id,
            sql="cleanup.sql",
        )
    )  # type: ignore


columwise_decomp_etl()

if __name__ == "__main__":
    columwise_decomp_etl().test()
