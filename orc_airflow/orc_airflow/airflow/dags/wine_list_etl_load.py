"""
write the load DAG, which in this context creates a denormalised table collecting the normalised results of the
tform DAGs and outputs a CSV file (for now).
"""

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
    SQLTableCheckOperator,
)
import logging
from orc_airflow.airflow.dags import defs

# airflow doesnt give option to set path in config or rel to proj root
logger = logging.getLogger(__name__)


@dag(
    dag_id="wine_list_etl_load",
    template_searchpath=str(defs.INCLUDE / "wine_list_etl_load"),
)
def wine_list_etl_load():
    """ """
    duckdb_conn_id = "wine_list_etl_transform"

    (
        SQLExecuteQueryOperator(
            task_id="create_insert_wine_list_wine",
            conn_id=duckdb_conn_id,
            sql="create_insert_wine_list_wine.sql",
        )
        >> SQLTableCheckOperator(
            task_id="wine_list_wine_rowcount_check",
            conn_id=duckdb_conn_id,
            table="wine_list_wine",
            checks={"row_count_check": {"check_statement": "count (*) > 0"}},
        )
        >> SQLExecuteQueryOperator(
            task_id="output_wine_list_wine_to_csv",
            conn_id=duckdb_conn_id,
            sql="output_wine_list_wine_to_csv.sql",
            params={"wine_list_wine_outpath": str(defs.INCLUDE / "test.csv")},
        )
    )


wine_list_etl_load()
