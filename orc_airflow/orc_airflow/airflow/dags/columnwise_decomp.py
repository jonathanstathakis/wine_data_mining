from airflow.sdk import task, dag
from orc_airflow.definitions import RESOURCES
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from textwrap import dedent
import logging
import os
from pathlib import Path

# airflow doesnt give option to set path in config or rel to proj root
TEMPLATE_SEARCHPATH = (
    Path(os.environ.get("AIRFLOW_HOME")) / "include" / "columnwise_decomp"
)

logger = logging.getLogger(__name__)


@dag(dag_id="columnwise_decomp", template_searchpath=str(TEMPLATE_SEARCHPATH))
def columwise_decomp_etl():
    """
    depends on DAG 'extract_wine_list_words' results.

    extract the fields of the wine list columnwise on a
    page by page basis.

    Parametrizing the query allows us to bump the x0 limit
    again on a page by page basis.

    How to parameterize?

    Cant use prepared statements in multi-statement file calls, so need
    to separate. Makes sense I  guess.

    So to have testing on a page by page basis we need to have 1 statement
    to get the rows, and another to complete the extraction.

    It also means we need to know before hand how many tables we want and
    dump all the results into that table.

    Rather than prepared statements the simplest solution is to hard-code
    the page selection and run all pages together. Makes debugging more
    difficult but development much simpler. Then seperate

    so.

    1. dump base page results into staging table.
    2. fine tune cleanup
    3. run bespoke decomposer for 6, 7, 9 (riesling) and 27
    4. combine all.
    """
    duckdb_conn_id = "data_mining_db_test"

    ()  # type: ignore
    SQLExecuteQueryOperator(
        task_id="decompose_page",
        conn_id=duckdb_conn_id,
        sql="base_page_decomp.sql",
        return_last=True,
    )


columwise_decomp_etl()

if __name__ == "__main__":
    columwise_decomp_etl().test()  #
