from airflow.sdk import task, dag
import duckdb as db
from orc_airflow.definitions import DB_PATH, RESOURCES
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

import logging

logger = logging.getLogger(__name__)


@dag(dag_id="wine_list_etl")
def dag_wine_list_etl():
    @task
    def run_wine_list_etl():
        duckdb_conn_id = "data_mining_db"
        from wine_list_etl.etl import run_etl

        duckdb_hook = DuckDBHook(duckdb_conn_id=duckdb_conn_id)
        conn = duckdb_hook.get_conn()

        pdf_path = RESOURCES / "bennelong_wine_list.pdf"
        page_range = (6, -1)
        run_etl(conn=conn, pdf_path=pdf_path, page_range=page_range)

    @task
    def load_wine_list():
        """
        load the wine_list table into another table of the same name
        with the merged text as a primary key, a run_id and run_date.
        """
        with db.connect(DB_PATH) as conn:
            query = """
            create table wine_list (
            line_num int,
            page_num int,

            )
            """

    run_wine_list_etl()

    logger.info("wine_list_etl dag complete.")


dag_wine_list_etl()

if __name__ == "__main__":
    dag_wine_list_etl().test()
