from airflow.sdk import task, dag
import duckdb as db
from orc_airflow.definitions import DB_PATH, RESOURCES

import logging

logger = logging.getLogger(__name__)


@dag(dag_id="wine_list_etl")
def dag_wine_list_etl():
    @task
    def run_wine_list_etl():
        from wine_list_etl.etl import run_etl

        pdf_path = RESOURCES / "bennelong_wine_list.pdf"
        page_range = (6, -1)
        with db.connect(DB_PATH) as conn:
            run_etl(conn=conn, pdf_path=pdf_path, page_range=page_range)

    run_wine_list_etl()

    logger.info("wine_list_etl dag complete.")


dag_wine_list_etl()

if __name__ == "__main__":
    dag_wine_list_etl().test()
