from operator import add
from airflow.sdk import task, dag
import duckdb as db
from orc_airflow.definitions import DB_PATH, RESOURCES
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.sdk import Variable
import logging
import os
from pathlib import Path

# airflow doesnt give option to set path in config or rel to proj root
TEMPLATE_SEARCHPATH = Path(os.environ.get("AIRFLOW_HOME")) / "include" / "wine_list_etl"

logger = logging.getLogger(__name__)


@dag(dag_id="wine_list_etl", template_searchpath=str(TEMPLATE_SEARCHPATH))
def dag_wine_list_etl():
    duckdb_conn_id = "data_mining_db"

    @task
    def run_wine_list_etl():
        from wine_list_etl.etl import run_etl

        hook = DuckDBHook.get_hook(duckdb_conn_id)
        conn = hook.get_conn()
        pdf_path = RESOURCES / "bennelong_wine_list.pdf"
        page_range = (5, -1)
        run_etl(conn=conn, pdf_path=pdf_path, page_range=page_range)

    run_wine_list_etl()

    @task
    def extract_doc_data():
        from orc_airflow.pdf_parser import tabulate_pages, tabulate_rects
        import pdfplumber

        pdf_path = RESOURCES / "bennelong_wine_list.pdf"

        hook = DuckDBHook.get_hook(duckdb_conn_id)
        conn = hook.get_conn()

        logger.info(f"parsing pdf at {pdf_path}..")

        pdf = pdfplumber.open(pdf_path)
        page_range = (5, -1)

        pages = pdf.pages[slice(page_range[0], page_range[1])]

        rects = pdf.rects[slice(page_range[0], page_range[1])]

        page_df = tabulate_pages(pages=pages)

        rect_df = tabulate_rects(rects=rects)

        logger.info("returning tables as dfs..")
        query_path = TEMPLATE_SEARCHPATH / "load_wine_list_pages.sql"
        with open(query_path, "r") as f:
            query_string = f.read()
        conn.execute(query_string)

    extract_doc_data = extract_doc_data()

    logger.info("wine_list_etl dag complete.")

    # load page data
    add_line_numbers = SQLExecuteQueryOperator(
        task_id="add_line_numbers", conn_id=duckdb_conn_id, sql="add_line_numbers.sql"
    )

    # aggregate lines
    aggregate_lines = SQLExecuteQueryOperator(
        task_id="aggregate_lines", conn_id=duckdb_conn_id, sql="aggregate_lines.sql"
    )

    # label sections
    label_sections = SQLExecuteQueryOperator(
        task_id="label_sections", conn_id=duckdb_conn_id, sql="label_sections.sql"
    )

    # decompose line text
    decompose_line_text = SQLExecuteQueryOperator(
        task_id="decompose_line_text",
        conn_id=duckdb_conn_id,
        sql="decompose_line_text.sql",
    )

    # cleanup
    cleanup = SQLExecuteQueryOperator(
        task_id="cleanup",
        conn_id=duckdb_conn_id,
        sql="drop table pagesraw; drop table rect; show tables;",
        show_return_value_in_logs=True,
    )

    (
        extract_doc_data
        >> add_line_numbers
        >> aggregate_lines
        >> label_sections
        >> decompose_line_text
        >> cleanup
    )


dag_wine_list_etl()

if __name__ == "__main__":
    dag_wine_list_etl().test()
