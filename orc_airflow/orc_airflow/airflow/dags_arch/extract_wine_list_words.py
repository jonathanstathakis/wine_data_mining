from enum import auto
from airflow.sdk import task, dag
from orc_airflow.definitions import RESOURCES
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from textwrap import dedent
import logging
import os
from pathlib import Path

# airflow doesnt give option to set path in config or rel to proj root
TEMPLATE_SEARCHPATH = Path(os.environ.get("AIRFLOW_HOME")) / "include" / "wine_list_etl"

logger = logging.getLogger(__name__)


@dag(dag_id="extract_wine_list_words", template_searchpath=str(TEMPLATE_SEARCHPATH))
def extract_line_list_words():
    """ """
    duckdb_conn_id = "test_pub_date"

    # add meta data such as data paths.

    SQLExecuteQueryOperator(
        task_id="setup_env", conn_id=duckdb_conn_id, sql="setup_env.sql"
    )

    # establish current run id
    SQLExecuteQueryOperator(
        task_id="create_run_id",
        conn_id=duckdb_conn_id,
        sql="create_insert_runid",
    )

    page_df_outpath = Path(TEMPLATE_SEARCHPATH) / "page_df.csv"
    rect_df_outpath = Path(TEMPLATE_SEARCHPATH) / "rect_df.csv"

    @task
    def extract_doc_data():
        from orc_airflow.pdf_parser import tabulate_pages, tabulate_rects
        import pdfplumber

        pdf_path = RESOURCES / "bennelong_wine_list.pdf"

        hook = DuckDBHook.get_hook(duckdb_conn_id)
        conn = hook.get_conn()

        logger.info(f"parsing pdf at {pdf_path}..")

        pdf = pdfplumber.open(pdf_path)
        page_range = (0, -1)

        page_slice = slice(page_range[0], page_range[1])

        pages = pdf.pages[page_slice]

        rects = [page.rects for page in pages]

        page_df = tabulate_pages(pages=pages)
        rect_df = tabulate_rects(rects=rects)

        page_df.to_csv(page_df_outpath)
        rect_df.to_csv(rect_df_outpath)

        # execute the query within this scope to take advantage of
        # duckdb's ability to read in-memory dfs.
        # logger.info("returning tables as dfs..")
        # query_path = TEMPLATE_SEARCHPATH / "load_wine_list_pages.sql"
        # with open(query_path, "r") as f:
        #     query_string = f.read()
        conn.execute(query_string)

    extract_doc_data = extract_doc_data()

    # establish a run id based on the parsed publication date.
    # first get publication date then upsert pub_date.
    # but then how do we get the correct pubdate from pub_date for
    # the parse.
    # currentRunDate.pub_date is the current run's parsed publication
    # date.

    # get publication date.
    SQLExecuteQueryOperator(
        task_id="insert_pub_date",
        conn_id=duckdb_conn_id,
        sql="insert_pub_date.sql",
    )

    """
    Am converting the etl to a snowflake schema with the run
    as the central fact table.

    currently need to do the following:

    - [ ] add create_input_pages.sql to ETL
    - [ ] populate and add create_insert_input_rects to ETL
    - [ ] convert rest of etl to schema.
    """

    # # add pub_date.id as foreign key to pagesRaw.
    # SQLExecuteQueryOperator(task_id="add_pub_date_to_raw_pages.sql")

    # load page data
    add_line_numbers = SQLExecuteQueryOperator(
        task_id="add_line_numbers",
        conn_id=duckdb_conn_id,
        sql="insert_page_lines.sql",
        autocommit=True,
    )

    # aggregate lines
    aggregate_lines = SQLExecuteQueryOperator(
        task_id="aggregate_lines", conn_id=duckdb_conn_id, sql="aggregate_lines.sql"
    )

    # label sections
    create_insert_wineListLines_label_sections = SQLExecuteQueryOperator(
        task_id="label_sections", conn_id=duckdb_conn_id, sql="label_sections.sql"
    )

    # test until label_sections..
    (
        extract_doc_data
        >> add_line_numbers
        >> aggregate_lines
        >> create_insert_wineListLines_label_sections
    )  # type: ignore

    # TODO: delete currentRunDate at the end of the run.


extract_line_list_words()

if __name__ == "__main__":
    extract_line_list_words().test()  # type: ignore
