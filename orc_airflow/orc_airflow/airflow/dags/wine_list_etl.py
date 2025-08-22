from enum import auto
from airflow.sdk import task, dag
from orc_airflow.definitions import RESOURCES
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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
    def extract_doc_data():
        from orc_airflow.pdf_parser import tabulate_pages, tabulate_rects
        import pdfplumber

        pdf_path = RESOURCES / "bennelong_wine_list.pdf"

        hook = DuckDBHook.get_hook(duckdb_conn_id)
        conn = hook.get_conn()

        logger.info(f"parsing pdf at {pdf_path}..")

        pdf = pdfplumber.open(pdf_path)
        page_range = (5, -1)

        page_slice = slice(page_range[0], page_range[1])

        pages = pdf.pages[page_slice]

        rects = [page.rects for page in pages]

        page_df = tabulate_pages(pages=pages)

        rect_df = tabulate_rects(rects=rects)

        logger.info("returning tables as dfs..")
        query_path = TEMPLATE_SEARCHPATH / "load_wine_list_pages.sql"
        with open(query_path, "r") as f:
            query_string = f.read()
        conn.execute(query_string)

    logger.info("wine_list_etl dag complete.")
    #
    # # cleanup
    # cleanup = SQLExecuteQueryOperator(
    #     task_id="cleanup",
    #     conn_id=duckdb_conn_id,
    #     sql="drop table pagesraw; drop table rect; drop table aggregated; drop table line_numbered_pages; drop table wine_list_staging; show tables;",
    #     show_return_value_in_logs=True,
    # )
    #
    # # exporting the results of the ETL. long term we'll have this
    # # dump directly into the cloud database for user review but during
    # # dev a csv dump is fine.
    #
    import os

    # TODO: add this env var in prod env.
    export_path_dir = Path(os.environ["WINE_LIST_EXP_DIR"])

    export_section_path = export_path_dir / "section.csv"
    export_subsection_path = export_path_dir / "subsection.csv"
    export_subsubsection_path = export_path_dir / "subsubsection.csv"
    export_wine_list_path = export_path_dir / "wine_list.csv"

    # test until label_sections..
    # integrate decompose_line_text

    (
        extract_doc_data()
        >> SQLExecuteQueryOperator(
            task_id="add_line_numbers",
            conn_id=duckdb_conn_id,
            sql="add_line_numbers.sql",
            autocommit=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="aggregate_lines", conn_id=duckdb_conn_id, sql="aggregate_lines.sql"
        )
        >> SQLExecuteQueryOperator(
            task_id="crete_wineListStaging",
            conn_id=duckdb_conn_id,
            sql="create_wine_list_staging.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="label_sections", conn_id=duckdb_conn_id, sql="label_sections.sql"
        )
        >> SQLExecuteQueryOperator(
            task_id="decompose_line_text",
            conn_id=duckdb_conn_id,
            sql="decompose_line_text.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="load_wine_list",
            conn_id=duckdb_conn_id,
            sql="load_wine_list.sql",
            show_return_value_in_logs=True,
        )
        # finer-grained information extraction, region, village, cuvee name, varieties etc.
        >> SQLExecuteQueryOperator(
            task_id="fine_grained_extract",
            conn_id=duckdb_conn_id,
            sql="fine_grained_extract.sql",
            show_return_value_in_logs=True,
        )
        # >> fine_grained_extract
        >> SQLExecuteQueryOperator(
            task_id="prepare_cat_subcat_tables",
            conn_id=duckdb_conn_id,
            sql="prepare_cat_subcat_tbls.sql",
            show_return_value_in_logs=True,
                    #
        # TODO: fix subsbusection missing rows.
        # >> fine_grained_extract
        >> SQLExecuteQueryOperator(
            task_id="prepare_subsubsection",
            conn_id=duckdb_conn_id,
            sql="prepare_subsubsection.sql",
            show_return_value_in_logs=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="export_section",
            conn_id=duckdb_conn_id,
            sql=f"copy section to '{export_section_path}';",
        )
        >> SQLExecuteQueryOperator(
            task_id="export_subsection",
            conn_id=duckdb_conn_id,
            sql=f"copy SubSection to '{export_subsection_path}';",
        )
        >> SQLExecuteQueryOperator(
            task_id="export_subsubsection",
            conn_id=duckdb_conn_id,
            sql=f"copy SubSubSection to '{export_subsubsection_path}';",
        )
        >> SQLExecuteQueryOperator(
            task_id="export_wine_list",
            conn_id=duckdb_conn_id,
            sql=f"copy wine_list to '{export_wine_list_path}';",
        )
        # >> cleanup
    )


dag_wine_list_etl()

if __name__ == "__main__":
    dag_wine_list_etl().test()
