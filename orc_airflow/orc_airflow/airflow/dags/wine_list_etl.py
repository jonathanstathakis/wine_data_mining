from enum import auto
from airflow.sdk import task, dag
from orc_airflow.definitions import RESOURCES
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from textwrap import dedent
import logging
import os
from pathlib import Path

import logging

logging.disable(logging.WARNING)
# airflow doesnt give option to set path in config or rel to proj root
INCLUDE = Path(os.environ.get("AIRFLOW_HOME")) / "include"

logger = logging.getLogger(__name__)

pages_outpath = INCLUDE / "pages_df.csv"
rect_outpath = INCLUDE / "rect_df.csv"
pdf_path = RESOURCES / "bennelong_wine_list.pdf"


@dag(
    dag_id="wine_list_etl_extract",
    template_searchpath=str(INCLUDE / "wine_list_etl_extract"),
)
def wine_list_etl_extract():
    """
    extract the document data from the pdf, outputting as 2 csv files.
    """

    duckdb_conn_id = "data_mining_db_test"

    @task
    def extract_doc_data():
        """ """

        # TODO: update logging.
        from orc_airflow.pdf_parser import tabulate_pages, tabulate_rects
        import pdfplumber

        logger.info(f"parsing pdf at {pdf_path}..")

        pdf = pdfplumber.open(pdf_path)
        page_range = (5, -1)

        page_slice = slice(page_range[0], page_range[1])

        pages = pdf.pages[page_slice]
        page_df = tabulate_pages(pages=pages)

        rects = [page.rects for page in pages]
        rect_df = tabulate_rects(rects=rects)

        page_df.to_csv(pages_outpath)
        rect_df.to_csv(rect_outpath)

    extract_doc_data()


@dag(
    dag_id="wine_list_etl_transform",
    template_searchpath=str(INCLUDE / "wine_list_etl_transform"),
)
def dag_wine_list_etl_transform():
    """ """
    duckdb_conn_id = "wine_list_etl_transform"

    (
        SQLExecuteQueryOperator(
            task_id="setup_env",
            conn_id=duckdb_conn_id,
            sql="setup_env.sql",
            params={"TEMPLATE_SEARCHPATH": str(INCLUDE / "wine_list_etl_transform")},
        )
        >> SQLExecuteQueryOperator(
            task_id="create_schema", conn_id=duckdb_conn_id, sql="create_schema.sql"
        )
        >> SQLExecuteQueryOperator(
            task_id="insert_run_id",
            conn_id=duckdb_conn_id,
            sql="insert_runid.sql",
            show_return_value_in_logs=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="insert_document",
            conn_id=duckdb_conn_id,
            sql="insert_document.sql",
            params={"pdf_path": str(pdf_path)},
        )
        # load page text csv into database
        >> SQLExecuteQueryOperator(
            task_id="insert_page_csv",
            conn_id=duckdb_conn_id,
            sql="insert_page_csv.sql",
            params={"pages_csv_path": str(pages_outpath)},
        )
        # create page index table.
        >> SQLExecuteQueryOperator(
            task_id="insert_page",
            conn_id=duckdb_conn_id,
            sql="insert_page.sql",
        )
        # add raw text as a child table of page.
        >> SQLExecuteQueryOperator(
            task_id="insert_raw_text",
            conn_id=duckdb_conn_id,
            sql="insert_raw_text.sql",
        )
        # add parsed rectangle data to db
        >> SQLExecuteQueryOperator(
            task_id="insert_rect_csv",
            conn_id=duckdb_conn_id,
            sql="insert_rect_csv.sql",
            params={"rect_csv_path": str(rect_outpath)},
        )
        # load rectangle table
        >> SQLExecuteQueryOperator(
            task_id="insert_rectangle",
            conn_id=duckdb_conn_id,
            sql="insert_rectangle.sql",
        )
        # identify and label lines.
        >> SQLExecuteQueryOperator(
            task_id="insert_pageline_link_rawtext",
            conn_id=duckdb_conn_id,
            sql="insert_pageline_link_rawtext.sql",
            autocommit=True,
        )
        # insert full line text into pageline
        >> SQLExecuteQueryOperator(
            task_id="insert_full_pageline_text",
            conn_id=duckdb_conn_id,
            sql="insert_pageline_full_text.sql",
            autocommit=True,
        )
        # # insert section labels
        # >> SQLExecuteQueryOperator(
        #     task_id="insert_section_label",
        #     conn_id=duckdb_conn_id,
        #     sql="insert_section_label.sql",
        # )
    )

    # aggregate_lines = SQLExecuteQueryOperator(
    #     task_id="aggregate_lines", conn_id=duckdb_conn_id, sql="aggregate_lines.sql"
    # )
    #
    # # label sections
    #
    # # decompose line text
    # decompose_line_text = SQLExecuteQueryOperator(
    #     task_id="decompose_line_text",
    #     conn_id=duckdb_conn_id,
    #     sql="decompose_line_text.sql",
    # )
    #
    # # load the wine_list table.
    # load_wine_list = SQLExecuteQueryOperator(
    #     task_id="load_wine_list",
    #     conn_id=duckdb_conn_id,
    #     sql="load_wine_list.sql",
    # )
    #
    # prepare_cat_subcat_tables = SQLExecuteQueryOperator(
    #     task_id="prepare_cat_subcat_tables",
    #     conn_id=duckdb_conn_id,
    #     sql="prepare_cat_subcat_tbls.sql",
    # )
    #
    # # exporting the results of the ETL. long term we'll have this
    # # dump directly into the cloud database for user review but during
    # # dev a csv dump is fine.
    #
    # export_path_dir = Path("/Users/jonathan/jonathan/projects/wine_wiki/wine_list_db")
    # export_section_path = export_path_dir / "section.csv"
    # export_subsection_path = export_path_dir / "subsection.csv"
    # export_wine_list_path = export_path_dir / "wine_list.csv"
    #
    # # export section
    # export_section = SQLExecuteQueryOperator(
    #     task_id="export_section",
    #     conn_id=duckdb_conn_id,
    #     sql=f"copy section to '{export_section_path}';",
    # )
    #
    # # export subsection
    # export_subsection = SQLExecuteQueryOperator(
    #     task_id="export_subsection",
    #     conn_id=duckdb_conn_id,
    #     sql=f"copy SubSection to '{export_subsection_path}';",
    # )
    #
    # # export wines
    # export_wine_list = SQLExecuteQueryOperator(
    #     task_id="export_wine_list",
    #     conn_id=duckdb_conn_id,
    #     sql=f"copy wine_list to '{export_wine_list_path}';",
    # )
    #
    # # test until label_sections..
    # (
    #     extract_doc_data
    #     >> add_line_numbers
    #     >> aggregate_lines
    #     >> create_insert_wineListLines_label_sections
    #     >> decompose_line_text
    #     >> load_wine_list
    #     # finer-grained information extraction, region, village, cuvee name, varieties etc.
    #     >> SQLExecuteQueryOperator(
    #         task_id="fine_grained_extract",
    #         conn_id=duckdb_conn_id,
    #         sql="fine_grained_extract.sql",
    #     )
    #     >> prepare_cat_subcat_tables
    #     >> export_section
    #     >> export_subsection
    #     >> export_wine_list
    # )  # type: ignore


wine_list_etl_extract()
dag_wine_list_etl_transform()

if __name__ == "__main__":
    dr = dag_wine_list_etl_transform().test()  # a single DagRun object
