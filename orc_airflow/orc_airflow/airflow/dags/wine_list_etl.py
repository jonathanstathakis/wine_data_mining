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


@dag(dag_id="wine_list_etl", template_searchpath=str(TEMPLATE_SEARCHPATH))
def dag_wine_list_etl():
    """
    Need documentation.

    1. extracts document data
    2. adds line numbers
    3. aggregates lines together
    4. creates a wine list staging table
    5. labels the sections
    6. decomposes the line text
    7. loads the wine list
    8. performs a fine grained text extraction
    9. creates category and subcategory tables
    10. exports section, subsection and wine list tables
    11. cleans up database.

    - should be able to move the export steps to a dependent DAG.

    The result is a database at duckdb_conn_id with
    a number of tables:

    Section:
        the sections of the wine list.

        section: names of each section.
        section_order: the linear order of the section as an integer.

    SubSection:
        the subsections of the wine list.

        section (fk): the Section.section the subsection row belongs to.
        subsection (pk): the subsection label.
        subsection_order: the absolute linear order of the subsetion irrespective of section.

    SubSubSection:
        the subsubsections of the wine list, in a heirarchy below Section and Subsection.

        Note: currently empty.

    WINELISTSTAGING:
        LINE_NUM_TOT:
        PAGE_NUM:
        LINE_NUM:
        SECTION:
        SUBSECTION:
        SUBSUBSECTION:
        MERGED_TEXT:
        VINTAGE:
        BASE_YEAR:
        CUVEE_NAME:
        DISGORG_YEAR:
        PRICE:
        MERGED_TEXT_EXT:

    aggregated:

    classRef:
        name:

    communeRef:
        name:

    countryRef:
        name:

    cuveeRef:
        name:

    decomposeMergeText:
        LINE_NUM_TOT:
        vintage:
        base_year:
        cuvee_name:
        disgorg_year:
        price:
        merged_text_ext:

    drynessRef:
        descriptor:

    line_numbered_pages:

    pagesraw:

    producerRef:
        name:

    regionRef:
        name:

    seriesRef:
        name:

    stateRef:
        name

    styleRef:
        name:

    subregionRef:
        name:

    varietyRef:
        name:

    vineyardRef:
        name:

    volumeRef:
        name:

    wine_list:
        pk
        line_num_tot
        page_num
        page_line_num
        section
        subsection
        subsubsection
        vintage
        merged_text_ext
        base_year
        cuvee_name
        disgorg_year
        price
        merged_text
        producer
        dryness
        country
        state
        region
        subregion
        commune
        vineyard
        style
        classification
        volume
        series
        variety

    The data lineage is as follows:

    1. tables pagesraw and rect added to db.
    2. add_line_numbers: identifies lines as groups of words within 4 units vertically and assigns line numbers as such.
    3. aggregate_lines: create merged strings and through all other data into a json field.

    """
    duckdb_conn_id = "data_mining_db_test"

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

    extract_doc_data = extract_doc_data()

    logger.info("wine_list_etl dag complete.")

    # load page data
    add_line_numbers = SQLExecuteQueryOperator(
        task_id="add_line_numbers",
        conn_id=duckdb_conn_id,
        sql="add_line_numbers.sql",
        autocommit=True,
    )

    # aggregate lines
    aggregate_lines = SQLExecuteQueryOperator(
        task_id="aggregate_lines", conn_id=duckdb_conn_id, sql="aggregate_lines.sql"
    )

    # label sections
    create_insert_wine_list_staging_label_sections = SQLExecuteQueryOperator(
        task_id="label_sections", conn_id=duckdb_conn_id, sql="label_sections.sql"
    )

    # decompose line text
    decompose_line_text = SQLExecuteQueryOperator(
        task_id="decompose_line_text",
        conn_id=duckdb_conn_id,
        sql="decompose_line_text.sql",
    )

    # load the wine_list table.
    load_wine_list = SQLExecuteQueryOperator(
        task_id="load_wine_list",
        conn_id=duckdb_conn_id,
        sql="load_wine_list.sql",
    )

    prepare_cat_subcat_tables = SQLExecuteQueryOperator(
        task_id="prepare_cat_subcat_tables",
        conn_id=duckdb_conn_id,
        sql="prepare_cat_subcat_tbls.sql",
    )

    # exporting the results of the ETL. long term we'll have this
    # dump directly into the cloud database for user review but during
    # dev a csv dump is fine.

    export_path_dir = Path("/Users/jonathan/jonathan/projects/wine_wiki/wine_list_db")
    export_section_path = export_path_dir / "section.csv"
    export_subsection_path = export_path_dir / "subsection.csv"
    export_wine_list_path = export_path_dir / "wine_list.csv"

    # export section
    export_section = SQLExecuteQueryOperator(
        task_id="export_section",
        conn_id=duckdb_conn_id,
        sql=f"copy section to '{export_section_path}';",
    )

    # export subsection
    export_subsection = SQLExecuteQueryOperator(
        task_id="export_subsection",
        conn_id=duckdb_conn_id,
        sql=f"copy SubSection to '{export_subsection_path}';",
    )

    # export wines
    export_wine_list = SQLExecuteQueryOperator(
        task_id="export_wine_list",
        conn_id=duckdb_conn_id,
        sql=f"copy wine_list to '{export_wine_list_path}';",
    )

    # test until label_sections..
    (
        extract_doc_data
        >> add_line_numbers
        >> aggregate_lines
        >> create_insert_wine_list_staging_label_sections
        >> decompose_line_text
        >> load_wine_list
        # finer-grained information extraction, region, village, cuvee name, varieties etc.
        >> SQLExecuteQueryOperator(
            task_id="fine_grained_extract",
            conn_id=duckdb_conn_id,
            sql="fine_grained_extract.sql",
        )
        >> prepare_cat_subcat_tables
        >> export_section
        >> export_subsection
        >> export_wine_list
    )  # type: ignore


dag_wine_list_etl()

if __name__ == "__main__":
    dag_wine_list_etl().test()  # type: ignore
