import duckdb as db
from wine_list_etl.definitions import BASE_DIR
from wine_list_etl.pdf_parser import parse_wine_list
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def exc_sql(conn, fname: str | Path):
    """execute sql file in "queries" at fname (can be a rel path from queries"""
    logger.info(f"reading {fname}..")
    with open(Path(BASE_DIR) / "queries" / fname, "r") as f:
        query = f.read()
    logger.info(f"executing {fname}..")
    conn.execute(query)


def load_page_data(
    conn: db.DuckDBPyConnection, pdf_path: str | Path, page_range: tuple[int, int]
):
    """Add pages and rectangle tables to database via dataframe objs.

    Args:
        conn (DuckDBPyConnection): database connection object.
        pdf_path (str|Path): path to the input pdf file.
        page_range (tuple[int, int]): a 2 element tuple representing the page range.
    """
    page_df, rect_df = parse_wine_list(pdf_path, page_range=page_range)

    fname = Path(BASE_DIR) / "queries" / "wine_list_etl" / "load_wine_list_pages.sql"

    with open(fname, "r") as f:
        query = f.read()

    conn.execute(query)


def wine_list_etl(
    conn: db.DuckDBPyConnection, pdf_path: str | Path, page_range: tuple[int, int]
):
    """Execute a ETL pipeline using the queries in queries/wine_list_etl to
    load the database connected with `conn` with a table wine_list representing
    the wine list as parsed from the pdf at `pdf_path`.

    Args:
        conn (DuckDBPyConnection): database connection object.
        pdf_path (str|Path): path to the input pdf file.
        page_range (tuple[int, int]): a 2 element tuple representing the page range.
    """

    # load pages raw and rects

    load_page_data(conn=conn, pdf_path=pdf_path, page_range=page_range)
    exc_sql(conn, "wine_list_etl/add_line_numbers.sql")
    exc_sql(conn, "wine_list_etl/aggregate_lines.sql")
    exc_sql(conn, "wine_list_etl/label_sections.sql")
    exc_sql(conn, "wine_list_etl/decompose_line_text.sql")

    breakpoint()

    logger.info("finished wine_list_etl!")
