import duckdb as db
from wine_list_etl.definitions import BASE_DIR
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


def wine_list_etl(
    conn: db.DuckDBPyConnection, pages_path: str | Path, rects_path: str | Path
):
    """ """

    # load pages raw and rects

    logger.debug("reading 'load_wine_list_pages.sql' query file..")

    with open(Path(BASE_DIR) / "queries" / "load_wine_list_pages.sql", "r") as f:
        load_query = f.read()

    subbed_load_query = load_query.replace("pagesraw_path", str(pages_path)).replace(
        "rect_path", str(rects_path)
    )

    logger.debug("executing query..")
    conn.execute(subbed_load_query)

    logger.debug("reading wine_list_etl.sql query..")

    exc_sql(conn, "add_line_numbers.sql")

    logger.info("finished wine_list_etl!")
