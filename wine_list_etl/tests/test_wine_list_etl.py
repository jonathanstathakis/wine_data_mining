import pytest
import duckdb as db
from wine_list_etl.wine_list_etl import wine_list_etl


@pytest.fixture
def db_path():
    return ""


@pytest.fixture
def pdf_path():
    return "tests/resources/bennelong_wine_list.pdf"


@pytest.fixture
def page_range():
    return (6, -1)


@pytest.fixture
def conn(db_path: str) -> db.DuckDBPyConnection:
    return db.connect(db_path)


def test_wine_list_etl(
    conn: db.DuckDBPyConnection, pdf_path: str, page_range: tuple[int, int]
):
    """
    test execution of wine_list_etl
    """
    wine_list_etl(conn=conn, pdf_path=pdf_path, page_range=page_range)
