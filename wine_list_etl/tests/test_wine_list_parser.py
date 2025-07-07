import pytest
from wine_list_etl.pdf_parser import parse_wine_list
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@pytest.fixture
def pdf_path():
    return "tests/resources/bennelong_wine_list.pdf"


def test_parse_wine_list(pdf_path: str, tmp_path):
    logger.("this is your logger!")

    parse_wine_list(
        pdf_path,
        pages_outpath=Path(tmp_path) / "pages.csv",
        rect_outpath=Path(tmp_path) / "rect.csv",
    )
