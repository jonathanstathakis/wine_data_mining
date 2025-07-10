import pdfplumber
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def tabulate_rects(rects: list):
    """
    convert list of rectangle data to a tabular format
    """

    columns = [
        "x0",
        "y0",
        "x1",
        "y1",
        "bottom",
        "top",
        "width",
        "height",
        "pts",
        "linewidth",
        "page_number",
    ]

    logger.info("extracting rectangles..")
    fields = [[rect[col] for col in columns] for rect in rects]
    logger.info("tabulating rectangles..")
    df = pd.DataFrame(fields, columns=columns)
    return df


def tabulate_pages(pages: list):
    page_dfs = []

    logger.info("extracting words..")
    for page in pages:
        words = page.extract_words(
            extra_attrs=["fontname", "size", "bottom", "page_number"]
        )
        df = pd.DataFrame(words)
        page_dfs.append(df)

    logger.info("tabulating page words..")
    pages_df = pd.concat(page_dfs)
    return pages_df


def parse_wine_list(pdf_path: str | Path, page_range: tuple[int, int]):
    """
    parse the wine list pdf and return pages and rects objects as csv files.
    """

    logger.info(f"parsing pdf at {pdf_path}..")

    pdf = pdfplumber.open(pdf_path)

    pages = pdf.pages[slice(page_range[0], page_range[1])]

    rects = pdf.rects[slice(page_range[0], page_range[1])]

    pages_df = tabulate_pages(pages=pages)

    rect_df = tabulate_rects(rects=rects)

    logger.info("returning tables as dfs..")

    return pages_df, rect_df
