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


def parse_wine_list(
    pdf_path: str | Path, pages_outpath: str | Path, rect_outpath: str | Path
):
    """
    parse the wine list pdf and return pages and rects objects as csv files.
    """

    logger.info(f"parsing pdf at {Path}..")

    pdf = pdfplumber.open(pdf_path)

    pages = pdf.pages

    rects = pdf.rects

    pages_df = tabulate_pages(pages=pages)

    rect_df = tabulate_rects(rects=rects)

    logger.info(f"writing page data to {pages_outpath}..")

    pages_df.to_csv(pages_outpath)

    logger.info(f"writing rect data to {rect_outpath}..")
    rect_df.to_csv(rect_outpath)

    logger.info("parsing complete!")
