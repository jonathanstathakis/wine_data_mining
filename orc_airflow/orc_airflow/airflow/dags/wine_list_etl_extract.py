from airflow.sdk import dag, task
import logging
from orc_airflow.airflow.dags import defs

logger = logging.getLogger(__name__)


@dag(
    dag_id="wine_list_etl_extract",
    template_searchpath=str(defs.INCLUDE / "wine_list_etl_extract"),
)
def wine_list_etl_extract():
    """
    extract the document data from the pdf, outputting as 2 csv files.
    """

    @task
    def extract_doc_data():
        """ """

        # TODO: update logging.
        from orc_airflow.pdf_parser import tabulate_pages, tabulate_rects
        import pdfplumber

        logger.info(f"parsing pdf at {defs.pdf_path}..")

        pdf = pdfplumber.open(defs.pdf_path)
        page_range = (0, -1)

        page_slice = slice(page_range[0], page_range[1])

        pages = pdf.pages[page_slice]
        page_df = tabulate_pages(pages=pages)

        rects = [page.rects for page in pages]
        rect_df = tabulate_rects(rects=rects)

        page_df.to_csv(defs.pages_outpath)
        rect_df.to_csv(defs.rect_outpath)

    extract_doc_data()


wine_list_etl_extract()
