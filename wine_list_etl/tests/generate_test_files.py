from pathlib import Path
from wine_list_etl.pdf_parser import parse_wine_list
import logging

logging.basicConfig()
logging.root.setLevel("INFO")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent


if __name__ == "__main__":
    parse_wine_list(
        pdf_path=BASE_DIR / "resources/bennelong_wine_list.pdf",
        pages_outpath=BASE_DIR / "datasets/pages.csv",
        rect_outpath=BASE_DIR / "datasets/rects.csv",
    )
