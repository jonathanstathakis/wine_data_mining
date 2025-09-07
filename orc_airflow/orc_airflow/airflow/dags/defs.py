import os
from pathlib import Path
from orc_airflow.definitions import RESOURCES

# airflow doesnt give option to set path in config or rel to proj root
INCLUDE = Path(os.environ.get("AIRFLOW_HOME", "")) / "include"


pages_outpath = INCLUDE / "pages_df.csv"
rect_outpath = INCLUDE / "rect_df.csv"
pdf_path = RESOURCES / "bennelong_wine_list.pdf"
