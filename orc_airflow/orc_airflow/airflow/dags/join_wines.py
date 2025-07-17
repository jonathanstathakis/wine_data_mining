from airflow.sdk import task, dag
import pandas as pd
import duckdb as db
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
import logging
import os
from pathlib import Path

# airflow doesnt give option to set path in config or rel to proj root
TEMPLATE_SEARCHPATH = Path(os.environ.get("AIRFLOW_HOME")) / "include" / "join_wines"

logger = logging.getLogger(__name__)


def query_bepoz_wine(conn: db.DuckDBPyConnection, wine: str):
    """
    find the entry in bepoz corresponding to wine
    """
    query_path = TEMPLATE_SEARCHPATH / "fts_bp_raw_loading.sql"
    with open(query_path, "r") as f:
        query_str = f.read()

    match = conn.execute(query_str, [wine]).df()
    return match


@dag(dag_id="join_wines", template_searchpath=str(TEMPLATE_SEARCHPATH))
def join_wines():
    duckdb_conn_id = "data_mining_db"
    hook = DuckDBHook.get_hook(duckdb_conn_id)

    @task
    def get_wine_list_df():
        """
        get wine list as a df
        """
        with hook.get_conn() as conn:
            df = conn.execute("select * from wine_list").df()
        return df

    @task
    def merge_wines(wine_df: pd.DataFrame):
        """
        for each wine in wine list find a corresponding entry in bepoz
        """

        match_dfs = []
        with hook.get_conn() as conn:
            for wine in wine_df["merged_text"].str.lower():
                match_dfs.append(query_bepoz_wine(conn, wine))

        matches = pd.concat(match_dfs)
        return matches

    @task
    def matches_to_csv(match_df: pd.DataFrame):
        path = TEMPLATE_SEARCHPATH / "matches.csv"
        match_df.to_csv(path)

    df = get_wine_list_df()
    matches = merge_wines(df)
    matches_to_csv(matches)


join_wines()

if __name__ == "__main__":
    join_wines().test()
