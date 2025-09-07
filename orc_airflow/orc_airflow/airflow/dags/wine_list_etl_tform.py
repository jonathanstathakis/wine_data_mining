from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging
from orc_airflow.airflow.dags import defs

# TODO: check that dags work now have refactored.
# airflow doesnt give option to set path in config or rel to proj root
logger = logging.getLogger(__name__)


@dag(
    dag_id="wine_list_etl_tform",
    template_searchpath=str(defs.INCLUDE / "wine_list_etl_tform"),
)
def wine_list_etl_transform():
    """ """
    duckdb_conn_id = "wine_list_etl_transform"

    @task
    def delete_old_db():
        """
        check if db file exists, if so delete.
        long term viablity of this approach is questionable
        but essentially this db should only exist for 1 run
        and rather than manage the storage of multiple runs
        it is simpler to delete the file on execution of a
        new run.
        The alternative is tracking the parameter inputs of
        the extract dag PDF extraction or set a age limit
        for previous runs, etc. All too complicated for
        the project at this stage.
        """
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(duckdb_conn_id)
        from pathlib import Path

        if conn.host:
            Path(conn.host).unlink(missing_ok=True)

    (
        delete_old_db()
        >> SQLExecuteQueryOperator(
            task_id="setup_env",
            conn_id=duckdb_conn_id,
            sql="setup_env.sql",
            params={
                "TEMPLATE_SEARCHPATH": str(defs.INCLUDE / "wine_list_etl_transform")
            },
        )
        >> SQLExecuteQueryOperator(
            task_id="create_schema",
            conn_id=duckdb_conn_id,
            sql="create_tform_schema.sql",
        )
        # load page text csv into database
        >> SQLExecuteQueryOperator(
            task_id="insert_page_csv",
            conn_id=duckdb_conn_id,
            sql="insert_page_csv.sql",
            params={"pages_csv_path": str(defs.pages_outpath)},
        )
        # create page index table.
        >> SQLExecuteQueryOperator(
            task_id="insert_page",
            conn_id=duckdb_conn_id,
            sql="insert_page.sql",
        )
        # add raw text as a child table of page.
        >> SQLExecuteQueryOperator(
            task_id="insert_raw_text",
            conn_id=duckdb_conn_id,
            sql="insert_raw_text.sql",
        )
        # add parsed rectangle data to db
        >> SQLExecuteQueryOperator(
            task_id="insert_rect_csv",
            conn_id=duckdb_conn_id,
            sql="insert_rect_csv.sql",
            params={"rect_csv_path": str(defs.rect_outpath)},
        )
        # load rectangle table
        >> SQLExecuteQueryOperator(
            task_id="insert_rectangle",
            conn_id=duckdb_conn_id,
            sql="insert_rectangle.sql",
        )
        # identify and label lines.
        >> SQLExecuteQueryOperator(
            task_id="insert_pageline_link_rawtext",
            conn_id=duckdb_conn_id,
            sql="insert_pageline_link_rawtext.sql",
            autocommit=True,
        )
        # insert full line text into pageline
        >> SQLExecuteQueryOperator(
            task_id="insert_full_pageline_text",
            conn_id=duckdb_conn_id,
            sql="insert_pageline_full_text.sql",
            autocommit=True,
        )
    )


wine_list_etl_transform()
