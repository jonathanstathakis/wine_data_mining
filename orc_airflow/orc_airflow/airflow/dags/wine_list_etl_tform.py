from airflow.sdk import dag, task, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging
from orc_airflow.airflow.dags import defs

logger = logging.getLogger(__name__)


@dag(
    dag_id="wine_list_etl_tform",
    template_searchpath=str(defs.INCLUDE / "wine_list_etl_tform"),
)
def wine_list_etl_transform():
    """ """
    duckdb_conn_id = "wine_list_etl_transform"

    @task_group(group_id="pageLine")
    def task_group_pageline():
        (
            SQLExecuteQueryOperator(
                task_id="insert_pageline",
                conn_id=duckdb_conn_id,
                sql="insert_pageline.sql",
                autocommit=True,
            )
            >> SQLExecuteQueryOperator(
                task_id="update_rawtext_line_id",
                conn_id=duckdb_conn_id,
                sql="update_rawtext_line_id.sql",
            )
            # insert full line text into pageline
            >> SQLExecuteQueryOperator(
                task_id="create_insert_lineText",
                conn_id=duckdb_conn_id,
                sql="create_insert_lineText.sql",
                autocommit=True,
            )
            >> SQLExecuteQueryOperator(
                task_id="cleanup",
                conn_id=duckdb_conn_id,
                sql="pageline_cleanup.sql",
            )
        )

    @task_group(group_id="load_data")
    def task_group_load_data():
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
                task_id="create_insert_run",
                conn_id=duckdb_conn_id,
                sql="create_insert_run.sql",
            )
            >> SQLExecuteQueryOperator(
                task_id="create_insert_doc",
                conn_id=duckdb_conn_id,
                sql="create_insert_doc.sql",
                params={"pdf_path": str(defs.pdf_path)},
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
            # add parsed rectangle data to db
            >> SQLExecuteQueryOperator(
                task_id="insert_rect_csv",
                conn_id=duckdb_conn_id,
                sql="insert_rect_csv.sql",
                params={"rect_csv_path": str(defs.rect_outpath)},
            )
        )

    (
        task_group_load_data()
        # create page index table.
        >> SQLExecuteQueryOperator(
            task_id="insert_page",
            conn_id=duckdb_conn_id,
            sql="insert_page.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="create_insert_rawTextLoading",
            conn_id=duckdb_conn_id,
            sql="create_insert_rawTextLoading.sql",
        )
        # load rectangle table
        >> SQLExecuteQueryOperator(
            task_id="insert_rectangle",
            conn_id=duckdb_conn_id,
            sql="insert_rectangle.sql",
        )
        >> task_group_pageline()
        >> SQLExecuteQueryOperator(
            task_id="insert_rawText", conn_id=duckdb_conn_id, sql="insert_rawText.sql"
        )
        # Extract and insert document publication date
        >> SQLExecuteQueryOperator(
            task_id="create_insert_pub_date",
            conn_id=duckdb_conn_id,
            sql="create_insert_pub_date.sql",
            autocommit=True,
        )
    )


wine_list_etl_transform()

# if __name__ == "__main__":
#     wine_list_etl_transform.
