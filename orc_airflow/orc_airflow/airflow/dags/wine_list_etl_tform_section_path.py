from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from orc_airflow.airflow.dags import defs


@dag(
    dag_id="wine_list_etl_tform_section_path",
    template_searchpath=str(defs.INCLUDE / "wine_list_etl_tform_section_path"),
)
def wine_etl_transform_section_path():
    """
    create section path and add to pageline
    """
    duckdb_conn_id = "wine_list_etl_transform"
    (
        SQLExecuteQueryOperator(
            task_id="create_sectionPath_dim_tbl_insert_into_pageline",
            conn_id=duckdb_conn_id,
            sql="create_insert_section_path.sql",
            autocommit=True,
        )
    )


wine_etl_transform_section_path()
