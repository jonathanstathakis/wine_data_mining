from airflow.sdk import task, dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from orc_airflow.airflow.dags import defs

assert_table_exists_query = """
                SELECT CASE 
                    WHEN COUNT(*) = 1 THEN 'ok'
                    ELSE error('Table {{ params.table_name }} does not exist!')
                END
                FROM information_schema.tables
                WHERE table_name='{{ params.table_name }}';
            """


@dag(
    dag_id="wine_list_etl_tform_section_label",
    template_searchpath=str(defs.INCLUDE / "wine_list_etl_tform_section_label"),
)
def wine_etl_transform_section_label():
    """
    organise document sections labelling by line.

    The result is a table sectionPath containing the heirarchy
    of sections of the document (unordered) which is connected
    to pageLine via an fk sectionpath_id.
    """
    duckdb_conn_id = "wine_list_etl_transform"
    # insert section labels
    (
        SQLExecuteQueryOperator(
            task_id="create_schema",
            conn_id=duckdb_conn_id,
            sql="create_schema.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="create_insert_word0",
            conn_id=duckdb_conn_id,
            sql="create_insert_word0.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="insert_section_label",
            conn_id=duckdb_conn_id,
            sql="insert_section_label.sql",
        )
        >> SQLExecuteQueryOperator(
            task_id="insert_section_label_wide",
            conn_id=duckdb_conn_id,
            sql="create_insert_sectionLabelWide.sql",
            autocommit=True,
            split_statements=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="create_insert_allLinesWithSections",
            conn_id=duckdb_conn_id,
            sql="create_insert_allLinesWithSections.sql",
            autocommit=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="create_insert_sectionSubsectionFilled",
            conn_id=duckdb_conn_id,
            sql="create_insert_sectionSubsectionFilled.sql",
            autocommit=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="create_insert_sectionLabelsByLine",
            conn_id=duckdb_conn_id,
            sql="create_insert_sectionLabelsByLine.sql",
            autocommit=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="add_line_type_to_pageline",
            conn_id=duckdb_conn_id,
            sql="insert_line_type_pageline.sql",
            autocommit=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="create_insert_sectionPathLoading",
            conn_id=duckdb_conn_id,
            sql="create_insert_sectionPathLoading.sql",
            autocommit=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="insert_sectionpath",
            conn_id=duckdb_conn_id,
            sql="insert_sectionpath.sql",
            autocommit=True,
        )
        >> SQLExecuteQueryOperator(
            task_id="insert_sectionpathtopageline",
            conn_id=duckdb_conn_id,
            sql="insert_sectionpathtopageline.sql",
            autocommit=True,
        )
        ## delete cross-task dependency tables.
        >> SQLExecuteQueryOperator(
            task_id="cleanup", conn_id=duckdb_conn_id, sql="section_label_cleanup.sql"
        )
        >> SQLExecuteQueryOperator(
            task_id="check_sectionpathtopageline_exists",
            conn_id=duckdb_conn_id,
            sql=assert_table_exists_query,
            params={"table_name": "sectionPathtoPageLine"},
        )
    )


wine_etl_transform_section_label()
