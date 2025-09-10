#!/bin/bash
# run wine_list_etl dags in order.

file='/Users/jonathan/jonathan/projects/wine_wiki/wine_data_mining/orc_airflow/orc_airflow/airflow/include/test.csv'

rm $file
# airflow dags test wine_list_etl_extract &&
#   airflow dags test wine_list_etl_tform &&
#   airflow dags test wine_list_etl_tform_section_label &&
#   airflow dags test wine_list_etl_tform_col_decomp &&
airflow dags test wine_list_etl_load &&
  open $file
