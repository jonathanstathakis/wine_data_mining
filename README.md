# README

ETL Pipeline tool for the ingestion of Bennelong wine list into a database format for upload to a related webapp.

After installation activate venv and use Airflow. Dags are `wine_list_etl`, `bepoz_wine_etl`, and `join_wines`. `join_wines` however is only a prototype and a non-functional prototype at that as as currently the difference in the text between the extracted wine list data and bepoz data is too great.

Includes an example website scraping tool for gathering wine product data.

## Notes

### Design

ETL transformation tables should not have foreign key tables and should not track information over multiple runs i.e. run_id. that is for the load tables, not transformation. This simplifies the ETL and saves any error checking for Load. Of course the result is that mid-pipeline errors are not caught.
