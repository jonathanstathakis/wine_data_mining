# README

ETL Pipeline tool for the ingestion of Bennelong wine list into a database format for upload to a related webapp.

After installation activate venv and use Airflow. Dags are `wine_list_etl`, `bepoz_wine_etl`, and `join_wines`. `join_wines` however is only a prototype and a non-functional prototype at that as as currently the difference in the text between the extracted wine list data and bepoz data is too great.

Includes an example website scraping tool for gathering wine product data.

## TODO

- [ ] populate database with wines:
  - [x] fix join problem with section labelling
  - [x] load a wine_list table with parsed text as primary key, set line numbers starting from 0:
    - [x] write loading script
    - [x] add to dag
    - [x] test
  - [x] load bepoz parsed table into same database as wine_list:
    - [x] write loading script
    - [x] add to dag
    - [x] test
  - [ ] add rest-api to webapp
  - [ ] test uploading of data through api
  - [ ] upload to prod.
  - [ ] join wine_list, bepoz using fuzzy join see [Ingesting Bepoz/Wine List Join](./devlog.md#ingesting-bepoz/wine-list-join)
