# TODO

- [ ] populate database with wines:
  - [ ] fix join problem with section labelling
  - [ ] load a wine_list table with parsed text as primary key, set line numbers starting from 0:
    - [ ] write loading script
    - [ ] add to dag
    - [ ] test
  - [ ] load bepoz parsed table into same database as wine_list:
    - [ ] write loading script
    - [ ] add to dag
    - [ ] test
  - [ ] join wine_list, bepoz using fuzzy join see [Ingesting Bepoz/Wine List Join](./devlog.md#ingesting-bepoz/wine-list-join)
