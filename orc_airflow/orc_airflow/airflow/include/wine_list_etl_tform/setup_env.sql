/*
* add env vars.
* */
create table if not exists TEMPLATESEARCHPATH (
    ID int primary key default 1, PATH varchar
);
insert into TEMPLATESEARCHPATH (ID, PATH) values
(1 , '{{ params.TEMPLATE_SEARCHPATH }}')
on conflict do update 
set path = excluded.path
;
select * from TEMPLATESEARCHPATH;
