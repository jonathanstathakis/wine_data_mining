/* 
* create a document entry with a filepath as identifier
* */

create or replace sequence doc_seq;
create or replace table doc (
id int primary key default nextval('doc_seq'),
doc_path varchar unique,
run_id int references run(id)
);

insert into doc (
  doc_path,
  run_id
)
  select
    '{{ params.pdf_path }}' as doc_path,
    id as run_id
  from
    run;
