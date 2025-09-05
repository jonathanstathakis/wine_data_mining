/* 
* create a document entry with a filepath as identifier
* */

insert into document (
  run_id,
  doc_path
)
  select
    run_id as run_id,
    '{{ params.pdf_path }}' as doc_path
  from
    currRun;
select * from document;
    
