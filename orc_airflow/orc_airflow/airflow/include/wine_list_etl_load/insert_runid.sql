/*
Add a new run each execution and set current run id in currRun.
 */
insert into run
(id, start_time) values (default, default);

insert into currrun (
    run_id
)
select 
    currval('runid_seq') as run_id
on conflict do update
set
  run_id = excluded.run_id
;
select * from run;
select * from currrun;
