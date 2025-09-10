/*
Add a new run each execution and set current run id in currRun.
 */
create or replace sequence run_id_seq;
create or replace table run (
id int primary key default nextval('run_id_seq'),
run_dt datetime unique default now()
);
insert into run
(id, run_dt) values (default, default);

select * from run;
