create or replace sequence SECTIONPATH_SEQ;

create or replace table SECTIONPATH (
    ID int primary key default nextval('SECTIONPATH_SEQ'),
    PATH varchar not null unique
);



create or replace table sectionPathLoading (
line_id int unique not null,
line_num_tot int unique not null,
path varchar,
path_id int
);

insert into sectionPathLoading (
  line_id,
  line_num_tot,
  path
)
select
    line_id,
    line_num_tot,
    array_to_string([SECTION, SUBSECTION, SUBSUBSECTION], '/').replace('//','/') as path
from SECTIONLABELSBYLINE
order by
  line_num_tot;

insert into sectionPath (path)
select
    distinct path
from
    sectionPathLoading;

update sectionPathLoading s
set
  path_id = q.path_id
from (
select 
  l.line_id as line_id,
  l.path as left_path,
  p.path as right_path,
  p.id as path_id
from
  sectionPathLoading l
left join
  sectionPath p
on
  l.path = p.path
order by
  l.line_num_tot
) q
where
s.line_id = q.line_id
;

update pageline p
set
    sectionpath_id = l.path_id
from
    sectionPathLoading l
where
    l.line_id = p.id;

select 
  * 
from 
  pageLine  l
join
  sectionPath s
on
  l.sectionpath_id = s.id
order by line_num_tot;
