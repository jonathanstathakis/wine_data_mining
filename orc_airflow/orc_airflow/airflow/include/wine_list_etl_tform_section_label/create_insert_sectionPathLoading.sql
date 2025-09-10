/*
* creates a table sectionPath that contains a stringified heirarchy 
* path of the sections contained within the pages and links it to
* pageLine via a fk column sectionpath_id on pageLine.
* */

-- create a loading table to handle the aggregation.
create or replace table sectionPathLoading (
line_id int unique not null,
line_num_tot int unique not null,
path varchar,
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
