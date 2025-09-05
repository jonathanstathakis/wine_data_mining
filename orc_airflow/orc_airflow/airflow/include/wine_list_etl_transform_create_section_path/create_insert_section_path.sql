begin;
create or replace sequence SECTIONPATH_SEQ;

create or replace table SECTIONPATH (
    ID int primary key default nextval('sectionPath_seq'),
    PATH varchar not null unique
);

insert into sectionPath (path)
select
    distinct array_to_string([SECTION, SUBSECTION, SUBSUBSECTION], '/').replace('//','/') as path
from SECTIONLABELSBYLINE;

select * from sectionPath;
rollback;
