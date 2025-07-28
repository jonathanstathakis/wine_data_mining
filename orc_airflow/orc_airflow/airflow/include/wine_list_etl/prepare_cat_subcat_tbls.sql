-- begin transaction;
-- category table. need category ids and an order, based on line num.
drop table if exists SubSection CASCADE;
drop table Section CASCADE;
create or replace table Section(
    section varchar primary key,
    section_order int default -1
);

insert into Section
with ListSection as (
select 
  distinct section as section,
from wine_list
  order by line_num_tot
)
select
  section,
  row_number() over () -1 as section_order,
from
 ListSection 
on conflict (section) do update set 
  section_order = EXCLUDED.section_order
;

create or replace temp table SUBSECTIONSTAGING as
with LISTSUBSECTION as (
    select
        first(SECTION) as SECTION,
        first(SUBSECTION) as SUBSECTION,
        first(LINE_NUM_TOT) as line_num_tot,
        row_number() over (order by first(line_num_tot))-1  as subsection_order
    from WINE_LIST
    group by
        SECTION, SUBSECTION
    order by  
        subsection_order
)

select * from LISTSUBSECTION;

create or replace table SubSection(
section varchar references Section(section),
subsection varchar primary key,
subsection_order int default -1,
unique (section, subsection)
);

insert into SubSection 
select 
    section,
    subsection,
    subsection_order
    from SubSectionStaging
on conflict (subsection) do update set
  subsection_order = EXCLUDED.subsection_order
;

create or replace sequence subsubsection_seq start with 1;
create table if not exists SubSubSection(
section varchar references Section(section),
subsection varchar references SubSection(subsection),
subsubsection varchar,
subsubsection_order int default -1,
subsubsection_id int default nextval('subsubsection_seq') primary key,
unique (section, subsection, subsubsection)
);


 create or replace temp table SUBSUBSECTIONSTAGING as
 with LISTSUBSUBSECTION as (
     select
         first(SECTION) as SECTION,
         first(SUBSECTION) as SUBSECTION,
         first(SUBSUBSECTION) as SUBSUBSECTION,
         first(LINE_NUM_TOT) as line_num_tot,
         row_number() over (order by first(line_num_tot))-1  as subsubsection_order 
     from WINE_LIST
     group by
         SUBSUBSECTION
     order by  
         subsubsection_order
 )

 select * from LISTSUBSUBSECTION;

 insert into SubSubSection (section, subsection, subsubsection, subsubsection_order)
 select 
     section,
     subsection,
     subsubsection,
     subsubsection_order,
     -- DEFAULT as subsubsection_id
     from SubSubSectionStaging
 on conflict (subsubsection_id) do update set
     subsubsection_order = EXCLUDED.subsubsection_order
 ;

 -- rollback;
