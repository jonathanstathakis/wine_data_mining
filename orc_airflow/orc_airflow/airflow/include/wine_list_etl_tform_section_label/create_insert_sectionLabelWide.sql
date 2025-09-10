drop table if exists sectionLabelWide;
drop table if exists sectionLabelWideLoading;
create or replace table sectionLabelWide (
line_id int references pageLine(id), 
line_num_tot int not null,
section varchar, 
subsection varchar,
subsubsection varchar);

create or replace table sectionLabelWideLoading as
with sectionLabelWithText as (
select
      s.line_id,
      l.line_num_tot,
      s.section_type,
      t.line_text as text
from
      sectionLabel s
left join
      pageLine l
left join
      lineText t
on
      l.id = t.line_id
on
      s.line_id = l.id
),

sectionLabelPivoted as (

select
    line_id,
    line_num_tot,
    section,
    subsection,
    subsubsection
from
(
  PIVOT sectionLabelWithText s
  ON section_type 
  using
    first(text)
  order by line_id
)
)

select * from sectionLabelPivoted
  ;

select case when count(*) > 0 then 'ok' else error('sectionLabelWideLoading is empty') end from sectionLabelWideLoading;
--
insert into sectionLabelWide (
  line_id,
  line_num_tot,
  section,
  subsection,
subsubsection
 )
from
sectionLabelWideLoading;
--
select * from sectionLabelWide limit 5;
-- drop table sectionLabelWideLoading;j
