-- /* can expand the section label to lael all lines by type: section or text.
--  This logic is complicated. Is as follows:
--
-- - if no section: fill.
-- - if no subsection: fill down.
-- - if no subsubsection:
-- - only fill until next subsection.
--
-- Possibility:
--
-- Make condition of..
--insert_label_section_all_line
-- coalesce subsection subsubsection then cumsum
-- joining on that value.
--
-- */
--
-- join to line_id to get all lines in correct order.
-- logic for subsubsection is slightly more complicated. Subsubsections
-- are optional and may be implicit. Thus an easy method of handling is
-- to coalesce with subsection, which is always explicit, and then
-- fill. Any rows where subsubsection = subsection after filling can be 
-- reverted back to null retain the implicit subsubsection labeling.
-- first coalesce the unfilled to establish boundaries, fill then mask
-- with filled subsection column to return to nulls.

create or replace table sectionLabelsByLine (
  line_id int not null unique,
  line_num_tot int not null unique,
  section varchar not null,
  subsection varchar not null,
  subsubsection varchar not null
);
insert into sectionLabelsByLine (
    line_id,
    line_num_tot,
    section,
    subsection,
    subsubsection
  ) with mer_subsub_sub as (
    select line_id,
      line_num_tot,
      subsection,
      subsubsection,
      ifnull(subsubsection, subsection) as mer_subsub_sub
    from 
      allLinesWithSections_Null
  ),
  mer_subsub_sub_jk as (
    select line_id,
      line_num_tot,
      subsection,
      subsubsection,
      mer_subsub_sub,
      sum (
        case
          when mer_subsub_sub is not null then 1
        end
      ) over (
        order by line_num_tot
      ) as mer_subsub_sub_jk
    from mer_subsub_sub
  ),
  merg_subsub_label_map as (
    select mer_subsub_sub_jk,
      first(mer_subsub_sub) as mer_subsub_sub,
      first(line_num_tot) as line_num_tot,
      first(line_id) as line_id
    from mer_subsub_sub_jk
    group by mer_subsub_sub_jk
    having mer_subsub_sub_jk is not null
    order by line_num_tot
  ),
  merg_subsub_sub_fill as (
    select a.line_id,
      a.line_num_tot,
      a.subsection,
      a.subsubsection,
      b.mer_subsub_sub as mer_subsub_sub_filled,
      from mer_subsub_sub_jk a
      left join merg_subsub_label_map b on a.mer_subsub_sub_jk = b.mer_subsub_sub_jk
  ),
  subsection_masked as (
    select b.*,
      a.mer_subsub_sub_filled,
      case
        when a.mer_subsub_sub_filled = b.subsection then null
        else a.mer_subsub_sub_filled
      end as subsubsection
    from merg_subsub_sub_fill a

      left join 
          sectionSubsectionFilled b 
      on a.line_id = b.line_id
    order by a.line_num_tot
  )
select line_id,
  line_num_tot,
  ifnull(section,''),
  ifnull(subsection, ''),
  ifnull(subsubsection, '')
from subsection_masked;
--
-- select * from sectionLabelsByLine;
