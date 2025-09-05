begin;


-- select * from rawTextStaging;

create temp table word0Map as 
select
    first(c.run_id) as run_id,
    first(t.id) as rawtext_id,
    first(t.line_id) as line_id
from
    currRun as c
inner join
    rawTextStaging as t
    on
        c.run_id = t.run_id
inner join
    pageline as l
    on
        t.line_id = l.id
        and
        c.run_id = l.run_id
group by
l.line_num_tot
order by
    l.line_num_tot;

-- filter to lines directly above rectangles to form a map of line number to rect text line
create temp table subsectionMap as (
select
    w.run_id as run_id,
    w.rawtext_id as word0Map_id,
    w.line_id as line_id,
    'subsection' as section_type
from
word0Map w
inner join
rawTextStaging t
on w.rawtext_id = t.id
and w.run_id = t.run_id
inner join rectangle r
on 
  r.bottom + 1 > t.bottom
and
  t.bottom > (r.bottom - t.height)
and w.run_id = r.run_id
and
  r.page_id = t.page_id
);

-- -- section map
-- -- find all text greater than 200 points right and less than 100 points down.
create temp table sectionMap as (
select
    w.line_id as line_id,
    w.run_id as run_id,
    w.rawtext_id as word0Map_id,
    'section' as section_type,
from
    word0Map as w
join
    rawTextStaging t
on
    w.rawtext_id = t.id
where
    t.x0 > 200 
and
    t.top < 100
);


create temp table subsubsectionMap as (
select
    w.run_id as run_id,
    w.rawtext_id as word0Map_id,
    w.line_id as line_id,
    'subsubsection' as section_type,
from
    word0Map as w
join 
rawtextStaging t
on
w.rawtext_id = t.id
and 
w.run_id = t.run_id
where
    'Italic' in t.fontname
and w.line_id not in (
select 
  line_id
from
  subsectionMap
)
);

 -- exclude line_ids matching the paragraph on skin contact
 -- dirty fix.
 create temp table skin_contact_lines as (
 select
   first(s.run_id) as run_id,
   first(s.word0Map_id) as word0Map_id,
   first(s.line_id) as line_id,
   first(s.section_type) as section_type,
   first(t.x0) as first_x0,
   last(t.x0) as last_x0,
   first(t.top) as first_top,
   last(t.top) as last_top,
   string_agg(t.text, ' ') as text
 from
   subsubsectionMap s
 join
   rawtextStaging t
 on
   s.line_id = t.line_id
and
   s.run_id = t.run_id
 group by
   s.line_id
 having
   text like '%Skin contact aka, orange wine, is made from white grapes vinified in a similar manner to red wine. Skin%'
   or text like '%contact can turn up the volume of the varietal. Adding texture, tannin and unique varietal aromatics.%'
 order by
   section_type,
   line_id,
   first_x0,
   first_top);

 delete from 
     subsubsectionMap a 
 using
     skin_contact_lines b
 where
     a.line_id = b.line_id
 and
     a.run_id = b.run_id;


create temp table sectionLabel as 
with sectionLabel_ as (
    select
        run_id,
        word0Map_id,
        line_id,
        section_type
    from
        sectionMap
    union
        select
          run_id,
          word0Map_id,
          line_id,
          section_type
        from
          subsectionMap
    union
        select
          run_id,
          word0Map_id,
          line_id,
          section_type
        from
          subsubsectionMap
  )

select
  first(s.run_id) as run_id,
  first(s.line_id) as line_id,
  first(s.section_type) as section_type,
  string_agg(t.text, ' ') as text
from
  sectionLabel_ s
join
  rawTextStaging t
on
  s.line_id = t.line_id
and
  s.run_id = t.run_id
group by
  s.section_type,
  s.line_id 
having
  text not ilike '%continued%' -- page-based repeat headers.
order by
  line_id
;

/* can expand the section label to lael all lines by type: section or text.
 This logic is complicated. Is as follows:

- if no section: fill.
- if no subsection: fill down.
- if no subsubsection:
- only fill until next subsection.

Possibility:

Make condition of..

coalesce subsection subsubsection then cumsum
joining on that value.

*/

create temp table sectionLabel_wide as (

PIVOT sectionLabel s
ON section_type 
using
  first(text)
order by line_id
  );
    
select
    first(c.run_id) as run_id,
    first(t.id) as rawtext_id,
    first(t.line_id) as line_id
from
    currRun as c
inner join
    rawTextStaging as t
    on
        c.run_id = t.run_id
inner join
    pageline as l
    on
        t.line_id = l.id
        and
        c.run_id = l.run_id
group by
l.line_num_tot
order by
    l.line_num_tot;
-- slect * from pageLine;
-- -- join to line_id to get all lines in correct order.
-- create temp table all_lines_with_sections as (
-- select
--     l.run_id,
--     l.id as line_id,
--     s.word0Map_id as section_id,
--     l.line_num_tot,
--     s.section,
--     s.subsection,
--     s.subsubsection
-- from
--     pageline l
-- left join
--     sectionLabel_wide s
-- on
--     l.id = s.line_id
-- and
--     l.run_id = s.run_id
-- order by
--     l.line_num_tot
-- );
-- -- select * from all_lines_with_sections limit 20;
--
-- create temp table section_subsection_filled as (
-- with 
-- sectionMap as (
--   select
--     run_id,
--     line_id,
--     section,
--     sum(case when section is not null then 1 else 0 end) over (order by line_num_tot)  as section_self_join_key,
--   from
--     all_lines_with_sections
--   where
--     section is not null
-- ),
-- subsectionMap as (
--   select
--     line_id,
--     run_id,
--     subsection,
--     sum(case when subsection is not null then 1 else 0 end) over (order by line_num_tot)  as subsection_self_join_key,
--   from
--     all_lines_with_sections
--   where
--     subsection is not null
-- ),
-- with_join_key as (
-- select 
--   line_id,
--   run_id,
--   line_num_tot,
--    -- as self_join_key,
--   section,
--   subsection,
--   subsubsection,
--   sum(case when section is not null then 1 else 0 end) over (order by line_num_tot)  as section_self_join_key,
--   sum(case when subsection is not null then 1 else 0 end) over (order by line_num_tot)  as subsection_self_join_key,
--
-- from
--   all_lines_with_sections),
--
-- section_and_subsection_joined as (
-- select 
--   a.run_id,
--   a.line_id,
--   line_num_tot,
--   b.section,
--   c.subsection
-- from 
--   with_join_key a
-- left join
--   sectionMap b
-- on
--   a.section_self_join_key = b.section_self_join_key
-- and
--   a.run_id = b.run_id
-- left join
--   subsectionMap c
-- on
--   a.subsection_self_join_key = c.subsection_self_join_key
-- and
--   a.run_id = c.run_id
-- )
--   select * from section_and_subsection_joined
-- );
--
-- -- logic for subsubsection is slightly more complicated. Subsubsections
-- -- are optional and may be implicit. Thus an easy method of handling is
-- -- to coalesce with subsection, which is always explicit, and then
-- -- fill. Any rows where subsubsection = subsection after filling can be 
-- -- reverted back to null retain the implicit subsubsection labeling.
-- -- first coalesce the unfilled to establish boundaries, fill then mask
-- -- with filled subsection column to return to nulls.
--
-- create temp table subsubsection_filled as (
--     with mer_subsub_sub as (
--       select
--       run_id,
--       line_id,
--       line_num_tot,
--       subsection,
--       subsubsection,
--       ifnull(subsubsection, subsection) as mer_subsub_sub,
--     from
--       all_lines_with_sections),
--
--     mer_subsub_sub_jk as (
--       select
--           line_id,
--           run_id,
--           line_num_tot,
--           subsection,
--           subsubsection,
--           mer_subsub_sub,
--           sum (case when mer_subsub_sub is not null then 1 end) over (order by line_num_tot) as mer_subsub_sub_jk
--       from
--             mer_subsub_sub
--     ),
--
--     merg_subsub_label_map as (
--       select 
--           mer_subsub_sub_jk,
--           first(mer_subsub_sub) as mer_subsub_sub,
--           first(line_num_tot) as line_num_tot,
--           first(line_id) as line_id,
--           first(run_id) as run_id
--       from 
--           mer_subsub_sub_jk
--       group by
--           mer_subsub_sub_jk
--       having
--           mer_subsub_sub_jk is not null
--
--       order by
--           line_num_tot
--     ),
--
--     merg_subsub_sub_fill as (
--     select
--         a.line_id,
--         a.line_num_tot,
--         a.subsection,
--         a.subsubsection,
--         b.mer_subsub_sub as mer_subsub_sub_filled,
--         a.run_id as run_id,
--     from
--       mer_subsub_sub_jk a
--     left join merg_subsub_label_map b
--     on
--         a.mer_subsub_sub_jk = b.mer_subsub_sub_jk
--     ),
--
--     subsection_masked as (
--     select
--       b.*,
--       a.run_id, a.mer_subsub_sub_filled,
--       case
--           when 
--               a.mer_subsub_sub_filled = b.subsection 
--           then null 
--           else
--               a.mer_subsub_sub_filled
--           end as subsubsection
--     from
--       merg_subsub_sub_fill a
--     left join
--       section_subsection_filled b
--     on
--       a.line_id = b.line_id
--     order by
--       a.line_num_tot
--     )
--     select * from subsection_masked
-- );
--
-- /* 
-- * There are many options for how to store a heiarachy in a RBDMS. One is to
-- * construct and store the path for each entry, which is what we've chosen to do 
-- * here. If we later want to use the levels of the section heirarchy we can simply
-- * partition the string.
-- * */
-- create temp table sectionPathStaging as (
--     select
--       a.run_id as run_id,
--       a.line_id as line_id,
--       a.line_num_tot,
--       b.section,
--       b.subsection,
--       a.subsubsection,
--       -- cant concat null so handle with .
--       case 
--         when 
--             a.subsubsection is not null and b.section is not null and b.subsection is not null
--         then 
--             b.section || '/' || b.subsection || '/' || a.subsubsection 
--         when
--             a.subsubsection is null and b.subsection is not null
--         then
--             b.section || '/' || b.subsection
--         else
--             b.section
--
--         end
--       as section_path
--     from
--       subsubsection_filled a
--     left join
--       section_subsection_filled b
--     on
--       a.line_id = b.line_id
--     and
--       a.run_id = b.run_id
-- );
--  --
--  -- TODO: move heirarchy paths to own dim table with fk on pageline
--  -- TODO: add line_type category column to pageline with two levels: header or body. 
--  -- TODO: add *full_text* column to pageline with the complete merged text of the line - useful for development.
--  -- TODO: cleanup all queries.
--
--  -- to insert the sectionPath id into pageline use section_path_stging as a
--  -- bridge. first insert into sectionPath then join back to 
--  -- sectionPathStaging to get the line_id of the path
--
--  insert into sectionPath (run_id, path)
--  select 
--    first(run_id) as run_id,
--    section_path as path
--  from 
--    sectionPathStaging 
--  group by
--    section_path;
--
--  create temp table SectionPathBridge as (
--  select
--    a.run_id,
--    a.line_id,
--    a.line_num_tot,
--    a.section_path,
--    b.path,
--    b.id as sectionpath_id
--  from 
--  sectionPathStaging a
--  left join
--  sectionPath b
--  on
--    a.section_path = b.path
--  );
--
-- update pageLine l
-- set
--     sectionpath_id = b.sectionpath_id
-- from
--     SectionPathBridge b
-- where
--      l.run_id = b.run_id
--  and
--      l.id = b.line_id;
--
-- select * from pageLine;
--
-- /*
-- * something is causing sectionPathBridge to have null entries.
-- * when ordered by line_num_tot it appeas that only the header lines are being joined with sectionpath_id, else its null as left joining.
-- * WHy? I thought sectionPathStaging had paths for all lines.
-- * nope.
-- * sectionPathStaging is left join of subsubsection_filled and section_subsection_filled. 
-- * expect section and subsection fields from section_subsection_filled to be 
-- * just that, filled. But they are not. They do not appear to have entries for
-- * every line, just the header lines.
-- * check subsubsection_filled while we're here.
-- * subsbusectoin_filled is more sane but missing run_ids for non-header rows.
-- * probably caused by the same problem.
-- * the root cause may be all_lines_with_sections, which 
-- * currently all_lines_with_sections sources its run_id from sectionLabel_wide,
-- * which is a pivot table, not pageline.
-- * changing it to l.run_id from pageLine fixes that problem.
-- * still getting error tho.
-- * still getting the error tho.
-- * so...
-- * big problem is that we cant know what table is throwing up the constraint
-- * error, although we know it's raw text..
-- * This is a fairly silly scenario tho, as we now need join tables to avoid
-- * redefining pageLine.
-- * or dont use pageLine as the fact table.
-- * ok so potentially there is a solution:
-- *
-- * 1. drop rawText, move the current data into rawTextStaging 
-- * 2. update pageLine
-- * 3. recreate rawText
-- *
-- * No, it appears that even after dropping rawText still getting the error.
-- *
-- * got it. There was another table sectionLabel I was unaware of. 
-- * duckdb_constraints proved to be useful in solving this problem.
-- *
-- * So now what? I need to duplicate the rawText definition.. or some sort of
-- * definition. Going to have to.
-- *
-- */
--
--
 rollback;
