
 create or replace temp table SUBSUBSECTIONSTAGING as
 with LISTSUBSUBSECTION as (
select 
    distinct subsubsection, 
    section, 
    subsection,
    line_num_tot
  from 
    wine_list 
  order by 
    line_num_tot
 )

 select * from LISTSUBSUBSECTION;

 insert into SubSubSection (section, subsection, subsubsection, subsubsection_order)
 select 
      
     section,
     subsection,
     subsubsection,
     row_number() over (order by line_num_tot) as subsubsection_order,
     from SubSubSectionStaging
 on conflict (subsubsection_id) do update set
     subsubsection_order = EXCLUDED.subsubsection_order
 ;

 -- rollback;
