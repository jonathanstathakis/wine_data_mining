/*
* create the denormalized wine_list_wine table ready for export
* to the webapp.
* TODO: grosse lage still being read as volume.
*/

create or replace table wine_list_wine (
    filepath varchar,
    pub_date varchar,
    run_dt varchar,
    line_num_tot int,
    vintage varchar,
    prod_wine_name varchar,
    geo_int varchar,
    vol varchar,
    price int,
    section_path varchar,
    page_number int
);

insert into wine_list_wine (
    line_num_tot,
    vintage,
    prod_wine_name,
    geo_int,
    vol,
    price,
    section_path,
    page_number
)

select
    c.line_num_tot,
    c.vintage,
    c.prod_wine_name,
    c.geo_int,
    c.vol,
    c.price,
    s.path as section_path,
    p.page_number
from
    columndecomp c
left join
    pageline l
    using (line_num_tot)
left join
    page p
    on
        l.page_id = p.id
left join
    sectionpath s
    on
        l.sectionpath_id = s.id
order by
    p.page_number,
    line_num_tot;

select * from wine_list_wine;
