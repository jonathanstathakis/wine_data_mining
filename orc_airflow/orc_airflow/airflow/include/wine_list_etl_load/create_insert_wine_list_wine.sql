/*
* create the denormalized wine_list_wine table ready for export
* to the webapp.
* TODO: grosse lage still being read as volume.
*/

create or replace table wine_list_wine (
    filepath varchar not null,
    pub_date varchar not null,
    run_dt varchar not null,
    line_num_tot int not null,
    vintage varchar not null,
    prod_wine_name varchar not null,
    geo_int varchar not null,
    vol varchar not null,
    price int not null,
    section_path varchar not null,
    page_number int not null
);

insert into wine_list_wine (
    filepath,
    pub_date,
    run_dt,
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
    d.doc_path as filepath,
    pd.pub_date as pub_date,
    r.run_dt as run_dt,
    l.line_num_tot,
    c.vintage,
    c.prod_wine_name,
    c.geo_int,
    c.vol,
    c.price,
    s.path as section_path,
    p.page_number
from
    wine c
left join
    pageline l
    on
        l.id = c.line_id
left join
    page p
    on
        l.page_id = p.id
left join
    doc d
    on
        p.doc_id = d.id
left join
    run r
    on
        d.run_id = r.id
left join
    pub_date pd
    on
        pd.doc_id = d.id
left join
    sectionpathtopageline spp
    on
        spp.line_id = l.id
left join
    sectionpath s
    on
        spp.sectionpath_id = s.id
order by
    p.page_number,
    line_num_tot;

select * from wine;
