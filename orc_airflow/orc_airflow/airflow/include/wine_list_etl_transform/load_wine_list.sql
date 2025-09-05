create or replace table wine_list (
    pk int primary key,
    line_num_tot int not null,
    page_num int not null,
    page_line_num int not null,
    section varchar(40) not null,
    subsection varchar(40) not null,
    subsubsection varchar(40) not null,
    vintage varchar(4) not null,
    merged_text_ext varchar(90) not null,
    base_year int,
    cuvee_name varchar(40),
    disgorg_year int,
    price int not null,
    merged_text varchar(90) not null,
    producer varchar default null,      
    dryness varchar default null,       
    country varchar default null,       
    state varchar default null,         
    region varchar default null,        
    subregion varchar default null,     
    commune varchar default null,       
    vineyard varchar default null,      
    style varchar default null,         
    classification varchar default null,
    volume varchar default null,        
    series varchar default null,        
    variety varchar default null,       
);

insert into wine_list by name (
select
    row_number() over (
        order by line_num_tot
    ) - 1::int as pk,
    row_number() over (
        order by line_num_tot
    ) - 1::int as line_num_tot,
    page_num::int as page_num,
    row_number() over (partition by page_num) - 1::int as page_line_num,
    section.lower()::varchar as section,
    subsection.lower()::varchar as subsection,
    subsubsection.lower()::varchar as subsubsection,
    vintage.lower()::varchar as vintage,
    merged_text_ext.lower()::varchar as merged_text_ext,
    case when base_year = '' then null else base_year end::int as base_year,
    cuvee_name.lower()::varchar as cuvee_name,
    case when disgorg_year = '' then null else disgorg_year end::int
        as disgorg_year,
    price,
    merged_text.lower() as merged_text
from
   wineListLines 
order by
    line_num_tot);

select * from wine_list;
