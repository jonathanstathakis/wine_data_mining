-- import region label column from xlsx file
create temp table if not exists region_label_raw as (
    select *
    from
        read_xlsx(
            'datasets/wine_regions/vineyard_estimates_2015.xlsx',
            sheet = 'GI Region',
            range = 'B6:B'
        )
);
-- select unique region label values, clean them up.
create table if not exists region_label as (
select distinct "Region label".replace('- Other','').trim() as region_label
from
    region_label_raw
where region_label is not null
order by
    region_label
);

drop table region_label_raw;
select * from region_label;

