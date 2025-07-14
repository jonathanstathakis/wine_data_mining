-- load raw data
create or replace table
bp_raw_loading (
    product_id int primary key,
    product_name varchar,
    long_name varchar,
    datetime_last_sale varchar,
    _comment varchar,
    normal_size1 double,
    sunday_pricing_size1 double,
    public_holiday_size1 double
);

insert into bp_raw_loading
select *
from
    read_csv(
        '{INPUT_DATAFILE_PATH}',
        normalize_names = true
    )
