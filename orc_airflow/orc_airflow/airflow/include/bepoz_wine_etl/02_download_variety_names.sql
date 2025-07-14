
create temp table varieties_raw as (
    select
        variety
    from
        read_csv(
            'https://raw.githubusercontent.com/VandelayArt/wine_data/refs/heads/main/grape_names.csv',
            header = true,
            names = ['idx', 'variety']
        )
);

insert into
    varieties_raw
values
    ('shiraz'),
    ('gsm'),
    ('cabernet blend'),
    ('Mourvèdre'),
    ('fume blanc'),
    ('touriga'),
    ('cy'),
    ('savagnin'),
    ('mataro');

create or replace table varieties (pk int, variety varchar unique, variety_cleaned varchar);

insert into
    varieties
select
    row_number() over () as pk,
    variety,
    variety.replace('-', ' ').strip_accents().lower() as variety_cleaned
from
    varieties_raw;

drop table varieties_raw;
select
    *
from
    varieties;
