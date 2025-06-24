import marimo

__generated_with = "0.14.7"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Analyzing Raw Bepoz Data

    To automate a sync between the bepoz databse and wine_wiki, we need to add some translations between the two data models.
    The most useful source of discriminatory data is the comment field, which contains the full wine name, producer, region, 
    vintage, volume and more. Unfortunately different formats have emerged over time. While an endgoal would be a formalisation
    and correction of the source data, at the moment we will need to define some translation functions. To do this we need to
    identify subgroups, seperate them, then define functions which unify the data ready for ingestion into the wine-wiki database.

    At this time this notebook is not contained within the wine-wiki project and thus integration with the ORM is not possible,
    so we will focus on unification until we can integrate into that project.
    """
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Loading the Data""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- load raw data
        create or replace table bp_raw_loading (
            product_id int primary key,
            product_name varchar,
            long_name varchar,
            datetime_last_sale varchar,
            qty_onhand varchar,
            _comment varchar,
            normal_size1 double,
            sunday_pricing_size1 double,
            public_holiday_size1 double,
            costinc_last double
        );

        insert into
            bp_raw_loading
        select
            *
        from
            read_csv(
                'Product List_ Jonathan_05Jun2025_234706.csv',
                normalize_names = true
            );

        -- seperate costs into a dim table
        create or replace sequence dimCostsSeq start 1;

        create or replace table dimCosts (
            pk int default nextval('dimCostsSeq'),
            normal_size1 double,
            sunday_pricing_size1 double,
            public_holiday_size1 double,
            costinc_last double,
            product_id int
        );

        insert into
            dimCosts (
                normal_size1,
                sunday_pricing_size1,
                public_holiday_size1,
                costinc_last,
                product_id
            )
        select
            normal_size1,
            sunday_pricing_size1,
            public_holiday_size1,
            costinc_last,
            product_id
        from
            bp_raw_loading
        order by
            product_id desc;

        alter table bp_raw_loading
        add column if not exists dimCosts int;

        update bp_raw_loading as a
        set
            dimCosts = pk
        from
            dimCosts as b
        where
            a.product_id = b.product_id;

        alter table bp_raw_loading
        drop column normal_size1;

        alter table bp_raw_loading
        drop column sunday_pricing_size1;

        alter table bp_raw_loading
        drop column public_holiday_size1;

        alter table bp_raw_loading
        drop column costinc_last;

        alter table dimCosts drop column product_id;

        alter table bp_raw_loading
        rename long_name to allocation;

        -- create comments dim - will be expanding this branch according to extraction routines.
        create or replace sequence dimCommentSeq start 1;

        create or replace table dimComment (
            pk int default nextval('dimCommentSeq') primary key,
            _comment varchar,
            product_id int
        );

        insert into
            dimComment (_comment, product_id)
        select
            _comment,
            product_id
        from
            bp_raw_loading
        order by
            product_id desc;

        alter table bp_raw_loading
        add column dimComment int;

        update bp_raw_loading as a
        set
            dimComment = b.pk
        from
            dimComment as b
        where
            a.product_id = b.product_id;

        alter table bp_raw_loading
        drop column _comment;

        alter table dimComment drop column product_id;

        -- move sales info to another dim
        create or replace sequence dimSalesSeq start 1;

        create or replace table dimSales (
            pk int default nextval('dimSalesSeq') primary key,
            qty_onhand varchar,
            datetime_last_sale varchar,
            product_id int
        );

        insert into
            dimSales (qty_onhand, datetime_last_sale, product_id)
        select
            qty_onhand,
            datetime_last_sale,
            product_id
        from
            bp_raw_loading
        order by
            product_id desc;


        alter table bp_raw_loading
        add column dimSales int;

        update bp_raw_loading as a
        set
            dimSales = b.pk
        from
            dimSales as b
        where
            a.product_id = b.product_id;

        alter table bp_raw_loading
        drop column qty_onhand;

        alter table bp_raw_loading
        drop column datetime_last_sale;

        alter table dimSales drop column product_id;

        -- cleaning dimComment
        update dimComment set _comment = _comment.lower().replace('sagiovese','sangiovese');
        update dimComment set _comment = _comment.lower().replace('mouvedre','mourvedre');

        select
            *
        from
            bp_raw_loading
        limit
            10;
        """
    )
    return bp_raw_loading, dimComment, dimCosts, dimSales


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""we will focus on the comment field first, using Product Id as the primary key.""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    As we can see there are various formats found. We can also see some spelling errors which will need correction.

    Cleanup includes removing punctuation and whitespace from the ends of the strings. Stock with 0 qty on hand are also excluded, as they are not immediately pertinant to the wine wiki, and this a useful method of removing meta items such as pairing charges and other special menu items.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Identifiying which Entries end with (<volume\>)

    An evident discrimnatory factor is whether the string ends with a volume value or not.
    """
    )
    return


@app.cell
def _(dimComment, mo):
    _df = mo.sql(
        f"""
        select
            count(_comment)
        from
           dimComment 
        where
            regexp_matches(_comment, 'ml\\)$');
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""only 607 entries end with 'ml)'. What do the others end with?""")
    return


@app.cell
def _(dimComment, mo):
    _df = mo.sql(
        f"""
        select
            _comment
        from
           dimComment 
        where
            not regexp_matches(_comment, 'ml\\)$');
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""A mish-mash. We should ignore these for now as they will need to be fixed manually.""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Dividing the Comments into Common Patterns

    There are a number of different formats of string, some which will require more work than others. To do this we will copy the 'raw_comment' table then move entries from the copy to their own tables, continuing until 'raw_comment_copy' is empty.
    """
    )
    return


@app.cell
def _(dimComment, mo):
    _df = mo.sql(
        f"""
        describe dimComment;
        """
    )
    return


@app.cell
def _(dimComment, mo):
    _df = mo.sql(
        f"""
        select count(*) from dimComment;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Segregating Champagnes""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Champagnes are significantly different, as they do not end with the volume.""")
    return


@app.cell
def _(dimComment, mo):
    _df = mo.sql(
        f"""
        alter table dimComment add column if not exists clean_grp_label varchar default null;

        update dimComment
            set clean_grp_label = 'champagne'
            where
                regexp_matches(_comment, '.*(Discorg|Disgorg|D-|D\\d{2}).*', 'i');

        select * from dimComment where clean_grp_label = 'champagne';
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Building Volume Field

    As we progress in cleaning up the comment string, important information should be moved into dedicated fields, simplifying the remaining comment string. An easy one is the bottle volume. The majority have "(750ml)" in the string, or "(1500ml)", but some do not. First we will add a volume field then fill it out, then seperate entries without a standard volume expression to create specialised logic before merging back in.
    """
    )
    return


@app.cell
def _(coalesced, dimComment, extracted, mo, unnested):
    _df = mo.sql(
        f"""
        create or replace sequence dimVolSeq start 1;

        create or replace table dimVol (
            pk int primary key default nextval('dimVolSeq'),
            volume varchar,
            ext_comment varchar,
            dimComment int
        );

        insert into
            dimVol (volume, ext_comment, dimComment)
        select
            volume_extract.volume as volume,
            -- ext_comment is the working field, slowly getting broken down into its component fields. Hence for all
            -- entries that didint get matched in regexp_extract because of no volume field, add some content to the
            -- ext_comment field for downstream processing.
            -- ifnull(volume_extract.ext_comment.nullif(''), volume_extract._comment) 
            volume_extract.ext_comment as ext_comment,
            -- ext_comment = volume_extract.ext_comment
            volume_extract.pk as dimComment,
        FROM
            (
                WITH
                    extracted AS (
                        SELECT
                            pk,
                            _comment,
                            regexp_extract(
                                _comment,
                                '^(.*)\\s?\\((.*?)\\s?ml\\)',
                                ['ext_comment', 'volume'],
                                'i'
                            ) AS extract_struct
                        FROM
                            dimComment
                    ),
                    unnested AS (
                        SELECT
                            pk,
                            _comment,
                            extract_struct.ext_comment,
                            extract_struct.volume
                        FROM
                            extracted
                    ),
                coalesced as (
                select
                    pk,
                    coalesce(unnested.ext_comment.nullif(''), _comment) as ext_comment,
                    unnested.volume
                from
                    unnested
                )
                select
                    *
                from
                    coalesced
            ) as volume_extract;

        alter table dimComment
        add column if not exists dimVol int;

        update dimComment as a
        set
            dimVol = b.pk
        from
            dimVol as b
        where
            a.pk = b.dimComment;

        alter table dimVol
        drop column dimComment;

        update dimVol as a
        set
            volume = 500
        from
                    dimComment as b
                where
                    regexp_matches(b._comment, 'aqua santa', 'i')
            and a.pk = b.dimVol;

        select
            _comment,
            volume,
            a.pk
        from
            dimVol as a
            join dimComment as b on a.pk = b.dimVol
        where
            volume = '500';

        -- -- magnum
        update dimVol as a
        set
            volume = 1500
        from
                    dimComment as b
                where
                    regexp_matches(b._comment, 'magnum', 'i')
            and a.pk = b.dimVol;
        -- -- all remaining wines are considered to be 750ml
        update dimVol 
        set
            volume = 750
        where
            volume = '';
        -- -- Inspect unmatched volumes
        SELECT
            *
        FROM
           dimVol 
        WHERE
            -- check for any who dont have an entry by matching 3 or 4 digit numbers.
            volume !~ '([0-9][0-9][0-9])[0-9]*';
        """
    )
    return (dimVol,)


@app.cell
def _(dimVol, mo):
    _df = mo.sql(
        f"""
        select * from dimVol;
        """
    )
    return


@app.cell
def _(dimComment, dimVol, mo):
    _df = mo.sql(
        f"""
        select
            b._comment,
            a.*
        from
            dimVol as a
            join dimComment as b on a.pk = b.dimVol
        where
            ext_comment = ''
            ;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Extracting Vintage""")
    return


@app.cell
def _(coalesced, combined, dimComment, mo, unnested):
    _df = mo.sql(
        f"""
        begin transaction;
        create or replace sequence dimVintageSeq start 1;

        create or replace table dimVintage (
            pk int default nextval('dimVintageSeq'),
            dimComment int,
            ext_comment varchar,
            vintage varchar
        );

        insert into
            dimVintage (dimComment, ext_comment, vintage)
        with
            unnested as (
                select
                    pk as dimComment,
                    _comment as input_comment,
                    unnest(
                        regexp_extract(
                            _comment,
                            '(.*?)(\\d\\d\\d\\d|NV)(.*)',
                            ['before_vintage', 'vintage', 'after_vintage']
                        )
                    )
                from
                    dimComment
            ),
            combined as (
                select
                    dimComment,
                    input_comment,
                    trim(before_vintage || ' ' || after_vintage) as ext_comment,
                    vintage
                from
                    unnested
            ),
            coalesced as (
                select
                    dimComment,
                    coalesce(ext_comment.nullif (''), input_comment) as ext_comment,
                    vintage
                from
                    combined
            )
            -- currently 33 entries without an input comment
        select
            dimComment,
            ext_comment,
            vintage
        from
            coalesced;

        -- add dimVintage key to dimComment
        alter table dimComment
        add column if not exists dimVintage int;

        update dimComment a
        set
            dimVintage = b.pk
        from
            dimVintage as b
        where
            a.pk = b.dimComment;
        -- finish transaction by dropping dimComment from dimVintage;

        alter table dimVintage drop column dimComment;

        commit;

        select
            *
        from
            dimComment;
        """
    )
    return (dimVintage,)


@app.cell
def _(dimVintage, mo):
    _df = mo.sql(
        f"""
        select * from dimVintage;
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Extracting Varietals

    The next step is to extract all varietals. First we need to add a list of varietals then match the comment string to the varietal (or list of) then remove the varietals from the string.
    """
    )
    return


@app.cell
def _(mo, varieties):
    _df = mo.sql(
        f"""
        create table varieties_raw as (
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
            ('savagnin');

        create or replace table varieties (pk int, variety varchar, variety_cleaned varchar);

        insert into
            varieties
        select
            row_number() over () as pk,
            variety,
            variety.replace('-', ' ').strip_accents().lower() as variety_cleaned
        from
            varieties_raw;

        select
            *
        from
            varieties;
        """
    )
    return (varieties_raw,)


@app.cell
def _(dimComment, mo, varieties):
    _df = mo.sql(
        f"""
        -- need to loop over dimComment._comment and varieties.varieties.cleanedm..
        -- append each value or 


        with RECURSIVE t(pk, _comment, remaining, matched, iteration) using key (pk) AS (

           SELECT 
                pk as dimComment,
                _comment.lower() as _comment, 
                _comment.lower() as remaining,
                []::varchar[] as matched,
                1 as iteration
            from
                (select pk as dimComment, * from dimComment limit 100)
            union 

            select
                e.pk as dimComment,
                _comment,
                regexp_replace(remaining, v.variety_cleaned, '', 'g') as remaining,
            array_append(matched, v.variety_cleaned) as matched,
                iteration + 1 as iteration,
            from
                t e
            join
                varieties v
            on
                POSITION (v.variety_cleaned in e.remaining) > 0
        )
        select * from t order by _comment;
        """
    )
    return


if __name__ == "__main__":
    app.run()
