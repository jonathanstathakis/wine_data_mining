create or replace table WINELISTSTAGING (
    LINE_NUM_TOT int,
    PAGE_NUM int,
    LINE_NUM int,
    SECTION varchar,
    SUBSECTION varchar,
    SUBSUBSECTION varchar,
    MERGED_TEXT varchar,
    -- PRICE_EXT varchar,
    -- VINTAGE_EXT varchar,
    VINTAGE varchar,
    -- BASE_YEAR_EXT varchar,
    -- DISGORG_YEAR_EXT varchar,
    BASE_YEAR varchar,
    CUVEE_NAME varchar,
    DISGORG_YEAR varchar,
    PRICE varchar,
    MERGED_TEXT_EXT varchar
);
