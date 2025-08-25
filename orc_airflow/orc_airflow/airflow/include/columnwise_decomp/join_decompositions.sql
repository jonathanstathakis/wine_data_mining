/*
* combine the tables formed in the columnar decompositions into 1 table.
*
*/
show tables;

create or replace table COLDECOWINES (
    LINE_NUM_TOT int primary key,
    VINTAGE varchar default '',
    BASE_YEAR varchar default '',
    PROD_WINE_NAME varchar default '',
    DRYNESS varchar default '',
    DISGORGE_DATE varchar default '',
    GEO_INT varchar default '',
    CLASS varchar default '',
    VOL varchar default '',
    PRICE int
);

/* populate line_num_tot as per other tables */

insert into COLDECOWINES (LINE_NUM_TOT)
select distinct LINE_NUM_TOT
from WINELISTLINES
order by LINE_NUM_TOT;

-- champagne
update COLDECOWINES A set
    VINTAGE = B.VINTAGE,
    BASE_YEAR = B.BASE_YEAR,
    PROD_WINE_NAME = B.PROD_WINE_NAME,
    DRYNESS = B.DRYNESS,
    DISGORGE_DATE = B.DISGORGE_DATE,
    GEO_INT = B.GEO_INT,
    VOL = B.VOL,
    PRICE = B.PRICE
from
    COLDECOCHAMPAGNE B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

-- dry sherry
update COLDECOWINES A set
    PROD_WINE_NAME = B.PROD_WINE_NAME,
    GEO_INT = B.GEO_INT,
    VOL = B.VOL,
    PRICE = B.PRICE
from
    DRYSHERRY B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

-- aus sparkling
update COLDECOWINES A set
    VINTAGE = B.VINTAGE,
    BASE_YEAR = B.BASE_YEAR,
    PROD_WINE_NAME = B.PROD_WINE_NAME,
    DISGORGE_DATE = B.DISGORGE_DATE,
    GEO_INT = B.GEO_INT,
    VOL = B.VOL,
    PRICE = B.PRICE
from
    COLDECOPAGE7AUSSPK B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

-- german riesling
update COLDECOWINES A set
    VINTAGE = B.VINTAGE,
    PROD_WINE_NAME = B.PROD_WINE_NAME,
    GEO_INT = B.GEO_INT,
    CLASS = B.CLASS,
    VOL = B.VOL,
    PRICE = B.PRICE
from
    COLDECOPAGE9RIZ B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

-- everything else
describe columnDecomp;
update COLDECOWINES A set
    VINTAGE = B.VINTAGE,
    PROD_WINE_NAME = B.PROD_WINE_NAME,
    GEO_INT = B.GEO_INT,
    VOL = B.VOL,
    PRICE = B.PRICE
from
    columnDecomp B
where
    A.LINE_NUM_TOT = B.LINE_NUM_TOT;

select * from COLDECOWINES;
