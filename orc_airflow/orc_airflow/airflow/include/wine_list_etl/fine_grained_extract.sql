/*
another dig at a finer grain extraction from the wine list as cant easily join with bepoz.
First it corrects errors in merged_text_ext (merged_text_ext_corr) then iterates (not literally) through a number of manually constructed tables to extract information from merged_text_ext_corr, adding the extracted information back to wineExt. The result is a cleaned and organised dataset.

This script should be run then the resulting wineExt table used in downstream operations.

Frankly this is a very rough attempt and should be rewritten at a later date. The primary 
problem has been handling extraction where one string, such as 'barossa' is a sub-string of
another, such as 'barossa valley', and matching the correct string to a given row. The
solution was to default to the longer string if there were multiple matches through an involved groupby, and repeating the logic for every field extraction where a competition
was present, but the repetition of code ballooned the size of this text file and is not
efficient, neither in runtime or in maintainability.

TODO: extract useful information from section, subsection and subsubsection.

*/

-- load the data
create or replace temp table raw_wine as select * from wine_list;
-- create a working table from the loaded input data
create or replace temp table wineExt as select * from raw_wine;
-- add columns to be populated with extracted information 
alter table wineExt add column if not exists producer varchar default null;
alter table wineExt add column if not exists dryness varchar default null;
alter table wineExt add column if not exists country varchar default null;
alter table wineExt add column if not exists state varchar default null;
alter table wineExt add column if not exists region varchar default null;
alter table wineExt add column if not exists subregion varchar default null;
alter table wineExt add column if not exists commune varchar default null;
alter table wineExt add column if not exists vineyard varchar default null;
alter table wineExt add column if not exists varieties varchar [] default [];
alter table wineExt add column if not exists merged_text_ext_corr varchar default null;
alter table wineExt add column if not exists style varchar default null;
alter table wineExt add column if not exists classification varchar default null;
alter table wineExt add column if not exists volume varchar default null;
alter table wineExt add column if not exists series varchar default null;
alter table wineExt add column if not exists variety varchar default null;


-- merged_text_ext has many errors which will need to be corrected during extraction
-- this sets a working column as the merged_text_ext values which will then be corrected as we go
update wineExt set merged_text_ext_corr = a.merged_text_ext
from wineExt as a
where wineExt.pk = a.pk;

-- start correcting the text.
update wineExt
set
merged_text_ext_corr=merged_text_ext_corr
                      .replace('- ','-')
                      .replace('gibert','gilbert')
                      .replace('brü ndlmayer','bründlmayer')
                      .replace(' .', '.')
                      .replace('sp inifex','spinifex')
                      .replace(' ’','’')
                      .replace('jint aro','jintaro')
                      .regexp_replace('^now\s','n.o.w. ')
                      .replace('andr é', 'andré')
                      .replace('mathe w', 'mathew')
                      .replace('mathew','matthew')
                      .replace('valle y', 'valley')
                      .replace('by. ott','by ott')
                      .replace('pierre-yves-colin-morey','pierre-yves colin-morey')
                      .replace('domaine de la grand’ cour', 'domaine de la grand’cour')
                      .replace('thé venet','thévenet')
                      .replace('a rnaud','arnaud')
                      .replace('’ ','’')
                      .replace('moun t', 'mount')
                      .replace('- ','-')
                      .replace('extra brut','extra-brut')
                      .regexp_replace('\stas(\s|$)', 'tasmania')
                      .replace('müller-cat oir','müller-catoir')
                      .replace(',tasmania1.5l', ', tasmania 1.5l')
                      .replace('tasmania blend','tasmania')
                      .replace('sadie family blend','sadie family')
                      .replace('s avagnin','savagnin')
                      .replace(' i t', ' it')
                      .replace('mourvè dre','mourvèdre')
                      .replace('mal bec','malbec')
                      .replace('malb ec','malbec')
                      .replace('ne rello','nerello')
                      .replace('xin omavro', 'xinomavro')
                      .replace('bl end','blend')
                      .replace('t oscana','toscana')
                      .replace('schist/limestone','')
                      .replace('schist/limestone','')
                      .replace('clay/schist','')
                      .replace('clay/schist','')
                      .replace('tasmania`','tasmania')
                      .replace('belliviè re','bellivière')
                      .replace('‘riserva’','riserva')
                      .replace('y.','y')


;

-- finally, add a column to be broken down with the extracted text, indicating information
--  that has not been retrieved.

alter table wineExt add column if not exists leftover_text varchar default null;
update wineExt set leftover_text = a.merged_text_ext_corr from wineExt as a
where wineExt.pk = a.pk;

-- now for some bulk updates.
-- champagnes 'section' = region
update wineExt set region = a.subsection
from wineExt as a
where wineExt.pk = a.pk and wineExt.subsection = 'champagne';

-- all champagne 'subsubsection' also = subregion
update wineExt
set
  subregion = a.subsubsection
from
  wineExt as a
where
  wineExt.section = 'sparkling wine'
and
  wineExt.subsection = 'champagne'
and
  wineExt.pk = a.pk;

-- start on producers




CREATE or replace TABLE producerRef(name TEXT);
-- Or insert via CSV, script, or VALUES
INSERT INTO producerRef VALUES 
  ('vouette et sorbée'),
  ('cedric bouchard'),
  ('pascal agrapart'),
  ('agrapart & fils'),
  ('ulysse collin'),
  ('jacques selosse'),
  ('dom pérignon'),
  ('krug'),
  ('pertois-moriset'),
  ('jérôme prévost'),
  ('larmandier-bernier'),
  ('egly-ouriet'),
  ('frédéric savart'),
  ('bérêche et fils'),
  ('charles heidsieck'),
  ('henriot'),
  
  ('piper-heidsieck'),
  ('taittinger'),
  ('r.pouillon'),
  ('deutz'),
  ('jean michel'),
  ('françoise bedel'),
  ('jacquesson'),
  ('gilbert'),
  ('nadeson collis'),
  ('oakridge'),
  ('arras'),
  ('daosa'),
  ('deviation road'),
  ('voyager estate'),
  ('clonakilla'),
  ('eden road'),
  ('mount majura'),
  ('ocean aged'),
  ('best’s'),
  ('crawford river'),
  ('mac forbes'),
  ('domaine simha'),
  ('frogmore creek'),
  ('glaetzer-dixon'),
  ('dc shaw'),
  ('grosset'),
  ('ministry of clouds'),
  ('mount horrocks'),
  ('wines by kt'),
  ('henschke'),
  ('pewsey vale'),
  ('woods crampton'),
  ('frankland estate'),
  ('greywacke'),
  ('pegasus bay'),
  ('jintaro yura'),
  ('trimbach'),
  ('marcel deiss'),
  ('bründlmayer'),
  ('nikolaihof'),
  ('veyder-malberg'),
  ('heymann-löwenstein'),
  ('dr. loosen'),
  ('a.christmann'),
  ('dr bürklin-wolf'),
  ('jülg'),
  ('weingut rebholz'),
  ('gunderloch'),
  ('müller-catoir'),
  ('keller'),
  ('wittmann'),
  ('schloss johannisberg'),
  ('hofgut falkenstein'),
  ('joh. jos. prüm'),
  ('selbach oster'),
  ('tyrrell’s'),
  ('thomas wines'),
  ('spinifex'),
  ('ocean eight'),
  ('hoddles creek'),
  ('haddow + dineen'),
  ('domaine de l’envol'),
  ('de salis'),
  ('josh cooper'),
  ('mount mary'),
  ('terre à terre'),
  ('domaine didier dagueneau'),
  ('claude riffault'),
  ('domaine gérard boulay'),
  ('pascal cotat'),
  ('ashes & diamonds'),
  ('vinea marson'),
  ('clarence house'),
  ('jintaro yura'),
  ('poppelvej'),
  ('mmad'),
  ('l.a.s. vino'),
  ('wines of merritt'),
  ('n.o.w.'),
  ('millton'),
  ('domaine belargus'),
  ('domaine guiberteau'),
  ('f. duveau'),
  ('nicolas joly'),
  ('françois chidaine'),
  ('domaine taille aux loups'),
  ('huet'),
  ('naudé'),
  ('gabriëlskloof'),
  ('the sadie family'),
  ('vinden'),
  ('sherrah'),
  ('by farr'),
  ('inkwell'),
  ('andré perret'),
  ('david duclaux'),
  ('r. rostaing'),
  ('domaine collotte'),
  ('thick as thieves'),
  ('la violetta'),
  ('valdesil valdeorras'),
  ('benito ferrara'),
  ('crft'),
  ('place of changing winds'),
  ('yeringberg'),
  ('marc sorrel'),
  ('ktima zafeirakis'),
  ('pierre luneau-papin'),
  ('iggy'),
  ('michael hall'),
  ('claude quenard et fils'),
  ('domaine de beaurenard'),
  ('soumah'),
  ('domaine du pélican'),
  ('szepsy'),
  ('seppeltsfield'),
  ('gullyview estate'),
  ('ló pez de heredia'),
  ('renzaglia wines'),
  ('matthew atallah'),
  ('nick o’leary'),
  ('eastern peake'),
  ('bannockburn'),
  ('irrewarra'),
  ('wickhams road'),
  ('latta'),
  ('tarrington'),
  ('bindi'),
  ('cobaw ridge'),
  ('elanto vineyard'),
  ('montalto'),
  ('moorooduc estate'),
  ('ben haines'),
  ('giant steps'),
  ('timo mayer'),
  ('wantirna estate'),
  ('lowestoft'),
  ('tolpuddle'),
  ('pooley'),
  ('sailor seeks horse'),
  ('les fruits'),
  ('moorak'),
  ('shaw + smith'),
  ('tapanappa'),
  ('cullen'),
  ('deep woods estate'),
  ('domaine naturaliste'),
  ('leeuwin estate'),
  ('pyramid valley'),
  ('moss wood'),
  ('hubert lamy'),
  ('fabien coche'),
  ('jane eyre'),
  ('billaud-simon'),
  ('séguinot-bordet'),
  ('vincent dauvissat'),
  ('christian moreau'),
  ('albert bichot'),
  ('samuel billaud'),
  ('pierre girardin'),
  ('pierre-yves colin-morey'),
  ('antoine jobard'),
  ('benjamin leroux'),
  ('caroline morey'),
  ('mee godard'),
  ('domaine thibert'),
  ('jerôme arnoux'),
  ('mayacamas'),
  ('franchetti'),
  ('la petite mort'),
  ('les arches de bellivière'),
  ('joshua cooper'),
  ('praeter'),
  ('domaine tempier'),
  ('tour du bon'),
  ('by ott'),
  ('jérôme arnoux'),
  ('maison saint aix'),
  ('domaine de la mordorée'),
  ('ochota barrels'),
  ('windy view'),
  ('lark hill'),
  ('bass phillip'),
  ('garden of earthly delights'),
  ('shadowfax'),  
  ('william downie'),
  ('kooyong'),
  ('onannon'),
  ('paradigm hill'),
  ('yabby lake'),
  ('yal yal estate'),
  ('picardy'),
  ('kumeu river'),
  ('felton road'),
  ('jean-marie fourrier'),
  ('joseph drouhin'),
  ('domaine de la romanée-conti') ,
  ('domaine génot-boulanger'),
  ('traviarti'),
  ('jasper hill'),
  ('lethbridge'),
  ('cascina delle rose'),
  ('ceretto'),
  ('gaja'),
  ('la spinetta'), 
  ('massolino'),
  ('giacomo conterno'),
  ('giulia negri'),
  ('luigi pira'),
  ('mascarello'),
  ('paolo scavino'),
  ('sandrone'),
  ('sottimano'),
  ('dal zotto'),
  ('the other wine co'),
  ('il marroneto'),
  ('pian dell’orino'),
  ('uccelliera'),
  ('isole e olena'),
  ('le corti'),
  ('le ragnaie'),
  ('il poggione'),
  ('sinapius'),
  ('empire of dirt'),
  ('farr rising'),
  ('louis boillot'),
  ('camille mélinand'),
  ('domaine de la grand’cour'),
  ('thévenet'),
  ('chalmers'),
  ('quintarelli'), 
  ('breaking ground'),
  ('bacchus'),
  ('mchenry hohnen'),
  ('swinney'),
  ('tim smith'),
  ('yangarra'),
  ('hewitson'),
  ('tenuta terre nere'),
  ('eldorado road'),
  ('sassafras'),
  ('thymiopoulos'),
  ('gertie'),
  ('parley'),
  ('arnaud lambert'),
  ('antoine sanzay'),
  ('robert stein'),
  ('bloodwood wines'),
  ('yarra yering'),
  ('yalumba'),
  ('hickinbotham'),
  ('ashbrook estate'), 
  ('domaine de trévallon'),
  ('carruades de lafite'),
  ('le petit mouton de mouton rothschild'),
  ('tenuta dell’ornellaia'),
  ('kin by alkina'),
  ('alkina'),
  ('charles melton'),
  ('tscharke'),
  ('munda'),
  ('bourke & travers'),
  ('bulman'),
  ('serafino'),
  ('willunga'),
  ('rusden'),
  ('xavier vignon'),
  ('domaine la bouîssiere'),
  ('clos mogador'),
  ('cullarin'),
  ('mount pleasant'),
  ('syrahmi'),
  ('mount langi ghiran'),
  ('dalwhinnie'),
  ('bloody hill villages'),
  ('levantine hill'),
  ('hentley farm'),
  ('torbreck'),
  ('clarendon hills'),
  ('domaine rostaing'),
  ('jean-luc jamet'),
  ('alain graillot'),
  ('paul jaboulet aîné'),
  ('domaine de la grange des pères'),
  ('jonata'),
  ('margan'),
  ('château filhot'),
  ('clos lapeyre'),
  ('domaine des baumard'),
  ('j.j. prüm'),
  ('pfeiffer'),
  ('stanton & killeen'),
  ('chambers'),
  ('lindeman’s'),
  ('equipo navazos'),
  ('toro albala')

-- check pycm hyphenation, matthew atalah spelling.

-- add producer column into table
  ;

UPDATE wineExt AS a
SET producer= b.name
FROM producerRef as b
WHERE POSITION(b.name IN a.merged_text_ext_corr) > 0;


-- add dryness column
create or replace table drynessRef (descriptor varchar);

insert into DrynessRef values 
  ('extra-brut'),
  (' brut'),
  ('brut nature'),
  ('demi-sec'),
  ('off-dry'),
  ('dry')
;

UPDATE wineExt AS a
SET dryness = b.descriptor
FROM drynessRef as b
WHERE POSITION(b.descriptor IN a.merged_text_ext_corr) > 0;

-- commune
create or replace table communeRef (name varchar);
insert into communeRef values 
  ('barséquanais'),
  ('celles-sur-ource'),
  ('avize/ambonnay'),
  ('avize'),
  ('ambonnay'),
  ('le mesnil-sur-oger'),
  ('congy'),
  ('mesnil/puisieulx'),
  ('vertus'),
  ('barbonne-fayel'),
  ('ecueil'),
  ('grande montagne'),
  ('reims'),
  ('tauxières'),
  ('vrigny'),
  ('aÿ'),
  ('bisseuil'),
  ('coteaux d’epernay'),
  ('crouttes-sur-marne'),
  ('dizy'),
  ('trigny'),
  ('gueux'),
  ('fleurie'),
  ('moulin-à-vent'),
  ('régnié'),
  ('naoussa'),
  ('tyrnavos'),
  ;

UPDATE wineExt AS a
SET commune= b.name
FROM communeRef as b
WHERE POSITION(b.name IN a.merged_text_ext_corr) > 0;

-- style
create or replace table styleRef (name varchar);
insert into styleRef 
    values
        ('rosé'),
        ('blanc de blancs'),
        ('vintage'),
        ('late disgorged'),
        ('blanc'),
        ('rouge')
;
      

UPDATE wineExt AS a
SET style= b.name
FROM styleRef as b
WHERE POSITION(b.name IN a.merged_text_ext_corr) > 0;

-- regions
create or replace table regionRef (name varchar);
insert into regionRef
    values
        ('champagne'),
        ('orange'),
        ('henty'),
        ('king valley'),
        ('yarra valley'),
        ('pipers river'),
        ('adelaide hills'),
        ('margaret river'),
        ('canberra district'),
        ('grampians'),
        ('coal river valley'),
        ('clare valley'),
        ('eden valley'),
        ('alsace'),
        ('marlborough'),
        ('north canterbury'),
        ('pfalz'),
        ('kamptal'),
        ('wachau'),
        ('mosel'),
        ('rheinhessen'),
        ('rheingau'),
        ('hunter valley'),
        ('mornington peninsula'),
        ('mornington and king valley'),
        ('tamar river'),
        ('pyrenees'),
        ('gisborne'),
        ('mclaren vale'),
        ('swan valley'),
        ('alpine valleys'),
        ('derwent valley'),
        ('western cape'),
        ('bourgogne'),
        ('great southern'),
        ('napa valley'),
        ('loire valley'),
        ('rhône valley'),
        ('heathcote'),
        ('barossa valley'),
        ('barossa'),
        ('ballarat'),
        ('geelong'),
        ('gippsland'),
        ('macedon ranges'),
        ('tumbarumba'),
        ('bathurst'),
        ('huon valley'),
        ('jurançon'),
        ('rutherglen'),
        ('bordeaux'),
        ('wrattonbully'),
        ('provence'),
        ('pemberton'),
        ('fleurieu peninsula'),
        ('auckland'),
        ('priorat'),
        ('tokaji'),
        ('savoie'),
        ('jura'),
        ('granite belt'),
        ('mornington peninsula'),
        ('central otago'),
        ('beechworth'),
        ('piemonte'),
        ('toscana'),
        ('sicily'),
        ('veneto'),
        ('langhorne creek'),
        ('mudgee'),
        ('great western'),
        ('macdeon/pyrenees'),
        ('multi-regional'),
        ('coal river/derwent valley'),
        ('languedoc'), 
        ('clare valley')
;

create or replace temp table regionsJoined as (
    with ranked as (
    select 
        w.pk,
        w.merged_text_ext_corr,
        a.name as region,
        dense_rank()  over (partition by w.pk order by len(region))::int as d_rank,
    from
      (
          select
              pk,
              merged_text_ext_corr
          from
              wineExt
    ) as w
    left join
        regionRef as a
    on
      REGEXP_MATCHES(w.merged_text_ext_corr, '(\b|  )' || a.name || '( |$|,)')
    order by w.pk
),
  rank_max as (
select
    pk,
    max(region) as region,
    max(d_rank) as longest_match
from
    ranked 
group by
    pk
)

select
  a.pk,
  a.merged_text_ext_corr,
  b.region
from
  wineExt as a
inner join
  rank_max as b
on a.pk=b.pk);

UPDATE wineExt AS a
SET region= b.region
from
  regionsJoined as b
where
  a.pk=b.pk
and b.region is not null
;


-- state 
create or replace table stateRef (name varchar);
insert into stateRef 
    values
        ('nsw'),
        ('vic'),
        ('tasmania'),
        ('wa'),
        ('sa'),
        ('california'),
        ('qld'),
        ('macedonia'),
        ('larissa'),
;

UPDATE wineExt AS a
SET state= b.name
FROM stateRef as b

WHERE REGEXP_MATCHES(a.merged_text_ext_corr, '\b' || b.name || '($|\s|,)');
-- WHERE POSITION(b.name IN a.merged_text_ext_corr) > 0;


-- classifications 
create or replace table classRef (name varchar);
insert into classRef 
    values
      ('spätlese trocken'),
      ('otw erste lage'),
      ('trocken'),
      ('kabinett'),
      ('spätlese'),
      ('gg'),
      ('gc'),
      ('federspiel'),
      ('1 er cru'),
      ('grand cru'),
      ('brunello di montalcino'),
      ('brunello di montalcino riserva'),
      ('chianti classico'),
      ('rosso di montalcino'),
      ('amarone della valpolicella'),
      ('valpolicella superiore'),
      ('dolcetto d''alba'),
      ('toscana rosso'),
      ('toscana riserva'),
      ('auslese'),
      ('ratafia'),
      ('greco di tufo'),
      ('chignin-bergeron'),
      ('topaque'),
      ('palo cortado'),
      ('amontillado'),
      ('tawny'),
      ('port'),
      ('vdf')

;

create or replace temp table classJoined as (
    with merged_text as (
          select
              pk,
              merged_text_ext_corr
          from
              wineExt
    ),

    joined as (
        select 
            w.pk,
            w.merged_text_ext_corr,
            a.name as classification,
        from 
            merged_text as w
        left join
            classRef as a
        on
          REGEXP_MATCHES(w.merged_text_ext_corr, '(\b|  )' || a.name.replace('(','\(').replace(')','\)') || '( |$|,)')
        order by w.pk
    ),
    -- rank potential matches by length, longer is a higher rank
    ranked as (
        select
            pk,
            merged_text_ext_corr,
            classification,
            dense_rank()  over (
                                partition by pk order by len(classification)
                                  )::int as length_rank
        from
            joined
    ),
-- filtering for highest rank
    rank_max as (
        select
            pk,
          max(length_rank) as max_length_rank
        from
            ranked
        group by
            pk
        having max(length_rank)
        ), 
  -- groupby filter selecting the match whose rank is highest
    rank_joined as (
    select
      a.pk,
      a.max_length_rank,
      b.classification
    from
      rank_max as a
    left join
      ranked as b
    on
      a.pk = b.pk
    and 
      a.max_length_rank = b.length_rank
    ),
    -- end result
    result as (
    select
      a.pk,
      b.merged_text_ext_corr,
      a.classification,
    from
      rank_joined as a
    left join
      wineExt as b
    on a.pk=b.pk)

select 
    * 
from 
result
)
;


UPDATE wineExt AS a
SET classification = b.classification
from
  classJoined as b
where
  a.pk=b.pk
and b.classification is not null
;

-- vineyard 
create or replace table vineyardRef (name varchar);
insert into vineyardRef 
    values
      ('kirchspiel'),
      ('coulée de serrant'),
      ('naboth''s'),
      ('la tache'),
      ('san lorenzo')
;

UPDATE wineExt AS a
SET vineyard= b.name
FROM vineyardRef as b
WHERE POSITION(b.name IN a.merged_text_ext_corr) > 0;

-- volume
create or replace table volumeRef(name varchar);
insert into volumeRef 
    values
      ('1.5l'),
      ('750ml'),
      ('375ml'),
      ('500ml'),
;

UPDATE wineExt AS a
SET volume= b.name
FROM volumeRef as b
WHERE POSITION(b.name IN a.merged_text_ext_corr) > 0;

-- series, for example jerome prevost la closerie: les beguines
create or replace table seriesRef(name varchar);
insert into seriesRef 
    values
        ('la closerie'),
        ('clos de ste. anne'),
        ('marli russell by mount mary'),
        ('dhillon by bindi'),
        ('farvie')
;

UPDATE wineExt AS a
SET series= b.name
FROM seriesRef as b
WHERE POSITION(b.name IN a.merged_text_ext_corr) > 0;

-- subregion, for example poilly fumé
create or replace table subregionRef(name varchar);
insert into subregionRef 
    values
        ('coteaux champenois'),
        ('montlouis sur loire'),
        ('vin de france (vouvray)'),
        ('anjou'),
        ('bolgheri superiore'),
        ('saumur-champigny'),
        ('saumur'),
        ('savennières'),
        ('vouvray'),
        ('sancerre'),
        ('pouilly-fumé'),
        ('condrieu'),
        ('stellenbosch'),
        ('swartland'),
        ('rioja alta'),
        ('châteauneuf-du-pape'),
        ('hermitage'),
        ('chablis'),
        ('muscadet sèvre et maine'),
        ('saint-aubin'),
        ('meursault'),
        ('chassagne-montrachet'),
        ('beaujolais'),
        ('mâcon-prissé'),
        ('mount veeder'),
        ('auxey-duresses'),
        ('bandol'),
        ('tavel'),
        ('marsannay'),
        ('vosne-romanée'),
        ('pommard'),
        ('volnay'),
        ('monthélie'),
        ('santenay'),
        ('barbaresco'),
        ('romanée-st-vivant'),
        ('beaune'),
        ('chambolle-musigny'),
        ('barolo'),
        ('langhe'),
        ('ballard canyon santa ynez valley'),
        ('waipara valley'),
        ('etna'),
        ('alba'),
        ('champigny'),
        ('pauillac'),
        ('les baux-de-provence'),
        ('sauternes'),
        ('gigondas'),
        ('hautes-côtes-de-beaune'),
        ('côte-rôtie'),
        ('crozes hermitage'),
        ('santa ynez valley'),
        ('frankland river'),
        ('darling')

;

/*
given 2 possible matches to a string where 1 is longer than the other,
get both and compare the length, preferencing the longer match.

1. 
*/
create or replace temp table subregionsJoined as (
    with merged_text as (
          select
              pk,
              merged_text_ext_corr
          from
              wineExt
    ),

    joined as (
        select 
            w.pk,
            w.merged_text_ext_corr,
            a.name as subregion,
        from 
            merged_text as w
        left join
            subregionRef as a
        on
          REGEXP_MATCHES(w.merged_text_ext_corr, '(\b|  )' || a.name.replace('(','\(').replace(')','\)') || '( |$|,)')
        order by w.pk
    ),
    -- rank potential matches by length, longer is a higher rank
    ranked as (
        select
            pk,
            merged_text_ext_corr,
            subregion,
            dense_rank()  over (
                                partition by pk order by len(subregion)
                                  )::int as length_rank
        from
            joined
    ),
-- filtering for highest rank
    rank_max as (
        select
            pk,
          max(length_rank) as max_length_rank
        from
            ranked
        group by
            pk
        having max(length_rank)
        ), 
  -- groupby filter selecting the match whose rank is highest
    rank_joined as (
    select
      a.pk,
      a.max_length_rank,
      b.subregion
    from
      rank_max as a
    left join
      ranked as b
    on
      a.pk = b.pk
    and 
      a.max_length_rank = b.length_rank
    ),
    -- end result
    result as (
    select
      a.pk,
      b.merged_text_ext_corr,
      a.subregion,
    from
      rank_joined as a
    left join
      wineExt as b
    on a.pk=b.pk)

select 
    * 
from 
result
)
;

-- select * from subregionsJoined;


UPDATE wineExt AS a
SET subregion= b.subregion
from
  subregionsJoined as b
where
  a.pk=b.pk
and b.subregion is not null
;


-- country, for example fr, ger
create or replace table countryRef(name varchar);
insert into countryRef 
    values
        ('south africa'),
        ('austria'),
        ('fr'),
        ('ger'),
        ('germany'),
        ('esp'),
        ('it'),
        ('hungary'),
        ('usa'),
        ('nz'),
        ('gr'),
;

UPDATE wineExt AS a
SET country= b.name
FROM countryRef as b
WHERE REGEXP_MATCHES(a.merged_text_ext_corr, '\b' || b.name || '($|\s)');

-- variety, for example arneis
create or replace table varietyRef(name varchar);
insert into varietyRef 
    values
      ('sauvignon blend'),
      ('sauvignon/semillon'),
      ('semillon'),
      ('arneis'),
      ('colombard'),
      ('aligoté'),
      ('gewürztraminer blend'),
      ('godello'),
      ('greco'),
      ('grüner veltliner'),
      ('marsanne blend'),
      ('marsanne/roussanne'),
      ('roussanne/marsanne'),
      ('marsanne'),
      ('roussanne'),
      ('rhône blend'),
      ('savagnin'),
      ('vermentino'),
      ('viura'),
      ('rebula'),
      ('malagousia'),
      ('furmint'),
      ('agliancio'),
      ('barbera'),
      ('cinsault/grenache'),
      ('cinsault'),
      ('corvina blend'),
      ('mencia'),
      ('mondeuse'),
      ('malbec blend'),
      ('mourvèdre'),
      ('nerello mascalese'),
      ('nero d''avola'),
      ('sagrantino'),
      ('xinomavro'),
      ('pinot meunier'),
      ('dolcetto'),
      ('nero d’avola blend'),
      ('tempranillo'),
      ('muscat'),
      ('marsanne blend'),
      ('riesling'),

;

create or replace temp table varietyJoined as (
    with merged_text as (
          select
              pk,
              merged_text_ext_corr
          from
              wineExt
    ),

    joined as (
        select 
            w.pk,
            w.merged_text_ext_corr,
            a.name as variety,
        from 
            merged_text as w
        left join
            varietyRef as a
        on
          REGEXP_MATCHES(w.merged_text_ext_corr, '(\s|  |^)' || a.name.replace('(','\(').replace(')','\)') || '(\\b| |$|,)')
        order by w.pk
    ),
    -- rank potential matches by length, longer is a higher rank
    ranked as (
        select
            pk,
            merged_text_ext_corr,
            variety,
            dense_rank()  over (
                                partition by pk order by len(variety)
                                  )::int as length_rank
        from
            joined
    ),
-- filtering for highest rank
    rank_max as (
        select
            pk,
          max(length_rank) as max_length_rank
        from
            ranked
        group by
            pk
        having max(length_rank)
        ), 
  -- groupby filter selecting the match whose rank is highest
    rank_joined as (
    select
      a.pk,
      a.max_length_rank,
      b.variety
    from
      rank_max as a
    left join
      ranked as b
    on
      a.pk = b.pk
    and 
      a.max_length_rank = b.length_rank
    ),
    -- end result
    result as (
    select
      a.pk,
      b.merged_text_ext_corr,
      a.variety,
    from
      rank_joined as a
    left join
      wineExt as b
    on a.pk=b.pk)

select 
    * 
from 
result
)
;

-- select * from subregionsJoined;


UPDATE wineExt AS a
SET variety= b.variety
from
  varietyJoined as b
where
  a.pk=b.pk
and b.variety is not null
;

-- cuvee, for example alpilles 
create or replace table cuveeRef(name varchar);
insert into cuveeRef 
    values
      ('alpilles'),
      ('100'),
      ('demi'),
      ('terra d’uva'),
      ('ngadjuri and peramangk country')
;

create or replace temp table cuvee_nameJoined as (
    with merged_text as (
          select
              pk,
              merged_text_ext_corr
          from
              wineExt
    ),

    joined as (
        select 
            w.pk,
            w.merged_text_ext_corr,
            a.name as cuvee_name,
        from 
            merged_text as w
        left join
            cuveeRef as a
        on
          REGEXP_MATCHES(w.merged_text_ext_corr, '(\b|  )' || a.name.replace('(','\(').replace(')','\)') || '( |$|,)')
        order by w.pk
    ),
    -- rank potential matches by length, longer is a higher rank
    ranked as (
        select
            pk,
            merged_text_ext_corr,
            cuvee_name,
            dense_rank()  over (
                                partition by pk order by len(cuvee_name)
                                  )::int as length_rank
        from
            joined
    ),
-- filtering for highest rank
    rank_max as (
        select
            pk,
          max(length_rank) as max_length_rank
        from
            ranked
        group by
            pk
        having max(length_rank)
        ), 
  -- groupby filter selecting the match whose rank is highest
    rank_joined as (
    select
      a.pk,
      a.max_length_rank,
      b.cuvee_name
    from
      rank_max as a
    left join
      ranked as b
    on
      a.pk = b.pk
    and 
      a.max_length_rank = b.length_rank
    ),
    -- end result
    result as (
    select
      a.pk,
      b.merged_text_ext_corr,
      a.cuvee_name,
    from
      rank_joined as a
    left join
      wineExt as b
    on a.pk=b.pk)

select 
    * 
from 
result
)
;


UPDATE wineExt AS a
SET cuvee_name = b.cuvee_name
from
  cuvee_nameJoined as b
where
  a.pk=b.pk
and b.cuvee_name is not null
;


-- TODO: add these to cuveeRef where appropriate
-- more fine-grained modifications
update wineExt
  set
    leftover_text = null
  where
    pk = 20;

update wineExt
  set
    cuvee_name = 'les crayères, vv, bdn',
    leftover_text = null
  where
    pk = 17;
update wineExt
  set
    cuvee_name = 'grand cru vp',
    leftover_text = null
  where
    pk = 18;

update wineExt
  set
    cuvee_name = 'triolet',
  where
    pk = 122;

update wineExt
  set
    cuvee_name = 'blend no.5',
  where
    pk = 131;

update wineExt
  set
    cuvee_name = 'clos du bourg',
  where
    pk = 152;

update wineExt
  set
    cuvee_name = 's.r.h',
  where
    pk = 197;

update wineExt
  set
    cuvee_name = 'la grange',
  where
    pk = 176;

update wineExt
  set
    cuvee_name = 'terre siciliane',
  where
    pk = 272;

update wineExt
  set
    variety = 'aglianico'
where
  pk = 392;

update wineExt
set
  classification = 'dolcetto d''alba',
where
  pk = 401;

update
    wineExt
set
    variety = 'mourvèdre blend'
where
    pk = 407;

update
    wineExt
set
    region = 'macedon/pyrenees'
where
    pk = 440;

update
  wineExt
set
  cuvee_name = 'demi by syrahmi'
where
  pk = 500;

update
  wineExt
set
  region = 'coal river/derwent valley'
where
  pk = 518;

update
  wineExt
set
  subregion = 'vdp de l’hérault'
where
  pk = 537;

update
  wineExt
set
  cuvee_name = 'quarts de chaume'
where
  pk = 549;

update
  wineExt
set
  volume = '1500ml'
where
  pk = 553;

update
  wineExt
set
  cuvee_name = '21yo tawny'
where
  pk = 555;

update
    wineExt
set
    commune = 'sanlúcar'
where
    pk = 549;

update
    wineExt
set
    style = 'noble'
where
    pk = 545;

update
    wineExt
set
    commune='montilla-moriles'
where
    pk = 560;

update
    wineExt
set
    commune='montilla-moriles',
    style='oloroso'
where
    pk = 561;

update
    wineExt
set
    commune='montilla-moriles',
    style='p.x.'
where
    pk = 562;

update wineExt set variety='chenin blanc', style='late harvest' where pk = 544;
update wineExt set vintage = 'nv' where pk = 561;
update wineExt set cuvee_name = 'la bota 102 florpower mmx' where pk = 558;

-- extract all found text from leftover_text to see whats left.
update wineExt as a
set leftover_text = b.leftover_text
                      .replace(ifnull(b.series,''),'')
                      .replace(b.producer,'')
                      .replace(ifnull(b.cuvee_name,''),'')
                      .replace(ifnull(b.dryness,''),'')
                      .replace(ifnull(b.style,''),'')
                      .replace(ifnull(b.classification,''),'')
                      .replace(ifnull(b.subregion,''),'')
                      .replace(ifnull(b.region,''),'')
                      .replace(ifnull(b.vineyard,''),'')
                      .replace(ifnull(b.volume,''),'')
                      .replace(ifnull(b.country,''),'')
                      .replace(ifnull(b.variety,''),'')
                      .replace(ifnull(b.commune,''),'')
                      .replace(ifnull(b.state,''),'')
                      .replace(ifnull(b.vintage,''),'')

                      .replace('  ','')
                      .regexp_replace(',','')
                      .regexp_replace('[|.|,|\|/|#|!|$|%|\^|&|\*|;|:|{|}|=|\-|_|`|~|\(|\)|]|“|’','')
                      .replace('’','')
                      .replace('(','')
                      .replace(')','')
                      .replace('‘','')
                      .replace(',','')
                      .trim()
                      .nullif('')

from wineExt as b
where
  a.pk = b.pk;

-- add extracted values to wineList;

update wine_list as a
    set
        producer = b.producer,
        dryness = b.dryness,
        country = b.country,
        state = b.state,
        region = b.region,
        subregion = b.subregion,
        commune = b.commune,
        vineyard = b.vineyard,
        style = b.style,
        classification =b.classification,
        volume = b.volume,
        series = b.series,
        variety = b.variety
    from
        wineExt as b
    where
        a.pk = b.pk
;

