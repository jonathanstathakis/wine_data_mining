
PRAGMA create_fts_index('bp_raw_loading','product_id','_comment', overwrite=1);

SELECT
    'Arras Blanc de Blancs Sparkling NV (750ml)' as sstr,
    fts_main_bp_raw_loading.match_bm25(a.product_id,sstr) AS score,
    a.product_id,
    a._comment
FROM  bp_raw_loading as a
WHERE score IS NOT NULL
ORDER BY score DESC
limit 5;
;

