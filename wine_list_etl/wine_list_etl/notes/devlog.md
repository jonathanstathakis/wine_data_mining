# wine_list_etl Dev Log

2025-07-06T10:26:00

A Stall in Parsing the Wine List.

Have managed to parse the champagne page successfully, however the variations in patterns and lack of delimiters in the still wines has made the task too difficult, too individual. There are two options now - revisit the pdf parsing routine to be column-aware (this can be based on the difference in x0, would require a window function, similar to how we've worked on observation time smoothing in the past), or binning, etc. The creation of a column_num column labelling the columns, followed by a seperation on inter-column delimiters. The second option is to create a number of tables of wine terms - regions, GI, PGI, PDO, etc. This is a potentially more fruitful task long-term as it would provide me with a useful set of information against which to validate parsed text, search terms, etc. A source of truth. However this is a very different approach, requiring research and understsanding of the systems used country by country. Considering there appears to be no central database providing a useful data format, we may have to proceed country by country, i.e. EU, USA, Australia, New Zealand, South Africa, etc. Problematic. Another option could be to parse the GuildSomm website, as they have lists of relevant regions by country. This is a trustworthy datasource as the community verifies and validates the data. 3 options. Frankly I think option 1 would be the most rewarding atm, and will attempt to do so.

2025-07-06T10:35:56

Detecting Columns.

Each wine page in the wine list contains a number of columns. For example The champagne page (page 6) contains 5 columns - vintage, (Producer, Cuvee name), Sweetness, village, Price. Still wines contain vintage, (Producer, Cuvee Name), (Region, Country/State), Volume, Price. Unfortunately this is not ALWAYS the case, as for example, Austrian and german riesling has an additional column for their respective quality wine heirarchies - OTW for Austria, VDP for Germany. Furthermore the text in these columns often overflows into the volume column. That is not a problem if we base column designation on x0, however. Thankfully, it appears that the presence of the quality column is respected within the other sections, if un-used/overflowed into. To label these columns, we would identify islands of x0. i.e., a column seperation is represented by a _larger than 1 space_ number of pixels, a columns words represented by a run of close-by x0. Start with Aussie riesling and generalise. See how we go. Get a result in 15 mins.

2025-07-06T23:43:14

Compromising Parsing in Favor of Progress

There's a non-zero chance that we'll be able to join the bepoz table and wine list table without 100% decomposition of the wine line. Thus in favor of haste, moving on to the fuzzy join stage of the pipeline is prudent. Once the most available fields are extracted (vintage, base vintage, cuvee name, price, disgorgement date) we can move on to the join. This will require wrapping both the bepoz pipeline and wine list pipeline in python functions and executing the pipeline in a 3rd project.
