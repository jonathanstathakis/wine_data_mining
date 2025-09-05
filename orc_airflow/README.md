Need documentation.

1. extracts document data
2. adds line numbers
3. aggregates lines together
4. creates a wine list staging table
5. labels the sections
6. decomposes the line text
7. loads the wine list
8. performs a fine grained text extraction
9. creates category and subcategory tables
10. exports section, subsection and wine list tables
11. cleans up database.

- should be able to move the export steps to a dependent DAG.

The result is a database at duckdb_conn_id with
a number of tables:

Section:
the sections of the wine list.

      section: names of each section.
      section_order: the linear order of the section as an integer.

SubSection:
the subsections of the wine list.

      section (fk): the Section.section the subsection row belongs to.
      subsection (pk): the subsection label.
      subsection_order: the absolute linear order of the subsetion irrespective of section.

SubSubSection:
the subsubsections of the wine list, in a heirarchy below Section and Subsection.

      Note: currently empty.

WINELISTSTAGING: LINE_NUM_TOT: PAGE_NUM: LINE_NUM: SECTION: SUBSECTION: SUBSUBSECTION: MERGED_TEXT: VINTAGE: BASE_YEAR:
CUVEE_NAME:
DISGORG_YEAR:
PRICE:
MERGED_TEXT_EXT:

aggregated:

classRef:
name:

communeRef:
name:

countryRef:
name:

cuveeRef:
name:

decomposeMergeText:
LINE_NUM_TOT:
vintage:
base_year:
cuvee_name:
disgorg_year:
price:
merged_text_ext:

drynessRef:
descriptor:

pageLines:

pagesraw:

producerRef:
name:

regionRef:
name:

seriesRef:
name:

stateRef:
name

styleRef:
name:

subregionRef:
name:

varietyRef:
name:

vineyardRef:
name:

volumeRef:
name:

wine_list:
pk
line_num_tot
page_num
page_line_num
section
subsection
subsubsection
vintage
merged_text_ext
base_year
cuvee_name
disgorg_year
price
merged_text
producer
dryness
country
state
region
subregion
commune
vineyard
style
classification
volume
series
variety

The data lineage is as follows:

1. tables pagesraw and rect added to db.
2. add_line_numbers: identifies lines as groups of words within 4 units vertically and assigns line numbers as such.
   a. aggregate_lines: create merged strings and throw all other data into a json field.
   b. label_sections: create a table of wine list lines with section labels.

aggregated -> wineListLines through line_num_tot.

pageLines -> aggregated is a many to one join through page_num + line_num.

A cleanup could have a lineage through pageLines and aggregated to ensure normalization.
