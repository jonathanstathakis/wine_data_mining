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

## Relationships

### Document to Line

There are two core entitites in this ETL: the input document and the run parameters. If we represent the run parameters as the run datetime, then we simply need a method of identifying the input document and the run datetime. The input document can be represented by the publication date of the document and the input file path. Therefore a Document entity may have a combination input filepath and publication date as its unique identifier.

One document may have many pages, and obtaining the publication date can only be achieved part-way through the pipeline, so starting with the filepath as unique identifer is wise. Addition of the publication date is useful for downstream processess but not for the ETL itself, therefore the filepath should be used to generate the primary key. The relationship should also go Document -> Page -> Line. A document has many Pages and many lines. So add that link.

A document has one publication date. Duckdb does not permit the update of rows that are used as relationship keys so
a sperate pubDate table is the simplest method. pubDate.doc_id to link it to the document.

In summary we can add a document identifier by generating it on the filepath then add a doc_id to pubDate and page. For brevity it would be sensible to also add it to Line or discard a Page table at all, but it's already there.

## TODO

- [ ] add table checks throughout ETL for sanity check and validation.
- [ ] recombine seperate dags into one and use task groups instead (?)
- [ ] re-evaluate schema.
