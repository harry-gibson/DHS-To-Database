DHS hierarchical data extraction
---------------------------------------


This repository contains python code for parsing and data from the [Demographic and Health Surveys](https://dhsprogram.com) (DHS) Program into a relational database format, enabling large-scale pooled and/or cross sectional analyses of multiple surveys in a way that is both reproducible and repeatable.

This methodology and the code in this repository, and the associated reverse-engineering of the CSPro data format and the DHS survey schema, has been developed by Harry Gibson.

The methods developed and presented here have been used to collate and prepare datasets underpinning numerous published studies. [Here](doc/bibliography.md) is a list of the main ones I am aware of.


## About the surveys

The Demographic and Health Surveys (DHS) Program is a US-based and funded organisation that conducts surveys around the world and then makes those data available for research. Surveys are conducted around the developing world, from Afghanistan to Zimbabwe.

Each survey consists of at least several hundred questions that are asked of thousands of respondents. Eligible survey participants are women of reproductive age (defined as age 15-49) and, in surveys that include men, men aged 15-59. Questions cover subjects as diverse as how many motorbikes are owned by a household, to the type of treatment a child had for fever. 


## Survey structure - relational model

Questions relate to various different "levels" of subject: to a household and its properties (*"how many people live here?"*); to individual respondents - primarily women of child-bearing age, but also their male partners - (e.g.: *"how old are you?"*); and to children (*"have they been vaccinated?"*). 

However it is important to note that these are **all part of the same survey**: there is no such thing as a "household" survey as opposed to an "individual" survey; they are just different questions within the same overall piece of work. These terms do unfortunately get used quite commonly in relation to DHS data, causing confusion, but it is important to note this. More on this below. 

The questions are divided into sections (e.g. *"Respondent's Basic Data"*), which each have codenames such as *"REC01"*. **In relational database terms, these are tables**. 

Within each such section, the questions have codenames such as *"v010"* which correspond to a given question text such as *"Respondent's year of birth"*. **In relational database terms, these are columns**.

Each question may have a "valueset" defined, which determines the numerical values which are allowable responses to the question. This valueset may consist of a range, (e.g. 1:12 for a variable representing month-of-year) or specific values which code for certain textual answers (e.g. 1=Male, 2=Female).  **In relational database terms these are check constraints or foreign key constraints**.

As mentioned above, each table (and the questions therein) relates to a particular level of subject, i.e. they contain one row for each entity in that category. These categories are:
- Household (tables contain one row per physical household)
- Household member (tables contain one row per member of the household)
- Woman respondent (tables contain one row per individual woman personally responding to the survey)
- Child (tables contain one row per child who is aged under 5 and is the offspring of a woman respondent)
- Man respondent (tables contain one row per individual man personally responding to the survey)

### What this approach enables 

Key to the analyses that are made possible by this code is that responses from the different sections can (normally) be referenced to each other: e.g. the identifier for women's responses contains a reference to the identifier of the household they are in, and a reference to the same woman's information in the household-member tables. 

Thus we can construct queries that JOIN the data from the tables in these different survey sections. For example to create a dataset at household level that summarises the number of children in the household who have/have not been vaccinated; or a dataset at child level that contains information on the household's sanitation arrangements. 

Not only can we produce combined dataset extractions like this for a single survey, but we can create these datasets spanning across many surveys at once - this is subject to limitations discussed below.

## The point of this approach

Whilst such datasets can certainly be produced from the various downloadable DHS datasets by other, largely manual means, this tends to take a great deal of time, careful manual manipulation of huge (thousands of column) spreadsheets/dataframes, and is generally fraught with difficulty and prone to hard-to-detect errors.

Here, by contrast, because the ultimate mechanism for creating a particular extracted dataset is simply an SQL query, the process is repeatable, reproducible, and can be documented and made subject to source control.  Queries are fast to run and a large, complex extraction spanning the entire DHS survey database can typically be executed in a few minutes. Thus, additions and alterations to the extracted schema can be made quickly, and traceably, as a study develops.


## Limitation: Inter-survey consistency 

All DHS surveys are conducted based on a common framework of questions from which a given survey will take a subset. This framework has changed at various times over the years (giving a change in the core set of questions); these changes are termed "phases" by DHS. At present we are in phase 8. 

Each phase is a development of the prior one - generally new questions will be added and perhaps a smaller number of questions may be dropped; however, it is very unusual (but not impossible) for a given question (or the location of a question within the survey structure) to fundamentally change over time. That is, the question `V010` in section `REC01` will always mean `"Respondent's year of birth"` and never something else, and this is true for almost all (**but not all!**) questions. The converse is also generally true: the information `"Respondent's year of birth"` will almost always be found in `REC01 / V010`. 

This mapping *should* remain totally consistent within a survey phase, but *may* change between phases. In practice it remains consistent across phases, but questions may sometimes be dropped altogether or new ones added. A human-readable form of the standard question structure for surveys in a given phase is published by the DHS as a "Recode Manual" - e.g. the Phase 7 Recode Manual can be found [here](https://dhsprogram.com/pubs/pdf/DHSG4/Recode7_DHS_10Sep2018_DHSG4.pdf).

Any changes introduced within a phase may be stored in "country-specific" sections / questions. For example a new question *"Do you like flying kites"* may be introduced in a particular survey in a country-specific (non-standard) location. For the next phase of surveys it may be decided that penchant for kite-flying should be assessed more widely, and in subsequent surveys it would be allocated a consistent location in the structure.

The  implication of this is that before creating an SQL query to extract a common dataset across numerous surveys, you first need to check that the meaning of all the variables you plan to reference remains consistent across those surveys, and use filters (e.g. `CASE WHEN`...) if it does not. Likewise if a given piece of information occurs sometimes in country-specific locations and sometimes in a standard location, your query might need to combine those columns into a single output column (`COALESCE`...). Fortunately, thanks to the presence of the survey metadata tables, these checks are reasonably easy to undertake.


## Data availability and formats

The DHS data are available (to registered users) in their (mostly) standardised form as described above at individual response level. However the downloads listed are somewhat confusing. For example [this download page](https://dhsprogram.com/data/dataset/Benin_Standard-DHS_2017.cfm?flag=0) (for the Benin 2017 survey) lists several different data formats (Stata, SAS...) under each of several headings such as "Births Recode", "Children's Recode", "Household Recode", etc. 

I believe that this is what gives rise to the misconception described above that there are separate "household" and "child" **surveys**. This isn't correct. These files are all from the **same survey** and all contain different subsets of the **same data**. They have merely been pre-processed into wide, flat tables/spreadsheets to "aid" analysis. For example the Children's Recode contains one single table, with one row per child (under 5) and almost 1200 columns (!). All columns relating directly to that child are present, along with a selected subset of maybe-relevant data at other levels such as the type of house they live in, how many other children the mother has had, etc. These are all extractions that could theoretically be reproduced using an SQL query from the database that is maintained using this code.


## What this code is for

The (**vital**) exception to this set of downloadable files is the listing for "Hierarchical ASCII data" which appears under "Individual Recode". This dataset contains *all* the data about children, women, and households, in a section-by-section, table-by-table format. (Information about men, if gathered, is the exception; it is in a second hierarchical ASCII data listing under Men's Recode.)

Hopefully it's now clear where we're going with this. The "Hierarchical ASCII data" contains the data in its original structure, which is that of a relational database: separate tables of information which have common keys to link and join them in whatever way we choose. 

**This repository, then, contains code for working with these "Hierarchical ASCII data" downloads.** 

We parse the datafiles into standalone CSV format tables, and then load those tables to a PostgreSQL database. From there we can, with care and acknowledgement of limitations, write SQL queries to interrogate the whole database at once and thus extract cross-sectional data across the whole body of DHS surveys.

## Other data to note

### Spatial data 

Some, but not all surveys, collect georeferencing information giving the approximate point location of the responses. These are not included with any of the above-listed download formats, but are listed separately under Geographic Datasets. (Users must register separately to gain access to these, presumably for some kind of privacy-related reasoning, which is why they're separate.) These downloads are just ESRI point shapefiles, which contain a cluster identifier that can be linked to those in the main datasets. We do not provide code in this repository for loading the shapefiles to the database; this can be done with any GIS or PostGIS compatible library (e.g. GeoPandas).

These geographic data, whilst vital for any kind of spatial analysis, have limitations: they are only available at the "cluster" level (a single location for a group of houses), and they are subject to "displacement" i.e. random offset from their real position by up to 2km in urban areas / 10km in rural areas. Again this is for privacy reasons and we have to deal with it. 

### Indicator data

DHS also provide a range of aggregated summaries of things derived from surveys, which they call "indicators". These are available at national or regional (~admin 1) level and summarise things of analytic interest e.g. sample-weighted proportion of children with fever, etc. All of these indicators can, at least in theory, be derived from the individual level database that the code in this repository will create. (In practice this can take trial and error as DHS have many unpublished exceptions in how they calculate the indicators.)

### Regional boundaries

DHS also provide polygon datasets representing the "regions" used in the survey. These often correspond to Admin-1 units for the country in question; the actual boundaries can be downloaded from the DHS [Spatial Data Repository](https://spatialdata.dhsprogram.com/boundaries/). The polygon regions can be linked with the survey data via a region ID variable.


--------------------------------
# Summary of Usage
--------------------------------

Broadly the steps for each survey, implemented in this repository, are: 

- Identify the surveys which are available for download and which are not currently present in your database -> [01_Check_For_Survey_Updates.ipynb](01_Check_For_Survey_Updates.ipynb)
- Download the "hierarchical ASCII data" from the DHS website for all the relevant surveys. This code will not do the downloading for you - DHS do not look kindly on web-scraping of their data. They provide a bulk download tool (see [here](https://dhsprogram.com/data/Access-Instructions.cfm#multiplesurveys)) and it is recommended that you use it; this code will take the URL list the DHS tool generates to assist with the parsing stage.
- Download and unzip the "hierarchical ASCII data" for all the surveys you wish. -> [02_Unzip_Organise_Parse.ipynb](02_Unzip_Organise_Parse.ipynb)
- For each download, parse the .DCF file. This is a [CSPro](https://www.census.gov/data/software/cspro.html) dictionary specification. It specifies the schema of the data table(s) included in the associated .DAT file -> [02_Unzip_Organise_Parse.ipynb](02_Unzip_Organise_Parse.ipynb)
- Use the parsed schema information to parse the .DAT (data) file into multiple .CSV files, one for each table ("recordtype") used in the survey -> [02_Unzip_Organise_Parse.ipynb](02_Unzip_Organise_Parse.ipynb)
- Load the parsed schema information to two metadata tables in the database [03_DHS_Update_Metadata.ipynb](03_DHS_Update_Metadata.ipynb)
- Load each data .CSV file into the correct table in the database, after adjusting the schema of the database table if necessary. [04_DHS_Load_New_Tables.ipynb](04_DHS_Load_New_Tables.ipynb)

Slightly more complete usage information is in [Usage.md](/doc/Usage.md)

Once the database is loaded, you can query it to create the custom extractions you want. Due to the presence of country-specific and other exceptions to the standard schemas, care is needed in construction of queries to ensure that columns referenced always mean what they ought to, and that all columns containing a particular piece of information are referenced. The metadata tables make this process much less arduous than it would otherwise be. Example queries are found in other repositories. 
