## The "hierarchical ASCII data" format

The files downloadable from DHS as "hierarchical ASCII data" (or indeed, as "Flat ASCII data") are from the [CSPro system](https://www.census.gov/data/software/cspro.html). I don't know why DHS don't call them "CSPro data files".

CSPro software is freely available and can be used to browse the data in an exploratory way, but not to systematically extract and cross-join it all in the way we need. 
 
Within each survey, the questions are divided into "sections". These sections are what, in database terms, we would call "tables", whilst in CSPro itself they are (confusingly) called Records (or Recordtypes, or Record names).

*(As described in the [readme](../README.md), the Flat ASCII data contain a single table (or Record) with a very large number of columns. Don't use these! Use the Hierarchical ASCII Data.)*

The questions themselves are called variables (within the DHS documentation) or "items" within the CSPro system, and are equivalent to columns/fields in database 
terms.

Each question has a range of acceptable values that can be stored, which are either direct numeric values (e.g. month of year or interview, or age in years of respondent) or coded values (integers that correspond to a particular meaning).

As can be seen from the above, there are some confusing terminologies in the format. What we would call in database terms a "table" is called a "Record" or "RecordType" within CSPro. What we would call in database terms a "column" is called an "Item" within CSPro.

## The downloads 

Each downloaded survey dataset zipfile consists of one file that specifies the format of the data (e.g. `BJIR71.DCF`), and another which actually contains the data (e.g. `BJIR71.DAT`). 

There are also other files we don't care about: `BJIR71.FRQ`, `BJIR71.FRW`, and `BJIR71.MAP` - the first two of these are simply diagnostics from when the data files were created; the last contains a human-readable version of the information we'll be parsing from the .DCF file.

## The .DCF format

### Description of the format

These "dictionary specification" files are plain text files which describe the schema of the data file(s) in a machine-readable way. 

In terms of what we are interested in, they contain the schema for every table: the name of the table, the description of the table; the name and description of each column within the table; how to parse each column from the fixed-width data lines; the values each column can take; and the descriptions associated with those values.

The files are read and parsed sequentially in blocks. Each block begins with a line containing an item in [square brackets] and ends with a blank line. The blocks occur in sequences that are effectively a nested description of each table ("Record"), each column ("item") within that table, and each value that item can have. When we encounter a "Record" block, the following "Item" blocks all describe columns for that table, until we encounter another "Record" block. 

Thus the following fragment of a .DCF file:

```
[Dictionary]
Version=CSPro 6.3
Label=Standard Recode DHS-VII   1.1.1 - Benin, 2017
Name=RECODE7
RecordTypeStart=16
RecordTypeLen=3
Positions=Absolute
ZeroFill=No
DecimalChar=No

[Level]
Label=HOUSEHOLD
Name=HOUSEHOLD

[IdItems]

[Item]
Label=Case Identification
Name=HHID
Start=1
Len=12
DataType=Alpha

...

[Record]
Label=Household's basic data
Name=RECH0
RecordTypeValue='H00'
RecordLen=172

[Item]
Label=Country code and phase
Name=HV000
Start=19
Len=3
DataType=Alpha

[Item]
Label=Cluster number
Name=HV001
Start=22
Len=6

...

[Item]
Label=Month of interview
Name=HV006
Start=49
Len=2

[ValueSet]
Label=Month of interview
Name=HV006_VS1
Value=1:12

...

[Item]
Label=Result of household interview
Name=HV015
Start=76
Len=1

[ValueSet]
Label=Result of household interview
Name=HV015_VS1
Value=1;Completed
Value=2;No Household member/no competent member at home
Value=3;Entire Household absent for extended period of time
Value=4;Postponed
Value=5;Refused
Value=6;Dwelling vacant or address not a dwelling
Value=7;Dwelling destroyed
Value=8;Dwelling not found
Value=9;Other

...

```

...corresponds to the following representation in a hopefully-illustrative markup language I just made up:

```xml
<Globals>
    <RecordTypeLocation StartPos="16" Len="3"/>
</Globals>
<TableGroup Label="HOUSEHOLD" IdentifierStartPos="1" IdentifierLen="12" IdentifierName="HHID">
    <Record Name="RECH0" Label="Household's basic data" Codename="H00" TotalLineLength="172">
        <Item Name="HV000" Label="Country code and phase" StartPos="19" Length="3"/>
        <Item Name="HV001" Label="Cluster number" StartPos="22" Length="6"/>
        ...
        <Item Name="HV006" Label="Month of interview" StartPos="49" Length="2">
            <IntegerValueRange Min=1 Max=12/>
        </Item>
        ...
        <Item Name="HV015" Label="Result of household interview" StartPos="49" Length="2">
            <IntegerValue Label="Completed">1</IntegerValue>
            <IntegerValue Label="No Household member/no competent member at home">2</IntegerValue>
            <IntegerValue Label="Entire Household absent for extended period of time">3</IntegerValue>
            <IntegerValue Label="Postponed">4</IntegerValue>
            <IntegerValue Label="Refused">5</IntegerValue>
            <IntegerValue Label="Dwelling vacant or address not a dwelling">6</IntegerValue>
            <IntegerValue Label="Dwelling destroyed">7</IntegerValue>
            <IntegerValue Label="Dwelling not found">8</IntegerValue>
            <IntegerValue Label="Other">9</IntegerValue>
        </Item>
    </Record>
</TableGroup>
```

In this example: 
- Every line in the data file contains a 3-character identifier in character position 16-18 which defines the table the line belongs to; this is constant across the whole file.
- Any table defined as being part of the "HOUSEHOLD" group contains an identifier in character position 1-12; the column name of this identifier is `HHID`.
  - **Note** that this leaves three blank characters in positions 13-15 for these lines. Other tables in the file that are part of the individual level group has a longer, 15-character identifier, and since the position of the record type identifier is constant at 16-18 the blank characters are necessary).
  - Because the HHID may not be as long as 12 characters, it may also be space-padded within its 12 character "slot".  Because of the need to match with the longer CASEIDs this needs carefil handling
  - For example consider a HHID of `1.0.10`, using dots instead of spaces for clarity here. The 12 characters stored in the data may then be `..1.0.10....` or `......1.0.10` or even `1.0.10......`
  - Meanwhile later in the file (not shown in this example) a woman living in this household would be identified with a CASEID consisting of the HHID plus three further character spaces, some of which may also be blank  such as `..1.0.10....2..` or `..1.0.10......2`
  - Therefore to reconstruct HHID from CASEID we simply remove the last three characters, treating spaces like any other character. 
  - It is therefore crucial that we DO NOT strip leading or trailing spaces from HHID and CASEID variables but store them exactly as they occur in the datafiles.

- Next we define a part of the table `"RECH0"`. The table contains information relating to "Household's basic data". Rows in the .DAT file that are destined for this table will have the identifier (in position 16-18) `"H00"` and will be 172 characters long in total.

Here we define 4 columns of that table. 
- Column `HV000` contains the answer to the question "Country code and phase" and the data for this column can be found in characters 19-21 of a line in the .DAT file that corresponds to this table. The values can be anything.
 
- Column `HV001` contains the answer to the question "Cluster number" and the data for this question can be found in characters 22-27. 
  - NB where the actual cluster number is shorter than 6 characters (which it normally, if not always, will be), the value is padded with spaces (on either or both sides). 
  - It is crucial that this value SHOULD have the padding stripped away on parsing in case the same clusterid is stored differently within its padded field in different tables (such as `V001` in `REC01`).  This is true for all data fields, and is the opposite to how HHID/CASEID should be treated.
- Column `HV006` contains the answer to the question "Month of interview" and can be an integer value from 1-12, with no further definition of the meaning of those self-explanatory values
- Column `HV015` contains the answer to the question "Result of household interview" and can be an integer value from 1-9, each of which codes for a description as given.


### Processing of the format

In stage 02, we process the .DCF file into a "FlatRecordSpec" and a "FlatValuesSpec" file. 

The FlatRecordSpec contains one row for each Item defined in the .DCF, recording the table name ("Recordname"); the column name ("name"); the narrative description ("Label") - which corresponds to the question text in the survey - ; the start position; and the length, of the data item in the fixed-width data file. A further column also contains the numeric identifier of the survey.

The FlatValuesSpec file contains one row for each legal value of each Item defined in the DCF, containing the column name it relates to, the value, and the description of this value. This is because, as well as integer ranges such as month number as illustrated above, other columns contain coded values e.g. a value of 2 in a particular survey/table/column might correspond to the name of a particular antimalarial drug that was taken. A further column also contains the numeric identifier of the survey.


## The .DAT format
        
These data files are a fixed-width text format, i.e. each row contains the data fields for a single row (record) in a table. As such, once we have the information from the .DCF file, splitting each row into the data fields is a simple string subsetting operation. 

A single .DAT file contains the data for many different tables, all interspersed - each row belongs to a different table with a different schema. So, as described above, each row contains - at a known fixed position - an identifier telling which table the row contains data for - the "RecordTypeValue" from the above fragment. This identifier is in a fixed position for all rows. 

So we need to read this identifier and use it to retrieve the schema information to tell us how to split that row (substring positions).

We then retrieve the start positions and lengths of all the columns specified for this table (recordtypevalue) in this survey, and use this to substring the data row into the individual data fields, before writing it out to a CSV specific to that table.

Here is a sample of three lines of data from a .DAT file. The second line is a record for the RECH0 table that was described above. (The first line is a record destined for table `W98` and the third is for the table `H01`)

```
       1   3  4W98 2 011100 201010101010101100101010100001111101
       1   9   H00BJ7     1     9 24   1 1129772 1201814174312934 5 03433 91292  103   0   1   2 2 12 0       0  100 8001    9 226 511  1  3  3014461747961 1 22018141843132
       1   9   H01 1 1101450   00       1100000   0
```

## The database metadata tables

The "FlatRecordSpec" files from all surveys are loaded into a single table in the database (distinguished, of course, by the surveyid). Likewise the "FlatValuesSpec" files are all loaded into a single table.

Having the data all in a single database makes it *possible* to create extractions across many surveys, but it is not always *straightforward*. The metadata tables provide the necessary information to identify which values you need to extract from which columns to assemble the data you need, for example:

- That a given column (i.e. question, or variable name) has the same meaning between surveys, with a query along the lines of:
```sql
    SELECT DISTINCT(label) FROM dhs_table_specs_flat WHERE name='hv007';
```
- Conversely, to find out what column a particular question text (e.g. one containing the word "malaria") appeared in across multiple surveys
- To find out what a given answer to a given question means across multiple surveys, to check that you are always looking for a response with the same meaning


## The database data tables

Each survey table is loaded into a table in the database for each table type - for example, the "REC01" data for all surveys go into a single "REC01" table in the database. As described in the [readme](../README.md), the columns of those tables are subject to slight change over time between surveys. The core columns generally remain unchanged, but new columns are sometimes added and occasionally columns are dropped. 

Therefore, for the "RECH1" table in the database to contain data from the RECH1 table of all DHS surveys, it needs to contain more columns than are necessarily present in any one survey, as the unioned set of all columns that are ever present in a table called "RECH1". Many columns will thus be NULL a lot of the time - the data are relatively "sparse".

A few tables are dedicated to "country specific" information. This means that they tend to contain columns (questions) that are unique to a particular survey. Therefore, to keep the data for these tables from all surveys in one place, means that the database table would potentially need thousands of columns, which is not feasible. In these cases, we identify which of the columns represent the identifier information for a row (that is the columns on which we might join the data to a different table). We store these columns in the table as normal, but for all the other columns we pack them into a single "data" column using the JSONB datatype. This allows data of an arbitrary schema to be stored in one table.

At the present time, only tables that would have >500 columns in total, or that are named as being "Country Specific" are stored as JSONB data, the remainder are all first-class tables. Querying the database therefore needs to take acccount of the different syntax needed to extract a field from a JSONB column where appropriate.

Other than the JSONB columns, all other columns in all the data tables are stored as VARCHAR(N) type. Whilst in theory many columns could more efficiently be stored as INTEGER types, in practice it's hard to sniff those datatypes in advance across a constantly-updating corpus of data, and there's no guarantee they wouldn't get changed under our feet. We leave it to the user to cast values as necessary.

