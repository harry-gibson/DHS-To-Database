
## Instructions

Each of the notebooks is self-documenting. A summary of the steps they do is as follows:

1. Identify which surveys need to be obtained, using [01_Check_For_Survey_Updates](./DHS_To_Database/01_Check_For_Survey_Updates.ipynb):
    - Get list of surveys available according to the [DHS program API](https://api.dhsprogram.com/#/index.html) and compare to the surveys present in the local database
    - Remove surveys with known issues (e.g. listed in the API but not actually available)
    -  Provide the user with a listing of surveys they should download. There is no automated process for this, as DHS do not provide one and took steps to prevent an earlier web-scraping method from working. 
2. Unzip the downloaded data files, organise them according to numerical survey ID, and parse the files, using [02_Unzip_Organise_Parse](./DHS_To_Database/02_Unzip_Organise_Parse.ipynb)
    - Data are extracted to subdirectories numbered by numeric survey ID, within the `downloads` subdirectory of the given staging directory, and each file is also prepended with the numeric survey ID
    - Extract schema information from the .DCF CSPro format data specification files, and save this information to CSV files in the `parsed_specs` subdirectory of the given staging directory
    - Read the schema specification CSV files and use these to parse the data (.DAT) files into individual tables (record types) which will be saved as CSVs in the `tables` subdirectory of the given staging directory
3. Load all the new metadata into the two metadata tables in the database, checking that only expected download types are present and that data are not already present, or have been updated.
4. Load all data table CSVs to the relevant data tables in the database. If the tables do not exist (the CSV is for a newly-introduced table) then they will be created. If new columns have been added to the pre-existing tables, these will be added. If the columns exist but need widening (new data have longer values), this will be done. Data for tables stored as JSONB data will be packed into the JSONB attribute, leaving the columns used for indexing/joining as first-class columns.

