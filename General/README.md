Contains workbenches for loading all DHS data, once extracted to CSV files, to a single database with one table for each recordtype, each of which will contain data from all surveys for that record type.

There is a version for loading to postgres, and a version for loading to spatialite.
The spatialite version took about 2 days to run and the postgres version about 20 hours. Total number of rows is in the region of 100 million.

The loader workbenches rely on a flat mapping of the required output table schemas (this cannot be determined from a single CSV file for a given record type, becasue not all surveys contain all columns in a given recordtype; the output table needs to be the unioned set of columns). This mapping is generated with the Read_All_CSV_Schemas workbench (Which needs modifying to add a writer: at present you have to save the output from the Inspector!), and then the merge_table_schemas workbench. This writes a CSV file which is suitable as a schema definition input for the dynamic writers in the loader workbenches.

However note that the datatypes aren't always picked up quite right, in particular the length of varchar attributes. This causes no problem with Spatialite as FME maps these to a text column type. For Postgres, this causes an error when it hits an input file with too long a value. 

To get round this we have to modify the schema mapping to make FME create "text" rather than "varchar(n)" columns in postgres. This took some digging around as Safe's documentation on data type mappings is abysmal, but it turns out to be "fme_buffer".




