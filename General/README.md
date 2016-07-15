Contains workbenches for loading all DHS data, once extracted to CSV files, to a single database with one table for each recordtype, each of which will contain data from all surveys for that record type.

The current versions are under DB_Load. Versions under Old should not be used.

The loader workbenches rely on a flat mapping of the required output table schemas (this cannot be determined from a single CSV file for a given record type, because not all surveys contain all columns in a given recordtype; the output table needs to be the unioned set of columns). This mapping is generated with the Merge_table_schemas_from_metadata workbench. This writes a FFS file which is suitable as a schema definition input for the dynamic writer in the loader workbench.

However note that the datatypes aren't always picked up quite right, in particular the length of varchar attributes. This would cause no problem with Spatialite as FME maps these to a text column type. For Postgres, this causes an error when it hits an input file with too long a value. To get round this we could modify the schema mapping to make FME create "text" rather than "varchar(n)" columns in postgres. This took some digging around as Safe's documentation on data type mappings is abysmal, but it turns out to be "fme_buffer".

When loaded, indexes should be created on the database. The ipython notebook DHS_Database_Add_Indexes.ipynb provides some basic code that will do this. 
