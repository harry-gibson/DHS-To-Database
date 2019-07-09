This folder contains the two workbenches used to actually load the survey data.

Load_DHS_Data_To_PG_With_Checks.fmw must be run after the metadata have been loaded and the scheme updates run. For every CSV file it checks if the contents are all present in the DB, and if not then the file will be loaded.

