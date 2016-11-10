DHS hierarchical data extraction
--------------------------------

This repository contains code (as python scripts and FME workbenches) for parsing DHS data in the so-called "hierarchical" (CSPro) format, to go from downloaded data files to a relational database for easy querying across multiple surveys. 
The code in this repository has been developed by Harry Gibson of the Malaria Atlas Project, Oxford.

Broadly the steps are: 

- (0. Download and unzip the hierarchical data to subdirectories numbered according to the survey id)
- 1. Extract schema information from the .DCF CSPro format data specification files, and save this information to CSV files (code under MetadataManagment)
- 2. Read the schema specification CSV files and use these to parse the data (.DAT) files into individual tables (record types) which will be saved as CSVs. (Code under DataFileParsing).
- 3. Create database tables for holding the data and load the CSVs into them (under General)
