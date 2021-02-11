The following query encapsulates the checks made by TableDataHelper when it works out what columns need to be added to existing tables. 

cols_missing_db = pd.read_sql(f"""
-- Identify columns described in the metadata that are missing from the data,
-- and need to be added to data tables
WITH 
-- all the columns that exist in the data tables schema
existing_cols AS 
    (
        SELECT table_name, column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = '{DATA_SCHEMA}'
    )
-- all the columns that are described in the metadta
, req_cols AS 
(
    SELECT recordname, LOWER(name) AS name, MAX(len) AS maxlen
    FROM {TABLE_SPEC_TABLE}
    GROUP BY recordname, name
)
-- all the tables described in the metadata which are missing in their entirety
, whole_tables_missing AS 
    (
        SELECT DISTINCT recordname from {TABLE_SPEC_TABLE}
        WHERE recordname NOT IN (
            SELECT DISTINCT table_name AS recordname 
            FROM information_schema.columns
            WHERE table_schema = '{DATA_SCHEMA}')
    )
-- all the tables which have been stored as JSON type, so don't have all their 
-- described columns present in the tables according to information_schema
, json_tables AS 
    (
        SELECT DISTINCT table_name 
        FROM information_schema.columns WHERE data_type = 'jsonb'
    )

SELECT 
    req_cols.* 
    --, existing_cols.column_name AS test_joined
    --, json_tables.table_name AS json_tbl
    --, whole_tables_missing.recordname AS missing_tbl
FROM req_cols
-- NOT IN type checks seem to never complete whereas left join then check null 
-- is almost instant
LEFT JOIN existing_cols ON
 req_cols.name = existing_cols.column_name --AND req_cols.recordname = existing_cols.table_name 
LEFT JOIN json_tables ON
 req_cols.recordname = json_tables.table_name
LEFT JOIN whole_tables_missing ON
 req_cols.recordname = whole_tables_missing.recordname
WHERE existing_cols.column_name is NULL
AND json_tables.table_name IS null 
AND whole_tables_missing.recordname is null
--AND req_cols.recordname != '*'
ORDER BY recordname, name
""", con=engine)