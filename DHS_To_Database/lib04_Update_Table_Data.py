import sqlalchemy as sa
from sqlalchemy import create_engine
import pandas as pd
import warnings
import os
import json
import io
import csv

class TableDataHelper:

    _MAX_COLUMN_THRESHOLD = 500

    @staticmethod
    def parse_table_name(filename):
        """"For a path to a parsed data table file, return a tuple of the 
        (survey_number, country_code, file_data_type, survey_version, table_name)
        e.g. /path/to/511.CMIR71.REC43.csv -> (511, 'CM', 'IR', '71', 'REC43)"""
        surveyid, code, tablename, _ = os.path.basename(filename).split('.')
        loc = code[0:2].lower()
        filetype = code[2:4].lower()
        version = code[4:]
        return surveyid, loc, filetype, version, tablename
        
    def __init__(self, conn_str, 
        table_spec_table, value_spec_table, spec_schema,
        data_schema, dry_run=True):
        self._engine = create_engine(conn_str)
        self._TABLE_SPEC_TABLENAME = table_spec_table
        self._VALUE_SPEC_TABLENAME = value_spec_table
        self._SPEC_SCHEMA = spec_schema
        self._TABLE_SPEC_TABLE = ".".join([spec_schema, table_spec_table])
        self._VALUE_SPEC_TABLE = ".".join([spec_schema, value_spec_table])
        self._DATA_SCHEMA = data_schema
        self._is_dry_run = dry_run
        
        self._populate_JSON_table_list()
        
        self._known_tables = set()
        self._modified_tables = set()
        self._verified_tables = set()

    
    def _populate_JSON_table_list(self):
        json_tables = pd.read_sql(f"""
            SELECT DISTINCT table_name 
            FROM information_schema.columns 
            WHERE table_schema = '{self._DATA_SCHEMA}' AND data_type = 'jsonb'""",
                con=self._engine)
        self._json_tables = set(json_tables['table_name'])


    def _does_data_table_exist(self, table_name):
        if table_name in self._known_tables:
            return True
        exists = pd.read_sql(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = '{self._DATA_SCHEMA}'
                AND    table_name   = '{table_name}'
            );""", con=self._engine)['exists'][0]
        if exists:
            self._known_tables.add(table_name)
            return True
        return False


    def prepare_db_for_file(self, table_name):
        if not table_name in self._verified_tables:
            if not self._does_data_table_exist(table_name):
                self.create_data_table(table_name)
            else:
                self.check_cols_against_metadata(table_name)
            self._verified_tables.add(table_name)


    def create_data_table(self, table_name):
        """Creates a data table with all the columns that are currently specified in the metadata.
        
        As throughout this software, all columns are created with VARCHAR(n) type where n = the 
        maximum width currently specified for that table/column in any survey
        
        The exception if there are more than TableDataHelper._MAX_COLUMN_THRESHOLD columns 
        specified in the metadata, in which case only columns believed to be indexes will be 
        first-class columns, and a jsonb column named data will be added for storing the remainder."""
        # Note that were we just loading one single CSV we could do something like this to initialize 
        # the table.
        #  df[:0].to_sql(table, engine, if_exists=if_exists)
        column_clauses, is_json = self._get_column_clauses(table_name)
        create_stmt = f"""
            CREATE TABLE {self._DATA_SCHEMA}."{table_name}"({column_clauses})
            TABLESPACE pg_default;
            ALTER TABLE {self._DATA_SCHEMA}."{table_name}" OWNER to admin;"""
        if self._is_dry_run:
            print (f"Would create {'JSON-type ' if is_json else ''}table {table_name} with \n" + create_stmt + "\n" )
        else:
            print (f"Creating new {'JSON-type ' if is_json else ''}data table {table_name} with \n" + create_stmt)
            r = self._engine.execute(create_stmt)
            if is_json:
                self._json_tables.add(table_name)
            index_sql = self._create_or_replace_indices(table_name, is_json)
            if len(index_sql) > 0:
                print("Executing the following to drop / recreate indices: \n" + index_sql)
                self._engine.execute(index_sql)
        

    def _col_shld_be_firstclass(self, col_name):
        _c = col_name.lower()
        if (("idx" in _c) or (_c.startswith("ix")) # or (_c.endswith("id"))):
            or _c in(['surveyid','caseid','mcaseid','hhid'])):
            # TODO check label as well, has to say "index" or "line number"
            # e.g. 'idx94', 'ixh4', 'surveyid','caseid','mcaseid','hhid',
            # but not e.g. 'shidioma'
            return True
        return False


    def _table_should_be_json(self, table_name, n_cols):
        if n_cols > TableDataHelper._MAX_COLUMN_THRESHOLD:
            return True
        is_cs =  pd.read_sql(f"""
            SELECT EXISTS (
                SELECT FROM {self._TABLE_SPEC_TABLE} 
                WHERE  recordname='{table_name}'
                AND lower(recordlabel) LIKE 'cs:%' OR lower(recordlabel) LIKE 'country specific'
            );""", con=self._engine)['exists'][0]
        return is_cs


    def _get_column_clauses(self, table_name):
        """Gets the columns that a new data table should have, according to the metadata tables.
        
        Returns them as a string for use in a statement of the form 
        CREATE TABLE tablename (result) """
        
        # list all columns that are specified for this datatable in the survey metadata (unioned 
        # set across all surveys: not all surveys will have all columns)
        whats_needed = pd.read_sql(f"""
            SELECT lower(name) AS name, MAX(itemtype) AS itemtype, MAX(len) AS length, MAX(start) AS start
            FROM {self._TABLE_SPEC_TABLE}
            WHERE recordname='{table_name}'
            GROUP BY name
            ORDER BY start;
        """, con=self._engine)
        
        # In the case of some country-specific tables, where the columns are different in almost 
        # every survey, the number of columns becomes very large and horribly inefficient to store 
        # (it is sparse). In these cases we store those tables with a single JSONB column for the data 
        # plus the ID/joining columns as first-class columns
        is_json = self._table_should_be_json(table_name, len(whats_needed))
        if is_json:
            whats_needed = whats_needed[whats_needed['name'].apply(self._col_shld_be_firstclass)]
            whats_needed.loc[len(whats_needed)] = ('data', 'JSON', '', 99999999)
            is_json=True

        # of course the metadata tables don't specify surveyid so we add that manually
        whats_needed.loc[-1] = ('surveyid', 'Item', '3', 0)
        whats_needed.index = whats_needed.index + 1
        whats_needed.sort_index(inplace=True)
        
        def _clause_from_row(r):
            if r["itemtype"] == "JSON":
                type_clause = "jsonb"
                collate_clause= ""
            else:
                type_clause = f'character varying({r["length"]})'
                collate_clause = 'COLLATE pg_catalog."default"'
            clause = f'{r["name"]} {type_clause} {collate_clause}'
            return clause 

        # convert each row in the df to a clause for use in the CREATE TABLE statement
        clauses = list(whats_needed.apply(_clause_from_row, axis=1))
        return (",\n".join(clauses), is_json)


    def _create_or_replace_indices(self, table_name, is_json=False, replace_existing=False):
        # TODO create GIN index on any JSON columns?
        idx_sql_template = 'CREATE INDEX {0} ON {1}."{2}"({3});'
        idx_name_template = '{0}_{1}'
        clean_sql_template = 'DROP INDEX IF EXISTS {0}.{1};'

        res = self._engine.execute("SELECT relname FROM pg_class WHERE relkind='i';")
        existing_indices = [i[0] for i in res.fetchall()]

        tbl_cols = self._engine.execute(f'SELECT * FROM dhs_data_tables."{table_name}" LIMIT 0').keys()
        idx_fields = [c for c in tbl_cols if (self._col_shld_be_firstclass(c))]

        drop_idx_stmts = []
        idx_stmts = []

        for c in idx_fields:
            idx_name = idx_name_template.format(c, str.lower(table_name))
            idx_sql = idx_sql_template.format(idx_name, self._DATA_SCHEMA, table_name, c)
            if idx_name in existing_indices:
                if replace_existing:
                    drop_idx_stmt = clean_sql_template.format(self._DATA_SCHEMA, idx_name)
                    drop_idx_stmts.append(drop_idx_stmt)
                    idx_stmts.append(idx_sql)
                    print("Replacing index " + idx_name)
                else:
                    print("Skipped existing index " + idx_name)
            else:
                idx_stmts.append(idx_sql)
                print("Adding index "+idx_name)
        
        # also create a single covering index on all joining columns
        if len(idx_fields) > 1:
            idx_name = idx_name_template.format("allidx", str.lower(table_name))
            idx_sql = idx_sql_template.format(idx_name, self._DATA_SCHEMA, table_name, ",".join(idx_fields))
            if idx_name in existing_indices:
                if replace_existing:
                    drop_idx_stmt = clean_sql_template.format(self._DATA_SCHEMA, idx_name)
                    drop_idx_stmts.append(drop_idx_stmt)
                    idx_stmts.append(idx_sql)
                    print("Replacing covering index " + idx_name)
                else:
                    print("Skipped existing covering index " + idx_name)
            else:
                idx_stmts.append(idx_sql)
                print("Adding covering index "+idx_name)
        
        # also create a covering index on the first two joining columns if there are three 
        # (or all except the last one, if there's more)
        # e.g. surveyid and caseid but not bidx (the cols are in the appropriate order in the CSVs)
        if len(idx_fields) > 2:
            idx_name = idx_name_template.format("twoidx", str.lower(table_name))
            idx_sql = idx_sql_template.format(idx_name, self._DATA_SCHEMA,
                                            table_name, ",".join(idx_fields[:-1]))
            if idx_name in existing_indices:
                if replace_existing:
                    drop_idx_stmt = clean_sql_template.format(self._DATA_SCHEMA, idx_name)
                    drop_idx_stmts.append(drop_idx_stmt)
                    idx_stmts.append(idx_sql)
                    print ("Replacing secondary covering index " + idx_name)
                else:
                    print ("Skipped existing secondary covering index " + idx_name)
            else:
                idx_stmts.append(idx_sql)
                print ("Adding secondary covering index " + idx_name)
        
        drop_indices_sql = "\n".join(drop_idx_stmts)
        create_indices_sql = "\n".join(idx_stmts)

        return drop_indices_sql + "\n" + create_indices_sql
  

    def check_cols_against_metadata(self, table_name):
        """Ensures that the varchar columns that the metadata states should be present in 
        the given table are actually present in the corresponding data table and that they 
        have the necessary width."""
        self._ensure_columns_presence(table_name)
        self._ensure_column_widths(table_name)
        

    def _widen_column(self, table_name, column_name, req_width, cur_width=0):
        if self._is_dry_run:
            print(f"""Column {self._DATA_SCHEMA}.{table_name}.{column_name} would be widened
                to {req_width}{" (from "+str(cur_width)+")" if cur_width>0 else ""}""")
        else:
            print(f"""Widening column {self._DATA_SCHEMA}.{table_name}.{column_name} 
                to {req_width}{" (from "+str(cur_width)+")" if cur_width>0 else ""}""")
            sql = f"""ALTER TABLE {self._DATA_SCHEMA}."{table_name}" 
                    ALTER COLUMN {column_name} TYPE character varying({req_width});"""
            return self._engine.execute(sql)


    def _ensure_column_widths(self, table_name):
        """Check that for all columns in the specified table, the varchar column 
        in the database is at least wide as the maximum length specified for that table 
        in any survey in the currently-loaded metadata, and widen it if not.
        
        See _check_column_widths_from_df in lib03 for doing an equivalent check directly 
        from a CSV of data (loaded to a dataframe), which is needed on the metadata tables 
        themselves"""
        
        # Because this does an INNER join to information_schema.columns, columns that 
        # are specified in the spec table but which have been stored packed in a JSON table
        # are simply not returned by this query, which is what we need, so that we won't 
        # attemptt to widen or check the width of a column that doesn't exist (because it's 
        # in the JSON)
        length_specs = pd.read_sql(f"""
            SELECT 
	            lower(s.name) AS name, MAX(s.len) as req_len, 
	            COALESCE(MAX(i.character_maximum_length),999999) as actual_len
            FROM 
                {self._TABLE_SPEC_TABLE} s
            INNER JOIN 
                information_schema.columns i
            ON 
                s.recordname = i.table_name 
                AND 
                'dhs_data_tables'=i.table_schema 
                AND 
                lower(s.name)=i.column_name
            WHERE s.recordname='{table_name}'
            GROUP BY s.name"""
        , con=self._engine)
        to_widen = length_specs[length_specs['req_len']>length_specs['actual_len']]
        for _, row in to_widen.iterrows():
            self._widen_column(table_name, row['name'], row['req_len'], row['actual_len'])
            self._modified_tables.add(table_name)
        
    
    def _add_varchar_column(self, table_name, column_name, req_width):
        self._modified_tables.add(table_name)
        if self._is_dry_run:
            print(f"""Column named {column_name.lower()} would be added to 
            {self._DATA_SCHEMA}.{table_name} with width {req_width}""")
        else:
            print(f"""Adding column named {column_name.lower()} to 
            {self._DATA_SCHEMA}.{table_name} with width {req_width}""")
            sql = f"""
                ALTER TABLE {self._DATA_SCHEMA}."{table_name}" 
                ADD COLUMN {column_name.lower()} CHARACTER VARYING ({req_width})"""
            return self._engine.execute(sql)


    def _ensure_columns_presence(self, table_name):
        """For a given data table, check the metadata tables to see what all the columns 
        needed are, and then check whether they all exist. Returns a dataframe of column names 
        that are missing from the table, if the table is not one with a JSONB column."""
         
        is_json = table_name in self._json_tables

        if is_json:
            # we only need the indexing columns to be present
            data_cols_needed = pd.read_sql(f"""
                SELECT LOWER(name) AS name, MAX(len) AS maxlen
                FROM {self._TABLE_SPEC_TABLE}
                WHERE recordname = '{table_name}'
                GROUP BY name;""", con=self._engine)
            data_cols_needed = data_cols_needed[data_cols_needed['name'].apply(
                self._col_shld_be_firstclass)]
            data_cols_needed.loc[len(data_cols_needed)] = ('data',  99999999)

        else:
            data_cols_needed = pd.read_sql(f"""
                SELECT LOWER(name) AS name, MAX(len) AS maxlen 
                FROM {self._TABLE_SPEC_TABLE}
                WHERE recordname = '{table_name}'
                GROUP BY name;""", con=self._engine)
            
        data_cols_present = pd.read_sql(f"""
            SELECT table_name, column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = '{self._DATA_SCHEMA}' ORDER BY table_name, column_name""", 
            con=self._engine)
        
        data_cols_not_present = data_cols_needed[~data_cols_needed['name'].isin(
            data_cols_present['column_name'])]

        for _, row in data_cols_not_present.iterrows():
            self._add_varchar_column(table_name, row['name'], row['maxlen'])

        #and_not_in_json_tbl = data_cols_not_present[~data_cols_not_present['recordname'].isin(
        #    self._json_tables)]
        return data_cols_not_present
    
  
    def load_table(self, table_filename, use_bulk_copy=True):
        _, _, _, _, table_name = TableDataHelper.parse_table_name(table_filename)
        is_json = table_name in self._json_tables
        if is_json:
            self._load_file_to_json_table(table_filename, use_bulk_copy)
        else:
            self._load_file_to_standard_table(table_filename, use_bulk_copy)


    def _load_file_to_standard_table(self, table_filename, use_bulk_copy=True):
        surveyid, _, file_type, _, table_name = TableDataHelper.parse_table_name(table_filename)
        file_data = pd.read_csv(table_filename)
        file_data.columns = file_data.columns.str.lower()
        file_data['surveyid'] = surveyid
        if self._is_dry_run:
            print(f'''Would insert data from {os.path.basename(table_filename)} to 
                {self._DATA_SCHEMA}."{table_name}" using {"BULK COPY" if use_bulk_copy 
                else "standard INSERTs"}''')
        else:
            if use_bulk_copy:
                # use the underlying psycopg2 connection's cursor to do a streaming bulk copy insert 
                # from an in-memory file, this is WAY faster
                # https://stackoverflow.com/a/44181653, https://stackoverflow.com/a/44181653
                
                # write the DF to an in-memory buffer as TSV (tab separated)
                print(f'''Inserting data from {os.path.basename(table_filename)} to 
                    {self._DATA_SCHEMA}."{table_name}" using BULK COPY''')
                buffer = io.StringIO()
                file_data.to_csv(buffer, sep='\t', header=False, index=False)
                qual_table = self._DATA_SCHEMA + "." + table_name
                conn = self._engine.raw_connection()
                cursor = conn.cursor()
                cursor.copy_from(buffer, qual_table, sep='\t', columns=list(file_data.columns))
                conn.commit()
                cursor.close()
            else:
                print(f'''Inserting data from {os.path.basename(table_filename)} to 
                    {self._DATA_SCHEMA}."{table_name}" using INSERTs''')
                file_data.to_sql(name=table_name, schema=self._DATA_SCHEMA,
                    con=self._engine, index=False, if_exists='append', method='multi')


    def _load_file_to_json_table(self, table_filename, use_bulk_copy=True):
        surveyid, _, _, _, table_name = TableDataHelper.parse_table_name(table_filename)
        # force all columns to be read as string, otherwise pandas' type sniffer will set them 
        # to be integer where possible. For the data being stored in first class tables where the 
        # columns are all varchar, that doesn't matter as it gets cast back to string on insert. 
        # But when we're storing JSON, we need to ensure they're strings before creating the json 
        # so that they are quoted. Otherwise when it comes to using the data, the JSON numbers, being 
        # stored as numbers, would be inconsistent with those in first-class tables which are always stored 
        # as varchar.
        file_data = pd.read_csv(table_filename, dtype=str).fillna('')
        # convert column names to lowercase and add surveyid column
        file_data.columns = file_data.columns.str.lower()
        file_data['surveyid'] = str(surveyid)

        # convert all EXCEPT the columns with "id" in the name to a single JSON column
        # To do this, set the id columns to be the dataframe's index just to "hide" them 
        # from the to_dict conversion, then reset the index afterwards
        file_data.set_index([c for c in file_data.columns if 'id' in c], inplace=True)
        # use the built-in to_dict function to pack each row into a python dictionary and 
        # save it into a new column called 'data'
        file_data['data'] = file_data.to_dict('records')
        # keep only this new column (plus the "hidden" index ones)
        file_data = file_data[['data']]
        file_data.reset_index()
        # type of data column is now a python dictionary
        # type(test['data'].iloc[0]) == dict
        # https://stackoverflow.com/a/50825030
        def dict2json(dict):
            return json.dumps(dict, ensure_ascii=False)
        file_data['data'] = file_data.data.map(dict2json)
        # type of data column is now a JSON string
        # type(test['data'].iloc[0]) == str

        if self._is_dry_run:
            print(f'''Would insert data from {os.path.basename(table_filename)} to 
                JSON table {self._DATA_SCHEMA}."{table_name}" using 
                {"BULK COPY" if use_bulk_copy else "standard INSERTs"}''')
        else:
            if use_bulk_copy:
                # use the underlying psycopg2 connection's cursor to do a streaming bulk copy insert 
                # from an in-memory file, this is WAY faster
                # https://stackoverflow.com/a/44181653, https://stackoverflow.com/a/44181653
                
                # write the DF to an in-memory buffer as TSV (tab separated)
                print(f'''Inserting data from {os.path.basename(table_filename)} to 
                JSON table {self._DATA_SCHEMA}."{table_name}" using BULK COPY''')
                buffer = io.StringIO()
                file_data.to_csv(buffer, sep='\t', header=False, index=False)
                qual_table = self._DATA_SCHEMA + "." + table_name
                conn = self._engine.raw_connection()
                cursor = conn.cursor()
                cursor.copy_from(buffer, qual_table, sep='\t', columns=list(file_data.columns))
                conn.commit()
                cursor.close()
            
            else:
                print(f'''Inserting data from {os.path.basename(table_filename)} to 
                JSON table {self._DATA_SCHEMA}."{table_name}" using INSERTs''')
                file_data.to_sql(name=table_name, schema=self._DATA_SCHEMA,
                    con=self._engine, index=False, if_exists='append', method='multi')
            

    def drop_and_reload(self, tbl_fn, msg="Unknown reason"):
        """For a given data table CSV, drop the data for this surveyid from the appropriate DB table
        and then reload it."""
        surveyid, _, file_type, _, table_name = TableDataHelper.parse_table_name(tbl_fn)
        self.delete_table_entries_for_survey(surveyid, table_name)
        self.load_table(tbl_fn)


    def does_survey_exist_in_table(self, surveyid, tablename):
        return self.get_db_survey_table_rowcount(surveyid, tablename) > 0


    def get_db_survey_table_rowcount(self, surveyid, tablename):
        _sql = f"""SELECT count(*) nrows_db FROM {self._DATA_SCHEMA}."{table_name}"
            WHERE surveyid={surveyid}"""
        return self._engine.execute(sql).fetchone()[0]


    def list_modified_tables(self):
        return self._modified_tables


    def delete_table_entries_for_survey(self, surveyid, table_name):
        if self._is_dry_run:
            print(f"Would drop all data rows for survey {surveyid} from {table_name}")
        else:
            print(f"Dropping all data rows for survey {surveyid} from {table_name}")
            meta = sa.MetaData()
            tablespec_db = sa.Table(table_name, meta, schema=self._DATA_SCHEMA, 
                                    autoload=True, autoload_with=self._engine)
            cond = tablespec_db.c.surveyid == surveyid
            delete = tablespec_db.delete().where(cond)  
            res = self._engine.execute(delete)
    