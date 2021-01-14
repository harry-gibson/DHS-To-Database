import sqlalchemy as sa
from sqlalchemy import create_engine
import pandas as pd
import warnings
import os

class TableDataHelper:
    _MAX_COLUMN_THRESHOLD = 1000
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
        self._checked_column_tables = set()

    
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
        if not self._does_data_table_exist(table_name):
            self.create_data_table(table_name)
        else:
            self.check_cols_against_metadata(table_name)


    def create_data_table(self, table_name):
        """Creates a data table with all the columns that are currently specified in the metadata.
        
        As throughout this software, all columns are created with VARCHAR(n) type where n = the 
        maximum width currently specified for that table/column in any survey
        
        The exception if there are more than TableDataHelper._MAX_COLUMN_THRESHOLD columns 
        specified in the metadata, in which case only columns with "id" in the name will be 
        first-class columns, and a jsonb column named data will be added for storing the remainder."""
        column_clauses, is_json = self._get_column_clauses(table_name)
        create_stmt = f"""
            CREATE TABLE {self._DATA_SCHEMA}."{table_name}"({column_clauses})
            TABLESPACE pg_default;
            ALTER TABLE {self._DATA_SCHEMA}."{table_name}" OWNER to admin;"""
        if self._is_dry_run:
            return ("Would create table with \n" + create_stmt + "\n" 
            + " then create indices with " + self._create_or_replace_indices(table_name, is_json))
        else:
            r = self._engine.execute(create_stmt)
            if is_json:
                self._json_tables.add(table_name)
            index_sql = self._create_or_replace_indices(table_name, is_json)
            if len(index_sql) > 0:
                print("Executing the following to drop / recreate indices: \n" + index_sql)
                self._engine.execute(index_sql)
        

    def _get_column_clauses(self, table_name):
        """Gets the columns that a new data table should have, according to the metadata tables.
        
        Returns them as a string for use in a statement of the form 
        CREATE TABLE tablename (result) """
        
        # list all columns that are specified for this datatable in the survey metadata (unioned 
        # set across all surveys: not all surveys will have all columns)
        whats_needed = pd.read_sql(f"""
            SELECT lower(name) AS name, itemtype, MAX(len) AS length, MAX(start) AS start
            FROM {self._TABLE_SPEC_TABLE}
            WHERE recordname='{table_name}'
            GROUP BY name, itemtype
            ORDER BY start;
        """, con=self._engine)
        
        # In the case of some country-specific tables, where the columns are different in almost 
        # every survey, the number of columns becomes very large and horribly inefficient to store 
        # (it is sparse). In these cases we store those tables with a single JSONB column for the data 
        # plus the ID/joining columns as first-class columns
        is_json = False
        if len(whats_needed) > TableDataHelper._MAX_COLUMN_THRESHOLD:
            whats_needed = whats_needed[whats_needed['name'].str.contains('id')]
            whats_needed.loc[len(whats_needed)] = ('data', 'JSON', '', 99999999)
            is_json=True

        # of course the metadata tables don't specify surveyid so we add that manually
        whats_needed.loc[-1] = ('surveyid', 'Item', '3', 0)
        whats_needed.index = whats_needed.index + 1
        whats_needed.sort_index(inplace=True)
        
        def _clause_from_row(r):
            if r["itemtype"] == "JSON":
                type_clause = "jsonb"
            else:
                type_clause = f'character varying({r["length"]})'
            clause = f'{r["name"]} {type_clause} COLLATE pg_catalog."default"'
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
        idx_fields = [c for c in tbl_cols if c.find('id') != -1]

        drop_idx_stmts = []
        idx_stmts = []

        for c in idx_fields:
            idx_name = idx_name_template.format(c, str.lower(table_name))
            idx_sql = idx_sql_template.format(idx_name, self._DATA_SCHEMA, table_name, c)
            if idx_name in existing_indices:
                if replace_existing:
                    drop_idx_stmt = clean_sql_template.format(self._DATA_SCHAME, idx_name)
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
  

    def get_db_survey_table_rowcount(self, surveyid, tablename):
        pass


    def check_and_update_column_width(self, schema_name, table_name, column_name, 
                                req_width, update_if_needed=False):
        """Check that a varchar(n) column in the database is at least req_width wide and widen it if not"""
        df = pd.read_sql(f'''
            SELECT character_maximum_length as maxlen 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' and column_name = '{column_name}';''', con=self._engine)
        current = df['maxlen'][0]
        if current < req_width and update_if_needed:
            print(f"Column {schema_name}.{table_name}.{column_name} will be widened from"+
                "{current} to {req_width}")
            sql = f'''ALTER TABLE {schema_name}."{table_name}" 
                        ALTER COLUMN {column_name} TYPE character varying({req_width})'''
            return self._engine.execute(sql)
        else:
            return current >= req_width


    def check_destination_col_widths(self, df, schema_name, table_name):
        """Check that for all object columns in the provided dataframe, the equivalent column 
        in the database is at least wide enough to hold it, and widen it if not."""
        obj_cols = df.select_dtypes(include=['object']).columns
        lengths = {col:(df[col].str.len().max()) for col in obj_cols}
        for col, incoming_length in lengths.items():
            self.check_and_update_column_width(schema_name, table_name, col, incoming_length, True)

    
    def check_missing_cols(self, table_name):
        """For a given data table, check the metadata tables to see what all the columns 
        needed are, and then check whether they all exist. Returns a dataframe of column names 
        that are missing from the table, if the table is not one with a JSONB column."""
         
        is_json = table_name in self._json_tables

        if is_json:
            data_cols_needed = pd.read_sql(f"""
                SELECT LOWER(name) AS name, MAX(len) AS maxlen, MAX(start) as start
                FROM {self._TABLE_SPEC_TABLE}
                WHERE recordname = '{table_name}' AND lower(name) LIKE '%id%'
                GROUP BY name""", con=self._engine)
            data_cols_needed.loc[len(data_cols_needed)] = ('data', 99999999, 99999999)

        else:
            data_cols_needed = pd.read_sql(f"""
                SELECT LOWER(name) AS name, MAX(len) AS maxlen 
                FROM {self._TABLE_SPEC_TABLE}
                WHERE recordname = '{table_name}'
                GROUP BY name
                ORDER BY name, start""", con=self._engine)
            
        data_cols_present = pd.read_sql(f"""
            SELECT table_name, column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = '{self._DATA_SCHEMA}' ORDER BY table_name, column_name""", 
            con=self._engine)
        
        data_cols_not_present = data_cols_needed[~data_cols_needed['name'].isin(
            data_cols_present['column_name'])]

        #and_not_in_json_tbl = data_cols_not_present[~data_cols_not_present['recordname'].isin(
        #    self._json_tables)]
        return data_cols_not_present
    

        

    def add_varchar_col_to_table(self, schema_name, table_name, column_name, req_width):
        sql = f"""
            ALTER TABLE {schema_name}."{table_name}" 
            ADD COLUMN {column_name.lower()} CHARACTER VARYING ({req_width})"""
        return engine.execute(sql)


    def delete_table_entries_for_survey(surveyid, table_name, dry_run=True):
        # only drop ones for matching columns because we get different ones from the IR and MR files
        if dry_run:
            print(f"Would drop all data rows for survey {surveyid} from {table_name}")
        else:
            print(f"Dropping all data rows for survey {surveyid} from {table_name}")
            meta = sa.MetaData()
            tablespec_db = sa.Table(table_name, meta, schema=self._DATA_SCHEMA, 
                                    autoload=True, autoload_with=engine)
            cond = tablespec_db.c.surveyid == surveyid
            delete = tablespec_db.delete().where(cond)  
            res = engine.execute(delete)