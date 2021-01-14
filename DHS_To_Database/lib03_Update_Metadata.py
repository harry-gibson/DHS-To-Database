from collections import defaultdict
from sqlalchemy import create_engine, and_, or_
import sqlalchemy as sa
import pandas as pd
import warnings
import os

DB_TABLESPEC_COLS = ['itemtype', 'recordname', 'recordtypevalue', 'recordlabel', 'name',
       'label', 'start', 'len', 'fmetype', 'surveyid', 'id', 'filecode']
DB_VALUESPEC_COLS = ['col_name', 'value', 'value_desc', 'value_type', 'surveyid', 'id',
       'filecode']

def parse_survey_info(filename):
    """"For a path to a parsed FlatRecordSpec file, return a tuple of the 
    (survey_number, country_code, file_data_type, survey_version)
    e.g. /path/to/511.CMMR71.FlatValuesSpec -> (511, 'CM', 'MR', '71')"""
    surveyid, code, _, _ = os.path.basename(filename).split('.')
    loc = code[0:2].lower()
    filetype = code[2:4].lower()
    version = code[4:]
    return surveyid, loc, filetype, version
    #return os.path.basename(filename).split('.')[0]

def build_spec_files_dict(tablefiles_list):
    tbl_files_dict = defaultdict(lambda:defaultdict(list))
    for f in tablefiles_list: 
        svy_id, loc, filetype, version = parse_survey_info(f)
        tbl_files_dict[svy_id][filetype].append((f, version, loc))
    return tbl_files_dict


class SurveyMetadataHelper:
    
    def __init__(self, conn_str, table_spec_table, value_spec_table, spec_schema, dry_run=True):
        self._engine = create_engine(conn_str)
        self._TABLE_SPEC_TABLENAME = table_spec_table
        self._VALUE_SPEC_TABLENAME = value_spec_table
        self._SPEC_SCHEMA = spec_schema
        self._TABLE_SPEC_TABLE = ".".join([spec_schema, table_spec_table])
        self._VALUE_SPEC_TABLE = ".".join([spec_schema, value_spec_table])
        self._is_dry_run = dry_run

    def get_existing_table_surveys(self):
        return pd.read_sql(f'SELECT DISTINCT surveyid FROM {self._TABLE_SPEC_TABLE}', 
            con=self._engine)

    def get_existing_value_surveys(self):
        return pd.read_sql(f'SELECT DISTINCT surveyid FROM {self._VALUE_SPEC_TABLE}', 
            con=self._engine)


    def get_db_survey_version_vals(self, surveyid, file_type):
        return self._get_db_survey_version(surveyid, file_type, self._VALUE_SPEC_TABLE)
    

    def get_db_survey_version_cols(self, surveyid, file_type):
        return self._get_db_survey_version(surveyid, file_type, self._TABLE_SPEC_TABLE)


    def _get_db_survey_version(self, surveyid, file_type, search_table):
        db_filecodes = pd.read_sql(f'''
            SELECT DISTINCT filecode 
            FROM {search_table} 
            WHERE surveyid = '{surveyid}'
            AND filecode ilike '__{file_type}__';''', con=self._engine
        )
        if len(db_filecodes) > 1:
            warnings.warn(f"Warning, more than one set of metadata found in DB "+
            "({search_table}) for {surveyid} and filecode {file_type}, cleanup required")
        elif len(db_filecodes) == 0:
            warnings.warn(f"Warning, no matching data found in DB ({search_table}) "+
                "for survey {surveyid} type {file_type}")
            return '00'
        return max(db_filecodes['filecode'].str.slice(-2)), len(db_filecodes)==1


    def get_any_in_db_cols(self, surveyid, file_type):
        return self._get_any_in_db(surveyid, file_type, self._TABLE_SPEC_TABLE)


    def get_any_in_db_vals(self, surveyid, file_type):
        return self._get_any_in_db(surveyid, file_type, self._VALUE_SPEC_TABLE)


    def _get_any_in_db(self, surveyid, file_type, qual_table):
        # check the filecode column, eventually 
        db_filecodes = pd.read_sql(f'''
            SELECT COUNT(*) 
            FROM {qual_table}
            WHERE surveyid = '{surveyid}'
            AND filecode ilike '__{file_type}__';''', con=self._engine
        )
        n = db_filecodes.iloc[0]['count']
        if n > 1:
            return True
        return False


    def get_multiple_in_db(self, surveyid, file_type):
        check_var = 'MV001' if file_type.lower() == 'mr' else 'V001'
        db_content = pd.read_sql(f'''
            SELECT COUNT(*) 
            FROM {self._TABLE_SPEC_TABLE}
            WHERE surveyid = '{surveyid}'
            AND name = '{check_var}';''', con=self._engine
        )
        n = db_content.iloc[0]['count']
        if n > 1:
            warnings.warn(f"*******WARNING survey {surveyid} with filetype {file_type}\
            seems to be in DB ({self._TABLE_SPEC_TABLE}) multiple times!*******")
            return True
        return False


    def file_is_cols_or_vals(self, tbl_fn):
        cols_or_vals = "COLS" if "FlatRecordSpec" in tbl_fn else "VALS"
        return cols_or_vals


    def load_new_metadata_file(self, tbl_fn):
        if self.file_is_cols_or_vals(tbl_fn) == "COLS":
            self.load_new_table_file(tbl_fn)
        else:
            self.load_new_values_file(tbl_fn)


    def load_new_table_file(self, tbl_fn):
        """Load a dhs table specification CSV to the database tablespec table, 
        renaming columns to match the schema, dropping columns not needed in the DB, 
        and splitting the filecode into numeric and original parts"""
      
        if self._is_dry_run:
            print(f"Would insert {os.path.basename(tbl_fn)} to {self._TABLE_SPEC_TABLE}") 
        else:
            print(f"Inserting {os.path.basename(tbl_fn)} to {self._TABLE_SPEC_TABLE}") 
            file_data = pd.read_csv(tbl_fn)
            # db column names are all lowercase
            file_data.columns = file_data.columns.str.lower()
            # in the DB we will store numeric survey ID, and filecode as the original filename part 
            # i.e. sans the numeric id
            file_data['surveyid'] = file_data['filecode'].str.split('.', expand=True)[0]
            file_data['filecode']=file_data['filecode'].str.split('.', expand=True)[1]
            # the CSV tablespec files contain a few columns that we don't transfer to the DB
            # for historical reasons
            for c in file_data.columns:
                if c not in DB_TABLESPEC_COLS:
                    del(file_data[c])
            self.check_destination_col_widths(file_data, 
                schema_name=self._SPEC_SCHEMA, table_name=self._TABLE_SPEC_TABLENAME)
            file_data.to_sql(name=self._TABLE_SPEC_TABLENAME, con=self._engine, 
                schema=self._SPEC_SCHEMA, index=False,
                if_exists='append', method='multi')
    

    def load_new_values_file(self, val_fn):
        """Load a dhs values description CSV to the database valuespec table, 
        renaming columns to match the schema and splitting the filecode into numeric 
        and original parts"""

        if self._is_dry_run:
            print(f"Would insert {os.path.basename(val_fn)} to {self._VALUE_SPEC_TABLE}")
        
        else:
            print(f"Inserting {os.path.basename(val_fn)} to {self._VALUE_SPEC_TABLE}") 
            file_data = pd.read_csv(val_fn)
            # db column names are all lowercase
            file_data.columns = ['filecode', 'col_name', 'value', 'value_desc', 'value_type']
            # in the DB we will store numeric survey ID, and filecode as the original filename part 
            # i.e. sans the numeric id
            file_data['surveyid'] = file_data['filecode'].str.split('.', expand=True)[0]
            file_data['filecode'] = file_data['filecode'].str.split('.', expand=True)[1]
            for c in file_data.columns:
                if c not in DB_VALUESPEC_COLS:
                    del(file_data[c])
            self.check_destination_col_widths(file_data, 
                schema_name=self._SPEC_SCHEMA, table_name=self._VALUE_SPEC_TABLENAME)
            file_data.to_sql(name=self._VALUE_SPEC_TABLENAME, con=self._engine, 
                schema=self._SPEC_SCHEMA, index=False,
                if_exists='append', method='multi')


    def check_destination_col_widths(self, df, schema_name, table_name):
        """Check that for all object columns in the provided dataframe, the equivalent column 
        in the database is at least wide enough to hold it, and widen it if not."""
        obj_cols = df.select_dtypes(include=['object']).columns
        lengths = {col:(df[col].str.len().max()) for col in obj_cols}
        for col, incoming_length in lengths.items():
            self.check_and_update_column_width(schema_name, table_name, col, incoming_length, True)


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


    def drop_and_reload(self, tbl_fn, msg="unknown reason"):
        surveyid, loc, file_type, version = parse_survey_info(tbl_fn)
        cols_or_vals = self.file_is_cols_or_vals(tbl_fn) 
        if cols_or_vals == "COLS":
            dest = self._TABLE_SPEC_TABLENAME
        else:
            dest = self._VALUE_SPEC_TABLENAME
        if self._is_dry_run:
            print(f"""Would drop and reload rows from {dest} for survey {surveyid}, filetype {file_type}, 
                from file {tbl_fn}, reason: {msg}""")
        else:
            print(f"""Dropping and reloading rows from {dest} for survey {surveyid}, filetype {file_type}, 
                from file {tbl_fn}, reason: {msg}""")
            if cols_or_vals == "COLS":
                self.delete_table_col_entries_for_svy_filetype(surveyid, file_type)
                self.load_new_table_file(tbl_fn)
            else:
                self.delete_value_entries_for_svy_filetype(surveyid, file_type)
                self.load_new_values_file(tbl_fn)


    def delete_table_col_entries_for_svy_filetype(self, surveyid, filetype):
        self._delete_metadata_rows_for_svy_filetype(
            surveyid, filetype, self._SPEC_SCHEMA, self._TABLE_SPEC_TABLENAME)


    def delete_value_entries_for_svy_filetype(self, surveyid, filetype):
        self._delete_metadata_rows_for_svy_filetype(
            surveyid, filetype, self._SPEC_SCHEMA, self._VALUE_SPEC_TABLENAME)


    def _delete_metadata_rows_for_svy_filetype(self, surveyid, filetype, schema_name, table_name):
        if self._is_dry_run:
            print(f"Would drop {filetype} rows for survey {surveyid} from {table_name}")
        else:
            print(f"Dropping {filetype} rows for survey {surveyid} from {table_name}")
            meta = sa.MetaData()
            tablespec_db = sa.Table(table_name, meta, schema=schema_name, 
                                    autoload=True, autoload_with=self._engine)
            cond = and_((tablespec_db.c.surveyid == surveyid), 
                        (tablespec_db.c.filecode.ilike(f'%{filetype}%')))
            delete = tablespec_db.delete().where(cond)
            res = self._engine.execute(delete)
    

    def get_tablespec_rows_for_svy_filetype(self, surveyid, filetype):
        db_cols_data = pd.read_sql(f'''
            SELECT name, recordname, label, len, filecode
            FROM {self._TABLE_SPEC_TABLE}
            WHERE surveyid = '{surveyid}' 
            AND filecode ILIKE '%{filetype}%'
            ;''', con=self._engine)
        return db_cols_data


    def get_valuespec_rows_for_svy_filetype(self, surveyid, filetype):
        db_vals_data = pd.read_sql(f'''
            SELECT col_name, value, value_desc, value_type, filecode
            FROM {self._VALUE_SPEC_TABLE}
            WHERE surveyid = '{surveyid}' 
            AND filecode ILIKE '%{filetype}%'
            ;''', con=self._engine)
        return db_vals_data