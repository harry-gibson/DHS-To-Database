from collections import defaultdict
from sqlalchemy import create_engine, and_, or_
import sqlalchemy as sa
import pandas as pd
import warnings
import os
import io

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
        """Returns a dataframe of all the surveyids currently in the column definitions 
        metadata table. Returns dataframe with one column named surveyid."""
        return pd.read_sql(f'SELECT DISTINCT surveyid FROM {self._TABLE_SPEC_TABLE}', 
            con=self._engine)


    def get_existing_value_surveys(self):
        """Returns a dataframe of all the surveyids currently in the value descriptions 
        metadata table. Returns dataframe with one column named surveyid."""
        return pd.read_sql(f'SELECT DISTINCT surveyid FROM {self._VALUE_SPEC_TABLE}', 
            con=self._engine)
    
    
    def get_tablespec_rows_for_svy_filetype(self, surveyid, filetype):
        """Returns a dataframe of all the column definition metadata currently present 
        for the specified surveyid and filetype (IR/MR). 
        
        Dataframe has columns `name, recordname, label, len, filecode`"""
        db_cols_data = pd.read_sql(f'''
            SELECT name, recordname, label, len, filecode
            FROM {self._TABLE_SPEC_TABLE}
            WHERE surveyid = '{surveyid}' 
            AND filecode ILIKE '%{filetype}%'
            ;''', con=self._engine)
        return db_cols_data


    def get_valuespec_rows_for_svy_filetype(self, surveyid, filetype):
        """Returns a dataframe of all the value description metadata currently present 
        for the specified surveyid and filetype (IR/MR). 
        
        Dataframe has columns `col_name, value, value_desc, value_type, filecode`"""
        db_vals_data = pd.read_sql(f'''
            SELECT col_name, value, value_desc, value_type, filecode
            FROM {self._VALUE_SPEC_TABLE}
            WHERE surveyid = '{surveyid}' 
            AND filecode ILIKE '%{filetype}%'
            ;''', con=self._engine)
        return db_vals_data

    def get_db_survey_version_vals(self, surveyid, file_type):
        """Get the most recent version present in the value descriptions metadata 
        table for the specified surveyid and file type (IR or MR).
        
        Prints a warning if multiple versions are detected, but does not correct 
        the situation.
        
        Returns '00' if no matching metadata are found."""
        return self._get_db_survey_version(surveyid, file_type, self._VALUE_SPEC_TABLE)
    

    def get_db_survey_version_cols(self, surveyid, file_type):
        """Get the most recent version present in the column definitions metadata 
        table for the specified surveyid and file type (IR or MR).

        Prints a warning if multiple versions are detected, but does not correct 
        the situation.
        
        Returns '00' if no matching metadata are found."""
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
        """See if any entries for the given surveyid and file type (IR or MR) are 
        present in the column definitions metadata table.
        
        If False then the whole file will need loading; if True then version and 
        multiple-entry checks should be done to see if the DB matches the file. """
        return self._get_any_in_db(surveyid, file_type, self._TABLE_SPEC_TABLE)


    def get_any_in_db_vals(self, surveyid, file_type):
        """See if any entries for the given surveyid and file type (IR or MR) are 
        present in the value descriptions metadata table.
        
        If False then the whole file will need loading; if True then version and 
        multiple-entry checks should be done to see if the DB matches the file."""
        return self._get_any_in_db(surveyid, file_type, self._VALUE_SPEC_TABLE)


    def _get_any_in_db(self, surveyid, file_type, qual_table):
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
        """Check whether a metadata file of a given surveyid and filetype (i.e. IR or MR) 
        is in the metadata database multiple times. This may imply that a new version has 
        been (mistakenly) loaded without removing the old."""
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


    def _file_is_cols_or_vals(self, csv_fn):
        cols_or_vals = "COLS" if "FlatRecordSpec" in csv_fn else "VALS"
        return cols_or_vals


    def load_new_metadata_file(self, csv_fn):
        if self._file_is_cols_or_vals(csv_fn) == "COLS":
            self.load_new_table_file(csv_fn)
        else:
            self.load_new_values_file(csv_fn)


    def load_new_table_file(self, tbl_fn):
        """Load a dhs table specification CSV to the database tablespec table, 
        renaming columns to match the schema, dropping columns not needed in the DB, 
        and splitting the filecode into numeric and original parts"""
      
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
        self._check_column_widths_from_df(file_data, 
            table_name=self._TABLE_SPEC_TABLENAME)
        if self._is_dry_run:
            print(f"Would insert {os.path.basename(tbl_fn)} to {self._TABLE_SPEC_TABLE}") 
        else:
            print(f"Inserting {os.path.basename(tbl_fn)} to {self._TABLE_SPEC_TABLE}") 
            file_data.to_sql(name=self._TABLE_SPEC_TABLENAME, con=self._engine, 
                schema=self._SPEC_SCHEMA, index=False,
                if_exists='append', method='multi')


    def load_new_values_file(self, val_fn, use_bulk_copy=True):
        """Load a dhs values description CSV to the database valuespec table, 
        renaming columns to match the schema and splitting the filecode into numeric 
        and original parts"""

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
        self._check_column_widths_from_df(file_data, 
                table_name=self._VALUE_SPEC_TABLENAME)
        if self._is_dry_run:
            print(f"Would insert {os.path.basename(val_fn)} to {self._VALUE_SPEC_TABLE}")
        else:
            if use_bulk_copy:
                print(f"Inserting {os.path.basename(val_fn)} to {self._VALUE_SPEC_TABLE} using BULK COPY") 
                buffer = io.StringIO()
                file_data.to_csv(buffer, sep='\t', header=False, index=False)
                buffer.seek(0)
                qual_table=self._VALUE_SPEC_TABLE
                conn = self._engine.raw_connection()
                cursor = conn.cursor()
                cursor.copy_from(buffer, qual_table, sep='\t', columns=list(file_data.columns))
                conn.commit()
                cursor.close()
            else:
                print(f"Inserting {os.path.basename(val_fn)} to {self._VALUE_SPEC_TABLE}") 
                file_data.to_sql(name=self._VALUE_SPEC_TABLENAME, con=self._engine, 
                    schema=self._SPEC_SCHEMA, index=False,
                    if_exists='append', method='multi')


    def _check_column_widths_from_df(self, df, table_name):
        """Check that for all object columns in the provided dataframe, the equivalent column 
        in the database is at least wide enough to hold it, and widen it if not.
        
        For use when there is no metadata available to tell us what the maximum width should 
        be across ALL surveys, rather than just this one CSV/dataframe. Specifically, for 
        adjusting the metadata tables themselves when loading the metdata CSVs. 
        
        See _check_column_widths_from_metadata in lib04 for an equivalent check spanning 
        the whole survey corpus based on the metadata tables, for use when loading data."""
        obj_cols = df.select_dtypes(include=['object']).columns
        lengths = {col:(df[col].str.len().max()) for col in obj_cols}
        for col, incoming_length in lengths.items():
            self._check_and_update_column_width(table_name, col, incoming_length)


    def _check_and_update_column_width(self, table_name, column_name, req_width):
        """Check that a varchar(n) column in the database is at least req_width wide 
        and widen it if not. Returns False if the column was already ok, or the result 
        of the SQL execution otherwise (or True if dry run)"""
        df = pd.read_sql(f"""
            SELECT character_maximum_length as maxlen 
            FROM information_schema.columns 
            WHERE 
                table_schema = '{self._SPEC_SCHEMA}' 
            AND table_name = '{table_name}' 
            AND column_name = '{column_name}';"""
        , con=self._engine)
        current = df['maxlen'][0]
        if req_width > current:
            if self._is_dry_run:
                print(f"""Column {self._SPEC_SCHEMA}.{table_name}.{column_name} would be widened 
                from {current} to {req_width}""")    
                return True
            else:
                sql = f"""ALTER TABLE {self._SPEC_SCHEMA}."{table_name}" 
                        ALTER COLUMN {column_name} TYPE character varying({req_width});"""
                return self._engine.execute(sql)
        else:
            return False


    def drop_and_reload(self, tbl_fn, msg="unknown reason"):
        """For a given metadata CSV, drops all metadata for that surveyid and filetype (IR/MR) 
        (for any version) and then reloads the file contents."""
        surveyid, _, file_type, _ = parse_survey_info(tbl_fn)
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
        """Deletes all column definition metadata for a given surveyid and filetype (IR/MR)"""
        self._delete_metadata_rows_for_svy_filetype(
            surveyid, filetype, self._TABLE_SPEC_TABLENAME)


    def delete_value_entries_for_svy_filetype(self, surveyid, filetype):
        """Deletes all value description metadata for a given surveyid and filetype (IR/MR)"""
        self._delete_metadata_rows_for_svy_filetype(
            surveyid, filetype, self._VALUE_SPEC_TABLENAME)


    def _delete_metadata_rows_for_svy_filetype(self, surveyid, filetype, table_name):
        if self._is_dry_run:
            print(f"Would drop {filetype} rows for survey {surveyid} from {table_name}")
        else:
            print(f"Dropping {filetype} rows for survey {surveyid} from {table_name}")
            meta = sa.MetaData()
            tablespec_db = sa.Table(table_name, meta, schema=self._SPEC_SCHEMA, 
                                    autoload=True, autoload_with=self._engine)
            cond = and_((tablespec_db.c.surveyid == surveyid), 
                        (tablespec_db.c.filecode.ilike(f'%{filetype}%')))
            delete = tablespec_db.delete().where(cond)
            res = self._engine.execute(delete)
            return res
    

