# CSPro .DAT fixed-width text datafile parser
# For use with DHS hierarchical data
# Harry Gibson 2015-2021

import os
from operator import itemgetter
import csv
from chardet.universaldetector import UniversalDetector

def parse_dat_file(dat_path, spec_csv_path, out_folder):
    """Parse a .DAT file (CSPro fixed-width text datafile) into a series of CSV files 
    containing the tabular data for each table contained in the .DAT and described in the 
    associated .DCD file. 
    
    Developed for use in particular with DAT files provided in the "hierarchical data"
    from DHS, but may be more generally applicable to CSPro format files. The .DCF file 
    must be parsed first, using DCF_Parser, and the table specification file it 
    generates is used by this function to parse the data file.
    
    Produces one CSV data file for every table (recordtype) defined in the .DCF and occurring in 
    the .DAT. """
    filecode = os.path.extsep.join(os.path.basename(dat_path).split(os.path.extsep)[:-1])

    # See if we've already done this one
    test_fn = os.path.join(out_folder, f"{filecode}.REC01.csv")
    if os.path.exists(test_fn):
        print("Already parsed " + filecode)
        return
    print("Parsing "+dat_path)

    # read the parsed file specification in CSV form which was created by parsing the .dcf file
    # The first row specifies where, on all subsequent rows, the "record type" is found i.e. the identifier
    # that specifies which table the row defines a variable for. This is constant throughout the file.
    # Each remaining item in the parsed DCF spec defines one field from one table, specifying what position that
    # field's data is found in the fixed-width text format row when the row's record_type_info
    # (destination table name) is for this table
    with open(spec_csv_path, 'r') as dict_file:
        dict_file_reader = csv.DictReader(dict_file)
        # the record type position info must be in the first line
        recordtype_info = next(dict_file_reader)
        rt_start = int(recordtype_info['Start']) - 1
        rt_end = int(recordtype_info['Len']) + rt_start
        all_vars_this_file = [row for row in dict_file_reader]
    for field_info in all_vars_this_file:
        field_info['Start'] = int(field_info['Start'])
        field_info['Len'] = int(field_info['Len'])
    # sort them by record type (i.e. destination table) then position in the row (order of fields)
    sorted_fields = sorted(all_vars_this_file, key=(itemgetter('RecordTypeValue', 'Start')))

    # build a dictionary of record type (i.e. tablename) : list of its fields (i.e. field infos)
    rt_field_info = {}
    for field_info in sorted_fields:
        record_tag = field_info['RecordTypeValue']
        if record_tag not in rt_field_info:
            rt_field_info[record_tag] = []
        rt_field_info[record_tag].append(field_info)

    # now parse the data file
    result = {}
    n_cols_per_table = {}

    detector = UniversalDetector()
    with open(dat_path, 'rb') as f:
        for line in f:
            detector.feed(line)
            if detector.done: break
        detector.close()
        enc = detector.result['encoding']

    with open(dat_path, 'r', encoding=enc) as data:
        for i, line in enumerate(data):
            #if i == 0 and line.startswith(codecs.BOM_UTF8):
            #    print(f"File {dat_path} appears to contain BOM; ignoring it")
            #    line = line[len(codecs.BOM_UTF8):]
            record_type = line[rt_start:rt_end]
            if record_type not in rt_field_info:
                print("Specification for recordtype '{0!s}' not found in file for {1!s} at line {2!s}".format(
                    record_type, filecode, i))
                continue
            record_spec = rt_field_info[record_type]
            if record_type not in result:
                result[record_type] = []

            # split the column-aligned text according to the row specification

            # The .DAT format allows a fixed width for each column of each recordtype.
            # Should we strip the whitespace on shorter values? This is difficult.
            # In general, yes we should, because values are stored as fixed-width and where 
            # shorter than the field, are padded with spaces, which would take up unnecessary space 
            # and would prevent joining/comparison between surveys. 
            # HOWEVER in the case of the CASEID / HHID variables we must NOT strip the whitespace. 
            # The HHID is usually the CASEID with the last 3 chars trimmed off, but if we
            # trim "some" whitespace from HHID here then we can break that association and
            # damage referential integrity.
            # On the other hand some joins are based on e.g. BIDX (recorded as len 2)
            # to MIDX (recorded as len 1, despite containing the same data), and we need
            # to join on a single digit found in both so BIDX would need to be stripped.

            # Define a lambda to strip or not strip accordingly, and use it in a list comp to
            # split the row into its field values
            strip_or_not = lambda data, name: data if name in ('CASEID', 'HHID') else data.strip()
            rowParts = [strip_or_not(
                (line[i['Start'] - 1: i['Start'] + i['Len'] - 1]),
                i['Name'])
                for i in record_spec]

            if record_type not in n_cols_per_table:
                n_cols_per_table[record_type] = len(rowParts)
            else:
                assert len(rowParts) == n_cols_per_table[record_type]
            # add as a list to the list of rows for this record type
            result[record_type].append(rowParts)  # (",".join(rowParts))

    for record_type, field_infos in rt_field_info.items():
        if not record_type in result:
            print(f"No rows were found for record type {record_type} in file {filecode} despite DCF specification")
            continue
        field_header = [i['Name'] for i in field_infos]
        field_records = set([i['RecordName'] for i in field_infos])
        assert len(field_records) == 1
        rec_name = field_records.pop()
        if not os.path.exists(out_folder):
            os.makedirs(out_folder)
        out_fn = os.path.join(out_folder, f"{filecode}.{rec_name}.csv")
        with open(out_fn, 'w', newline='', encoding='utf-8') as out_csv:
            csv_writer = csv.writer(out_csv)
            csv_writer.writerow(field_header)
            csv_writer.writerows(result[record_type])
