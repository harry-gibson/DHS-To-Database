import os
import glob
import zipfile

from cspro_parser.DCF_Parser import DCF_Parser
from cspro_parser.DAT_Parser import parse_dat_file

def unzip_and_sort(zip_path, survey_num, out_folder):
    """Extracts the root contents of the zipfile zip_path to a folder out_folder/survey_num, prepending
    'survey_num.' to each extracted filename"""
    if str.lower(zip_path).find('.zip') == -1:
        raise ValueError("Apparently not a zip file")
    out_dir = os.path.join(out_folder, survey_num)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    output_files = []
    with zipfile.ZipFile(zip_path) as zf:
        l = zf.namelist()
        for zipped_file in l:
            if zipped_file.endswith('/'):
                continue
            zipped_file_filename = zipped_file.split('/')[-1]
            unzipped_filename = '.'.join((survey_num, zipped_file_filename))
            unzipped_fn_path = os.path.join(out_dir, unzipped_filename)
            if not os.path.exists(unzipped_fn_path):
                print(' -> '.join((zipped_file, unzipped_fn_path)))
                out_file = zf.extract(zipped_file, out_dir)
                os.rename(out_file, unzipped_fn_path)
            output_files.append(unzipped_fn_path)
    return output_files

def parse_download_spec(download_urls_file):
    """Parse the text file of download URLs provided by the DHS download manager to extract
    the downloaded filename and the numerical survey id it corresponds to, return dictionary
    mapping the local filename to numerical survey id."""
    results = {}
    with open(download_urls_file, 'r') as url_file:
        for line in url_file:
            params = line.split('?')[1]
            fn, tp, ctry, survid, _, _ = params.split('&')
            filename = fn.split('=')[1].upper()
            ctry_code = ctry.split('=')[1].upper()
            survey_num = survid.split('=')[1]
            results[filename] = survey_num
    return results

def organise(download_urls_list, staging_folder):
    survey_num_mapping = parse_download_spec(download_urls_list)

    downloaded_files_folder = os.path.dirname(download_urls_list)
    extracted_files_folder_root = os.path.join(staging_folder, "downloaded")
    in_files = glob.glob(os.path.join(downloaded_files_folder, "*.zip"))

    all_unzipped_files = []
    for f in in_files:
        basename = os.path.basename(f).upper()
        if basename not in survey_num_mapping:
            print(f"Details not found for existing file {basename}, skipping")
            continue
        survey_num = survey_num_mapping[basename]
        unzipped_files = unzip_and_sort(f, survey_num, extracted_files_folder_root)
        all_unzipped_files.extend(unzipped_files)
    in_files_upper = [os.path.basename(f).upper() for f in in_files]
    missing = [f for f in survey_num_mapping.keys() if f not in in_files_upper]
    for m in missing:
        print(f"{m} has not been downloaded, skipping")
    return all_unzipped_files

get_filecode = lambda filename:os.path.extsep.join(os.path.basename(filename).split(os.path.extsep)[:-1])

def run(download_urls_list, staging_folder, parse_dcfs=False, parse_data=False):
    unzipped = organise(download_urls_list, staging_folder)
    dcf_files = [f for f in unzipped if f.lower().endswith('.dcf')]
    dat_files = [f for f in unzipped if f.lower().endswith('.dat')]
    parsed_spec_folder = os.path.join(staging_folder, "parsed_specs")
    parsed_data_folder = os.path.join(staging_folder, "tables")

    if parse_dcfs:
        for dcf_file in dcf_files:
            parser = DCF_Parser(dcf_file, parsed_spec_folder)
            if parser.done():
                print(f"{dcf_file} is already done, skipping")
                continue
            parser.parse()
            parser.write()

    if parse_data:
        for dat_file in dat_files:
            filecode = get_filecode(dat_file)
            spec_file = os.path.join(parsed_spec_folder, f"{filecode}.FlatRecordSpec.csv")
            # all surveys have a REC01 table so see if this exists
            test_output_fn = os.path.join(parsed_data_folder, f"{filecode}.REC01.csv")
            if os.path.exists(test_output_fn):
                print(f"{filecode} already parsed to datafiles, skipping")
                continue
            parse_dat_file(dat_file, spec_file, parsed_data_folder)


if __name__ == '__main__':
    run(r"C:\Users\zool1301\Documents\Data\DHS\dhs_hierarchical\download_urls.txt", r"C:\Users\zool1301\Documents\Data\DHS\staging", False, True)