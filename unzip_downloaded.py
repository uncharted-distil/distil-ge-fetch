import argparse
import zipfile
import os
import shutil
import tqdm

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--download_location', type=str)
    parser.add_argument('--output_location', type=str)

    return parser.parse_args()

args = parse_args()

# create temp dir in the output dir
temp_path = os.path.join(args.output_location, 'temp')
os.makedirs(temp_path, exist_ok=True)

# read zip file and save geohash, time
entries = os.listdir(args.download_location)
for entry in tqdm.tqdm(entries):
    id, date = os.path.splitext(entry)[0].split('_')
    with zipfile.ZipFile(os.path.join(args.download_location, entry), 'r') as zip_ref:
        zip_ref.extractall(temp_path)
        temp_entries = os.listdir(temp_path)
    for temp_entry in temp_entries:
        # generate a new sentinel-like file name for the unzipped entry

        # get the band into sentinel format, ignore the quality file
        # 'B1' -> 'B01'
        _, band, _ = temp_entry.split('.')
        if band == 'QA60':
            continue
        if len(band) < 3:
            band = f'{band[0]}0{band[1]}'

        # get the date into the sentinel format
        # 20201112 -> 20201112T000000
        year, month, day = date.split('-')
        datestr = f'{year}{month}{day}T000000'

        output_file = os.path.join(args.output_location,  f'{id}_{datestr}_{band}.tif')
        # copy the file to the output dir using its new name
        shutil.copyfile(os.path.join(temp_path,  temp_entry), output_file)
    shutil.rmtree(temp_path)

