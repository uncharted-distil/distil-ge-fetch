"""
Utility for unzipping data fetched from google earth engine and renaming according to the
scheme used by Sentinel-2 for distribution, which is {ID}_{BAND}_{TIMESTAMP}.tif.

Examples:

FFA1_B01_20201112T000000.tif
FFA1_B02_20201112T000000.tif

A points-of-interest file can be provided to allow for tiles that contain a point (or points) to be
treated as a "positive" sample, with other being tread as "negative" samples.  Sub-directories will
be created based on the postive_label and negative_label arg values, with tiles being saved to each
accordingly.

"""

import argparse
import zipfile
import os
import shutil
import tqdm
from pathlib import Path

AREA_DIR = "area"
POI_DIR = "poi"


def process_entries(download_location, entries, output_location):
    for entry in tqdm.tqdm(entries):
        try:
            geohash, date = os.path.splitext(entry)[0].split("_")
            with zipfile.ZipFile(
                os.path.join(download_location, entry), "r"
            ) as zip_ref:
                zip_ref.extractall(temp_path)
                temp_entries = os.listdir(temp_path)
            for temp_entry in temp_entries:
                # generate a new sentinel-like file name for the unzipped entry

                # get the band into sentinel format, ignore the quality file
                # 'B1' -> 'B01'
                _, band, _ = temp_entry.split(".")
                if band == "QA60":
                    continue
                if len(band) < 3:
                    band = f"{band[0]}0{band[1]}"

                # get the date into the sentinel format
                # 20201112 -> 20201112T000000
                year, month, day = date.split("-")
                datestr = f"{year}{month}{day}T000000"

                format_str = f"{geohash}_{datestr}_{band}.tif"

                output_file = os.path.join(output_location, format_str)

                # copy the file to the output dir using its new name
                p = Path(output_file).parent
                p.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(os.path.join(temp_path, temp_entry), output_file)
        except:
            continue

        shutil.rmtree(temp_path)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Utility for unzipping data fetched from google earth engine "
        + "and renaming according to the scheme used by Sentinel-2 for distribution"
    )
    parser.add_argument("--download_location", type=str, required=True)
    parser.add_argument("--output_location", type=str, default=".")
    parser.add_argument("--positive_label", type=str, default="positive")
    parser.add_argument("--negative_label", type=str, default="negative")
    parser.add_argument("--flatten", type=bool, default=False)
    parser.add_argument("--remove_metadata", type=bool, default=True)

    return parser.parse_args()


args = parse_args()

# create temp dir in the output dir
temp_path = os.path.join(args.output_location, "temp")
os.makedirs(temp_path, exist_ok=True)

# read zip file and save geohash, time
entries = os.listdir(args.download_location)

# write to pos/neg folders
if POI_DIR in entries:
    poi_path = os.path.join(args.download_location, POI_DIR)
    poi_output_path = (
        os.path.join(args.output_location, args.positive_label)
        if not args.flatten
        else args.output_location
    )
    poi_entries = os.listdir(poi_path)
    process_entries(poi_path, poi_entries, poi_output_path)

area_path = os.path.join(args.download_location, AREA_DIR)
area_output_path = (
    os.path.join(args.output_location, args.negative_label)
    if not args.flatten
    else args.output_location
)
area_entries = os.listdir(area_path)
process_entries(area_path, area_entries, area_output_path)


# copy over metadata file
if not args.remove_metadata:
    shutil.copyfile(
        os.path.join(args.download_location, "metadata.json"),
        os.path.join(args.output_location, "metadata.json"),
    )
