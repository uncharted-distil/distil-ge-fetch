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
import geojson
import geohash
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Utility for unzipping data fetched from google earth engine "
        + "and renaming according to the scheme used by Sentinel-2 for distribution"
    )
    parser.add_argument("--download_location", type=str, required=True)
    parser.add_argument("--output_location", type=str, default=".")
    parser.add_argument("--poi_file", type=str, default="")
    parser.add_argument("--precision", type=str, default=5)
    parser.add_argument("--positive_label", type=str, default="positive")
    parser.add_argument("--negative_label", type=str, default="negative")

    return parser.parse_args()


args = parse_args()

# generate geohashes for poi file
poi_included = args.poi_file != None and args.poi_file != ""
poi_geohashes = set()
if poi_included:
    # load the points of interest and store their geohashes as a set
    feature_collection = geojson.load(open(args.poi_file))
    for feature in feature_collection.features:
        poi_geohashes.add(
            geohash.encode(
                feature.geometry.coordinates[1],
                feature.geometry.coordinates[0],
                precision=args.precision,
            )
        )

# create temp dir in the output dir
temp_path = os.path.join(args.output_location, "temp")
os.makedirs(temp_path, exist_ok=True)

# read zip file and save geohash, time
entries = os.listdir(args.download_location)
for entry in tqdm.tqdm(entries):
    try:
        geohash, date = os.path.splitext(entry)[0].split("_")
        with zipfile.ZipFile(
            os.path.join(args.download_location, entry), "r"
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
            output_file = ""
            # generate the output file based on poi status
            if not poi_included:
                output_file = os.path.join(args.output_location, format_str)
            else:
                if geohash in poi_geohashes:
                    output_file = os.path.join(
                        args.output_location, args.positive_label, format_str
                    )
                else:
                    output_file = os.path.join(
                        args.output_location, args.negative_label, format_str
                    )

            # copy the file to the output dir using its new name
            p = Path(output_file).parent
            p.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(os.path.join(temp_path, temp_entry), output_file)
    except:
        continue
    shutil.rmtree(temp_path)

# copy over metadata file
shutil.copyfile(
    os.path.join(args.download_location, "metadata.json"),
    os.path.join(args.output_location, "metadata.json"),
)
