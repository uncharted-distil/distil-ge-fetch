import pandas as pd
import geojson
import argparse
from pathlib import Path
from shapely.geometry import Point


def parse_args():
    parser = argparse.ArgumentParser(
        description="Converts a CSV file containing columns of latitude, "
        + " longitude and dates to a geojson representation."
    )
    parser.add_argument("--features_csv", type=str, required=True)
    parser.add_argument("--output_file", type=str, default="output.geojson")
    parser.add_argument("--latitude_col", type=str, required=True)
    parser.add_argument("--longitude_col", type=str, required=True)
    parser.add_argument("--date_col", type=str, required=True)
    return parser.parse_args()


args = parse_args()

features_df = pd.read_csv(args.features_csv)

cols = [args.longitude_col, args.latitude_col, args.date_col]
samples = features_df[cols]
all_points = [(Point(x[0], x[1]), x[2]) for x in samples.values.tolist()]

geojson_points = []
for point in all_points:
    geojson_point = geojson.Feature(
        geometry=geojson.Point((point[0].x, point[0].y)),
        properties={"date": point[1]},
    )
    geojson_points.append(geojson_point)

geojson_points = geojson.FeatureCollection(geojson_points)

p = Path(args.output_file).parent
p.mkdir(parents=True, exist_ok=True)
with open(args.output_file, "w") as outfile:
    geojson.dump(geojson_points, outfile, indent=2)
