import argparse
import pandas as pd
from shapely import geometry
import helpers

CELL_SIZE_X = 360.0 / 4320.0
CELL_SIZE_Y = 180.0 / 2160.0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Converts SPAM2017 crop information into a geohashed CSV"
    )
    parser.add_argument("--input_file", type=str, required=True)
    parser.add_argument("--output_file", type=str, default="./output.csv")
    parser.add_argument("--country_iso", type=str)
    parser.add_argument("--crop_columns", nargs="+", type=str, required=True)
    parser.add_argument("--geohash_level", type=int, default=5)

    return parser.parse_args()


def main():
    args = parse_args()

    # load the csv file into pandas for cleanup
    df = pd.read_csv(args.input_file)

    # filter down to country of interest records
    if args.country_iso is not None:
        df = df.loc[df["iso3"] == args.country_iso]

    # extract x, y locations and crop of interest
    df = df[(["x", "y"] + args.crop_columns)]
    df = df.reset_index()

    # loop over the x, y which are the cell centroids, and generate a bounding box based on
    # the cell size (taken from the associated geotiff resolution)
    centroids = zip(df["x"], df["y"])
    bounds = [
        geometry.box(
            c[0] - CELL_SIZE_X / 2,
            c[1] - CELL_SIZE_Y / 2,
            c[0] + CELL_SIZE_X / 2,
            c[1] + CELL_SIZE_Y / 2,
        )
        for c in centroids
    ]

    # loop through the bounds we've created an intersect each with the intended geohash grid
    geohashes = [
        helpers.polygon_to_geohashes(b, precision=args.geohash_level, inner=False)
        for b in bounds
    ]

    # flatten  gh set for each cell preserving index - no clean way to do this in pandas
    flattened_gh = []
    for idx, gh_set in enumerate(geohashes):
        for gh in gh_set:
            bounds_str = helpers.geohash_to_array_str(gh)
            flattened_gh.append((idx, gh, bounds_str))

    # store as a dataframe with any geohashes that were part of 2 cells reduced to 1
    # a better implementation of this would take the value of both cells into  account and
    # compute a final adjusted value for the given geohash
    geohash_df = pd.DataFrame(flattened_gh, columns=["cell", "geohash", "bounds"])
    geohash_df = geohash_df.drop_duplicates(subset="geohash", keep="first")
    geohash_df = geohash_df.set_index("cell")

    joined = pd.merge(df, geohash_df, left_index=True, right_index=True)
    joined = joined.drop(columns=["x", "y", "index"])

    joined.to_csv(args.output_file, index=False)


if __name__ == "__main__":
    main()
