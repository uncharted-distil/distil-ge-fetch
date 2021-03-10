import argparse
import json
import os
import random
from joblib import Parallel, delayed
from tqdm import tqdm
import ee
import helpers


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetches tiles from a Google Earth Engine collection based on time range, "
        + "coverage and geohash level.  All tiles within the coverage area will be fetched by default. "
        + "If a list of points of interest is provided, tiles overlapping the points, and within a specified "
        + "cell radius, will be clipped to the coverage area and fetched."
    )
    parser.add_argument("--collection", type=str, default=helpers.SENTINEL_2_CHANNELS)
    parser.add_argument("--outdir", type=str, default="output")
    parser.add_argument("--input_file", type=str)
    parser.add_argument("--coverage_file", type=str)
    parser.add_argument("--poi_file", type=str, default="")
    parser.add_argument("--precision", type=int, default=5)
    parser.add_argument("--start_date", type=str, default="2020-01-01")
    parser.add_argument("--end_date", type=str, default="2020-02-01")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--n_jobs", type=int, default=30)
    parser.add_argument("--sampling", type=float, default=1.0)
    parser.add_argument("--expansion", type=int, default=10)
    parser.add_argument(
        "--save_requests", dest="save_requests", default=False, action="store_true"
    )
    parser.add_argument(
        "--skip_fetch", dest="skip_fetch", default=False, action="store_true"
    )

    return parser.parse_args()


def main():
    # initialize earth engine
    ee.Initialize()

    args = parse_args()
    print(args)

    # for now we define bands by collection, but this can be made more general by supplying
    # as an argument or via config
    bands = helpers.SENTINEL_2_CHANNELS
    collection = helpers.SENTINEL_2_COLLECTION
    if args.collection == helpers.SENTINEL_2_COLLECTION:
        collection = args.collection
        bands = helpers.SENTINEL_2_CHANNELS
    elif args.collection == helpers.COPERNICUS_LAND_COVER_COLLECTION:
        collection = args.collection
        bands = helpers.COPERNICUS_LAND_COVER_CHANNELS

    locs = []
    is_geo_json = False

    if args.input_file:
        # loading previously saved requests
        input_json = json.load(open(args.input_file))
        locs = input_json

    if args.coverage_file:
        # loading coverage polygon from geo json file
        is_geo_json = True
        coverage_geojson = json.load(open(args.coverage_file))

        if args.poi_file is None:
            locs = helpers.generate_fetch_requests(
                coverage_geojson,
                args.precision,
                args.start_date,
                args.end_date,
                args.interval,
                args.sampling,
            )
        else:
            # load points of interest from geo json file
            poi_geojson = json.load(open(args.poi_file))

            locs = helpers.generate_fetch_requests_poi(
                coverage_geojson,
                poi_geojson,
                args.precision,
                args.start_date,
                args.end_date,
                args.interval,
                args.expansion,
                sampling_rate=args.sampling,
            )

    # save the metadata associated with the collection and bands we are fetching
    helpers.fetch_metadata(args.outdir, collection, bands)

    # Prepare to load data
    os.makedirs(args.outdir, exist_ok=True)

    # save fetched tile info to json if required
    if args.save_requests and is_geo_json:
        output_path = os.path.join(args.outdir, "requests.json")
        with open(output_path, "w+") as json_file:
            json.dump(locs, json_file, indent=4)

    # Run jobs in parallel
    if not args.skip_fetch:
        jobs = []
        for loc in locs:
            job = delayed(helpers.fetch_tile)(loc, args.outdir, collection, bands)
            jobs.append(job)

        random.shuffle(jobs)

        _ = Parallel(
            backend="multiprocessing", n_jobs=args.n_jobs, verbose=1, batch_size=4
        )(tqdm(jobs))


if __name__ == "__main__":
    main()
