import argparse
from datetime import date, timedelta
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
        + "coverage and geohash level."
    )
    parser.add_argument("--collection", type=str, default=helpers.SENTINEL_2_CHANNELS)
    parser.add_argument("--outdir", type=str, default="output")
    parser.add_argument("--input_file", type=str)
    parser.add_argument("--precision", type=int, default=5)
    parser.add_argument("--start_date", type=str, default="2020-01-01")
    parser.add_argument("--end_date", type=str, default="2020-02-01")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--n_jobs", type=int, default=30)
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

    # get AoI geometry or load from a json file
    locs = []
    input_json = json.load(open(args.input_file))
    is_geo_json = False

    # assume list is a previously saved set of requests, dict is geojson
    if type(input_json) is dict:
        is_geo_json = True
        aoi = input_json["features"][0]
        polygon = ee.Geometry.Polygon(aoi["geometry"]["coordinates"])

        # determine geohashes that overlap our AoI
        geohashes_aoi = helpers.poly_to_geohashes(
            polygon, precision=args.precision, coarse_precision=args.precision
        )

        # generate request times
        delta = date.fromisoformat(args.end_date) - date.fromisoformat(args.start_date)
        interval = args.interval
        for i in range(int(delta.days / interval)):
            start_date = date.fromisoformat(args.start_date) + timedelta(
                days=i * interval
            )
            end_date = start_date + timedelta(days=interval)

            # Create records for each geohash
            for geohash in geohashes_aoi:
                locs.append(
                    {
                        "date_start": str(start_date),
                        "date_end": str(end_date),
                        "geohash": geohash,
                    }
                )
    else:
        # assume this a previously saved set of requests
        locs = input_json

    # save the metadata associated with the collection and bands we are fetching
    helpers.fetch_metadata(locs[0], args.outdir, collection, bands)

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
