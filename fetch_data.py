import argparse
import json
import os
import random
from joblib import Parallel, delayed
from tqdm import tqdm
import ee
import helpers
from pathlib import Path

AREA_DIR = "area"
POI_DIR = "poi"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetches tiles from a Google Earth Engine collection based on time range, "
        + "coverage and geohash level.  All tiles within the coverage area will be fetched by default. "
        + "If a list of points of interest is provided, tiles overlapping the points will be clipped to the"
        + "coverage area and fetched."
    )
    parser.add_argument("--collection", type=str, default=helpers.SENTINEL_2_COLLECTION)
    parser.add_argument(
        "--channels", nargs="+", type=str, default=helpers.SENTINEL_2_CHANNELS
    )
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
    parser.add_argument("--seed", type=int, default=42)
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

    # for now we define bands by collection, but this can be made more general by supplying
    # as an argument or via config
    bands = helpers.SENTINEL_2_CHANNELS
    collection = helpers.SENTINEL_2_COLLECTION
    if args.collection == helpers.SENTINEL_2_COLLECTION:
        collection = args.collection
        bands = helpers.SENTINEL_2_CHANNELS
    else:
        collection = args.collection
        bands = args.channels

    requests = []
    is_geo_json = False

    if args.input_file:
        # loads previously saved requests
        input_json = json.load(open(args.input_file))
        requests = input_json

    else:
        # loading coverage polygon from geo json file
        is_geo_json = True
        coverage_geojson = json.load(open(args.coverage_file))

        # generate geohashes covered by the AoI
        geohashes_aoi = helpers.geohashes_from_geojson_poly(
            coverage_geojson, args.precision
        )

        # generate geohash + intervals, applying sampling
        request_data = helpers.generate_fetch_requests(
            geohashes_aoi,
            args.start_date,
            args.end_date,
            args.interval,
            args.sampling,
        )

        if args.poi_file is not None:
            # load points of interest from geo json file
            poi_geojson = json.load(open(args.poi_file))

            # generate the geohashes containing each PoI, clipped geospatially and
            # temporally to the AoI and time bounds
            geohashes_poi = helpers.geohashes_from_geojson_points(
                geohashes_aoi,
                poi_geojson,
                args.start_date,
                args.end_date,
                args.precision,
            )

            # merge the AoI geohash samples with the PoI data
            request_data = helpers.generate_fetch_requests_poi(
                request_data,
                geohashes_poi,
                args.interval,
            )

        # encode request data as json
        for d in request_data:
            requests.append(
                {
                    "geohash": d[0],
                    "poi": d[1],
                    "date_start": str(d[2][0]),
                    "date_end": str(d[2][1]),
                }
            )

    # save the metadata associated with the collection and bands we are fetching
    helpers.fetch_metadata(args.outdir, collection, bands)

    if args.poi_file != "":
        Path(os.path.join(args.outdir, AREA_DIR)).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(args.outdir, POI_DIR)).mkdir(parents=True, exist_ok=True)
    else:
        Path(args.outdir).mkdir(parents=True, exist_ok=True)

    # save fetched tile info to json if required
    if args.save_requests and is_geo_json:
        output_path = os.path.join(args.outdir, "requests.json")
        with open(output_path, "w+") as json_file:
            json.dump(requests, json_file, indent=4)

    # Run jobs in parallel
    if not args.skip_fetch:
        jobs = []
        for request in requests:
            # generate target path based on POI presence
            outdir = args.outdir
            if args.poi_file != "":
                subdir = POI_DIR if request["poi"] else AREA_DIR
                outdir = os.path.join(outdir, subdir)

            job = delayed(helpers.fetch_tile)(request, outdir, collection, bands)
            jobs.append(job)

        random.Random(args.seed).shuffle(jobs)

        _ = Parallel(
            backend="multiprocessing", n_jobs=args.n_jobs, verbose=1, batch_size=4
        )(tqdm(jobs))


if __name__ == "__main__":
    main()
