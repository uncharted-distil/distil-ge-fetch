import argparse
import json
import os
import random
from joblib import Parallel, delayed
from tqdm import tqdm
import ee
import helpers


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=str, default="output")
    parser.add_argument("--input_file", type=str)
    parser.add_argument("--precision", type=int, default=5)
    parser.add_argument("--start_date", type=str, default="2020-01-01")
    parser.add_argument("--end_date", type=str, default="2020-02-01")
    parser.add_argument("--n_jobs", type=int, default=30)
    parser.add_argument(
        "--save_requests", dest="save_requests", default=False, action="store_true"
    )
    parser.add_argument(
        "--skip_fetch", dest="skip_fetch", default=False, action="store_true"
    )

    return parser.parse_args()


def main():

    ee.Initialize()

    args = parse_args()
    print(args)

    # get aoi geometry or load from a json file
    locs = []
    input_json = json.load(open(args.input_file))
    is_geo_json = False
    # assume list is a previously saved set of requests, dict is geojson
    if type(input_json) is dict:
        is_geo_json = True
        aoi = input_json["features"][0]
        polygon = ee.Geometry.Polygon(aoi["geometry"]["coordinates"])

        # determine geohashes that overlap our AoI
        geohashes_aoi = helpers.polygon2geohash(
            polygon, precision=args.precision, coarse_precision=args.precision
        )

        # Create records for each geohash
        for geohash in geohashes_aoi:
            locs.append(
                {
                    "date_start": args.start_date,
                    "date_end": args.end_date,
                    "geohash": geohash,
                }
            )
    else:
        # assume this a previously saved set of requests
        locs = input_json

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
            job = delayed(helpers.get_one_sentinel)(loc, outdir=args.outdir)
            jobs.append(job)

        random.shuffle(jobs)

        _ = Parallel(
            backend="multiprocessing", n_jobs=args.n_jobs, verbose=1, batch_size=4
        )(tqdm(jobs))


if __name__ == "__main__":
    main()
