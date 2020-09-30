import argparse
import json
import os
import random
from datetime import date, timedelta
from joblib import Parallel, delayed
from tqdm import tqdm
import ee
import helpers


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=str, default="output")
    parser.add_argument("--aoi_geojson", type=str)
    parser.add_argument("--precision", type=int, default=5)
    parser.add_argument("--start_date", type=str, default='2020-01-01')
    parser.add_argument("--end_date", type=str, default='2020-02-01')
    parser.add_argument("--n_jobs", type=int, default=30)
    return parser.parse_args()


def main():

    ee.Initialize()

    args = parse_args()

    # get aoi geometry
    aoi = json.load(open(args.aoi_geojson))["features"][0]
    polygon = ee.Geometry.Polygon(aoi["geometry"]["coordinates"])

    # determine geohashes that overlap our AoI
    geohashes_aoi = helpers.polygon2geohash(
        polygon, precision=args.precision, coarse_precision=args.precision
    )

    # Prepare to load data
    os.makedirs(args.outdir, exist_ok=True)

    # Create records for each geohash
    locs = []
    for geohash in geohashes_aoi:
        locs.append(
            {
                "date_start": args.start_date,
                "date_end": args.end_date,
                "geohash": geohash,
            }
        )

    # Run jobs in parallel
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
