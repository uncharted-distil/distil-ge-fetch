import argparse
import json
import zipfile
from tqdm import tqdm
import os
import random
from glob import glob
from typing import Dict, List, Tuple
from zipfile import ZipFile

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from ee.ee_number import Number
from tifffile import imread


def zip2numpy_sentinel(inpath: str) -> np.ndarray:
    """- parses zip file and combines all tiffs into ndarray"""
    inpath = inpath.strip()
    ghash = os.path.basename(inpath).split('.')[0]
    ghash, _ = ghash.split('_')
    with ZipFile(inpath) as handle:
        tile = np.stack([
            imread(handle.open(f'{ghash}.B1.tif')),
            imread(handle.open(f'{ghash}.B2.tif')),
            imread(handle.open(f'{ghash}.B3.tif')),
            imread(handle.open(f'{ghash}.B4.tif')),
            imread(handle.open(f'{ghash}.B5.tif')),
            imread(handle.open(f'{ghash}.B6.tif')),
            imread(handle.open(f'{ghash}.B7.tif')),
            imread(handle.open(f'{ghash}.B8.tif')),
            imread(handle.open(f'{ghash}.B8A.tif')),
            imread(handle.open(f'{ghash}.B9.tif')),
            imread(handle.open(f'{ghash}.B11.tif')),
            imread(handle.open(f'{ghash}.B12.tif')),
            imread(handle.open(f'{ghash}.QA60.tif'))
        ])
        tile = tile.astype(np.int32)

    return tile

def parse_geo_hash(file_path: str) -> str:
    inpath = file_path.strip()
    ghash = os.path.basename(inpath).split('.')[0]
    ghash, _ = ghash.split('_')
    return ghash

def build_spatial_hash(inpath: str, level_of_precision: Number) -> Dict[str, List[str]]:
    zip_files = glob(inpath + "/area/*")
    spatial_map = {}
    for file in zip_files:
        geo_hash = parse_geo_hash(file)[:level_of_precision]
        if geo_hash not in spatial_map:
            spatial_map[geo_hash] = [file]
            continue
        spatial_map[geo_hash].append(file)
    return spatial_map

def get_NDVI(arr: np.ndarray) -> np.ndarray:
    """- returns an NDVI image created from B8 AND B4"""
    NIR = arr[7] #B8
    RED = arr[3] #B4
    return ((NIR - RED)/(NIR + RED))


def get_STR(arr: np.ndarray) -> np.ndarray:
    """
        - eq. 7 in paper \n
        - returns surface reflectance from B12
    """
    RSWIR = arr[11] #B12
    STR   = ((1-RSWIR)**2)/(2*RSWIR)
    return STR

def fit_quanmod(df: pd.DataFrame, q: Number) -> List[float]:
    """
        - quantile regression
        - returns array of floats [Intercept, NDVI]
    """
    mod = smf.quantreg('STR ~ NDVI', df)
    res = mod.fit(q=q)
    return [res.params['Intercept'], res.params['NDVI']]

def load_images(files: List[str]) -> Tuple[pd.DataFrame, Number]:
    NDVI, STR = [], []
    print("Loading images into memory", end="\r")
    # loop through each file
    for i in range(len(files)):
        img = zip2numpy_sentinel(files[i])
        if img.sum() == 0:
            continue
        # convert S2 level 1C image to Top-of-atmosphere scale (sec 3.2)
        img = img*0.0001
        NDVI.append(get_NDVI(img).flatten())
        STR.append(get_STR(img).flatten())
        
    # this seems redundant, instead of appending above it could be concatenate
    NDVI = np.concatenate(NDVI, axis=0)
    STR  = np.concatenate(STR, axis=0)

    # filter likely outliers
    idx = np.where(STR <  20.)[0]
    NDVI, STR = NDVI[idx], STR[idx]

    idx = np.where(NDVI >  0)[0]
    NDVI, STR = NDVI[idx], STR[idx]

    return pd.DataFrame(zip(NDVI,STR), columns=['NDVI', 'STR']), len(files)

def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculates the OPTRAM variables for the given dataset. Which can be used to calculate the moisture for sentinel-2 data."
    )
    parser.add_argument("--download_location", type=str, required=True)
    parser.add_argument("--precision", type=Number, default=3)
    parser.add_argument("--output_location", type=str, default=".")

    return parser.parse_args()

def calculate_optram_variables(files: List[str]):
    random.shuffle(files)
    df, num_of_samples = load_images(files)
    print("Fitting quantile regression", end="\r")
    models = [fit_quanmod(df, q) for q in [1/num_of_samples, .97]]
    models = pd.DataFrame(models, columns=['a', 'b'])
    #dry edge
    i_d = models.iloc[0]['a'] # intercept of quantile where each bucket is 1 item
    #wet edge
    i_w = models.iloc[-1]['a']# intercept of 0.97 quantile 
    #dry edge
    s_d = i_d + models.iloc[0]['b'] #NDVI of linear regression
    s_w = s_d + models.iloc[0]['b'] #NDVI of linear regression
    # object to dump to file
    optram_variables={
    "i_d": i_d,
    "i_w" : i_w,
    "s_d" : s_d,
    "s_w" : s_w
    }
    return optram_variables

def dump_optram_variables(output_location: str, optram_variables: object):
    output_file_name = output_location+"/optram_variables.json"
    print("Writing optram variables to location: ", output_file_name)
    with open(output_file_name, 'w') as outfile:
        json.dump(optram_variables, outfile)

def main():
    args = parse_args()
    # contains the optram variables for an array of geo hash locations
    geo_hashed_optram_variables = {}
    # parses directory of zip files and segregates files into geohash based on level of precision
    spatial_map = build_spatial_hash(args.download_location, args.precision)
    # iterate through each geo has and calculate the optram variables
    for item in tqdm(spatial_map.items()):
        geo_hash = item[0]
        print("processing location: ", geo_hash, end="\r")
        geo_hashed_optram_variables[geo_hash]=calculate_optram_variables(spatial_map[geo_hash])
    # dump to supplied output location
    dump_optram_variables(args.output_location, geo_hashed_optram_variables)
    
if __name__ == "__main__":
    main()
