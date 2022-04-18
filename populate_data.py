import pandas as pd
import numpy as np
import os
import shutil
from geopy.distance import distance
import tqdm
from functools import lru_cache
from fuzzywuzzy import fuzz, process
from multiprocessing import Pool
import logging, multiprocessing
import sys

# Data Generation / Startup Scripts
MILES_TOLERANCE = 30
REGENERATE_ALL_SCHOOLS_ON_STARTUP = False
RESULTS_SUBDIRECTORY = "pre_calculated_results/"
calc_cache = {}

def calc_dist_in_mi(lat1, lon1, lat2, lon2):
    # If distance is above our range, then boot out without calculation
    if abs(lat1 - lat2) >= MILES_TOLERANCE / 65 or abs(lon1 - lon2) >= MILES_TOLERANCE / 50:
        return np.inf
    # If we know this distance already, take from the cache
    if (lat1, lon1, lat2, lon2) in calc_cache:
        return calc_cache[(lat1, lon1, lat2, lon2)]
    if (lat2, lon2, lat1, lon1) in calc_cache:
        return calc_cache[(lat2, lon2, lat1, lon1)]
    # Else, if we know all the values, then calculate it and save it in the cache
    if not np.isnan(lat1) and not np.isnan(lon1) and not np.isnan(lat2) and not np.isnan(lon2):
        retVal = distance((lat1, lon1), (lat2, lon2)).mi
        calc_cache[(lat1, lon1, lat2, lon2)] = retVal
        return retVal
    return None

def add_leading_zeros(z):
    # Make sure all zipcodes are length == 5
    z = str(z)
    if len(z) >= 5:
        return z
    else:
        while len(z) < 5:
            z = "0" + z
        return z

def append_zeros(s):
    try:
        while len(s) < 12:
            s = '0' + s
        return s
    except:
        return s

def regenerate_all_schools_on_startup():
    if os.path.exists(RESULTS_SUBDIRECTORY) and os.path.isdir(RESULTS_SUBDIRECTORY):
        shutil.rmtree(RESULTS_SUBDIRECTORY)
    os.makedirs(RESULTS_SUBDIRECTORY)

def get_zipcode_school_file(zipcode):
    return RESULTS_SUBDIRECTORY + add_leading_zeros(zipcode) + ".csv"

# Create school database and attach lat-lon coordinate to each school
ratings = pd.read_csv("gs_2021_and_niche_2017_ratings.csv", dtype={"nces_id": str})
iims = pd.read_csv("SchoolData_April142022.csv", dtype={"SEEDID": str})
iims["SEEDID"] = iims["SEEDID"].apply(lambda x: append_zeros(x))
ratings["nces_id"] = ratings["nces_id"].apply(lambda x: append_zeros(x))
only_active = iims[iims["IsActive"] == 1]
schools_db = pd.merge(ratings, only_active, how='left', left_on="nces_id", right_on="SEEDID")

zip_codes = pd.read_csv("US.txt", delimiter = "\t")

def _calc_all_distances(zipcode):
    try:
        z_record = zip_codes[zip_codes["Zipcode"] == zipcode]
        # Calculate distance between school and currently processing zipcode
        schools_dist = schools_db.apply(lambda row: calc_dist_in_mi(row['lat'], row['long'], z_record.Latitude.item(), z_record.Longitude.item()), axis=1)
        # Filter for only schools within the tolerance and save those to disk
        schools_db[schools_dist <= MILES_TOLERANCE].to_csv(get_zipcode_school_file(zipcode))
        return
    except Exception as e:
        print(f"zipcode ${zipcode} cannot be processed! ", e)
        return

# This code will run on cluster start
# Read in file with all zipcodes and their lat-lon coordinates
zip_codes = pd.read_csv("US.txt", delimiter = "\t")

# Get all valid US zipcodes
all_zip_codes = set(zip_codes["Zipcode"])

# Switch to ensure that we don't erase full cache of zipcode-school files if server restarts
if (not REGENERATE_ALL_SCHOOLS_ON_STARTUP):
    # If zipcode-school file already exists, then don't re-run that zipcode. If subdirectory not found, default to creating all zipcode-school files
    if os.path.exists(RESULTS_SUBDIRECTORY) and os.path.isdir(RESULTS_SUBDIRECTORY):
        run_only_these_zip_codes = set()
        for z in all_zip_codes:
            if not os.path.isfile(get_zipcode_school_file(z)):
                run_only_these_zip_codes.add(z)
        all_zip_codes = run_only_these_zip_codes
    else:
        regenerate_all_schools_on_startup()
else:
    regenerate_all_schools_on_startup()

logger = multiprocessing.log_to_stderr()
logger.setLevel(logging.DEBUG)

with Pool() as p:
  r = list(tqdm.tqdm(p.imap(_calc_all_distances, list(all_zip_codes)), total=len(all_zip_codes)))

# Reset calc_cache so we free up RAM
calc_cache = {}
print("-------------------------- CACHE GENERATION COMPLETE --------------------------")