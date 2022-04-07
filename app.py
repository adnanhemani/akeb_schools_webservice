from flask import Flask
from flask import request
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

# Requires the following folders in the same directory: US.txt, merged_school_ratings_database.csv

# TODO: experiment with how to get initial data generation faster. Try dask first, then go for GeoPandas, Swifter, Multiprocessing if not satisfactory.

app = Flask(__name__)
RESULTS_SUBDIRECTORY = "pre_calculated_results/"

def get_zipcode_school_file(zipcode):
    return RESULTS_SUBDIRECTORY + add_leading_zeros(zipcode) + ".csv"

def add_leading_zeros(z):
    # Make sure all zipcodes are length == 5
    z = str(z)
    if len(z) >= 5:
        return z
    else:
        while len(z) < 5:
            z = "0" + z
        return z

# Routes / API Endpoints
@app.route("/")
def hello_world():
    return "Health check successful!"

@app.route("/all_schools")
def get_schools():
    # Get inputted zipcode and return the DataFrame object as a JSON string
    student_zipcode = request.args.get('zipcode')
    return get_schools_for_zipcode(student_zipcode).to_json(orient="records")

@app.route("/autocomplete")
def autocomplete():
    zipcode = request.args.get('zipcode')
    str_so_far = request.args.get('input_str')
    THRESHHOLD = 50
    # Get all possible schools for this zipcode
    possible_schools = get_schools_for_zipcode(zipcode)
    # Now, do a fuzzy-match to find which school this user is inputting within the possible schools; filter out schools with low relevance
    fuzzy_scores = filter(lambda matches: matches[1] >= THRESHHOLD, process.extract(str_so_far, list(possible_schools["School"])))
    # Create a table of schools and scores
    school_names, scores = zip(*fuzzy_scores)
    rating_tbl = pd.DataFrame({"School": school_names, "FW_Score": scores})
    # Merge them onto the all possible schools, return in order of fuzzy-matched schools by how relevant they are
    return pd.merge(rating_tbl, possible_schools, on="School", how="left").to_json(orient="records")


# LRU Caching Zipcodes - Get saved dataframe from disk, cache result to minimize IO, and return dataframe
@lru_cache(maxsize=128)
def get_schools_for_zipcode(zipcode):
    try:
        all_schools = pd.read_csv(get_zipcode_school_file(zipcode))
    except FileNotFoundError as e:
        print(e)
        raise e
    return all_schools[["ncessch", "School", "url", "overall_rating", "Zipcode"]]

