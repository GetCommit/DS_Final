import json
import sys
from os import getenv
from os import path
import pandas as pd
from mapbox import Geocoder
from tqdm import tqdm
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo

MAPBOX_TOKEN = getenv("MAPBOX_TOKEN")
geocoder = Geocoder(access_token=MAPBOX_TOKEN)
ONE_MINUTE = 60

@on_exception(expo, RateLimitException, max_tries=8)
@limits(calls=500, period=ONE_MINUTE)
def query_mapbox(lat, lon):
    response = geocoder.reverse(
        lon=lon,
        lat=lat
    )
    if response.status_code != 200:
        raise ValueError(response.text)
    try:
        return response.geojson()["features"][0]["context"][0]["text"]
    except:
        print(lon, lat)
        print(response.geojson())

def cache_geos(path):
    miss = 0
    hit = 0
    cache = {}
    df = pd.read_csv(path, header=0)
    for index, row in tqdm(df.iterrows()):
        coordinate = "("+str(row["Latitude"])+","+str(row["Longitude"])+")"
        if coordinate not in cache:
            cache[coordinate] = query_mapbox(lat=row["Latitude"], lon=row["Longitude"])
            miss += 1
        else:
            hit += 1

    return cache

def get_neighborhood(lat, lon):
    coordinate = "("+str(lat)+","+str(lon)+")"
    return cache[coordinate]
        
cache = {}
if not path.exists("cache.json"):
    print("Cache does not exist. Populating cache by querying MapBox API...")
    cache = cache_geos("DOHMH_New_York_City_Restaurant_Inspection_Results.csv")
    with open("cache.json", 'w') as outfile:
        json.dump(cache, outfile)
else:
    with open("cache.json", 'r') as f:
        cache = json.load(f)
