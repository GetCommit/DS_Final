# DS_Final

### Data we are using:

NYC Restaurant Inspections: https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j

NYC Property Sales (2010-2018): https://data.cityofnewyork.us/City-Government/DOF-Summary-of-Neighborhood-Sales-by-Neighborhood-/5ebm-myj7

### Coordinates to Neighborhood

We are using [MapBox API](https://account.mapbox.com/) to convert coordinates (longitude, latitude) to NYC neiborhoods. We are maintaining a cache for all the coordinates in our dataset to save API calls (money). This cache can be populated when the `cache` module is imported as such:

`from cache import *`

The total time to populate our cache is at least 1 hour due to the rate limit of 600 req/s of the MapBox API. Therefore, it is recommended to use the already-populated cache whenever possible (meaning do not delete `cache.json`)

### ETL
We used Spark to clean and join the datasets in etl.py. The program will first transform a column from each data frame using the cached Coordinates to Neighborhood mapping. The joined data frame will be saved in /Joined Data folder, and the size will be about 4.8 GB. 
