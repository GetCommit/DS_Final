# DS_Final

### Data we are using:

NYC Restaurant Inspections: https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j

NYC Property Sales (2010-2018): https://data.cityofnewyork.us/City-Government/DOF-Summary-of-Neighborhood-Sales-by-Neighborhood-/5ebm-myj7

### Coordinates to Neighborhood

We are using [MapBox API](https://account.mapbox.com/) to convert coordinates (longitude, latitude) to NYC neiborhoods. We are maintaining a cache for all the coordinates in our dataset to save API calls (money). This cache can be populated via the `populate_cache.py` script:

python3 populate_cache.py DOHMH_New_York_City_Restaurant_Inspection_Results.csv cache.json