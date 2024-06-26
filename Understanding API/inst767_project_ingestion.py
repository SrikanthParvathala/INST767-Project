# -*- coding: utf-8 -*-
"""INST767_Project_Ingestion_Sravya.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Q8bG6gVswrlB8YzIq_43xSA3UO1pdrJ5

File: main.py
Author: Sravya Lenka
Date: 04 April, 2024
Description: Testing APIs for Ingestion Step of Google Cloud
"""

#Import statements

import requests
import pandas as pd
from google.cloud import storage
import http.client, urllib.request, urllib.parse, urllib.error, base64

"""E-scooter - https://ddot.dc.gov/page/dockless-api

Bus or metro train - https://developer.wmata.com/docs/services/

Location - https://developers.google.com/maps/documentation/geocoding/requests-reverse-geocoding

#Bus data, Bike share data

#Capital Bike Share
"""

data_lyft = requests.get('https://gbfs.lyft.com/gbfs/1.1/dca-cabi/en/ebikes_at_stations.json')
json = data_lyft.json()

#need to unparse the ebikes into separate columns as well
# station_id_df =  pd.DataFrame(json['data']['stations']['station_id'])
lyft_df = pd.json_normalize(json['data']['stations'], record_path=['ebikes'], meta='station_id')
lyft_df = lyft_df[['station_id','is_lbs_internal_rideable','make_and_model','battery_charge_percentage','rideable_id','docking_capability']]
# df = pd.DataFrame(json['data']['stations'])
# df = pd.json_normalize(json['data']['stations'], max_level = 1)

lyft_df

data_lyft = requests.get('https://gbfs.lyft.com/gbfs/1.1/dca-cabi/en/free_bike_status.json')
json = data_lyft.json()

print(json)
#need to unparse the ebikes into separate columns as well
# station_id_df =  pd.DataFrame(json['data']['stations']['station_id'])
lyft_df = pd.json_normalize(json['data']['bikes'])
lyft_df = lyft_df[['bike_id','is_disabled','name','lon','fusion_lon','fusion_lat','lat','type','is_reserved']]
# lyft_df = lyft_df[['station_id','is_lbs_internal_rideable','make_and_model','battery_charge_percentage','rideable_id','docking_capability']]
# # df = pd.DataFrame(json['data']['stations'])
# # df = pd.json_normalize(json['data']['stations'], max_level = 1)

lyft_df

url = 'https://gbfs.lyft.com/gbfs/1.1/dca-cabi/en/free_bike_status.json'
if("lyft" in url):
  print("lyft")

import pytz
data_lyft = requests.get('https://gbfs.lyft.com/gbfs/1.1/dca-cabi/en/system_information.json')
json = data_lyft.json()

lyft_time_df = pd.json_normalize(json,meta='last_updated')
lyft_time_df = lyft_time_df[['last_updated','data.timezone']]

timezone = pytz.timezone(lyft_time_df['data.timezone'][0])
lyft_time_df['last_updated'] = pd.to_datetime(lyft_time_df['last_updated'], unit='s')

datetime_la = timezone.localize(lyft_time_df['last_updated'][0])
print(datetime_la)
lyft_time_df

lyft_df.insert(0, 'timestamp', datetime_la)
lyft_df

"""#Lime"""

lime_data = requests.get('https://data.lime.bike/api/partners/v1/gbfs/washington_dc/free_bike_status.json')
json = lime_data.json()
print(json)
timestamp = json["last_updated"]
timestamp = pd.to_datetime(timestamp, unit='s')
print(timestamp)

df = pd.json_normalize(json['data']['bikes'])
df = df[["bike_id","lat","lon","is_reserved","is_disabled","vehicle_type"]]
df.insert(0, 'timestamp', timestamp)
# df = pd.DataFrame(json['data']['stations'])
# df = pd.json_normalize(json['data']['stations'], max_level = 1)
df

"""#WMATA

Bus Positions API keeps updating based on the location of the respective bus, it won't make sense to use it unless we are trying to get the location of the bus
"""

#route_id can change - i gave 70 here
headers = {
    # Request headers
    'api_key': 'd8be252fdef441d195f6856645486af4',
}

params = urllib.parse.urlencode({
    # Request parameters
    'RouteID': '70',
    'Lat': '{number}',
    'Lon': '{number}',
    'Radius': '{number}',
})


try:
    conn = http.client.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/Bus.svc/json/jBusPositions?%s" % params, "{body}", headers)
    response = conn.getresponse()
    data = response.read()
    print(data)
    conn.close()
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))

#route_id can change - i gave 70 here
headers = {
    # Request headers
    'api_key': 'd8be252fdef441d195f6856645486af4',
}

params = urllib.parse.urlencode({
    # Request parameters
    'Lat': '38.8940',
    'Lon': '-76.9501',
    'Radius': '15',
})


try:
    conn = http.client.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/Bus.svc/json/jBusPositions?%s" % params, "{body}", headers)
    response = conn.getresponse()
    data = response.read()
    print(data)
    conn.close()
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))

"""Bus Route and Stop Methods - Stop Search - https://developer.wmata.com/docs/services/54763629281d83086473f231/operations/5476362a281d830c946a3d6d?

"""

headers = {
    # Request headers
    'api_key': 'd8be252fdef441d195f6856645486af4',
}

params = urllib.parse.urlencode({
    # Request parameters
    'Lat': '38.8940',
    'Lon': '-76.9501',
    'Radius': '100',
})

try:
    conn = http.client.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/Bus.svc/json/jStops?%s" % params, "{body}", headers)
    response = conn.getresponse()
    data = response.read()
    print(data)
    conn.close()
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))

headers = {
    # Request headers
    'api_key': 'd8be252fdef441d195f6856645486af4',
}

params = urllib.parse.urlencode({
})

try:
    conn = http.client.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/gtfs/bus-gtfs-static.zip?%s" % params, "{body}", headers)
    response = conn.getresponse()
    data = response.read()
    print(data)
    conn.close()
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))

"""#Reverse Geo Coding"""
#Removed API Key
geo_data = requests.get("https://maps.googleapis.com/maps/api/geocode/json?latlng=38.8940,-76.9501&result_type=street_address&key=xxx")
json = geo_data.json()
print(json)

geo_data

"""#Google Cloud Function Testing"""

import requests
import pandas as pd
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs



def api_to_gcs(url, filename):
    data = requests.get(url)
    json = data.json()
    df = pd.json_normalize(json['data']['stations'], record_path=['ebikes'], meta='station_id')
    df = df[['station_id','is_lbs_internal_rideable','make_and_model','battery_charge_percentage','rideable_id','docking_capability']]

    # df = pd.DataFrame(json['data'][endpoint])
    # client = storage.Client(project='inst767-417717')
    # bucket = client.get_bucket('transit-bucket')
    # blob = bucket.blob(filename)
    # # blob.upload_from_string(df.to_csv(index = False),content_type = 'csv')
    # # Convert DataFrame to Parquet and append to existing file
    table = pa.Table.from_pandas(df)
    if("/content/sample_data"+filename)
    pq.write_table(table, "/content/sample_data/"+filename)
    # else:
    #     # Use append for existing file
    #     pq.write_to_dataset(table, root_path=f"/{filename}", append = True)





api_to_gcs('https://gbfs.lyft.com/gbfs/1.1/dca-cabi/en/ebikes_at_stations.json','stations.parquet')

"""#Testing Parquet"""

existing_data = pd.read_parquet("/content/stations.parquet")

existing_data

existing_data_new = pd.read_parquet("/content/stations (1).parquet")

existing_data_new

"""#WMATA - Cloud Function

https://api.wmata.com/Bus.svc/json/jBusPositions - WMATA Bus Positions
"""

import json

headers = {
    # Request headers
    'api_key': 'd8be252fdef441d195f6856645486af4',
}


try:
    conn = http.client.HTTPSConnection('api.wmata.com')
    conn.request("GET", "/Bus.svc/json/jBusPositions" , {}, headers)
    response = conn.getresponse()
    print(response)
    json_data = response.read().decode('utf-8')  # Decode bytes to string

    print(json_data)
    data = json.loads(json_data)
    df = pd.DataFrame(data['BusPositions'])

except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))


df

"""#Weather API

"""

url = "https://api.tomorrow.io/v4/weather/realtime?location=washington%20dc&apikey=uKLdl7w0X5W8o5iCYr8K7LcnvlhvHK8A"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)
json = response.json()

df = pd.json_normalize(json)

df.columns = ['time', 'cloudBase', 'cloudCeiling',
       'cloudCover', 'dewPoint',
       'freezingRainIntensity', 'humidity',
       'precipitationProbability',
       'pressureSurfaceLevel', 'rainIntensity',
       'sleetIntensity', 'snowIntensity',
       'temperature', 'temperatureApparent',
       'uvHealthConcern', 'uvIndex',
       'visibility', 'weatherCode',
       'windDirection', 'windGust',
       'windSpeed', 'lat', 'lon',
       'locationName', 'locationType']
df

df.columns

