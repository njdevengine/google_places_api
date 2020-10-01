#300 places requests = $12
# ~4700 requests = ~$100
#https://developers.google.com/places/supported_types
#https://developers.google.com/places/web-service/search
#USAGE: https://console.cloud.google.com/apis/dashboard
#PRICING: https://cloud.google.com/maps-platform/pricing/sheet/

gkey = "your google api key"
# Dependencies

import requests
import json
import pandas as pd
from pandas import json_normalize 
import time
import os
from os import listdir
from os.path import isfile, join

df = pd.read_csv("/Users/username/Downloads/300_cities_geocoded.csv")

lats = list(df.Lat)
lons = list(df.Lon)
coords = []
for i in range(len(lats)):
    lat = str(lats[i])
    lon = str(lons[i])
    coord = lat+","+lon
    coords.append(coord)
    
def search(keyword,target,radius,start,end,directory):
    try:
        os.mkdir(directory)
    except: print("directory exists...")
    for i in range(len(lats))[start:end]:
        # geocoordinates
        target_coordinates = coords[i]
        #keywords
        target_search = keyword
        #radius in meters
        target_radius = radius
        #supported type
        target_type = target
        # set up a parameters dictionary
        params = {
            "location": target_coordinates,
            "keyword": target_search,
            "radius": target_radius,
            "type": target_type,
            "key": gkey
        }
        # base url
        base_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

        counter = 0
        # run a request using our params dictionary
        try:
            response = requests.get(base_url, params=params)
            p1 = json_normalize(response.json(), 'results')
            counter+=1
            time.sleep(5)
        except: pass
        try:
            token = response.json()["next_page_token"]
            next_page = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?pagetoken="+token
            response2 = requests.get(next_page, params=params)
            p2 = json_normalize(response2.json(), 'results')
            counter+=1
            time.sleep(5)
        except: pass
        try:
            token2 = response2.json()["next_page_token"]
            next_page2 = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?pagetoken="+token2
            response3 = requests.get(next_page2, params=params)
            p3 = json_normalize(response3.json(), 'results')
            counter+=1
            time.sleep(5)
        except: pass

        if counter == 1:
            try:
                result_set = p1
            except: pass
        elif counter == 2:
            try:
                result_set = pd.concat([p1,p2])
            except: pass
        elif counter == 3:
            try:
                result_set = pd.concat([p1,p2,p3])
            except: pass
        else:
            print("error")
        
        try:
            result_set.to_csv(directory+"/"+keyword+(df.City[i]+"_"+df.ST[i]).replace(" ","_").lower()+".csv")
            print("SAVED: "+df.City[i]+", "+df.ST[i],len(result_set))
        except:
            print("ERROR: "+df.City[i]+", "+df.ST[i])
        
        mypath = directory
        onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        frames = []
        for i in onlyfiles:
            frame = pd.read_csv(mypath+'/'+i)
            frames.append(frame)
        data = pd.concat(frames)
        clean = data.drop_duplicates()
        clean.to_csv(target+"_"+str(radius)+".csv")
# example query        
# search("liquor store","liquor_store",16000,10,101,"liquor_results")

mypath = r"liquor_results"
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

frames = []
for i in onlyfiles:
    frame = pd.read_csv(mypath+'/'+i)
    frames.append(frame)
data = pd.concat(frames)
clean = data.drop_duplicates()
#place_id is needed for place details search
places = list(clean["place_id"].unique())

details = []
#details search
for i in range(len(places))[:]:
    if i % 100 ==0:
        base_url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            "key": gkey,
            "place_id": places[i],
            "fields": "formatted_phone_number,website,formatted_address"
        }
        response = requests.get(base_url, params=params)
        d = json_normalize(response.json()["result"])
        d["place_id"] = places[i]
        details.append(d)
        time.sleep(1)
        
all_details = pd.concat(details)
master = clean.merge(all_details, how="left", on="place_id")
master.to_csv("liquor_store_data.csv")
        print(i)
    else:
        base_url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            "key": gkey,
            "place_id": places[i],
            "fields": "formatted_phone_number,website,formatted_address"
        }
        response = requests.get(base_url, params=params)
        d = json_normalize(response.json()["result"])
        d["place_id"] = places[i]
        details.append(d)
        time.sleep(1)
