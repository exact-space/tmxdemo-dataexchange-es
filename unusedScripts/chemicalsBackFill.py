import requests
from requests.exceptions import Timeout
import pandas as pd
import json
import os
import time
import datetime
from datetime import timedelta
import numpy as np
#import timeseries as ts
# import app_config as cfg
import paho.mqtt.client as paho
import math as m
# config = cfg.getconfig()
global cross_tags
from dataExchangelmpl import dataEx,config
dataEx = dataEx()
unitsId = "61c0c2aab45a623b64fc3b0e"
qb_df = dataEx.getTagmeta(unitsId)

currentTimeStamp = int(time.time()*1000)
currentTimeStamp = 1690828200000
tem = 1688149800000

query = {}
query["metrics"] = []
for metric in qb_df["dataTagId"]:
    query["metrics"].append({"name":metric.replace("QBX1","SMR")})
print(config["api"])
query["start_relative"] = {"value":"15","unit":"days"}
query["start_absolute"] = tem
query["end_absolute"] = currentTimeStamp

url =  config["api"]["datapoints"] + "/delete"
print(url)
res = requests.post(url, json=query)

print(res.status_code)
# exit()
while  currentTimeStamp > tem:
    currentTime = datetime.datetime.now()
    currentTime = dataEx.getDateFromTimeStamp(currentTimeStamp)
    currentDay = currentTime.day
    currentHour = currentTime.hour
    currentMinute =  currentTime.minute
    currentSecond = currentTime.second
    last5Minute = currentMinute - 5

    # if currentDay % 3 == 0:
        # validDay = 20
    # elif currentDay % 3 == 1:
        # validDay = 19
    # elif currentDay % 3 == 2:
        # validDay = 20
    validDay =  6 
    if currentHour % 2 == 0:
        currentHour = 1
    else:
        currentHour = 0

    print(currentDay,validDay,currentHour,currentMinute,last5Minute)

    

    # try:
        # dataEx.getLoginToken()
    # except:
        # dataEx.getLoginToken()

    

    # fileNames = []
    # for i in qb_df["dataTagId"]:
        # fileNames.append(i + ".csv")
    # print(fileNames)
    fileNames = ["tmx_chemicals_demo.csv"]
    dataEx.downloadingFileMultipleFiles(fileNames)
    maidf = pd.read_csv(fileNames[0],parse_dates=["Date"])
    for tag in qb_df["dataTagId"]:
        dataEx.dataExachangeChemicals([tag],validDay,currentHour,currentMinute,last5Minute,currentTimeStamp,maidf)
    currentTimeStamp = currentTimeStamp - 1*1000*60*5
    #dataEx.removeFiles(fileNames)  