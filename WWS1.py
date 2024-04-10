#!/usr/bin/env python
# coding: utf-8

import warnings
warnings.filterwarnings("ignore") 
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
# import paho.mqtt.client as paho
# config = cfg.getconfig()
import os
import time
import datetime
from datetime import timedelta
from dataExchangelmpl import dataEx,config




currentTimeStamp = int(time.time()*1000)
currentTime = datetime.datetime.now()
currentHour = currentTime.hour
currentMinute =  currentTime.minute
currentSecond = currentTime.second
last5Minute = currentMinute - 5
validHour = currentHour - 6*(currentHour//6)

fileName = "MVR_DATA Dump.xlsx"

dataEx().downloadingFileMultipleFiles([fileName])
df = pd.read_excel(fileName,engine="openpyxl")
df

df.drop(["description",'Unnamed: 2'],axis=1,inplace=True)

df.set_index(keys = 'dataTagId',inplace=True)
df = df.T
df.reset_index(inplace=True)

df.rename(columns = {"index":"time"},inplace=True)

df["time"]=pd.to_datetime(df['time'],format="%H:%M:%S")

df['Hour'] = df['time'].dt.hour
df['Minute'] = df['time'].dt.minute

df = df[:360]

valid_df = df[(df['Hour'] == validHour) &  (df["Minute"] > last5Minute) & (df["Minute"] <= currentMinute)]
if len(valid_df) < 1:
    valid_df = df[:5]

count = len(valid_df) -1
for i in valid_df.index:
    valid_df.loc[i,"timeStamp"] = currentTimeStamp - 1*1000*60*count
    count = count - 1


valid_df["timeStamp"].astype(int)


for col in valid_df.columns:
    if col != "time" and col != "timeStamp" and col != "Hour" and col != "Minute":
        tag = "WWS1_" + col
        post_url = config["api"]["datapoints"]
        post_array = valid_df[["timeStamp",col]].dropna().values.tolist()
        print(tag)
        post_body = [{"name":tag,"datapoints":post_array,"tags": {"type":"derived"}}]
        print(post_body)
        res1 = requests.post(post_url,json=post_body)
        print(res1.status_code)
        
        
dataEx().removeFiles([fileName])







