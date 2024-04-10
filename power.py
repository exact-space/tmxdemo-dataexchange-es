import gevent.monkey
gevent.monkey.patch_all()
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


startIdx = int((os.environ.get("idx")))
if startIdx == None:
    print("no idx passed. Exiting...")
    exit()
    
currentTimeStamp = int(time.time()*1000)


currentTime = datetime.datetime.now()
# currentMonth = currentTime.month 
# currentQuarter = (currentMonth-1)//3 + 1
currentDay = currentTime.day 
currentHour = currentTime.hour
currentMinute =  currentTime.minute
currentSecond = currentTime.second
last5Minute = abs(currentMinute - 5)
# validMonth = (currentMonth - (currentQuarter-1)*3)
validMonth = 5

startDate = "2023/{}/{} {}:{}:{}".format(validMonth,currentDay,currentHour,last5Minute,currentSecond)
endDate = "2023/{}/{} {}:{}:{}".format(validMonth,currentDay,currentHour,currentMinute,currentSecond)

print(startDate,endDate)
try:
    startDate = datetime.datetime.strptime(startDate, '%Y/%m/%d %H:%M:%S')
    endDate = datetime.datetime.strptime(endDate, '%Y/%m/%d %H:%M:%S')
except ValueError:
    startDate = "2023/{}/{} {}:{}:{}".format(6,28,currentHour,last5Minute,currentSecond)
    endDate = "2023/{}/{} {}:{}:{}".format(6,28,currentHour,currentMinute,currentSecond)
    
    startDate = datetime.datetime.strptime(startDate, '%Y/%m/%d %H:%M:%S')
    endDate = datetime.datetime.strptime(endDate, '%Y/%m/%d %H:%M:%S')


print(startDate,endDate)
startTimestamp=time.mktime(startDate.timetuple())*1000
endTimestamp=time.mktime(endDate.timetuple())*1000


unitsId = "65cdb12fd958e80007254cf3"
dataEx = dataEx()
# try:
    # dataEx.getLoginToken()
# except:
    # dataEx.getLoginToken()

tag_df = dataEx.getTagmeta(unitsId)

tagList = list(tag_df["dataTagId"])[startIdx:startIdx + 200]

print("time frame",startDate,endDate)
print("time frame",startTimestamp,endTimestamp)

def on_connect(client, userdata, flags, rc):
    print("connrect to mqtt")

def on_log(client, userdata, obj, buff):
    print("log: " + str(buff))
    pass

def on_message(client, userdata, msg):
    pass


client = paho.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log
try:
    username = config["BROKER_USERNAME"]
    password = config["BROKER_PASSWORD"]
    client.username_pw_set(username=username, password=password)
except:
    pass

client.connect(config["BROKER_ADDRESS"], 1883, 2800)

dataEx.dataExachangeHeating(tagList,startTimestamp,endTimestamp,client,unitsId)
print(time.time()*1000 - currentTimeStamp)