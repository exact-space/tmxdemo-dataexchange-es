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



currentTimeStamp = int(time.time()*1000)


currentTime = datetime.datetime.now()
currentMonth = currentTime.month 
currentQuarter = (currentMonth-1)//3 + 1
currentDay = currentTime.day 
currentHour = currentTime.hour
currentMinute =  currentTime.minute
currentSecond = currentTime.second
last5Minute = abs(currentMinute - 5)
validMonth = (currentMonth - (currentQuarter-1)*3)

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


unitsId = "61c0c34bb45a623b64fc3b12"
dataEx = dataEx()
# try:
    # dataEx.getLoginToken()
# except:
    # dataEx.getLoginToken()

tag_df = dataEx.getTagmeta(unitsId)

tagList = list(tag_df["dataTagId"]) + ['CEN1_Direct_Boiler_Efficiency'] + ['CEN1_BLR1_STEAM_GEN_HRLY','CEN1_BLR1_FUEL_CONS_HRLY','CEN1_1_boiler_Efficiency_prc_hourly']

startDate = "2022/{}/{} {}:{}:{}".format(validMonth,currentDay,currentHour,last5Minute,currentSecond)
endDate = "2022/{}/{} {}:{}:{}".format(validMonth,currentDay,currentHour,currentMinute,currentSecond)

print(startDate,endDate)
try:
    startDate = datetime.datetime.strptime(startDate, '%Y/%m/%d %H:%M:%S')
    endDate = datetime.datetime.strptime(endDate, '%Y/%m/%d %H:%M:%S')
except ValueError:
    startDate = "2022/{}/{} {}:{}:{}".format(6,28,currentHour,last5Minute,currentSecond)
    endDate = "2022/{}/{} {}:{}:{}".format(6,28,currentHour,currentMinute,currentSecond)

    startDate = datetime.datetime.strptime(startDate, '%Y/%m/%d %H:%M:%S')
    endDate = datetime.datetime.strptime(endDate, '%Y/%m/%d %H:%M:%S')


print(startDate,endDate)
startTimestamp=time.mktime(startDate.timetuple())*1000
endTimestamp=time.mktime(endDate.timetuple())*1000

# tagList = ["CEN1_BLR1_SFR_stmflw","CEN1_BLR1_FUEL_CONS_HRLY","CEN1_BLR1_STEAM_GEN_HRLY"]
tagList2 = ["CEN1_OverallEffGCV","CEN1_Direct_Boiler_Efficiency","CEN1_BOILER_LOAD","CEN1_PT2_7_SCALE_PV","CEN1_VFD_M3"]
print("time frame",startTimestamp,endTimestamp)
dataEx.dataExachangeHeating(tagList,startTimestamp,endTimestamp)
dataEx.dataExachangeHeating(tagList2,startTimestamp,endTimestamp)
