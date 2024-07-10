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
from apscheduler.schedulers.background import BackgroundScheduler


def on_connect(client, userdata, flags, rc):
    print("connrect to mqtt",config["BROKER_ADDRESS"])

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

sourceUnitsId = "62ff525f0053c325ccf27a1d"
destUnitId = "65cdb12fd958e80007254cf3"
sourcePredix = "SIK"
destPrefix = "YYM"

dataEx = dataEx()
# dataEx.mainFuncPower(sourceUnitsId,destUnitId,client,sourcePredix,destPrefix)
dataEx.mainFuncPowerBackFIll(sourceUnitsId,client,destUnitId,sourcePredix,destPrefix)
exit()
scheduler = BackgroundScheduler()
scheduler.add_job(func=dataEx.mainFuncPower,args=[sourceUnitsId,destUnitId,client,sourcePredix,destPrefix], trigger="interval", seconds=60*5,max_instances=3)
scheduler.start()

client.loop_forever(retry_first_connection=True)
