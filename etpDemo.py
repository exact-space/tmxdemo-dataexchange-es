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

sourceUnitsId = "63fc54b033cc8b00080e0b39"
destUnitId = "666bee57f5a60900074b2e15"
sourcePredix = "YYO"
destPrefix = "UQD"

dataEx = dataEx()
dataEx.mainFuncETP(sourceUnitsId,destUnitId,client,sourcePredix,destPrefix)

client.connect(config["BROKER_ADDRESS"], 1883, 2800)

scheduler = BackgroundScheduler()
scheduler.add_job(func=dataEx.mainFuncETP,args=[sourceUnitsId,destUnitId,client,sourcePredix,destPrefix], trigger="interval", seconds=60*5,max_instances=3)
scheduler.start()

client.loop_forever(retry_first_connection=True)
