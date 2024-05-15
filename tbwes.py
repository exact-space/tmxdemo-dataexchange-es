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
import paho.mqtt.client as mqtt
import math as m
# config = cfg.getconfig()
global cross_tags
from dataExchangelmpl import dataEx,config
from apscheduler.schedulers.background import BackgroundScheduler

 
def on_message(client, userdata, msg):
    # client2.publish(msg.topic,msg.payload)
    pass
   
def on_connect(client, userdata, flags, rc):
    pass


def on_log(client, userdata, obj, buff):
    print("log: " + str(buff))
    # pass

BROKER_ADDRESS = config["BROKER_ADDRESS"] 
print(BROKER_ADDRESS)
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log

port = os.environ.get("Q_PORT")
if not port:
    port = 1883
else:
    port = int(port)
print("Running port", port)

try:
    username = config["BROKER_USERNAME"]
    password = config["BROKER_PASSWORD"]
    client.username_pw_set(username=username, password=password)
except:
    pass

client.connect(config['BROKER_ADDRESS'], port, 60)

unitsId = "62e9106d75c9b4657aebc8fb"
destUnitId = "66223c4696d5a20006ef7f67"
dataEx = dataEx()
# dataEx.mainFuncTbwes(unitsId,client,destUnitId)
dataEx.mainFuncTbwesBackFill(unitsId,client,destUnitId)
# try:
    # dataEx.getLoginToken()
# except:
    # dataEx.getLoginToken()


scheduler = BackgroundScheduler()
scheduler.add_job(func=dataEx.mainFuncTbwes,args=[unitsId,client,destUnitId], trigger="interval", seconds=60*5,max_instances=1)
scheduler.start()
client.loop_forever(retry_first_connection=True)


