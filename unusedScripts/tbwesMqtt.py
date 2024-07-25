

import sys
import paho.mqtt.client as mqtt
import traceback
import requests
import json
import time
import platform
version = platform.python_version().split(".")[0]
if version == "3":
  import app_config.app_config as cfg
elif version == "2":
  import app_config as cfg

config = cfg.getconfig()


sourceUnitsId = "62e9106d75c9b4657aebc8fb"
destUnitId = "66223c4696d5a20006ef7f67"

sourcePrefix  = ""
destPrefix = "VGA_"
def on_message(client, userdata, msg):
    return
    # client2.publish(msg.topic,msg.payload)
    topic = msg.topic.replace(sourceUnitsId,destUnitId)
    topic = topic.split("/")
    topic[2] = "VGA_" + topic[2]
    topic = "/".join(topic)
    
    body = json.loads(msg.payload)
    newTag = topic.split("/")[2]
    client2.publish(topic,msg.payload)
    print(body)
    try:
        postBody = [{"name":newTag,"datapoints":[[body[0]["t"],body[0]["v"]]],"tags": {"type":"raw"}}]
        print(postBody)
        client2.publish("kairoswriteexternal",json.dumps(postBody))
    except:
        # postBody = [{"name":newTag,"datapoints":[[body[0]["t"],body[0]["v"]]],"tags": {"type":"raw"}}]
        postBody = [{"name":newTag,"datapoints":[[body[0]["t"],body[0]["r"]]],"tags": {"type":"raw"}}]
        print(postBody)
        client2.publish("kairoswriteexternal",json.dumps(postBody))
        pass
   


def on_connect(client, userdata, flags, rc):
    client.subscribe(f"u/{sourceUnitsId}/+/r")

def on_message2(client, userdata, msg):
    # print(msg.payload)
    body = json.loads(msg.payload)
    # print(body)

def on_connect2(client, userdata, flags, rc):
    client.subscribe(f"u/{destUnitId}/+/r")



def on_log(client, userdata, obj, buff):
    print("log: " + str(buff))
    # pass

BROKER_ADDRESS = config["BROKER_ADDRESS"] #(SUBSCRIBER broker address)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log
try:
    username = config["BROKER_USERNAME"]
    password = config["BROKER_PASSWORD"]
    client.username_pw_set(username=username, password=password)
except:
    pass

client.connect(BROKER_ADDRESS, 1883, 2800)

BROKER_ADDRESS = config["BROKER_ADDRESS"] #(PUBLISHER broker address)

client2 = mqtt.Client()
client2.on_connect = on_connect2
client2.on_message = on_message2
client2.on_log = on_log
try:
    username = config["BROKER_USERNAME"]
    password = config["BROKER_PASSWORD"]
    client2.username_pw_set(username=username, password=password)
except:
    pass

client2.connect(BROKER_ADDRESS, 1883, 2800)

client2.loop_start()
client.loop_forever()

