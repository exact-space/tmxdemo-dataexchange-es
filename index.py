import os

import platform
version = platform.python_version().split(".")[0]
if version == "3":
  import app_config.app_config as cfg
elif version == "2":
  import app_config as cfg
config = cfg.getconfig()


from multiprocessing import Process
import paho.mqtt.client as paho

config = cfg.getconfig()
client= paho.Client()


BROKER_ADDRESS = str(os.environ.get("BROKER_ADDRESS"))
if not BROKER_ADDRESS:
   BROKER_ADDRESS = config["BROKER_ADDRESS"]

print(BROKER_ADDRESS)
RUNTIME_ID = str(os.environ.get("OFFSET_ID"))
if not RUNTIME_ID:
   RUNTIME_ID = "1"

print(RUNTIME_ID)



SERVICE_NAME=str(os.environ.get("SERVICE_FILE_NAME"))
if SERVICE_NAME == None:
    SERVICE_NAME="ALL"
print(SERVICE_NAME)

PAGE_SIZE=str(os.environ.get("PAGE_SIZE"))
if PAGE_SIZE == None:
    PAGE_SIZE=str(1000)
print(PAGE_SIZE)

path = r"./"

tasks = ["power.py","tbwes.py","etpDemo.py","wrsDemo.py","ztpDemo.py"]
def foo(task):
    # print ('UNIT_ID='+' '+'BROKER_ADDRESS='+BROKER_ADDRESS+' python ' + path + task + ' > /tmp/log/'+'/data-calculation-b/'+task+'.log &2>1')
    #os.system('UNIT_ID='+' '+'BROKER_ADDRESS='+BROKER_ADDRESS+' python ' + path + task + ' > /tmp/log/'+'/data-calculation-b/'+task+'.log &2>1')
    # os.system('RUNTIME_ID='+RUNTIME_ID+' UNIT_ID='+' '+'BROKER_ADDRESS='+BROKER_ADDRESS+' python ' + path + task+ ' > /tmp/log/'+'/data-calculation-b/'+task+'.log &2>1')
    print('RUNTIME_ID='+RUNTIME_ID+' '+'LAS=yes PAGE_SIZE='+PAGE_SIZE+' '+'BROKER_ADDRESS='+BROKER_ADDRESS+' python ' + path + task)
    os.system('RUNTIME_ID='+RUNTIME_ID+' '+'LAS=yes PAGE_SIZE='+PAGE_SIZE+' '+'BROKER_ADDRESS='+BROKER_ADDRESS+' python ' + path + task)

for task in tasks:
    if (SERVICE_NAME=="ALL") or (SERVICE_NAME==task):
        print(task,"********")
        p = Process(target=foo, args=(task,))
        p.start()
