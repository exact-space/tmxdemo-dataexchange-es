import gevent.monkey
gevent.monkey.patch_all()
import warnings
import requests
from requests.exceptions import Timeout
import pandas as pd
pd.options.mode.chained_assignment = None
import json
import random
import os
import time
import datetime
from datetime import timedelta

import numpy as np
#import timeseries as ts
import platform
# version = platform.python_version().split(".")[0]
# if version == "3":
#     import app_config.app_config as cfg
# elif version == "2":
#     import app_config as cfg
# config = cfg.getconfig()
import paho.mqtt.client as paho
import grequests
import sys
import traceback
import redis
from requests.auth import HTTPBasicAuth 



# url = "https://edgelive.thermaxglobal.com/exactapi/configs"
# res = requests.get(url)
# config = json.loads(res.content)[0]

config = {"api":{}}
config["BROKER_ADDRESS"] = "edgelive.thermaxglobal.com"
config["api"]["meta"] = "https://edgelive.thermaxglobal.com/exactapi"
config["api"]["query"] = "https://edgelive.thermaxglobal.com/api/v1/datapoints/query"
config["api"]["datapoints"] = "https://edgelive.thermaxglobal.com/exactdata/api/v1/datapoints"
config["type"] = "manual"


def tr():
    print(traceback.format_exc())


redis = redis.StrictRedis(host='vml-2', db=0)

class dataEx:
    def __init__(self):
        self.url_kairos = config["api"]["query"]
        self.post_url = config["api"]["datapoints"] 
        print(self.post_url)
        self.now = int(time.time()*1000) 
    

    def lastUpdateValueRedis(self,unitId,tag):
        try:
            df = self.getLastValues([tag])
            res = redis.set(unitId+"-shadow",float(df.loc[0,"time"]))
            print("redis values:",res,redis.get(unitId+"-shadow"))
            
        except:
            tr()


    def getUnitdetails(self,unitsId):
        query = {"id":unitsId}
        urlQuery = config["api"]["meta"]+'/units/?filter={"where":' + json.dumps(query) + '}'
        response = requests.get(urlQuery)
        if(response.status_code==200):
            unitDetails = json.loads(response.content)
            for details in unitDetails:
                siteId = details["siteId"]                
                return siteId
        else:
            print("No unita....")

    def getDateFromTimeStamp(self,timestamp):
        return datetime.datetime.fromtimestamp(timestamp/1000)
        
        
    def getTagmeta(self,unitsId):
        query = {"unitsId":unitsId}
        url = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) + '}'
        print(url)
        # response = requests.get(url,headers={"Authorization": self.token})
        response = requests.get(url)
        if(response.status_code==200):
            # print(response.status_code)
            # print("Got tagmeta successfully.....")
            tagmeta = json.loads(response.content)
            
            df = pd.DataFrame(tagmeta)
        else:
            print("error in fetching tagmeta")
            df = pd.DataFrame()
        return df
        
    
    def getForms(self,unitsId):
        try:
            urlQuery = config["api"]["meta"] + "/units/" + unitsId + "/forms"
            print(urlQuery)
            response = requests.get(urlQuery)
            if(response.status_code==200):
                # print(response.status_code)
                # print("Got tagmeta successfully.....")
                tagmeta = json.loads(response.content)
                # print(tagmeta[0]["fields"])
                df = pd.DataFrame(tagmeta[0]["fields"])
            else:
                print("error in fetching tagmeta")
                df = pd.DataFrame()
            return df
        except:
            tr()
      
    
    def createBodyForForms(self,formBody):
        try:
            returnLst = []
            for i in formBody:
                if "fields" in i.keys():
                    for feild in i["fields"]:
                        body = {}
                        body["dataTagId"] = feild["dataTagId"]
                        returnLst.append(body)
            # print(returnLst)
            return returnLst
        except:
            tr()
    
    
    def getLoginToken(self):
        url = "https://pulse.thermaxglobal.com/exactapi/Users/login"
        res= requests.post(url,json={"email":"rohit.r@exactspace.co","password":"Thermax@123","ttl":0})
        res = json.loads(res.content)
        self.token =  res["id"]
        
        
    def get5MinValues(self,tagList):
        d = {
      "metrics": [
        {
          "tags": {},
          "name": ""
        }
      ],
      "plugins": [],
      "cache_time": 0,
      "start_relative": {
        "value": "300",
        "unit": "seconds"
      }
    }
        for tag in tagList:
            d['metrics'][0]['name'] = tag
        # print(d)
        # res=requests.post(url=self.url_kairos, headers={"Authorization": self.token}, json=d)
        res = requests.post(url=self.url_kairos, json=d)
        values=json.loads(res.content)
        temp=0
        for val in values['queries']:
            try:
                df1=pd.DataFrame(val['results'][0]['values'], columns=['Time', val['results'][0]['name']])
                if temp==1:
                    df=pd.merge(df,df1, on='Time', how="outer")
                else:
                    df=df1 
            except Exception as e:
                print(e)
            temp=1

        df=df.drop_duplicates(keep='first').reset_index(drop=True)
        df['Date']=pd.to_datetime(df['Time'],unit='ms')
        return df
    
   
    def getLastValues(self,taglist,end_absolute=0):
        if end_absolute !=0:
            query = {"metrics": [],"start_absolute": 1, end_absolute: end_absolute}
        else:
            query = {"metrics": [],"start_absolute":1}
        for tag in taglist:
            query["metrics"].append({"name": tag,"order":"desc","limit":1})
        try:
            res = requests.post(self.url_kairos,json=query).json()
            df = pd.DataFrame([{"time":res["queries"][0]["results"][0]["values"][0][0]}])
            for tag in res["queries"]:
                try:
                    if df.iloc[0,0] <  tag["results"][0]["values"][0][0]:
                        df.iloc[0,0] =  tag["results"][0]["values"][0][0]
                    df.loc[0,tag["results"][0]["name"]] = tag["results"][0]["values"][0][1]
                except:
                    pass
        
        except Exception as e:
            print(e)
            return pd.DataFrame()
        return df
        
        
    
    def getValuesV2(self,tagList,startTime, endTime,agg = [{"name": "avg","sampling": {"value": "1","unit": "minutes"},"align_end_time": True}]
                        ,manualTags =[],profiling=False):
        try:

            url = config["api"]["query"]
            print(url)

            metrics = []
            if agg:
                for tag in set(tagList):
                    tagDict = {
                        "tags":{},
                        "name":tag,
                        "aggregators": agg
                    }
                    metrics.append(tagDict)
            else:
                for tag in tagList:
                    tagDict = {
                        "tags":{},
                        "name":tag
                        }
                    metrics.append(tagDict)
                
            query ={
                "metrics":metrics,
                "plugins": [],
                "cache_time": 0,
                "start_absolute": int(startTime),
                "end_absolute": int(endTime)
                
            }
            print(startTime,endTime)
            # print(url)
            # print(json.dumps(query,indent=4))
            res=requests.post(url=url, json=query)
            print(res.status_code)
            values=json.loads(res.content)
            finalDF = pd.DataFrame()
            for i in values["queries"]:
                df = pd.DataFrame(i["results"][0]["values"],columns=["time",i["results"][0]["name"]])
                try:
                    # finalDF = pd.concat([finalDF,df.set_index("time")],axis=1)
                    finalDF = finalDF.join(df.set_index("time"),how="outer")

                except Exception as e:
                    print(e)
                    finalDF = pd.concat([finalDF,df],axis=1)
                
            # try:
            #     if profiling:
            #         profile = pro.ProfileReport(finalDF)
            #         profile.to_file(self.htmlFileName)
            #         self.uploadTrainingResults(self.htmlFileName)
            # except:
            #     pass

            finalDF.reset_index(inplace=True)
            # finalDF = self.fillnaV2(finalDF)
            # print(finalDF)
            # dates = pd.to_datetime(finalDF['time'],unit='ms').astype(str).tolist()

            finalDF = finalDF.loc[:,~finalDF.columns.duplicated()].reset_index(drop=True)
            return finalDF
        except Exception as e:
            print(traceback.format_exc())
            return pd.DataFrame()
        

    def dataExachangeCooling(self,taglist):
        try:
            df = self.get5MinValues(taglist)
        except:
            df = pd.DataFrame()
            
        if len(df) > 0 and df[taglist[0]].mean() >0:
            #print "HERE"
            df.dropna(inplace=True)
            df = df[df[taglist[0]]!='NaN']
            df.reset_index(drop=True,inplace=True)
            new_tag = taglist[0].replace('VDM','TTE')
            print(new_tag)
            # print(df)
            post_array = []
            for i in range(0,len(df)):
                if df.loc[i,taglist[0]] != None:
                    post = [int(df.loc[i,'Time']),float(df.loc[i,taglist[0]])]
                    post_array.append(post)
            # print(len(df),len(post_array))
            post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
            res1 = requests.post(self.post_url,json=post_body)
            print(post_body)
            print('*******************',res1.status_code,new_tag,'******************************')
            print(df)
        else:
            df_LV = self.getLastValues(taglist)
            # print(df_LV)
            if len(df_LV) > 0 and df_LV.loc[0,taglist[0]] > 0:
                # print(self.now)
                endTime = df_LV.loc[0,'time']
                startTime = endTime - 1*1000*60*20
                df = self.getValuesV2(taglist,startTime,endTime)
                df.dropna(inplace=True)
                df = df[df[taglist[0]]!='NaN']
                df.reset_index(drop=True,inplace=True)

                for i in range(0,len(df)):
                    df.loc[i,'Time'] = self.now - i * 1000*60
                    
                df['Date']=pd.to_datetime(df['Time'],unit='ms')
                new_tag = taglist[0].replace('VDM','TTE')
            
                print(new_tag)
                    # print(df)
                post_array = []
                for i in range(0,len(df)):
                    if df.loc[i,taglist[0]] != None:
                        try:
                            post = [int(df.loc[i,'Time']),float(df.loc[i,taglist[0]])]
                            post_array.append(post)
                        except:
                            post = [int(df.loc[i,'Time']),float(0)]
                            post_array.append(post)
                        
                # print(len(df),len(post_array))
                post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
                res1 = requests.post(self.post_url,json=post_body)
                print(post_body)
                print('*******************',res1.status_code,new_tag,'******************************')
                print(df)
            else:
                # print(self.now)
                endTime = 1659466200000
                startTime = 1659465000000
                df = self.getValuesV2(taglist,startTime,endTime)
                # print(df)
                df.dropna(inplace=True)
                df = df[df[taglist[0]]!='NaN']
                df.reset_index(drop=True,inplace=True)

                for i in range(0,len(df)):
                    df.loc[i,'Time'] = self.now - i * 1000*60
                    
                df['Date']=pd.to_datetime(df['Time'],unit='ms')
                new_tag = taglist[0].replace('VDM','TTE')
                
                print(new_tag)
                print(df)
                post_array = []
                for i in range(0,len(df)):
                    if df.loc[i,taglist[0]] != None:
                        post = [int(df.loc[i,'Time']),float(df.loc[i,taglist[0]])]
                        post_array.append(post)
                # print(len(df),len(post_array))
                post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
                res1 = requests.post(self.post_url,json=post_body)
                # print(post_body)
                print('*******************',res1.status_code,new_tag,'******************************')
                # print(df)
                
        
    def dataExachangeWWSWithoutCSV(self,taglist):
        try:
            df = self.get5MinValues(taglist)
        except:
            df = pd.DataFrame()
            
        if len(df) > 0 and df[taglist[0]].mean() >0:
            #print "HERE"
            df.dropna(inplace=True)
            df = df[df[taglist[0]]!='NaN']
            df.reset_index(drop=True,inplace=True)
            new_tag = taglist[0].replace('VDM','TTE')
            print(new_tag)
            # print(df)
            post_array = []
            for i in range(0,len(df)):
                if df.loc[i,taglist[0]] != None:
                    post = [int(df.loc[i,'Time']),float(df.loc[i,taglist[0]])]
                    post_array.append(post)
            # print(len(df),len(post_array))
            post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
            res1 = requests.post(self.post_url,json=post_body)
            # print(post_body)
            print('*******************',res1.status_code,new_tag,'******************************')
            print(df)
        else:
            df_LV = self.getLastValues(taglist)
            # print(df_LV)
            if len(df_LV) > 0 and df_LV.loc[0,taglist[0]] > 0:
                # print(self.now)
                endTime = df_LV.loc[0,'time']
                startTime = endTime - 1*1000*60*20
                df = self.getValuesV2(taglist,startTime,endTime)
                df.dropna(inplace=True)
                df = df[df[taglist[0]]!='NaN']
                df.reset_index(drop=True,inplace=True)

                for i in range(0,len(df)):
                    df.loc[i,'Time'] = self.now - i * 1000*60
                    
                df['Date']=pd.to_datetime(df['Time'],unit='ms')
                new_tag = taglist[0].replace('VDM','TTE')
            
                print(new_tag)
                    # print(df)
                post_array = []
                for i in range(0,len(df)):
                    if df.loc[i,taglist[0]] != None:
                        try:
                            post = [int(df.loc[i,'Time']),float(df.loc[i,taglist[0]])]
                            post_array.append(post)
                        except:
                            post = [int(df.loc[i,'Time']),float(0)]
                            post_array.append(post)
                        
                # print(len(df),len(post_array))
                post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
                res1 = requests.post(self.post_url,json=post_body)
                print(post_body)
                print('*******************',res1.status_code,new_tag,'******************************')
                print(df)
            else:
                # print(self.now)
                endTime = 1708454460000
                startTime = 1708453800000
                df = self.getValuesV2(taglist,startTime,endTime)
                # print(df)
                df.dropna(inplace=True)
                df = df[df[taglist[0]]!='NaN']
                df.reset_index(drop=True,inplace=True)

                for i in range(0,len(df)):
                    df.loc[i,'Time'] = self.now - i * 1000*60
                    
                df['Date']=pd.to_datetime(df['Time'],unit='ms')
                new_tag = taglist[0].replace('VDM','TTE')
                
                print(new_tag)
                print(df)
                post_array = []
                for i in range(0,len(df)):
                    if df.loc[i,taglist[0]] != None:
                        post = [int(df.loc[i,'Time']),float(df.loc[i,taglist[0]])]
                        post_array.append(post)
                # print(len(df),len(post_array))
                post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
                res1 = requests.post(self.post_url,json=post_body)
                # print(post_body)
                print('*******************',res1.status_code,new_tag,'******************************')
                # print(df)
    
      
    def deleteKairos(self,taglist,startTime,endTime,keywords):
        try:
            query = {}
            query["metrics"] = []
            for metric in taglist:
                if keywords in metric:
                    print(metric)
                    query["metrics"].append({"name":metric})
            
            query["start_absolute"] = startTime
            query["end_absolute"] = endTime
            print(startTime,endTime)
            # print(json.dumps(query,indent=4))
            
            url = config["api"]["datapoints"] + "/delete"
            res = requests.post(url, json=query)
            
            if res.status_code == 200 or res.status_code == 204:
                print("deleting successful...")
            else:
                print("deleting unsuccessful",res.status_code,res.content)
        except:
            tr()
            
  
                
    def backfillCooling(self,taglist,sourcePrefix,destPrefix,new_tag):
        
        print(taglist,"trying for backfill")
        et = time.time() * 1000 
        # et = 1705881600000
        st = et - 1*1000*60*60*24*7
        df = self.getValuesV2(taglist,st,et)
        

        df.dropna(inplace=True)
        df = df[df[taglist[0]]!='NaN']
        df.reset_index(drop=True,inplace=True)
        # print(df)
        
        if len(df) > 0:
            #yes, prefix at source is SIK_ , demo has YYM_
            # new_tag = taglist[0].replace(sourcePrefix,destPrefix)
            # new_tag = destPrefix +  taglist[0]
            print(new_tag)
            
            self.deleteKairos([new_tag],st,et)
            
            dataPointsReq = 5000
            for i in range(0,len(df),dataPointsReq):
                new_df =  df[["time",taglist[0]]][i:i+dataPointsReq]
                
                if len(new_df) > 0:
                    post_array = new_df[["time",taglist[0]]].values.tolist()
                    
                post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
                # print(post_body)
                res1 = requests.post(self.post_url,json=post_body)
                print('*******************',res1.status_code,new_tag,'******************************')

          
    def dataExachangeChemicals(self,taglist,validDay,currentHour,currentMinute,last5Minute,currentTimeStamp,df):
    #Get the valid Data
        print("checking for tag",taglist)
        try:
            # df = pd.read_csv(taglist[0]+".csv")
            df.dropna(axis=0,inplace=True)
            if len(df) >0:
                new_tag = taglist[0].replace("QBX1_","SMR_")
                
                df['Day'] = df['Date'].dt.day
                df['Hour'] = df['Date'].dt.hour
                df['Minute'] = df['Date'].dt.minute

                
                valid_df = df[(df["Day"] == validDay) & (df["Hour"] == currentHour)
                        & (df["Minute"] <= currentMinute) & (df["Minute"] >= last5Minute) ].copy()
               
                if len(valid_df) == 0:
                    valid_df = df[:5]
                # print(df)
                # print(valid_df)
                
                # valid_df.sort_values(by="Time",inplace=True,ascending=False)
                # valid_df = valid_df.sort_values(by="Time", ascending=False, ignore_index=True)
                valid_df.reset_index(drop = True,inplace=True)
                
                for i in valid_df.index:
                    valid_df.loc[i,'newTime'] = currentTimeStamp - i*1000


                valid_df['newDate']=pd.to_datetime(valid_df['newTime'],unit='ms')
                
                # print(valid_df)
                post_url = config["api"]["datapoints"]
                post_array = []
                for i in range(0,len(valid_df)):
                    if valid_df.loc[i,taglist[0]] != None:
                        post = [int(valid_df.loc[i,'newTime']),float(valid_df.loc[i,taglist[0]])]
                        post_array.append(post)
                 
                post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
                res1 = requests.post(post_url,json=post_body)
                # print(post_body)
                print("`"*30,str(len(post_array)),"`"*30)

                print("`"*30,str(new_tag),"`"*30)
                print("`"*30,str(res1.status_code),"`"*30)
        except Exception as e:
            print(e)
            pass
            
            
    def dataexHeating(self,miniList,startTime,endTime,noTag=False):
        exceptionsList = []
        if not noTag:
            maindf = self.getValuesV2(miniList,startTime,endTime)
            print(maindf)
        if noTag:
            et = time.time() * 1000
            st = et - 1*1000*60*20
            maindf = self.getValuesV2(miniList,st,et)
            if len(maindf) == 0:
                df_LV = self.getLastValues(miniList)
                # print(df_LV)
                if len(df_LV) > 0:
                    # print(self.now)
                    endTime = df_LV.loc[0,'time'] + 1*1000*60*5
                    startTime = endTime - 1*1000*60*20
                    maindf = self.getValuesV2(miniList,startTime,endTime)
                    if len(maindf):
                        maindf.dropna(inplace=True)
                        try:
                            maindf = maindf[maindf[miniList[0]]!='NaN']
                        except:
                            pass
                maindf.reset_index(drop=True,inplace=True)
        maindf.rename(columns={"index":"time"},inplace=True)
        print(maindf)
        for tag in miniList:
            
            try:
                try:
                    var = "time" 
                    df = maindf[["time",tag]]
                except:
                    var = "Time"
                    df = maindf[["Time",tag]]
                    
                df.dropna(how="any",inplace=True)
                # print(df)
      
                    
                new_tag = tag
                # df.sort_values(by="time",inplace=True,ascending=False)
                # df = df.sort_values(by=var, ascending=False, ignore_index=True)
                # df.reset_index(inplace=True,drop=True)
                
                # df['Date']=pd.to_datetime(df['time'],unit='ms',errors='coerce')
                if len(df) == 0 and not noTag :
                    # print("No data for ", tag,end)
                    self.noDataTags.append(tag)
                if len(df)!= 0:
                    # if (not df[tag].iloc[-1]) and (tag not in self.noDataTags):
                    #     # print("having only zeros" * 100)
                    #     self.noDataTags.append(tag)
                    #     #return
                    if (tag in exceptionsList) and (not df[tag].iloc[-1]):
                        print("in exc list")
                        df = self.getValuesV2([tag],1678645800000,1678645800000 + 1*1000*60*5)
                        #return
                    # df.sort_values(bya="time",inplace=True,ascending=False)
                    df = df.sort_values(by=var, ascending=False, ignore_index=True)
                    df.reset_index(inplace=True,drop=True)
                    for i in df.index:
                        df.at[i, 'newTime'] = self.now - i*1000*60
                    df['newDate']=pd.to_datetime(df['newTime'],unit='ms')
                        
                    post_url = config["api"]["datapoints"]
                    post_array = []
                    for i in range(0,len(df)):
                        if df.loc[i,tag] != None:
                            post = [int(df.loc[i,'newTime']),float(df.loc[i,tag])]
                            post_array.append(post)
                            
                    post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]

                    if self.unitsId:
                        topicLine = f"u/{self.unitsId}/{new_tag}/r"
                        pb = {"v":post_array[0][1],"t":post_array[0][0]}
                        self.client.publish(topicLine,json.dumps(pb))

                        
                    try:
                        res1 = requests.post(post_url,json=post_body)
                        print("`"*30,str(new_tag),"`"*30)
                        print("`"*30,str(res1.status_code),"`"*30)
                    except:
                        print(traceback.format_exc())
                    # print(post_body)
            except:
                print(traceback.format_exc())                
            
            
    def dataExachangeHeating(self,tagList,startTime,endTime,client = False,unitsId=False):

        if unitsId:
            self.client = client
            self.unitsId = unitsId

        stepSize = 20
        self.noDataTags = []
        for ss in range(0,len(tagList),stepSize):
            miniList = tagList[ss:ss+stepSize]
            self.dataexHeating(miniList,startTime,endTime)
            
        for ss in range(0,len(self.noDataTags),stepSize):
            print(miniList)
            miniList = self.noDataTags[ss:ss+stepSize]
            self.dataexHeating(miniList,startTime,endTime,True)

                
    def dataexTbwes(self,miniList,startTime,endTime,noTag=False):
        exceptionsList = []
        if not noTag:
            maindf = self.getValuesV2(miniList,startTime,endTime)
            print(maindf)

        if noTag:
            lower_bound = 1702319400000
            upper_bound = 1702405800000

            # Generate a random integer within the specified range
            # random_int = random.randint(lower_bound, upper_bound)
            et = random.randint(lower_bound, upper_bound)
            st = et - 1*1000*60*10
            maindf = self.getValuesV2(miniList,st,et)[0:6]
            if len(maindf) == 0:
                df_LV = self.getLastValues(miniList)
                # print(df_LV)
                if len(df_LV) > 0:
                    # print(self.now)
                    endTime = df_LV.loc[0,'time'] + 1*1000*60*5
                    startTime = endTime - 1*1000*60*20
                    maindf = self.getValuesV2(miniList,startTime,endTime)
                    maindf.dropna(inplace=True)
                    maindf = maindf[maindf[miniList[0]]!='NaN']
                maindf.reset_index(drop=True,inplace=True)

        maindf.rename(columns={"index":"time"},inplace=True)
        print(maindf)
        for tag in miniList:
            try:
                try:
                    var = "time" 
                    df = maindf[["time",tag]]
                except:
                    var = "Time"
                    df = maindf[["Time",tag]]
                    
                df.dropna(how="any",inplace=True)
                # print(df)
      
                if "_WTHR_TEMP_degC" in tag:
                    siteId = self.getUnitdetails(self.destUnitId)
                    new_tag = siteId  + "_WTHR_TEMP_degC"
                    print("weather tag",new_tag)
                else:
                    new_tag = "VGA_" + tag
                # df.sort_values(by="time",inplace=True,ascending=False)
                # df = df.sort_values(by=var, ascending=False, ignore_index=True)
                # df.reset_index(inplace=True,drop=True)
                
                # df['Date']=pd.to_datetime(df['time'],unit='ms',errors='coerce')
                if len(df) == 0 and not noTag :
                    # print("No data for ", tag)
                    self.noDataTags.append(tag)
                if len(df)!= 0:
                    # if (not df[tag].iloc[-1]) and (tag not in self.noDataTags):
                    #     # print("having only zeros" * 100)
                    #     self.noDataTags.append(tag)
                    #     #return
                    # if (tag in exceptionsList) and (not df[tag].iloc[-1]):
                    #     print("in exc list")
                    #     df = self.getValuesV2([tag],1678645800000,1678645800000 + 1*1000*60*5)
                        #return
                    # df.sort_values(bya="time",inplace=True,ascending=False)
                    df = df.sort_values(by=var, ascending=False, ignore_index=True)
                    df.reset_index(inplace=True,drop=True)
                    for i in df.index:
                        df.at[i, 'newTime'] = self.now - i*1000*60
                    df['newDate']=pd.to_datetime(df['newTime'],unit='ms')
                        
                    post_url = config["api"]["datapoints"]
                    post_array = []
                    for i in range(0,len(df)):
                        if df.loc[i,tag] != None:
                            post = [int(df.loc[i,'newTime']),float(df.loc[i,tag])]
                            post_array.append(post)
                            
                    post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
                    print(json.dumps(post_body))
                    
                    if self.unitsId:
                        print("publishing on mqtt")
                        topicLine = f"u/{self.destUnitId}/{new_tag}/r"
                        pb = {"v":post_array[0][1],"t":post_array[0][0]}
                        self.client.publish(topicLine,json.dumps(pb))


                    try:
                        res1 = requests.post(post_url,json=post_body)
                        print("`"*30,str(new_tag),"`"*30)
                        print("`"*30,str(res1.status_code),"`"*30)
                    except:
                        print(traceback.format_exc())
                    # print(post_body)
            except:
                print(traceback.format_exc())                
        
           
    def dataExachangeTbwes(self,unitsId,tagList,startTime,endTime,client = False):

        if unitsId:
            self.client = client
            self.unitsId = unitsId

        stepSize = 20
        self.noDataTags = []
        for ss in range(0,len(tagList),stepSize):
            miniList = tagList[ss:ss+stepSize]
            self.dataexTbwes(miniList,startTime,endTime)
            
        for ss in range(0,len(self.noDataTags),stepSize):
            print(miniList)
            miniList = self.noDataTags[ss:ss+stepSize]
            self.dataexTbwes(miniList,startTime,endTime,True)
    
    
    def getValidMonthAndYearTbwes(self,currentMonth):
        """
        data should be mapped between 1.11.2023 to 29.02.2024.
        """
        x = int(((currentMonth-0.1)//2))
        x1 = (((currentMonth + 1-0.1)//2))
        if x % 2 == 0 and x == x1:
            validMonth = 1
            validYear = 2024
        elif x % 2 == 0 and x != x1:
            validMonth = 2
            validYear = 2024
            
        elif x % 2 != 0 and x == x1:
            validMonth = 11
            validYear = 2023
            
        else:
            validMonth = 12
            validYear = 2023
        

        return validMonth,validYear


    def mainFuncTbwes(self,unitsId,client,destUnitId):
        self.destUnitId = destUnitId
        currentTimeStamp = int(time.time()*1000)


        currentTime = datetime.datetime.now()
        currentMonth = currentTime.month 
        currentQuarter = (currentMonth-1)//3 + 1
        currentDay = currentTime.day 
        currentHour = currentTime.hour
        currentMinute =  currentTime.minute
        currentSecond = currentTime.second
        last5Minute = abs(currentMinute - 5)
        validMonth,validYear = self.getValidMonthAndYearTbwes(currentMonth)
        if (validMonth == 11) and currentDay < 16:
            validMonth = 12
        startDate = "{}/{}/{} {}:{}:{}".format(validYear,validMonth,currentDay,currentHour,last5Minute,currentSecond)
        endDate = "{}/{}/{} {}:{}:{}".format(validYear,validMonth,currentDay,currentHour,currentMinute,currentSecond)
        startDate = datetime.datetime.strptime(startDate, '%Y/%m/%d %H:%M:%S')
        endDate = datetime.datetime.strptime(endDate, '%Y/%m/%d %H:%M:%S')
        print(startDate,endDate)

        startTimestamp=time.mktime(startDate.timetuple())*1000
        endTimestamp=time.mktime(endDate.timetuple())*1000
        self.now = time.time() * 1000

        tag_df = self.getTagmeta(unitsId)
        tagList = list(set(list(tag_df["dataTagId"])))
        self.dataExachangeTbwes(unitsId,tagList,startTimestamp,endTimestamp,client)

        self.lastUpdateValueRedis(self.destUnitId,tagList[0])

          
    

    def mainFuncTbwesBackFill(self,unitsId,client,destUnitId):
        print("Back filling")
        self.destUnitId = destUnitId
        currentTimeStamp = int(time.time()*1000)

        
        currentTime = datetime.datetime(2024, 5, 7,11,28,00)  - datetime.timedelta(hours = 5,minutes=30)
        currentTime = datetime.datetime(2024, 5, 8,11,50,00)  - datetime.timedelta(hours = 5,minutes=30)

        currentMonth = currentTime.month 
        currentQuarter = (currentMonth-1)//3 + 1
        currentDay = currentTime.day 
        currentHour = currentTime.hour
        currentMinute =  currentTime.minute
        currentSecond = currentTime.second
        last5Minute = abs(currentMinute - 5)
        validMonth,validYear = self.getValidMonthAndYearTbwes(currentMonth)

    
        startDate = "{}/{}/{} {}:{}:{}".format(validYear,validMonth,currentDay,currentHour,last5Minute,currentSecond)
        endDate = "{}/{}/{} {}:{}:{}".format(validYear,validMonth,currentDay,currentHour,currentMinute,currentSecond)
        startDate = datetime.datetime.strptime(startDate, '%Y/%m/%d %H:%M:%S')
        endDate = datetime.datetime.strptime(endDate, '%Y/%m/%d %H:%M:%S')
        print(startDate,endDate)

        startTimestamp=time.mktime(startDate.timetuple())*1000
        endTimestamp=time.mktime(endDate.timetuple())*1000
        self.now = time.mktime(currentTime.timetuple())*1000
        tag_df = self.getTagmeta(unitsId)
        for i in range(10):
            print("Back filling")
            tagList = list(set(list(tag_df["dataTagId"])))
            newList = ["VGA_" + x  for x in tagList]
            for i in range(0,len(newList),10):
                self.deleteKairos(newList[i:i+10],startTimestamp,endTimestamp)

            self.dataExachangeTbwes(unitsId,tagList,startTimestamp,endTimestamp,client)
            endTimestamp = startTimestamp
            startTimestamp = endTimestamp - 1*1000*60*6
            self.now = self.now - 1*1000*60*6

        # self.lastUpdateValueRedis(self.destUnitId,tagList[0])

    def mainFuncPowerBackFIll(self,unitsId,client,destUnitId,sourcePredix,destPrefix):
        print("Back filling")

        self.sourceUnitId = unitsId
        self.unitsId = unitsId
        self.client = client
        self.destUnitId = destUnitId
        self.sourcePrefix = sourcePredix
        self.destPrefix = destPrefix
        
        self.destUnitId = destUnitId
        currentTimeStamp = int(time.time()*1000)

        
        currentTime = datetime.datetime(2024, 5, 7,11,28,00)  - datetime.timedelta(hours = 5,minutes=30)
        currentTime = datetime.datetime(2024, 7, 6,16,00,00)  - datetime.timedelta(hours = 5,minutes=30)

        currentMonth = currentTime.month 
        currentQuarter = (currentMonth-1)//3 + 1
        currentDay = currentTime.day 

        if currentDay > 28 or currentDay in [7,8,9]:
                currentDay = currentDay - 4
        # currentDay = 25
        currentHour = currentTime.hour
        currentMinute =  currentTime.minute
        currentSecond = currentTime.second
        if currentMinute > 5:
            last5Minute = abs(currentMinute - 5)
        else:
            last5Minute = abs(55)
            currentHour = currentHour -1 
        # validMonth,validYear = self.getValidMonthAndYearTbwes(currentMonth)
        validMonth = 3
        validYear = 2024


    
        startDate = "{}/{}/{} {}:{}:{}".format(validYear,validMonth,currentDay - 1 ,currentHour,currentMinute,currentSecond)
        endDate = "{}/{}/{} {}:{}:{}".format(validYear,validMonth,currentDay,currentHour,last5Minute,currentSecond)
        startDate = datetime.datetime.strptime(startDate, '%Y/%m/%d %H:%M:%S')
        endDate = datetime.datetime.strptime(endDate, '%Y/%m/%d %H:%M:%S')
        print(startDate,endDate)

        startTimestamp=time.mktime(startDate.timetuple())*1000
        endTimestamp=time.mktime(endDate.timetuple())*1000
        self.now = time.mktime(currentTime.timetuple())*1000
        tag_df = self.getTagmeta(unitsId)
        print(startTimestamp,endTimestamp)

        for i in range(1):
            print("Back filling")
            tagList = list(set(list(tag_df["dataTagId"]))) 
            
            newList = [ x.replace(sourcePredix,destPrefix)  for x in tagList if sourcePredix in x]
            # for i in range(0,len(newList),25):
            #     et = int(time.time()*1000) 
            #     st = et - 1*1000*60*60*24
            #     self.deleteKairos(newList[i:i+25],st,et,self.destPrefix)
            
            # self.dataExachangePower(tagList,startTimestamp,endTimestamp,client)
            if startTimestamp > endTimestamp:
                self.dataExachangePower(tagList,endTimestamp,startTimestamp,client)
            else:
                self.dataExachangePower(tagList,startTimestamp,endTimestamp,client)

            
            endTimestamp = startTimestamp
            startTimestamp = endTimestamp - 1*1000*60*60*24
            self.now = self.now -  1*1000*60*60*24


    def downloadingFileMultipleFiles(self, fileNames):
        urls = []
        for file in fileNames:
            url = url = config['api']['meta']+"/attachments/reports/download/" +  file
            urls.append(url)
            # print(url)
            
        rs = (grequests.get(u) for u in urls)
        requests = grequests.map(rs)
        
        for i in range(len(requests)):
            if(requests[i].status_code==200):
                open(""+fileNames[i], "wb").write(requests[i].content)
                print("Downloading completed for file " + str(fileNames[i]))
            else:
                print("Downloading not completed for file " + str(fileNames[i]) , requests[i].status_code , )
                # print()
                # print(requests[i].content)
            
    def removeFiles(self,fileNames):
        for file in fileNames:
            try:
                os.remove(file)
            except:
                pass
    

             
    def dataexPower(self,miniList,startTime,endTime,noTag=False,dataEntry=False):
        exceptionsList = []
        print(miniList)
        if not noTag:
            maindf = self.getValuesV2(miniList,startTime,endTime)
            print(maindf)

        if noTag:
            
            lower_bound = 1710095400000
            upper_bound = 1711305000000

            et = random.randint(lower_bound, upper_bound)
            st = et - 1*1000*60*10
            maindf = self.getValuesV2(miniList,st,et)
            if len(maindf) == 0:
                maindf = self.getLastValues(miniList)
                # print(df_LV)
                # if len(df_LV) > 0:
                #     # print(self.now)
                #     endTime = df_LV.loc[0,'time'] + 1*1000*60*5
                #     startTime = endTime - 1*1000*60*20
                #     maindf = self.getValuesV2(miniList,startTime,endTime)
                #     if len(maindf):
                #         maindf.dropna(inplace=True)
                #         try:
                #             maindf = maindf[maindf[miniList[0]]!='NaN']
                #         except:
                #             pass
            maindf.reset_index(drop=True,inplace=True)
        maindf.rename(columns={"index":"time"},inplace=True)
        print("noTag:",noTag)
        print("maindf \n",maindf)
        for tag in miniList:
            if self.sourcePrefix not in tag:
                # print("sourceprefix not in tag")
                pass
            else:
                try:
                    try:
                        var = "time" 
                        df = maindf[["time",tag]]
                    except:
                        var = "Time"
                        df = maindf[["Time",tag]]
                        
                    df.dropna(how="any",inplace=True)
                    # print(df)
        
                    if dataEntry:
                        new_tag = tag.replace(self.sourcePrefix,self.destPrefix + "_" + self.destUnitId[-4:])
                    else:
                        new_tag = tag.replace(self.sourcePrefix,self.destPrefix).replace(self.destUnitId[-4:] + "_","")
                    # df.sort_values(by="time",inplace=True,ascending=False)
                    # df = df.sort_values(by=var, ascending=False, ignore_index=True)
                    # df.reset_index(inplace=True,drop=True)
                    
                    # df['Date']=pd.to_datetime(df['time'],unit='ms',errors='coerce')
                    if len(df) == 0 and not noTag :
                        # print("No data for ", tag)
                        self.noDataTags.append(tag)
                    if len(df)!= 0:
                        # if (not df[tag].iloc[-1]) and (tag not in self.noDataTags):
                        #     # print("having only zeros" * 100)
                        #     self.noDataTags.append(tag)
                        #     #return
                        if (tag in exceptionsList) and (not df[tag].iloc[-1]):
                            print("in exc list")
                            df = self.getValuesV2([tag],1678645800000,1678645800000 + 1*1000*60*5)
                            #return
                        # df.sort_values(bya="time",inplace=True,ascending=False)
                        df = df.sort_values(by=var, ascending=False, ignore_index=True)
                        df.reset_index(inplace=True,drop=True)
                        for i in df.index:
                            df.at[i, 'newTime'] = self.now - i*1000*60
                        df['newDate']=pd.to_datetime(df['newTime'],unit='ms')   
                            
                        post_url = config["api"]["datapoints"]
                        post_array = []
                        for i in range(0,len(df)):
                            if not pd.isnull(df.loc[i,tag]):
                                post = [int(df.loc[i,'newTime']),float(df.loc[i,tag])]
                                post_array.append(post)
                                
                        post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
                        print(post_body)
                        
                        if self.unitsId:
                            topicLine = f"u/{self.destUnitId}/{new_tag}/r"
                            pb = {"v":post_array[0][1],"t":post_array[0][0]}
                            self.client.publish(topicLine,json.dumps(pb))

                            
                        try:
                            res1 = requests.post(post_url,json=post_body)
                            # res1 = requests.post(post_url,json=post_body,auth = HTTPBasicAuth("es-user", "Albuquerque#871!"))
                            print("posting on",post_url)
                            print("`"*30,str(new_tag),"`"*30)
                            print("`"*30,str(res1.status_code),"`"*30)
                        except:
                            print(traceback.format_exc())
                        # print(post_body)
                except:
                    print(traceback.format_exc())                
    

            
    def dataExachangePower(self,tagList,startTime,endTime,client = False,unitsId=False,dataEntry = False):

        if unitsId:
            self.client = client
            self.unitsId = unitsId

        stepSize = 20
        self.noDataTags = []
        for ss in range(0,len(tagList),stepSize):
            miniList = tagList[ss:ss+stepSize]
            print(startTime,endTime)
            self.dataexPower(miniList,startTime,endTime,dataEntry=dataEntry)
        
        # count = 
        for ss in range(0,len(self.noDataTags),stepSize):
            miniList = self.noDataTags[ss:ss+stepSize]
            print("miniList",miniList)

            self.dataexPower(miniList,startTime,endTime,True,dataEntry=dataEntry)

    


    def mainFuncPower(self,sourceUnitId,destUnitId,client,sourcePrefix,destPrefix):
        try:
            self.sourceUnitId = sourceUnitId
            self.destUnitId = destUnitId
            self.sourcePrefix = sourcePrefix
            self.destPrefix = destPrefix

            currentTimeStamp = self.now = int(time.time()*1000)
            

            currentTime = datetime.datetime.now()
            # currentMonth = currentTime.month 
            # currentQuarter = (currentMonth-1)//3 + 1
            currentDay = currentTime.day 
            if currentDay > 28 or currentDay in [7,8,9]:
                currentDay = currentDay - 4
            currentHour = currentTime.hour
            currentMinute =  currentTime.minute
            currentSecond = currentTime.second
            if currentMinute > 5:
                last5Minute = abs(currentMinute - 5)
            else:
                last5Minute = abs(60 - currentMinute)
                currentHour = currentHour -1 
            # validMonth = (currentMonth - (currentQuarter-1)*3)
            validMonth = 3

            startDate = "2024/{}/{} {}:{}:{}".format(validMonth,currentDay,currentHour,last5Minute,currentSecond)
            endDate = "2024/{}/{} {}:{}:{}".format(validMonth,currentDay,currentHour,currentMinute,currentSecond)

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

            tag_df = self.getTagmeta(sourceUnitId)

            tagList = list(set(list(tag_df["dataTagId"])))
            
            print("time frame",startDate,endDate)
            print("time frame",startTimestamp,endTimestamp)



            if startTimestamp > endTimestamp:
                self.dataExachangePower(tagList,endTimestamp,startTimestamp,client,sourceUnitId)
            else:
                self.dataExachangePower(tagList,startTimestamp,endTimestamp,client,sourceUnitId)

            self.lastUpdateValueRedis(self.destUnitId,"YYM_21_MW_001")

        except:
            tr()

    def mainFuncETP(self,sourceUnitId,destUnitId,client,sourcePrefix,destPrefix):
        try:
            self.sourceUnitId = sourceUnitId
            self.destUnitId = destUnitId
            self.sourcePrefix = sourcePrefix
            self.destPrefix = destPrefix

            currentTimeStamp = self.now = int(time.time()*1000)
            
            currentTime = datetime.datetime.now()
            # currentMonth = currentTime.month 
            # currentQuarter = (currentMonth-1)//3 + 1
            currentDay = 22
            currentHour = currentTime.hour
            currentMinute =  currentTime.minute
            currentSecond = currentTime.second
            if currentMinute > 5:
                last5Minute = abs(currentMinute - 5)
            else:
                last5Minute = abs(60 - currentMinute)
                currentHour = currentHour -1 
            # validMonth = (currentMonth - (currentQuarter-1)*3)
            validMonth = 7

            startDate = "2024/{}/{} {}:{}:{}".format(validMonth,currentDay,currentHour,last5Minute,currentSecond)
            endDate = "2024/{}/{} {}:{}:{}".format(validMonth,currentDay,currentHour,currentMinute,currentSecond)

            print(startDate,endDate)
            startDate = datetime.datetime.strptime(startDate, '%Y/%m/%d %H:%M:%S')
            endDate = datetime.datetime.strptime(endDate, '%Y/%m/%d %H:%M:%S')
            # except ValueError:
            #     startDate = "2023/{}/{} {}:{}:{}".format(6,28,currentHour,last5Minute,currentSecond)
            #     endDate = "2023/{}/{} {}:{}:{}".format(6,28,currentHour,currentMinute,currentSecond)
                
            #     startDate = datetime.datetime.strptime(startDate, '%Y/%m/%d %H:%M:%S')
            #     endDate = datetime.datetime.strptime(endDate, '%Y/%m/%d %H:%M:%S')


            print(startDate,endDate)
            startTimestamp=time.mktime(startDate.timetuple())*1000
            endTimestamp=time.mktime(endDate.timetuple())*1000

            tag_df = self.getTagmeta(sourceUnitId)

            tagList = list(set(list(tag_df["dataTagId"])))

            print("time frame",startDate,endDate)
            print("time frame",startTimestamp,endTimestamp)

            if startTimestamp > endTimestamp:
                self.dataExachangePower(tagList,endTimestamp,startTimestamp,client,sourceUnitId)
            else:
                self.dataExachangePower(tagList,startTimestamp,endTimestamp,client,sourceUnitId)
            # self.lastUpdateValueRedis(self.destUnitId,"YYM_21_MW_001")

        except:
            tr()

          
    def dataExachangeWrs(self,taglist,currentHour,currentMinute,last5Minute,currentTimeStamp,df):
    #Get the valid Data
        print("checking for tag",taglist)
        try:
            # df = pd.read_csv(taglist[0]+".csv")
            df.dropna(axis=0,inplace=True)
            if len(df) >0:
                if self.sourcePrefix in taglist[0]:
                    new_tag = taglist[0].replace(self.sourcePrefix,self.destPrefix)
                else:
                    new_tag = self.destPrefix + "_" +taglist[0]
                    
            
                
                df['Day'] = df['Date'].dt.day
                df['Hour'] = df['Date'].dt.hour
                df['Minute'] = df['Date'].dt.minute

                
                valid_df = df[(df["Hour"] == currentHour)
                        & (df["Minute"] <= currentMinute) & (df["Minute"] >= last5Minute) ].copy()
               
                if len(valid_df) == 0:
                    valid_df = df[:5]
                
                valid_df.reset_index(drop = True,inplace=True)
                
                for i in valid_df.index:
                    valid_df.loc[i,'newTime'] = currentTimeStamp - i*1000


                valid_df['newDate']=pd.to_datetime(valid_df['newTime'],unit='ms')
                
                # print(valid_df)
                post_url = config["api"]["datapoints"]
                post_array = []
                for i in range(0,len(valid_df)):
                    if valid_df.loc[i,taglist[0]] != None:
                        post = [int(valid_df.loc[i,'newTime']),float(valid_df.loc[i,taglist[0]])]
                        post_array.append(post)
                 
                post_body = [{"name":new_tag,"datapoints":post_array,"tags": {"type":"derived"}}]
                # res1 = requests.post(post_url,json=post_body,auth = HTTPBasicAuth("es-user", "Albuquerque#871!"))
                res1 = requests.post(post_url,json=post_body)
                # print(post_body)
                print("`"*30,str(len(post_array)),"`"*30)

                print("`"*30,str(new_tag),"`"*30)
                print("`"*30,str(res1.status_code),"`"*30)
        except Exception as e:
            print(e)
            pass
       

    def mainFuncWRS(self,sourceUnitId,destUnitId,client,sourcePrefix,destPrefix):
        try:
            self.sourceUnitId = sourceUnitId
            self.destUnitId = destUnitId
            self.sourcePrefix = sourcePrefix
            self.destPrefix = destPrefix

            currentTimeStamp = self.now = int(time.time()*1000)
            

            currentTime = datetime.datetime.now()
            # currentMonth = currentTime.month 
            # currentQuarter = (currentMonth-1)//3 + 1
            currentDay = currentTime.day 
            currentHour = currentTime.hour
            currentMinute =  currentTime.minute
            currentSecond = currentTime.second
            if currentMinute > 5:
                last5Minute = abs(currentMinute - 5)
            else:
                last5Minute = abs(60 - currentMinute)
                currentHour = currentHour -1 
            # validMonth = (currentMonth - (currentQuarter-1)*3)

            fileName = "./Pepsico_ERS_Demo_Data.csv"

            # dataEx().downloadingFileMultipleFiles([fileName])
            maindf = pd.read_csv(fileName,parse_dates=["Date"])

            for tag in (list(maindf.columns[1:])):
                self.dataExachangeWrs([tag],currentHour,currentMinute,last5Minute,currentTimeStamp,maindf)
            
        except:
            tr()

