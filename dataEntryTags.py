from dataExchangelmpl import *


class dataEx2(dataEx):

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
            finalDF.fillna(method = "bfill",inplace=True)
            finalDF.fillna(method = "ffill",inplace=True)
            # finalDF.dropna(inplace=True)
            # finalDF = self.fillnaV2(finalDF)
            # print(finalDF)
            finalDF["dates"] = pd.to_datetime(finalDF['time'],unit='ms').astype(str).tolist()

            finalDF = finalDF.loc[:,~finalDF.columns.duplicated()].reset_index(drop=True)
            return finalDF
        except Exception as e:
            print(traceback.format_exc())
            return pd.DataFrame()
        

    
    def postOnKairos(self,df):
        try:
            for dataTagId in df.columns:
                if (dataTagId != "time") and (dataTagId != "dates"):
                    print("*"*30,str(dataTagId),"*"*30)
                    postUrl = config["api"]["datapoints"]
                    reqDataPoints = 100
                    print(f"len of df {len(df)}")
                    for i in range(0,len(df),reqDataPoints):
                        print(i)
                        new_df =  df[["time",dataTagId]][i:i+reqDataPoints]
                        new_df.dropna(inplace=True,axis=0)

                        if len(new_df) > 0:
                            postArray = new_df[["time",dataTagId]].values.tolist()
                            print(f"len of post array {len(postArray)}")
                            postBody = [{
                                "name":dataTagId,
                                "datapoints":postArray,
                                "tags":{"type":"derived"}
                            }]
                            print(postUrl)
                            print(postBody)
                            res = requests.post(postUrl,json=postBody)
                        
                            if res.status_code == 200 or res.status_code == 204:
                                print("posted on kairos successfully")
                            else:
                                print(res.status_code)
                                print(res.content)
        except:
            print(traceback.format_exc())
    
      
data = dataEx2()


sourceUnitsId = "62ff525f0053c325ccf27a1d"
destUnitId = "65cdb12fd958e80007254cf3"
sourcePredix = "SIK"
destPrefix = "YYM"

sourceTags  =  [

                    "SIK_Bataan_BLR_1_FUEL_1_COAL_FLOW",
                    "SIK_Bataan_BLR_1_FUEL_1_FC",
                    "SIK_Bataan_BLR_1_FUEL_1_VM",
                    "SIK_Bataan_BLR_1_FUEL_1_TM",
                    "SIK_Bataan_BLR_1_FUEL_1_SM",
                    "SIK_Bataan_BLR_1_FUEL_1_IM",
                    "SIK_Bataan_BLR_1_FUEL_1_ASH",
                    "SIK_Bataan_BLR_1_FUEL_1_GCV",
                    "SIK_Bataan_BLR_1_FUEL_1_SULPHUR",
                    "SIK_Bataan_BLR_1_FUEL_1_COST",
                    "SIK_Bataan_BLR_1_FUEL_2_COAL_FLOW",
                    "SIK_Bataan_BLR_1_FUEL_2_FC",
                    "SIK_Bataan_BLR_1_FUEL_2_VM",
                    "SIK_Bataan_BLR_1_FUEL_2_TM",
                    "SIK_Bataan_BLR_1_FUEL_2_SM",
                    "SIK_Bataan_BLR_1_FUEL_2_IM",
                    "SIK_Bataan_BLR_1_FUEL_2_ASH",
                    "SIK_Bataan_BLR_1_FUEL_2_GCV",
                    "SIK_Bataan_BLR_1_FUEL_2_SULPHUR",
                    "SIK_Bataan_BLR_1_FUEL_2_COST",
                    "SIK_Bataan_BLR_1_FUEL_3_COAL_FLOW",
                    "SIK_Bataan_BLR_1_FUEL_3_FC",
                    "SIK_Bataan_BLR_1_FUEL_3_VM",
                    "SIK_Bataan_BLR_1_FUEL_3_TM",
                    "SIK_Bataan_BLR_1_FUEL_3_SM",
                    "SIK_Bataan_BLR_1_FUEL_3_IM",
                    "SIK_Bataan_BLR_1_FUEL_3_ASH",
                    "SIK_Bataan_BLR_1_FUEL_3_GCV",
                    "SIK_Bataan_BLR_1_FUEL_3_SULPHUR",
                    "SIK_Bataan_BLR_1_FUEL_3_COST",
                    "SIK_Bataan_BLR_1_COInFlueGasPPM",
                    "SIK_Bataan_BLR_1_CO2",
                    "SIK_Bataan_BLR_1_COAL_FLOW",
                    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_FC",
                    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_VM",
                    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_TM",
                    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_SM",
                    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_IM",
                    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_ASH",
                    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_GCV",
                    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_SULPHUR",
                    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_COST",
                    "SIK_Bataan_BLR_1_FLY_ASH",
                    "SIK_Bataan_BLR_1_BED_ASH",
                    "SIK_Bataan_BLR_1_FLY_ASH_QUANT",
                    "SIK_Bataan_BLR_1_BED_ASH_QUANT",
                    "SIK_Bataan_BLR_1_AIR_HUMIDITY_FACTOR",
                    "SIK_Bataan_BLR_1_RADIATION",
                    "SIK_Bataan_BLR_1_LOSS_UNACCOUNTED",
                    "SIK_Bataan_CHIMNEY_CO",
                    "SIK_RAW_WATER_TOTALIZER",
                    "SIK_Bataan_DM_WATER",
                    "SIK_Bataan_AUC_POW_CONS_PRCNT",
                    "SIK_Bataan_AUX_POW_CONS_KW",
                    "SIK_Bataan_1_FORCED_OUTAGE",
                    "SIK_Bataan_1_TG_STARTUP",
                    "SIK_Generator System_1_Bataan_1_BLR_STARTUP",
                    "SIK_Generator System_1_Bataan_1_COAL_CONSUMPTION_STARTUP",
                    "SIK_Generator System_1_Bataan_1_TOT_BLOW_DOWN",
                    "SIK_Generator System_1_Bataan_1_FEED_WATER_TDS",
                    "SIK_Generator System_1_Bataan_1_MAX_WATER_TDS",
                    "SIK_Generator System_1_Bataan_1_LIFT_PRESSURE_SAFETY_VALVE",
                    "SIK_Generator System_1_Bataan_1_RESET_PRESSURE_SAFETY_VALVE",
                    "SIK_Generator System_1_Bataan_1_OPENING_SAFETY_VALVE"
            ]

sourceTags = [
    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_FC",
    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_VM",
    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_TM",
    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_SM",
    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_IM",
    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_ASH",
    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_GCV",
    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_SULPHUR",
    "SIK_Bataan_BLR_1_FUEL_WGT_AVG_COST"
]

sourceTags = ["SIK_Bataan_BLR_1_COAL_FLOW"]

destTagsDict = {i:i.replace(sourcePredix,destPrefix + "_" + destUnitId[-4:]) for i in sourceTags}
destTagsList = list(destTagsDict.values())
st = 1709231400000
et = 1711909799999

# data.deleteKairos(destTagsList,int(time.time() * 1000 - 1*1000*60*60*24*365),int(time.time() * 1000 + 1*1000*60*60*24*365*100),destPrefix)
agg = [{"name": "first","sampling": {"value": "1","unit": "days"},"align_start_time": True}]
df = data.getValuesV2(sourceTags,st,et,agg)
df.rename(columns=destTagsDict,inplace=True)
# data.postOnKairos(df)
for i in range(0,80):
    df["time"] = df["time"] + 1*1000*60*60*24*len(df)
    data.postOnKairos(df)
