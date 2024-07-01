from dataExchangelmpl import config,json,requests,time,pd

unitsId = "61dd822329c9e07656414708"
def getTagmeta(unitsId):
        query = {"unitsId":unitsId}
        url = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) + '}'
        print(url)
        # response = requests.get(url,headers={"Authorization": self.token})
        response = requests.get(url)
        if(response.status_code==200):
            # print(response.status_code)
            # print("Got tagmeta successfully.....")
            tagmeta = json.loads(response.content)
            return tagmeta
            df = pd.DataFrame(tagmeta)
        else:
            print("error in fetching tagmeta")
            df = pd.DataFrame()
        return df


def updateTagmeta(postBody,id):
        query ={
            "id":id
        }
        url = config["api"]["meta"] + '/tagmeta/update?where=' + json.dumps(query)
        response = requests.post(url,json=postBody)
        tag = postBody["dataTagId"]

        if response.status_code == 200 or response.status_code == 204:
            
            print(f"{tag} Tagmeta updating successful..")

        else:
            print(f"{tag} Tagmeta updating unsuccessful..")
            print(response.status_code,response.content)

def getValuesV2(tagList,startTime, endTime):
        url = config["api"]["query"]
        metrics = []
        for tag in tagList:
            tagDict = {
                  "tags": {},
                  "name": tag,
                  "aggregators": [
                    {
                      "name": "avg",
                      "sampling": {
                        "value": "1",
                        "unit": "years"
                      },
                      "align_end_time": True
                    }
                  ]
                }
            metrics.append(tagDict)
            
        query ={
            "metrics":metrics,
            "plugins": [],
            "cache_time": 0,
            "start_absolute": int(startTime),
            "end_absolute": int(endTime)
            
        }
    #     print(json.dumps(query,indent=4))
        try:
            res=requests.post(url=url, json=query)
        except:
            time.sleep(5)
            res=requests.post(url=url, json=query)

        if res.status_code != 200:
            print(res.status_code)
            time.sleep(5)
            try:
                res=requests.post(url=url, json=query)
            except:
                time.sleep(5)
                res=requests.post(url=url, json=query)

            print(res.status_code)


        values=json.loads(res.content)
        finalDF = pd.DataFrame()
        for i in values["queries"]:
    #         print(json.dumps(i["results"][0]["name"],indent=4))
            df = pd.DataFrame(i["results"][0]["values"],columns=["time",i["results"][0]["name"]])
    #         display(df)
    #         print("-"*100)
            try:
                finalDF = pd.concat([finalDF,df.set_index("time")],axis=1)
            except Exception as e:
                print(e)
                finalDF = pd.concat([finalDF,df],axis=1)
            
        finalDF.reset_index(inplace=True)
        return finalDF

tagmeta = getTagmeta(unitsId)
lst = []
et = time.time() * 1000
st = et - 1*1000*60*60*24*365*10
tagList =  [
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
for tag in tagList:
    df = getValuesV2([tag],st,et)
    if len(df):
        print(len(df),tag)
        lst.append(tag)
print(len(lst))
# for tag in tagmeta:
#     if "HRD_" in tag["dataTagId"]:
#         dataTagId = tag["dataTagId"].replace("HRD_","")
#         # print(dataTagId)
#         tag["dataTagId"] = dataTagId
#         updateTagmeta(tag,tag["id"])

