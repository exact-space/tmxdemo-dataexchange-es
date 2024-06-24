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
for tag in tagmeta:
    df = getValuesV2([tag["dataTagId"]],st,et)
    if len(df):
        print(len(df),tag["dataTagId"])
        lst.append(tag["dataTagId"])
print(len(lst))
# for tag in tagmeta:
#     if "HRD_" in tag["dataTagId"]:
#         dataTagId = tag["dataTagId"].replace("HRD_","")
#         # print(dataTagId)
#         tag["dataTagId"] = dataTagId
#         updateTagmeta(tag,tag["id"])

