from dataExchangelmpl import *

def postOnKairos(df):
    try:
        for dataTagId in df.columns:
            if (dataTagId != "time") and (dataTagId != "timestamp"):
                print("*"*30,str(dataTagId),"*"*30)
                postUrl = config["api"]["datapoints"]
                reqDataPoints = 5000
                print(f"len of df {len(df)}")
                for i in range(0,len(df),reqDataPoints):
                    print(i)
                    new_df =  df[["timestamp",dataTagId]][i:i+reqDataPoints]
                    new_df.dropna(inplace=True,axis=0)

                    if len(new_df) > 0:
                        postArray = new_df[["timestamp",dataTagId]].values.tolist()
                        print(f"len of post array {len(postArray)}")
                        postBody = [{
                            "name":dataTagId,
                            "datapoints":postArray,
                            "tags":{"type":"derived"}
                        }]
                        print(postUrl)
                        # print(postBody)
                        res = requests.post(postUrl,json=postBody,auth = HTTPBasicAuth("es-user", "Albuquerque#871!"))
                    
                        if res.status_code == 200 or res.status_code == 204:
                            print("posted on kairos successfully")
                        else:
                            print(res.status_code)
                            print(res.content)
    except:
        print(traceback.format_exc())


  
def deleteKairos(taglist,startTime,endTime,keywords):
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
        res = requests.post(url, json=query,auth = HTTPBasicAuth("es-user", "Albuquerque#871!"))
        
        if res.status_code == 200 or res.status_code == 204:
            print("deleting successful...")
        else:
            print("deleting unsuccessful",res.status_code,res.content)
    except:
        tr()
        
  

prefix = "VRP_"
currentTimeStamp = time.time() * 1000
st = currentTimeStamp - 1*1000*60*60*24*7
fileName1 = "Data for incidents(1).xlsx"
# fileName = "./Pepsico_ERS_Demo_Data(1).csv"

# dataEx().downloadingFileMultipleFiles([fileName])
# df = pd.read_csv(fileName)
df = pd.read_excel(fileName1)
df.drop(["description","measureType"],inplace=True,axis=1)

df = df.T

df.reset_index(inplace=True)
df.loc[0,"index"] = "Date"
lst = []
for i in list(prefix + (df.loc[0,:])):
    lst.append(i.replace(" ",""))
df.columns = lst
df = df[2:]

for i in df.index:
    df.loc[i,'timestamp'] = int(currentTimeStamp - i*1000*60)

print(df)

for i in df.columns[1:-1]:
    deleteKairos([i],st,currentTimeStamp,prefix)
    df[i] = df[i].astype(float)
    postOnKairos(df[["timestamp",i]])
