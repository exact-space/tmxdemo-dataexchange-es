from dataExchangelmpl import *

def postOnKairos(df):
    try:
        for dataTagId in df.columns:
            if (dataTagId != "time") and (dataTagId != "timestamp"):
                print("*"*30,str(dataTagId),"*"*30)
                postUrl = config["api"]["datapoints"]
                reqDataPoints = 100
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

prefix = "VRP_"
currentTimeStamp = time.time() * 1000
fileName = "./Pepsico_ERS_Demo_Data(1).csv"

# dataEx().downloadingFileMultipleFiles([fileName])
df = pd.read_csv(fileName)
df.drop(["description","measureType"],inplace=True,axis=1)

df = df.T

df.reset_index(inplace=True)
df.loc[0,"index"] = "Date"
df.columns = prefix + (df.loc[0,:])
df = df[1:]

for i in df.index:
    df.loc[i,'timestamp'] = int(currentTimeStamp - i*1000*60)


postOnKairos(df[["timestamp",prefix + "AEORP_1741"]])