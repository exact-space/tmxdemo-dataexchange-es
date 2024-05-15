from dataExchangelmpl import dataEx,config,requests,json,pd

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
        
        df = pd.DataFrame(tagmeta)
    else:
        print("error in fetching tagmeta")
        df = pd.DataFrame()
    return df,tagmeta

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


d = dataEx()
unitsId = "66223c4696d5a20006ef7f67"
tag_dfDest,tagmetaDest = getTagmeta(unitsId)
tag_dfDest = tag_dfDest[tag_dfDest["measureProperty"] == "Power"]
tag_dfDest = tag_dfDest[tag_dfDest["measureType"] == "Apc"]
tag_dfDest = json.loads(tag_dfDest.to_json(orient="records"))
# unitsId = "62e9106d75c9b4657aebc8fb"
# tag_df,tagmeta = getTagmeta(unitsId)
# tagListSource = list(tag_df["dataTagId"])
for tag in tag_dfDest:
    tag["measureUnit"] = "KwHr"
    updateTagmeta(tag,tag["id"])

# for tag in tagmetaDest:
#     if "VGA_7" in tag["dataTagId"]:
#         newTag = tag["dataTagId"].replace("7f67_Tg_1","SCSSSKL_60_c8fb_1")
#         tag["dataTagId"] = newTag
#         updateTagmeta(tag,tag["id"])


# for tag in tagmetaDest:
#     if ("VGA_Applications" not in tag["dataTagId"]) and ("VGA_SCSSSKL_60_c8fb_1" not in  tag["dataTagId"]) and ("VGA_60_TPH" not in  tag["dataTagId"]) and ("state__" not in tag["dataTagId"]) and ("VGA_62e9106075c9b4657aebc8f9" not in tag["dataTagId"]):
#         # print(tag["dataTagId"])
#         url = config["api"]["meta"] + '/tagmeta/'  + tag["id"]
#         res = requests.delete(url)
#         print(res.status_code)
        