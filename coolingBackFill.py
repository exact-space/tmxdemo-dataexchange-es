from dataExchangelmpl import dataEx,config

unitsId = "62e9106d75c9b4657aebc8fb"
sourcePrefix = ""
destPrefix = "VGA_"
dataEx = dataEx()
# try:
    # dataEx.getLoginToken()
# except:
    # dataEx.getLoginToken()

tag_df = dataEx.getTagmeta(unitsId)
tag_df["newTag"] = "VGA_" + tag_df["dataTagId"]
print(tag_df)

# for tag in range(0,len(tag_df)): 
#     taglist = [tag_df.loc[tag,'dataTagId']]
#     # taglist= ['VDM_CHW_OUT_TEMP']
#     # print(taglist)
#     try:
#         df = dataEx.getLastValues(taglist)
#         if not len(df):
#             print(taglist)
#         # print(df)
#         # break
#     except Exception as e:
#         print(e)       
        

# taglist= ['CEN1_M24_R']
# dataEx.dataExachangeCooling(taglist)
# print(len(tag_df))


for tag in range(0,len(tag_df)): 
    if sourcePrefix in tag_df.loc[tag,'dataTagId']:
        tag1 = tag_df.loc[tag,'dataTagId']
        # taglist= ['VDM_CHW_OUT_TEMP']
        # print(taglist)
        lst = ["validload__","flagLOAD__"] + ["loadLw_","loadUp_","pred_"] + ["predUp_","predLw_","flagModel__"]
        for i in lst:
            taglist = [i + tag1]
            new_tag = i + tag_df.loc[tag,'newTag']
            print(taglist,new_tag)
            try:
                dataEx.backfillCooling(taglist,sourcePrefix,destPrefix,new_tag)
                # break
            except Exception as e:
                print(e)       
            

tag_df = dataEx.getForms(unitsId)
# print(tag_df)

# taglist= ['CEN1_M24_R']
# dataEx.dataExachangeCooling(taglist)
# print(len(tag_df))


for tag in range(0,len(tag_df)): 
    if sourcePrefix in tag_df.loc[tag,'dataTagId']:
        tag1 = tag_df.loc[tag,'dataTagId']
        # taglist= ['VDM_CHW_OUT_TEMP']
        # print(taglist)
        lst = ["validload__","flagLOAD__"] + ["loadLw_","loadUp_","pred_"] + ["predUp_","predLw_","flagModel__"]
        for i in lst:
            taglist = [i + tag1]
            new_tag = i + tag_df.loc[tag,'newTag']
            print(taglist,new_tag)
            try:
                dataEx.backfillCooling(taglist,sourcePrefix,destPrefix,new_tag)
                # break
            except Exception as e:
                print(e)       
            