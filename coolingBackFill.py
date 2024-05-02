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
        taglist = [tag_df.loc[tag,'dataTagId']]
        # taglist= ['VDM_CHW_OUT_TEMP']
        # print(taglist)
        try:
            dataEx.backfillCooling(taglist,sourcePrefix,destPrefix)
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
        taglist = [tag_df.loc[tag,'dataTagId']]
        # taglist= ['VDM_CHW_OUT_TEMP']
        # print(taglist)
        try:
            dataEx.backfillCooling(taglist,sourcePrefix,destPrefix)
            # break
        except Exception as e:
            print(e)  
