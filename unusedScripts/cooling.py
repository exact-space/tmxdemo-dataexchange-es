from dataExchangelmpl import dataEx,config

unitsId = "61c4b51b515e2f6d59c00173"
dataEx = dataEx()
# try:
    # dataEx.getLoginToken()
# except:
    # dataEx.getLoginToken()

tag_df = dataEx.getTagmeta(unitsId)
print(tag_df)
# taglist= ['CEN1_M24_R']
# dataEx.dataExachangeCooling(taglist)
# print(len(tag_df))
for tag in range(0,len(tag_df)): 
    if 'VDM' in tag_df.loc[tag,'dataTagId']:
        taglist = [tag_df.loc[tag,'dataTagId']]
        # taglist= ['CEN1_M24_R']
        print(taglist)
        try:
            dataEx.dataExachangeCooling(taglist)
        except:
            pass