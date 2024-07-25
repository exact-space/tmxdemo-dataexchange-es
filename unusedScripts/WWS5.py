from dataExchangelmpl import dataEx,config

unitsId = "63288a244512494172eb0cde"
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
    if 'WWS' in tag_df.loc[tag,'dataTagId']:
        taglist = [tag_df.loc[tag,'dataTagId']]
        # taglist= ['CEN1_M24_R']
        print(taglist)
        try:
            dataEx.dataExachangeWWSWithoutCSV(taglist)
        except:
            pass