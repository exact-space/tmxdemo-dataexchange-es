import pandas as pd
fileName = "./Pepsico_ERS_Demo_Data.csv"

# dataEx().downloadingFileMultipleFiles([fileName])
df = pd.read_csv(fileName,parse_dates=["Date"])

print(df)
# df.to_csv(fileName,index=False)