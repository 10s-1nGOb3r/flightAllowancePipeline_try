import pandas as pd
import numpy as np
import os
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","dfsForWashUp.csv")
file_path2 = os.path.join(script_dir,"input","stationDb.csv")
save_at = os.path.join(script_dir,"output","detailNewAlgoDfsForWashUp.csv")
save_at2 = os.path.join(script_dir,"output","newAlgoCrewRouteValidation.csv")

df= pd.read_csv(file_path,sep=";")

df["DATE"] = pd.to_datetime(df["DATE"], format="%d/%m/%Y", errors="coerce")
df["DATE"] = df["DATE"].ffill()

collection = ["FLT","TYPE","REG","AC","DEP","ARR","STD","STA","Crew #","Crew"]
for field in collection:
    df[field] = df[field].ffill()
    df[field] = df[field].astype(str)

collection2 = ["STD","ATD","ATA"]
for field2 in collection2:
    df[field2] = pd.to_timedelta(df[field2] + ":00")
    df[field2] = df[field2].ffill()

collection11 = ["ActBlockOffDate","ActBlockOnDate"]
for field11 in collection11:
    df[field11] = pd.to_datetime(df[field11], format="%d/%m/%Y", errors="coerce")
    df[field11] = df[field11].ffill()

df["dateTimeAtd"] = df["ActBlockOffDate"] + df["ATD"]
df["dateTimeAta"] = df["ActBlockOnDate"] + df["ATA"]

df2 = pd.read_csv(file_path2,sep=";")

df = pd.merge(df,df2[['activityBase', 'TRANSITION HOUR']],left_on='DEP',right_on='activityBase',how='left')

df = df.rename(columns={'TRANSITION HOUR': 'transitionDep'}).drop(columns=['activityBase'])

df = pd.merge(df,df2[['activityBase', 'TRANSITION HOUR']],left_on='ARR',right_on='activityBase',how='left')

df = df.rename(columns={'TRANSITION HOUR': 'transitionArr'}).drop(columns=['activityBase'])

collection3 = ["transitionDep","transitionArr"]
for field3 in collection3:
    df[field3] = df[field3].fillna("0")
    df[field3] = df[field3].astype(int)

transitionDeltaDep = pd.to_timedelta(df["transitionDep"], unit="h")
df["dateTimeAtdLt"] = df["dateTimeAtd"] + transitionDeltaDep
transitionDeltaArr = pd.to_timedelta(df["transitionArr"], unit="h")
df["dateTimeAtaLt"] = df["dateTimeAta"] + transitionDeltaArr

df["monthCalculation"] = df["dateTimeAtdLt"].dt.month
df["yearCalculation"] = df["dateTimeAtdLt"].dt.year

conditions = [
            df["Crew"].str.contains("CPT", na=False),
            df["Crew"].str.contains("FO", na=False),
            df["Crew"].str.contains("FA1", na=False),
            df["Crew"].str.contains("FA", na=False)
]
choices = ["CPT","FO","FA1","FA"]
df["RANK"] = np.select(conditions,choices,default="0")

conditions2 = [
        (df["Crew"].str.contains("CPT", na=False)) & (df["Crew"].str.contains("DHC", na=False)),
        (df["Crew"].str.contains("FO", na=False)) & (df["Crew"].str.contains("DHC", na=False)),
        (df["Crew"].str.contains("FA1", na=False)) & (df["Crew"].str.contains("DHC", na=False)),
        (df["Crew"].str.contains("FA", na=False)) & (df["Crew"].str.contains("DHC", na=False)),
        df["Crew"].str.contains("CPT", na=False),
        df["Crew"].str.contains("FA1", na=False),
        df["Crew"].str.contains("FO", na=False),
        df["Crew"].str.contains("FA", na=False)        
]

choices2 = [
        df["Crew"].str.replace(r'[^0-9]', '', regex=True).str[-6:],
        df["Crew"].str.replace(r'[^0-9]', '', regex=True).str[-6:],
        df["Crew"].str.replace(r'[^0-9]', '', regex=True).str[-6:],
        df["Crew"].str.replace(r'[^0-9]', '', regex=True).str[-6:],
        df["Crew"].str[7:14],
        df["Crew"].str[7:14],
        df["Crew"].str[6:13],
        df["Crew"].str[6:13]
]
    
df["Crew"] = np.select(conditions2,choices2,default="0")
df["Crew"] = df["Crew"].astype(str).str.strip()

df = df.sort_values(by=["Crew", "dateTimeAtdLt"])

timeNow = datetime.now()
#If you need a certain time filtering, please activate 
#the script below, 
#deactivate scipt "timeNow = datetime.now()" above 
#and edit timeNow on the first line
#timeNow = "01/02/2026"
#timeNow = datetime.strptime(timeNow, "%d/%m/%Y")
#timeNow = timeNow.date()
currentMonth = timeNow.month
if currentMonth == 1:
    currentMonth = 12
else:
    currentMonth = currentMonth - 1

currentYear = timeNow.year
if currentMonth == 12:
    currentYear = currentYear - 1

df["monthValidation"] = np.where((df["dateTimeAtdLt"].dt.month == currentMonth) & (df["dateTimeAtdLt"].dt.year == currentYear),1,0)

df["groundTimeSeparation"] = np.where(df["Crew"] == df["Crew"].shift(1),df["dateTimeAtd"] - df["dateTimeAta"].shift(1),0)

df["groundTimeSeparationDec"] = df["groundTimeSeparation"] / pd.Timedelta(hours=1)
df["groundTimeSeparationDec"] = df["groundTimeSeparationDec"].round(2)

conditions3 = [df["groundTimeSeparationDec"] >= 13.5,
               df["groundTimeSeparationDec"] == 0.0]

choices3 = [1,1]

df["headJourneySeparator"] = np.select(conditions3,choices3,default=0)

conditions3 = [(df["headJourneySeparator"].shift(1) == 0) & (df["headJourneySeparator"] == 0) & (df["headJourneySeparator"].shift(-1) == 0),
               (df["headJourneySeparator"].shift(1) == 1) & (df["headJourneySeparator"] == 0) & (df["headJourneySeparator"].shift(-1) == 0),
               (df["headJourneySeparator"].shift(1) == 0) & (df["headJourneySeparator"] == 1) & (df["headJourneySeparator"].shift(-1) == 0),
               (df["headJourneySeparator"].shift(1) == 0) & (df["headJourneySeparator"] == 0) & (df["headJourneySeparator"].shift(-1) == 1),
               (df["headJourneySeparator"].shift(1) == 1) & (df["headJourneySeparator"] == 1) & (df["headJourneySeparator"].shift(-1) == 0),
               (df["headJourneySeparator"].shift(1) == 0) & (df["headJourneySeparator"] == 1) & (df["headJourneySeparator"].shift(-1) == 1),
               (df["headJourneySeparator"].shift(1) == 1) & (df["headJourneySeparator"] == 0) & (df["headJourneySeparator"].shift(-1) == 1),
               (df["headJourneySeparator"].shift(1) == 1) & (df["headJourneySeparator"] == 1) & (df["headJourneySeparator"].shift(-1) == 1),
               (df["headJourneySeparator"] == 1),
               (df["headJourneySeparator"] == 0)
]

choices3 = ["body",
            "body",
            "head",
            "tail",
            "head",
            "headtail",
            "tail",
            "headtail",
            "head",
            "tail"
]

df["journeyPart"] = np.select(conditions3,choices3,default="0")

isNewRoute = df["journeyPart"].isin(["head","headTail"])

df["routeNumber"] = isNewRoute.cumsum()
df["routeNumber"] = np.where(df["routeNumber"] == "0","0",df["routeNumber"])

df2 = df.groupby("routeNumber").agg(
    head = ("journeyPart",lambda x: (x == "head").sum()),
    body = ("journeyPart",lambda x: (x == "body").sum()),
    tail = ("journeyPart",lambda x: (x == "tail").sum()),
    headTail = ("journeyPart",lambda x: (x == "headTail").sum())
).reset_index()

conditions4 = [(df2["head"] > 0) & (df2["body"] > 0) & (df2["tail"] > 0) & (df2["headTail"] == 0),
               (df2["head"] == 0) & (df2["body"] == 0) & (df2["tail"] == 0) & (df2["headTail"] > 0),
               (df2["head"] > 0) & (df2["body"] == 0) & (df2["tail"] > 0) & (df2["headTail"] == 0),
               (df2["head"] > 0) & (df2["body"] == 0) & (df2["tail"] == 0) & (df2["headTail"] > 0)
]

choices4 = [1,1,1,1]

df2["crewRouteValidation"] = np.select(conditions4,choices4,default=0)
df2["routeNumber"] = df2["routeNumber"].astype(int)
df2 = df2.sort_values(by="routeNumber", ascending=True)

df2["crewRouteRate"] = ((df2["crewRouteValidation"].sum())/(df2["routeNumber"].max()))  * 100
df2["crewRouteRate"] = df2["crewRouteRate"].round(2)

df.info()

df.to_csv(save_at,sep=";",index=False)
df2.to_csv(save_at2,sep=";",index=False)
