import pandas as pd
import numpy as np
import os
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","smtMealsFdaCockpit.csv")
file_path2 = os.path.join(script_dir,"input","smtMealsResCabin.csv")
file_path3 = os.path.join(script_dir,"input","dfsForWashUp.csv")
file_path4 = os.path.join(script_dir,"input","stationDb.csv")
save_at = os.path.join(script_dir,"output","dailyRestForWashUp.csv")
save_at2 = os.path.join(script_dir,"output","detailDfsForWashUp.csv")

df= pd.read_csv(file_path3,sep=";")
df2 = pd.read_csv(file_path,sep=";")
df3 = pd.read_csv(file_path2,sep=";")
df4 = pd.concat([df2, df3], ignore_index=True)

df["DATE"] = pd.to_datetime(df["DATE"], format="%d/%m/%Y", errors="coerce")
df["DATE"] = df["DATE"].ffill()

collection = ["FLT","TYPE","REG","AC","DEP","ARR","STD","STA","Crew #","Crew"]
for field in collection:
    df[field] = df[field].ffill()
    df[field] = df[field].astype(str)

collection2 = ["ATD","ATA"]
for field2 in collection2:
    df[field2] = pd.to_timedelta(df[field2] + ":00")
    df[field2] = df[field2].ffill()

df["dateArrival"] = np.where(df["ATA"] < df["ATD"],df["DATE"] + pd.Timedelta(days=1),df["DATE"])

df["dateTimeAtd"] = df["DATE"] + df["ATD"]
df["dateTimeAta"] = df["dateArrival"] + df["ATA"]

df5 = pd.read_csv(file_path4,sep=";")

df = pd.merge(df,df5[['activityBase', 'TRANSITION HOUR']],left_on='DEP',right_on='activityBase',how='left')

df = df.rename(columns={'TRANSITION HOUR': 'transitionDep'}).drop(columns=['activityBase'])

df = pd.merge(df,df5[['activityBase', 'TRANSITION HOUR']],left_on='ARR',right_on='activityBase',how='left')

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
        df["Crew"].str.contains("CPT", na=False),
        df["Crew"].str.contains("FA1", na=False),
        df["Crew"].str.contains("FO", na=False),
        df["Crew"].str.contains("FA", na=False)
]

choices2 = [
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

df.info()

df.to_csv(save_at2,sep=";",index=False)
df4.to_csv(save_at,sep=";",index=False)
