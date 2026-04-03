import pandas as pd
import numpy as np
import os
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","smtMealsResCabin.csv")
file_path2 = os.path.join(script_dir,"input","cabinCrewSubBase.csv")
file_path3 = os.path.join(script_dir,"input","stationDb.csv")
save_at = os.path.join(script_dir,"output","detailCabinSmtRes.csv")

df = pd.read_csv(file_path,sep=";")

collection = ["col1","col2","col3","col4","col5","col6"]
for field in collection:
    df[field] = df[field].ffill()

collection2 = ["Begin","End","FDP"]
for field2 in collection2:
    df[field2] = df[field2].astype(str)
    df[field2] = np.where(df[field2].str.contains("[a-zA-Z]", regex=True, na=False),"00:00",df[field2])

df["Date"] = df["Date"].astype(str)
df["ID"] = np.where(df["Date"].str.len() == 6,df["Date"],np.nan)
df["ID"] = df["ID"].ffill()

df2 = pd.read_csv(file_path2,sep=";")
df2["ID"] = df2["ID"].astype(str)

df = pd.merge(df, df2[["ID","BASE"]], on="ID", how="left")
df["BASE"] = df["BASE"].fillna("CGK")
df= df.rename(columns={"BASE": "crewBase"})
df["crewBase"] = df["crewBase"].str.replace(" ","")

df["dateCount"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
df["dateCount"] = df["dateCount"].dt.strftime("%d/%m/%Y")
df["dateCount"] = df["dateCount"].fillna("21/09/1967")

conditions2 = [(df["Duty"].str.contains("*",na=False,regex=False) == True) & (df["FDP"] == "00:00"),
               (df["Duty"].str.contains("*",na=False,regex=False) == True) & (df["FDP"] != "00:00"),
               (df["Duty"].str.contains("*",na=False,regex=False) == False) & (df["FDP"] != "00:00"),
               (df["Duty"].str.contains("*",na=False,regex=False) == False) & (df["FDP"] == "00:00")
]
choices2 = [df["Duty"].str[:3],df["Duty"].str[:3],df["Duty"].str[:3],df["crewBase"]]
df["activityBase"] = np.select(conditions2,choices2,default="0")

df3 = pd.read_csv(file_path3,sep=";")

df3_unique = df3[["activityBase", "TRANSITION HOUR", "SIGN ON"]].drop_duplicates(subset=["activityBase"])

df = pd.merge(df, df3_unique, on="activityBase", how="left")

df["TRANSITION HOUR"] = df["TRANSITION HOUR"].astype(float)
df["SIGN ON"] = df["SIGN ON"].astype(float)

time_obj = pd.to_timedelta(df["Begin"].astype(str) + ":00", errors="coerce")
df["beginDec"] = time_obj.dt.total_seconds() / 3600
df["beginDec"] = df["beginDec"].fillna(0)
df["beginDec"] = df["beginDec"].round(2)

df["beginDecLt"] = df["beginDec"] + df["TRANSITION HOUR"] + df["SIGN ON"]

df["beginDecLt"] = np.where(df["beginDecLt"] >= 24,df["beginDecLt"] - 24,df["beginDecLt"])
df["beginDecLt"] = df["beginDecLt"].round(2)

df["dateCount"] = pd.to_datetime(df["dateCount"], format="%d/%m/%Y", errors="coerce")

df["dateCountFiltered"] = np.where(df["beginDecLt"] < df["TRANSITION HOUR"],df["dateCount"] + pd.Timedelta(days=1),df["dateCount"])

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

df["monthValidation"] = np.where((df["dateCountFiltered"].dt.month == currentMonth) & (df["dateCountFiltered"].dt.year == currentYear),1,0)

df["month"] = np.where(df["monthValidation"] == 1,df["dateCountFiltered"].dt.month,0)
df["year"] = np.where(df["monthValidation"] == 1,df["dateCountFiltered"].dt.year,0)

time_parts = df["FDP"].str.split(":")
hours = time_parts.str[0].astype(float)
minutes_decimal = time_parts.str[1].astype(float) / 60
fdpDecimal = hours + minutes_decimal
fdpDecimal = fdpDecimal.round(2)
df["fda"] = fdpDecimal

df["dhcDayOneBefore"] = np.where((df["Duty"].str.contains("*",na=False,regex=False) == True) & (df["fda"] == 0) & (df["monthValidation"] == 1),1,0)

df["activityBase2"] = df["activityBase"].replace("HLP","CGK")

df["sppdValidation"] = np.where(df["Duty"].str.contains("SPPD",na=False,regex=False),1,0)

conditions = [(df["monthValidation"] == 1) & (df["activityBase2"] == df["crewBase"]) & (df["fda"] > 0) & (df["dhcDayOneBefore"] == 0) & (df["sppdValidation"] == 0),
              (df["monthValidation"] == 1) & (df["activityBase2"] == df["crewBase"]) & (df["fda"] == 0) & (df["dhcDayOneBefore"] == 1) & (df["sppdValidation"] == 0)
]
choices = [1,1]

df["smtByDuty"] = np.select(conditions,choices,default=0)

df.to_csv(save_at,sep=";",index=False)
