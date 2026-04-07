import pandas as pd
import numpy as np
import os
import calendar
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","rawRonData.csv")
file_path2 = os.path.join(script_dir,"input","stationDb.csv")
save_at = os.path.join(script_dir,"output","detailAircrewRon.csv")

df = pd.read_csv(file_path,sep=";")

df = df.rename(columns={"(UTC time)":"onChock",
                        "(UTC time).1":"offChock",
                        "DATE":"dateOnChock",
                        "DATE.1":"dateOffChock",
                        "FLT":"flightNumberOnChock",
                        "FLT.1":"fligthNumberOffChock",
                        "Unnamed: 12":"telphoneNumber"})

df["Fax"] = df["Fax"].fillna("0")
df["ID"] = pd.to_numeric(df["ID"], errors='coerce').fillna(0).astype(int).astype(str)

collection = ["onChock","offChock"]
for field1 in collection:
    df[field1] = pd.to_numeric(df[field1], errors='coerce').fillna(0).astype(int).astype(str)
    df[field1] = df[field1].astype(str).str.zfill(4)
    df[field1] = pd.to_datetime(df[field1], format="%H%M").dt.time

for field2 in collection:
    temp_dt = pd.to_datetime(df[field2], format='%H:%M:%S', errors='coerce')
    df[f"{field2}Decimal"] = (temp_dt.dt.hour + (temp_dt.dt.minute / 60) + (temp_dt.dt.second / 3600)).round(2)

df2 = pd.read_csv(file_path2,sep=";")

df2_unique = df2[["activityBase","TRANSITION HOUR"]].drop_duplicates(subset=["activityBase"])

df = pd.merge(df, df2_unique,left_on="Port", right_on="activityBase", how="left")

df["TRANSITION HOUR"] = df["TRANSITION HOUR"].fillna("0")
df["TRANSITION HOUR"] = df["TRANSITION HOUR"].astype(int)

collection1 = ["dateOnChock","dateOffChock"]
for field3 in collection1:
    df[f"{field3}Lt"] = pd.to_datetime(df[field3], format="%d/%m/%Y", errors="coerce")

df["dateOnChockLt"] = np.where((df["onChockDecimal"] + df["TRANSITION HOUR"]) > 24,df["dateOnChockLt"] + pd.Timedelta(days=1),df["dateOnChockLt"])
df["dateOffChockLt"] = np.where((df["offChockDecimal"] + df["TRANSITION HOUR"]) > 24,df["dateOffChockLt"] + pd.Timedelta(days=1),df["dateOffChockLt"])

collection2 = ["on","off"]
for field4 in collection2:
    delta = pd.to_timedelta(df[f"{field4}ChockDecimal"] + df["TRANSITION HOUR"], unit='h')
    df[f"{field4}ChockLt"] = (df[f"date{field4.capitalize()}ChockLt"] + delta).dt.strftime("%d/%m/%Y %H:%M:%S")

timeNow = datetime.now()
currentMonth = timeNow.month
if currentMonth == 1:
    currentMonth = 12
else:
    currentMonth = currentMonth - 1

currentYear = timeNow.year
if currentMonth == 12:
    currentYear = currentYear - 1

dateBegin = pd.to_datetime(f"{currentYear}-{currentMonth}-01")
dateBegin = dateBegin.strftime("%d/%m/%Y")
dateEnd = pd.to_datetime(f"{currentYear}-{currentMonth}-{calendar.monthrange(currentYear, currentMonth)[1]}")
dateEnd = dateEnd.strftime("%d/%m/%Y")

#df["onChockLt"] = np.where(df["onChockLt"].dt.month < currentMonth,df["onChockLt"] == ""

df.to_csv(save_at,sep=";",index=False)
