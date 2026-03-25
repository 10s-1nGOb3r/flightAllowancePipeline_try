import pandas as pd
import numpy as np
import os
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","smtMealsFdaCockpit.csv")
file_path2 = os.path.join(script_dir,"input","crewSubBase.csv")
file_path3 = os.path.join(script_dir,"input","stationDb.csv")
save_at = os.path.join(script_dir,"output","detailCockpitSmtFda.csv")

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

df["dateCount"] = pd.to_datetime(df["Date"], format="%d/%m/%Y", errors="coerce")
df["dateCount"] = df["dateCount"].dt.strftime("%d/%m/%Y")
df["dateCount"] = df["dateCount"].fillna("21/09/1967")

df["activityBase"] = np.where(df["FDP"] == "00:00",df["crewBase"],df["Duty"].str[:3])

df3 = pd.read_csv(file_path3,sep=";")

df = pd.merge(df, df3[["activityBase","TRANSITION HOUR"]], on="activityBase", how="left")

df["TRANSITION HOUR"] = df["TRANSITION HOUR"].astype(float)

time_obj = pd.to_timedelta(df["Begin"].astype(str) + ":00", errors="coerce")
df["beginDec"] = time_obj.dt.total_seconds() / 3600
df["beginDec"] = df["beginDec"].fillna(0)
df["beginDec"] = df["beginDec"].round(2)

beginDecLt = df["beginDec"] + df["TRANSITION HOUR"]

df["dateCount"] = pd.to_datetime(df["dateCount"], format="%d/%m/%Y", errors="coerce")

df["dateCountFiltered"] = np.where(beginDecLt >= 24,df["dateCount"] + pd.Timedelta(days=1),df["dateCount"])

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

df["lastLeg"] = df["Duty"].str.strip().str.split(" ").str[-1].str.replace("-", "")

df["dhcLastLegValidation"] = np.where(df["lastLeg"].str.contains("*", na=False,regex=False),1,0)

df["lastLegFlightNumber"] = df["Duty"].str.strip().str.split(" ").str[-2].str.split("-").str[-1]

df["departureLastLegFlightNumber"] = df["Duty"].str.strip().str.split(" ").str[-2].str.split("-").str[-2]

df["arrivalLastLegFlightNumber"] = df["Duty"].str.strip().str.split(" ").str[-1].str.split("-").str[-1]

df = pd.merge(df,df3[["activityBase","TRANSITION HOUR"]].rename(columns={"activityBase": "activityBaseArrivalLastLeg","TRANSITION HOUR": "lastLegTransitionHour"}),left_on="arrivalLastLegFlightNumber",right_on="activityBaseArrivalLastLeg",how="left")

df["lastLegTransitionHour"] = df["lastLegTransitionHour"].astype(float)

time_parts = df["End"].str.split(":")
hours = time_parts.str[0].astype(float)
minutes_decimal = time_parts.str[1].astype(float) / 60
endDecimal = hours + minutes_decimal
endDecimal = endDecimal.round(2)

df["endDecimalLocalTime"] = np.where(endDecimal > 0,endDecimal + df["lastLegTransitionHour"],0)
df["endDecimalLocalTime"] = np.where(df["endDecimalLocalTime"] >= 24,df["endDecimalLocalTime"] - 24,df["endDecimalLocalTime"])
df["endDecimalLocalTime"] = df["endDecimalLocalTime"].round(2)

#df = pd.merge(df, df3[["activityBase","MIDNIGHT TIME"]],left_on="arrivalLastLegFlightNumber",right_on="activityBase",how="left")

df["dateSignOff"] = np.where(df["endDecimalLocalTime"] >= 24,df["dateCountFiltered"] + pd.Timedelta(days=1),df["dateCountFiltered"])

df.to_csv(save_at,sep=";",index=False)
