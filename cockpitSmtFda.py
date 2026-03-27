import pandas as pd
import numpy as np
import os
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","smtMealsFdaCockpit.csv")
file_path2 = os.path.join(script_dir,"input","crewSubBase.csv")
file_path3 = os.path.join(script_dir,"input","stationDb.csv")
file_path4 = os.path.join(script_dir,"output","detailFlightHourAllowance.csv")
file_path5 = os.path.join(script_dir,"input","smtCode.csv")
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

df["dateSignOff"] = np.where(df["endDecimalLocalTime"] >= 24,df["dateCountFiltered"] + pd.Timedelta(days=1),df["dateCountFiltered"])

df["dateSignOff"] = df["dateSignOff"].astype(str)
df["keyflightHourAllowance"] = np.where(df["dhcLastLegValidation"] == 1,df["dateSignOff"] + "." + df["lastLegFlightNumber"] + "." + df["departureLastLegFlightNumber"] + "." + df["arrivalLastLegFlightNumber"],0)

df4 = pd.read_csv(file_path4,sep=";")

df4_unique = df4[["keyCockpitSmtFda", "blockDec"]].drop_duplicates(subset=["keyCockpitSmtFda"])

df = pd.merge(df, df4_unique.rename(columns={"blockDec": "blockDecDhcLastLeg"}),left_on="keyflightHourAllowance", right_on="keyCockpitSmtFda", how="left")

df["blockDecDhcLastLeg"] = df["blockDecDhcLastLeg"].fillna(0)

time_parts2 = df["FDP"].str.split(":")
hours2 = time_parts2.str[0].astype(float)
minutes_decimal2 = time_parts2.str[1].astype(float) / 60
fdpDecimal = hours2 + minutes_decimal2
fdpDecimal = fdpDecimal.round(2)
df["fdpDec"] = fdpDecimal

df["fda"] = np.where((df["monthValidation"] == 1) & df["fdpDec"] > 0 & (df["dhcLastLegValidation"] == 1),df["fdpDec"] - df["blockDecDhcLastLeg"],0)
df["fda"] = df["fda"].round(2)

df["dhcDayOneBefore"] = np.where((df["Duty"].str.contains("*",na=False,regex=False) == True) & (df["fda"] == 0) & (df["monthValidation"] == 1),1,0)

df["activityBase2"] = df["activityBase"].replace("HLP","CGK")

df["sppdValidation"] = np.where(df["Duty"].str.contains("SPPD",na=False,regex=False),1,0)

conditions = [(df["monthValidation"] == 1) & (df["activityBase2"] == df["crewBase"]) & (df["fdpDec"] > 0) & (df["dhcDayOneBefore"] == 0) & (df["sppdValidation"] == 0),
              (df["monthValidation"] == 1) & (df["activityBase2"] == df["crewBase"]) & (df["fdpDec"] == 0) & (df["dhcDayOneBefore"] == 1) & (df["sppdValidation"] == 0)
]
choices = [1,1]

df["smtByDuty"] = np.select(conditions,choices,default=0)

#For smtByTraining beware of training code such as
#TR,PC,TRI,PCI of ATR Crew because its done in Bangkok
#and not suppose to receive SMT

df["groundPatternCode"] = np.where((df["monthValidation"] == 1) & (df["sppdValidation"] == 0) & (df["smtByDuty"] == 0) & (df["fdpDec"] == 0) & (df["dhcDayOneBefore"] == 0),df["Duty"],0)
df["groundPatternCode"] = df["groundPatternCode"].str.replace(" ","")
df["groundPatternCode"] = df["groundPatternCode"].fillna("0")

df5 = pd.read_csv(file_path5,sep=";")

df5_unique = df5[["TRAINING CODE", "LOC"]].drop_duplicates(subset=["TRAINING CODE"])

df = pd.merge(df, df5_unique,left_on="groundPatternCode",right_on="TRAINING CODE",how="left")

collection3 = ["TRAINING CODE","LOC"]
for field3 in collection3:
    df[field3] = df[field3].fillna("0")

conditions3 = [(df["col5"] == "ATR") & (df["TRAINING CODE"] == "TR"),
               (df["col5"] == "ATR") & (df["TRAINING CODE"] == "TRI"),
               (df["col5"] == "ATR") & (df["TRAINING CODE"] == "PC"),
               (df["col5"] == "ATR") & (df["TRAINING CODE"] == "PCI"),]
choices3 = ["BKK","BKK","BKK","BKK"]
df["LOC"] = np.select(conditions3,choices3,default=df["LOC"])

#df["smtByTraining"]

df.to_csv(save_at,sep=";",index=False)
