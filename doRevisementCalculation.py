import pandas as pd
import numpy as np
import os
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","smtMealsFdaCockpit.csv")
file_path2 = os.path.join(script_dir,"input","smtMealsResCabin.csv")
file_path3 = os.path.join(script_dir,"input","stationDb.csv")
file_path4 = os.path.join(script_dir,"input","unassignedDutyLabel.csv")
save_at = os.path.join(script_dir,"output","detailDailyRestForDoRevisement.csv")

df = pd.read_csv(file_path,sep=";")
df2 = pd.read_csv(file_path2,sep=";")
df3 = pd.concat([df, df2], ignore_index=True)

collection = ["col1","col2","col3","col4","col5","col6"]
for field in collection:
    df3[field] = df3[field].ffill()

collection2 = ["Begin","End","FDP"]
for field2 in collection2:
    df3[field2] = df3[field2].astype(str)
    df3[field2] = np.where(df3[field2].str.contains("[a-zA-Z]", regex=True, na=False),"00:00",df3[field2])

df3["Date"] = df3["Date"].astype(str)
df3["ID"] = np.where(df3["Date"].str.len() == 6,df3["Date"],np.nan)
df3["ID"] = df3["ID"].ffill()

df3["dateCount"] = pd.to_datetime(df3["Date"], format="%d/%m/%Y", errors="coerce")
df3["dateCount"] = df3["dateCount"].dt.strftime("%d/%m/%Y")
df3["dateCount"] = df3["dateCount"].fillna("21/09/1967")

df3["Duty"] = df3["Duty"].fillna("0")

df4 = pd.read_csv(file_path3,sep=";")

df3["depCat"] = df3["Duty"].str[:3]
df3 = pd.merge(df3,df4[['activityBase','TRANSITION HOUR']],left_on='depCat',right_on='activityBase',how='left')
df3 = df3.rename(columns={'TRANSITION HOUR': 'transitionDep'}).drop(columns=['activityBase'])
df3["transitionDep"] = df3["transitionDep"].astype(float)

conditions = [df3["depCat"] == "0",
              df3["depCat"] == "Bas"
]

choices = [0,0]

df3["transitionDep"] = np.select(conditions,choices,default=df3["transitionDep"])
df3["transitionDep"] = df3["transitionDep"].fillna(7)

df3["beginOffset"] = pd.to_datetime(df3["Begin"], format="mixed", errors="coerce").dt.strftime('%H:%M:%S')
df3["beginOffset"] = pd.to_timedelta(df3["beginOffset"])
df3["dateTimeSignOn"] = pd.to_datetime(df3["dateCount"], dayfirst=True) + df3["beginOffset"]

df3["transitionDep"] = pd.to_timedelta(df3["transitionDep"], unit='h')
df3["beginOnLocalTime"] = df3["dateTimeSignOn"] + df3["transitionDep"]

df3["arrCat"] = df3["Duty"].str.split('-').str[-1].str.strip()
df3 = pd.merge(df3,df4[['activityBase','TRANSITION HOUR']],left_on='arrCat',right_on='activityBase',how='left')
df3 = df3.rename(columns={'TRANSITION HOUR': 'transitionArr'}).drop(columns=['activityBase'])
df3["transitionArr"] = df3["transitionArr"].astype(float)

conditions2 = [df3["arrCat"] == "0",
               df3["arrCat"] == "Base:"]

choices2 = [0,0]

df3["transitionArr"] = np.select(conditions2,choices2,default=df3["transitionArr"])
df3["transitionArr"] = df3["transitionArr"].fillna(7)

df3["signOffDelta"] = pd.to_timedelta(0.5, unit='h')

df3["signOffDate"] = np.where(df3["Begin"] > df3["End"],df3["dateTimeSignOn"] + pd.Timedelta(days=1),df3["dateTimeSignOn"])
df3["signOffDate"] = df3["signOffDate"].dt.date
df3["signOffDate"] = pd.to_datetime(df3["signOffDate"], dayfirst=True)
df3["transitionArr"] = pd.to_timedelta(df3["transitionArr"], unit='h')
df3["endOffset"] = pd.to_datetime(df3["End"], format="mixed", errors="coerce").dt.strftime('%H:%M:%S')
df3["endOffset"] = pd.to_timedelta(df3["endOffset"])
df3["ataOnLocalTime"] = df3["signOffDate"] + df3["endOffset"]
df3["ataOnLocalTime"] = df3["ataOnLocalTime"] - pd.Timedelta(minutes=30)
df3["ataOnLocalTime"] = df3["ataOnLocalTime"] + df3["transitionArr"]

#timeNow = datetime.now()
#If you need a certain time filtering, please activate 
#the script below, 
#deactivate scipt "timeNow = datetime.now()" above 
#and edit timeNow on the first line
timeNow = "01/04/2026"
timeNow = datetime.strptime(timeNow, "%d/%m/%Y")
timeNow = timeNow.date()
currentMonth = timeNow.month
if currentMonth == 1:
    currentMonth = 12
else:
    currentMonth = currentMonth - 1

currentYear = timeNow.year
if currentMonth == 12:
    currentYear = currentYear - 1

df3["monthValidation"] = np.where((df3["beginOnLocalTime"].dt.month == currentMonth) & (df3["beginOnLocalTime"].dt.year == currentYear),1,0)

df3["dutyFieldForDayOffDetection"] = df3["Duty"].str.replace(" ", "")

df5 = pd.read_csv(file_path4,sep=";")

df3 = pd.merge(df3,df5[['dutyLabelUnassigned','unassignedValidation']].drop_duplicates(subset=['dutyLabelUnassigned']),left_on='dutyFieldForDayOffDetection',right_on='dutyLabelUnassigned',how='left')
df3 = df3.drop(columns=['dutyLabelUnassigned'])
df3["unassignedValidation"] = df3["unassignedValidation"].fillna("0") 
df3["unassignedValidation"] = df3["unassignedValidation"].astype(int)

conditions4 = [(df3["monthValidation"] == 1) & (df3["dutyFieldForDayOffDetection"].str.contains("D/O", na=False, case=False)),
               (df3["monthValidation"] == 1) & (df3["dutyFieldForDayOffDetection"].str.contains("DO01", na=False, case=False)),
               (df3["monthValidation"] == 1) & (df3["dutyFieldForDayOffDetection"].str.contains(">OFF", na=False, case=False))
]

choices4 = [1,1,1]

df3["dayoffValidation"] = np.select(conditions4,choices4,default=0)

df3["ataInDecimal"] = (df3["ataOnLocalTime"] - df3["ataOnLocalTime"].dt.normalize()) / pd.Timedelta(hours=1)
df3["ataInDecimal"] = df3["ataInDecimal"].round(2)

conditions3 = [(df3["monthValidation"] == 1) & (df3["unassignedValidation"] == 0) & (df3["dayoffValidation"] != 1),
               (df3["monthValidation"] == 1) & (df3["unassignedValidation"] == 1) & (df3["dayoffValidation"] == 1) & (df3["ataInDecimal"].shift(1) >= 22.00) & (df3["dutyFieldForDayOffDetection"].shift(1) != "Base:"),
]

choices3 = [1,1]

df3["assignableDayValidation"] = np.select(conditions3,choices3,default=0)

conditions4 = [(df3["monthValidation"] == 1) & (df3["unassignedValidation"] == 0) & (df3["dayoffValidation"] != 1),
]

choices4 = [1]

df3["assignableDayValidationForComparisson"] = np.select(conditions4,choices4,default=0)

df3.info()

df3.to_csv(save_at,sep=";",index=False)
