import pandas as pd
import numpy as np
import os
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","smtMealsFdaCockpit.csv")
file_path2 = os.path.join(script_dir,"input","smtMealsResCabin.csv")
file_path3 = os.path.join(script_dir,"input","dfsForWashUp.csv")
file_path4 = os.path.join(script_dir,"input","stationDb.csv")
save_at = os.path.join(script_dir,"output","detailDailyRestForWashUp.csv")
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

collection4 = ["col1","col2","col3","col4","col5","col6"]
for field4 in collection4:
    df4[field4] = df4[field4].ffill()

collection5 = ["Begin","End","FDP"]
for field5 in collection5:
    df4[field5] = df4[field5].astype(str)
    df4[field5] = np.where(df4[field5].str.contains("[a-zA-Z]", regex=True, na=False),"00:00",df4[field5])

df4["Date"] = df4["Date"].astype(str)
df4["ID"] = np.where(df4["Date"].str.len() == 6,df4["Date"],np.nan)
df4["ID"] = df4["ID"].ffill()

df4["dateCount"] = pd.to_datetime(df4["Date"], format="%d/%m/%Y", errors="coerce")
df4["dateCount"] = df4["dateCount"].dt.strftime("%d/%m/%Y")
df4["dateCount"] = df4["dateCount"].fillna("21/09/1967")

df4["depCat"] = df4["Duty"].str[:3]
df4 = pd.merge(df4,df5[['activityBase','ZONE','SIGN ON','SIGN ON INTER','TRANSITION HOUR']],left_on='depCat',right_on='activityBase',how='left')
df4 = df4.rename(columns={'ZONE': 'zoneDep','SIGN ON': 'signOnDep','SIGN ON INTER': 'signOnInterDep','TRANSITION HOUR': 'transitionDep'}).drop(columns=['activityBase'])
df4["transitionDep"] = df4["transitionDep"].astype(float)

df4["arrCat"] = df4["Duty"].str.split('-').str[-1].str.strip()
df4 = pd.merge(df4,df5[['activityBase','ZONE','TRANSITION HOUR']],left_on='arrCat',right_on='activityBase',how='left')
df4 = df4.rename(columns={'ZONE': 'zoneArr','TRANSITION HOUR': 'transitionArr'}).drop(columns=['activityBase'])
df4["transitionArr"] = df4["transitionArr"].astype(float)

collection6 = ["zoneDep","arrCat","zoneArr"]
for field6 in collection6:
    df4[field6] = df4[field6].fillna("0")

collection7 = ["signOnDep","signOnInterDep"]
for field7 in collection7:
    df4[field7] = df4[field7].astype(float)
    df4[field7] = df4[field7].fillna(0.00) 

conditions3 = [(df4["zoneDep"] == "DOM") & (df4["zoneArr"] == "DOM"),
               (df4["zoneDep"] == "DOM") & (df4["zoneArr"] == "INT"),
               (df4["zoneDep"] == "INT") & (df4["zoneArr"] == "DOM"),
               (df4["zoneDep"] == "INT") & (df4["zoneArr"] == "INT")]

choices3 = ["DOM","INT","INT","INT"]

df4["firstFlightType"] = np.select(conditions3,choices3,default="0")

conditions4 = [(df4["firstFlightType"] == "DOM"),
               (df4["firstFlightType"] == "INT")]

choices4 = [df4["signOnDep"],
            df4["signOnInterDep"]]

df4["signOnDep2"] = np.select(conditions4,choices4,default=0.00)

combinedDateSignOnDep = df4["dateCount"] + " " + df4["Begin"]
df4["dateTimeSignOn"] = pd.to_datetime(combinedDateSignOnDep,dayfirst=True)

df4["signOnDelta"] = pd.to_timedelta(df4["signOnDep2"], unit='h')
df4["transitionDep"] = pd.to_timedelta(df4["transitionDep"], unit='h')
df4["atdOnLocalTime"] = df4["dateTimeSignOn"] + df4["transitionDep"] + df4["signOnDelta"] 
atdMonthValue = df4["atdOnLocalTime"].dt.month
atdYearValue = df4["atdOnLocalTime"].dt.year
df4["monthValidation"] = np.where((currentMonth == atdMonthValue) & (currentYear == atdYearValue),"1","0")

df4["signOffDelta"] = pd.to_timedelta(0.5, unit='h')

collection8 = ["Begin","End"]
for field8 in collection8:
    df4[field8] = pd.to_timedelta(df4[field8] + ":00")

df4["signOffDate"] = np.where(df4["Begin"] > df4["End"],df4["dateTimeSignOn"] + pd.Timedelta(days=1),df4["dateTimeSignOn"])
df4["signOffDate"] = df4["signOffDate"].dt.date
df4["signOffDate"] = pd.to_datetime(df4["signOffDate"], dayfirst=True)
df4["transitionArr"] = pd.to_timedelta(df4["transitionArr"], unit='h')
df4["ataOnLocalTime"] = df4["signOffDate"] + df4["End"]
df4["ataOnLocalTime"] = df4["ataOnLocalTime"] - pd.Timedelta(minutes=30)
df4["ataOnLocalTime"] = df4["ataOnLocalTime"] + df4["transitionArr"]

collection9 = ["atdOnLocalTime","ataOnLocalTime"]
for field9 in collection9:
    df4[field9] = df4[field9].fillna(pd.Timestamp("1900-01-01"))

collection10 = ["transitionDep","transitionArr"]
for field10 in collection10:
    df4[field10] = df4[field10].fillna(pd.Timedelta(seconds=0))

df4["dateFromAtdOnLocalTime"] = df4["atdOnLocalTime"].dt.strftime('%d/%m/%Y')
df4["dateFromAtaOnLocalTime"] = df4["ataOnLocalTime"].dt.strftime('%d/%m/%Y')

df4["flightNumberDepForKey"] = df4["Duty"].str.split('-').str[1]
df4["flightNumberDepForKey"] = df4["flightNumberDepForKey"].str.replace("*", "", regex=False).str.strip()
df4["flightNumberDepForKey"] = df4["flightNumberDepForKey"].fillna("0")
df4["depForKey"] = np.where(df4["flightNumberDepForKey"] != "0",df4["Duty"].str[:3],"0")

df4["flightNumberArrForKey"] = df4["Duty"].str.split('-').str[-2]
df4["flightNumberArrForKey"] = df4["flightNumberArrForKey"].str.replace("*", "", regex=False).str.strip()
df4["flightNumberArrForKey"] = df4["flightNumberArrForKey"].fillna("0")
parts = df4["Duty"].str.split('-')
df4["arrForKey"] = np.where(df4["flightNumberArrForKey"] != "0",parts.str[-3].str.strip(),"0")

df4["headCrewRouteKey"] = np.where((df4["monthValidation"] == "1") & (df4["depForKey"] != "0"),df4["ID"] + "." + df4["dateFromAtdOnLocalTime"] + "." + df4["flightNumberDepForKey"] + "." + df4["depForKey"],"0")
df4["headCrewRouteValidation"] = np.where(df4["headCrewRouteKey"] != "0","1","0")

df4["tailCrewRouteKey"] = np.where((df4["monthValidation"] == "1") & (df4["arrForKey"] != "0"),df4["ID"] + "." + df4["dateFromAtaOnLocalTime"] + "." + df4["flightNumberArrForKey"] + "." + df4["arrForKey"],"0")
df4["tailCrewRouteValidation"] = np.where(df4["tailCrewRouteKey"] != "0","1","0")

df['dateFromDateTimeAtdLt'] = df['dateTimeAtdLt'].dt.strftime('%d/%m/%Y')
df['dateFromDateTimeAtaLt'] = df['dateTimeAtaLt'].dt.strftime('%d/%m/%Y')

df["headCrewRouteKey"] = np.where(df["monthValidation"] == 1,df["Crew"] + "." + df['dateFromDateTimeAtdLt'] + "." + df["FLT"] + "." + df["DEP"],"0")
df = pd.merge(df,df4[['headCrewRouteKey','headCrewRouteValidation']].drop_duplicates(subset=['headCrewRouteKey']),left_on='headCrewRouteKey',right_on='headCrewRouteKey',how='left')
df["headCrewRouteValidation"] = df["headCrewRouteValidation"].fillna("0")

#df.info()
#df4.info()

df.to_csv(save_at2,sep=";",index=False)
df4.to_csv(save_at,sep=";",index=False)
