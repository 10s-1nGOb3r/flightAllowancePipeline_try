import pandas as pd
import numpy as np
import os
import calendar
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","rawRonData.csv")
file_path2 = os.path.join(script_dir,"input","stationDb.csv")
save_at = os.path.join(script_dir,"output","detailAircrewRon.csv")
save_at2 = os.path.join(script_dir,"output","aircrewRon.csv")

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

df2_unique = df2[["activityBase","TRANSITION HOUR","ZONE"]].drop_duplicates(subset=["activityBase"])

df = pd.merge(df, df2_unique,left_on="Port", right_on="activityBase", how="left")

df["TRANSITION HOUR"] = df["TRANSITION HOUR"].fillna("0")
df["TRANSITION HOUR"] = df["TRANSITION HOUR"].astype(int)

df["onChockTimeLt"] = np.where((df["onChockDecimal"] + df["TRANSITION HOUR"]) > 24,(df["onChockDecimal"] + df["TRANSITION HOUR"]) - 24,df["onChockDecimal"] + df["TRANSITION HOUR"])
onChockDelta = pd.to_timedelta(df["onChockTimeLt"].round(2), unit='h')
df["onChockTimeLt"] = (pd.to_datetime('2026-01-01') + onChockDelta).dt.time

df["offChockTimeLt"] = np.where((df["offChockDecimal"] + df["TRANSITION HOUR"]) > 24,(df["offChockDecimal"] + df["TRANSITION HOUR"]) - 24,df["offChockDecimal"] + df["TRANSITION HOUR"])
offChockDelta = pd.to_timedelta(df["offChockTimeLt"].round(2), unit='h')
df["offChockTimeLt"] = (pd.to_datetime('2026-01-01') + offChockDelta).dt.time

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
#If you need a certain time filtering, please activate 
#the script below, 
#deactivate scipt "timeNow = datetime.now()" above 
#and edit timeNow on the first line
#timeNow = "01/02/2026"
#timeNow = datetime.strptime(timeNow, "%d/%m/%Y")
#timeNow = timeNow.date()
last_month_dt = timeNow - pd.DateOffset(months=1)
currentMonth = last_month_dt.month
currentYear = last_month_dt.year

dateBegin = f"01/{currentMonth:02d}/{currentYear}"
dateEnd = f"{calendar.monthrange(currentYear, currentMonth)[1]:02d}/{currentMonth:02d}/{currentYear}"

df["dateOnChockLt"] = pd.to_datetime(df["dateOnChockLt"])
df["dateOnChock"] = pd.to_datetime(df["dateOnChock"], format="%d/%m/%Y", errors="coerce")
df["dateOffChockLt"] = pd.to_datetime(df["dateOffChockLt"])
df["dateOffChock"] = pd.to_datetime(df["dateOffChock"], format="%d/%m/%Y", errors="coerce")

conditions = [(df["dateOnChockLt"].dt.month < currentMonth) & (df["dateOnChock"].dt.year == currentYear),
              (df["dateOnChockLt"].dt.month > currentMonth) & (df["dateOnChock"].dt.year == currentYear),
              (df["dateOnChockLt"].dt.month > currentMonth) & (df["dateOnChock"].dt.year < currentYear),
              (df["dateOnChockLt"].dt.month < currentMonth) & (df["dateOnChock"].dt.year > currentYear)
]

choices = [dateBegin,
           dateEnd,
           dateBegin,
           dateEnd
]

conditions2 = [(df["dateOffChockLt"].dt.month < currentMonth) & (df["dateOffChock"].dt.year == currentYear),
              (df["dateOffChockLt"].dt.month > currentMonth) & (df["dateOffChock"].dt.year == currentYear),
              (df["dateOffChockLt"].dt.month > currentMonth) & (df["dateOffChock"].dt.year < currentYear),
              (df["dateOffChockLt"].dt.month < currentMonth) & (df["dateOffChock"].dt.year > currentYear)
]

choices3 = [dateBegin,
            dateEnd,
            dateBegin,
            dateEnd
]

choices2 = ["00:00:00",
            "00:00:00",
            "00:00:00",
            "00:00:00"
]

df["onChockLt"] = np.select(conditions,choices,default=df["dateOnChockLt"].dt.strftime("%d/%m/%Y"))
df["onChockTimeLt"] = np.select(conditions,choices2,default=df["onChockTimeLt"].astype(str))

df["offChockLt"] = np.select(conditions2,choices3,default=df["dateOffChockLt"].dt.strftime("%d/%m/%Y"))
df["offChockTimeLt"] = np.select(conditions2,choices2,default=df["offChockTimeLt"].astype(str))

date_obj = pd.to_datetime(df["onChockLt"], format="%d/%m/%Y")
time_delta = pd.to_timedelta(df["onChockTimeLt"].astype(str))
df["onChockLt"] = date_obj + time_delta

date_obj2 = pd.to_datetime(df["offChockLt"], format="%d/%m/%Y")
time_delta2 = pd.to_timedelta(df["offChockTimeLt"].astype(str))
df["offChockLt"] = date_obj2 + time_delta2

dateEndConvertDateTime = pd.to_datetime(dateEnd,format="%d/%m/%Y")
dateEndDate = dateEndConvertDateTime.day
dateEndMonth = dateEndConvertDateTime.month
dateEndYear = dateEndConvertDateTime.year

offChockDate = df["offChockLt"].dt.day
offChockMonth = df["offChockLt"].dt.month
offChockYear = df["offChockLt"].dt.year

offChockDatePlusOne = (df["offChockLt"] + pd.Timedelta(days=1)).dt.normalize() + pd.Timedelta(hours=23, minutes=59)

df["offChockLt"] = np.where((dateEndDate == offChockDate) & (dateEndMonth == offChockMonth) & (dateEndYear == offChockYear),offChockDatePlusOne,df["offChockLt"])

df["ronDay"] = df["offChockLt"] - df["onChockLt"]

offChockDate2 = df["offChockLt"].dt.day
offChockMonth2 = df["offChockLt"].dt.month
offChockYear2 = df["offChockLt"].dt.year
onChockDate = df["onChockLt"].dt.day
onChockMonth = df["onChockLt"].dt.month
onChockYear = df["onChockLt"].dt.year
df["ronDay"] = np.where((offChockDate2 == onChockDate) & (offChockMonth2 == onChockMonth) & (offChockYear2 == onChockYear),df["ronDay"].dt.floor('D'),df["ronDay"])

ronDayDays = df["ronDay"]/pd.Timedelta(days=1)

conditions4 = [(ronDayDays > 0.5625) & (ronDayDays < 1),
               ronDayDays >= 1
]

choices5 = [np.floor(ronDayDays + 0.5),
            ronDayDays
]

ronDayDays = np.select(conditions4,choices5,default=0)
ronDayDays = ronDayDays.astype(int)
decimalHoursRonDay = df["ronDay"]/pd.Timedelta(days=1)
decimalHoursRonDay = np.where(decimalHoursRonDay > 0.5625,np.floor(decimalHoursRonDay + 0.5),0)

df["nonSplitDutyValidation"] = np.where((ronDayDays > 0) & (decimalHoursRonDay > 0.5625),1,0)

conditions3 = [(df["flightNumberOnChock"].str.contains("QG", case=False, na=False) == False) & (df["fligthNumberOffChock"].str.contains("QG", case=False, na=False) == True) & (df["TRANSITION HOUR"] != 0),
               (df["flightNumberOnChock"].str.contains("QG", case=False, na=False) == True) & (df["fligthNumberOffChock"].str.contains("QG", case=False, na=False) == False) & (df["TRANSITION HOUR"] != 0),
               (df["flightNumberOnChock"].str.contains("QG", case=False, na=False) == False) & (df["fligthNumberOffChock"].str.contains("QG", case=False, na=False) == False) & (df["TRANSITION HOUR"] != 0)
]

choices4 = [1,1,1]

df["trainingValidation"] = np.select(conditions3,choices4,default=0)

df["ronDayCount"] = np.where((df["nonSplitDutyValidation"] == 1) & (df["trainingValidation"] != 1),ronDayDays,0)
df["ronDayCount"] = np.where(df["trainingValidation"] == 1,0,df["ronDayCount"])

df["monthCalculation"] = currentMonth
df["yearCalculation"] = currentYear

df3 = df.groupby(["yearCalculation","monthCalculation","ZONE","ID"]).agg(
    ronSummary = ("ronDayCount","sum")
).reset_index()

#df.info()

df.to_csv(save_at,sep=";",index=False)
df3.to_csv(save_at2,sep=";",index=False)
