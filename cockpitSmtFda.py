import pandas as pd
import numpy as np
import os

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

df.to_csv(save_at,sep=";",index=False)
