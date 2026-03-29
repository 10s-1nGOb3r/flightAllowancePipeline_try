import pandas as pd
import numpy as np
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","dfsFhAllowance.csv")
save_at = os.path.join(script_dir,"output","flightHourAllowance.csv")
save_at2 = os.path.join(script_dir,"output","detailFlightHourAllowance.csv")

df = pd.read_csv(file_path,sep=";")

def dataCleansingFormatting():
    df["DATE"] = pd.to_datetime(df["DATE"], format="%d/%m/%Y", errors="coerce")
    df["DATE"] = df["DATE"].ffill()
    df["MONTH"] = df["DATE"].dt.month
    df["YEAR"] = df["DATE"].dt.year
    
    collection = ["FLT","TYPE","REG","DEP","ARR","STD","STA","ATD","ATA","Crew #","Crew","MONTH","YEAR","AC"]
    for field in collection:
        df[field] = df[field].ffill()
        df[field] = df[field].astype(str)

    conditions = [
            df["Crew"].str.contains("CPT", na=False),
            df["Crew"].str.contains("FO", na=False),
            df["Crew"].str.contains("FA1", na=False),
            df["Crew"].str.contains("FA", na=False)
    ]
    choices = ["CPT","FO","FA1","FA"]

    df["RANK"] = np.select(conditions,choices,default="0")

    conditions3 = [(df["DATE"].dt.day < 16) & (df["RANK"] == "CPT"),
                   (df["DATE"].dt.day >= 16) & (df["RANK"] == "CPT"),
                   (df["DATE"].dt.day < 16) & (df["RANK"] == "FO"),
                   (df["DATE"].dt.day >= 16) & (df["RANK"] == "FO"),
                   (df["DATE"].dt.day > 0) & (df["RANK"] == "FA1"),
                   (df["DATE"].dt.day > 0) & (df["RANK"] == "FA")
    ]
    choices3 = [1,2,1,2,2,2]
    df["fataPayment"] = np.select(conditions3,choices3,default=0)

    df["BLOCK"] = pd.to_timedelta(df["BLOCK"].astype(str) + ":00",errors="coerce")
    df["BLOCK"] = df["BLOCK"].ffill()
    df["DATE"] = df["DATE"].astype(str)
    df["keyCockpitSmtFda"] = df["DATE"] + "." + df["FLT"] + "." + df["DEP"] + "." + df["ARR"]
    df["blockDec"] = df["BLOCK"].dt.total_seconds() / 3600
    df["blockDec"] = df["blockDec"].round(2)

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

dataCleansingFormatting()

df2 = df[["YEAR","MONTH","fataPayment","RANK","Crew","blockDec"]]
df2 = df2.groupby(["YEAR","MONTH","fataPayment","RANK","Crew"]).agg(
    totalFlightHour = ("blockDec","sum")
).reset_index()

df2["totalFlightHour"] = df2["totalFlightHour"].round(2)
total_minutes = (df2["totalFlightHour"] * 60).round()
hours = (total_minutes // 60).astype("Int64")
minutes = (total_minutes % 60).astype("Int64")
df2["totalFlightHour HR:MM"] = hours.astype(str) + "h " + minutes.astype(str).str.zfill(2) + "m"

df.to_csv(save_at2,sep=";",index=False)
df2.to_csv(save_at,sep=";",index=False)
