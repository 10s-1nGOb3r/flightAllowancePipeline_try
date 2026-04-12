import pandas as pd
import numpy as np
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","smtMealsFdaCockpit.csv")
file_path2 = os.path.join(script_dir,"input","smtMealsResCabin.csv")
file_path3 = os.path.join(script_dir,"input","dfsForWashUp.csv")
save_at = os.path.join(script_dir,"output","dailyRestForWashUp.csv")
save_at2 = os.path.join(script_dir,"output","dfsDetailsForWashUp.csv")

df= pd.read_csv(file_path3,sep=";")
df2 = pd.read_csv(file_path,sep=";")
df3 = pd.read_csv(file_path2,sep=";")
df4 = pd.concat([df, df2], ignore_index=True)

df["DATE"] = pd.to_datetime(df["DATE"], format="%d/%m/%Y", errors="coerce")
df["DATE"] = df["DATE"].ffill()
df["MONTH"] = df["DATE"].dt.month
df["YEAR"] = df["DATE"].dt.year

collection = ["FLT","TYPE","REG","AC","DEP","ARR","STD","STA","Crew #","Crew","MONTH","YEAR"]
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

#df.info()

df.to_csv(save_at2,sep=";",index=False)
df4.to_csv(save_at,sep=";",index=False)
