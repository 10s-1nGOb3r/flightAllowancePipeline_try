import pandas as pd
import numpy as np
import os

#Creating file path for file reading and file exporting
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir,"input","dfsFhAllowance.csv")
save_at = os.path.join(script_dir,"output","flightHourAllowance.csv")
save_at2 = os.path.join(script_dir,"output","detailFlightHourAllowance.csv")

#Reading the files generated from AIMS
#the required fields such as
#DATE,FLT,TYPE,REG,AC,DEP,ARR,STD,STA,ATD,ATA,BLOCK,CREW #,Crew
df = pd.read_csv(file_path,sep=";")

#A function that is used for cleansing data and formatting
def dataCleansingFormatting():
    #Some fields are being reformated and cleansed
    #Also some fileds are made by using logic
    #such as DATE,MONTH,YEAR,RANK,fataPayment,keyCockpitSmtFda
    df["DATE"] = pd.to_datetime(df["DATE"], format="%d/%m/%Y", errors="coerce")
    df["DATE"] = df["DATE"].ffill()
    df["MONTH"] = df["DATE"].dt.month
    df["YEAR"] = df["DATE"].dt.year
    
    collection = ["FLT","TYPE","REG","DEP","ARR","STD","STA","ATD","ATA","Crew #","Crew","MONTH","YEAR","AC"]
    for field in collection:
        df[field] = df[field].ffill()
        df[field] = df[field].astype(str)

    #A condition to determine rank of a crew in one flight
    #The raw data is like -(CPT) 303421
    #Its about how we extract that "CPT" character
    #This rank will be used on the followed field "fataPayment"
    conditions = [
            df["Crew"].str.contains("CPT", na=False),
            df["Crew"].str.contains("FO", na=False),
            df["Crew"].str.contains("FA1", na=False),
            df["Crew"].str.contains("FA", na=False)
    ]
    choices = ["CPT","FO","FA1","FA"]

    df["RANK"] = np.select(conditions,choices,default="0")

    #A condition to define whether a flight will be counted
    #into first term of FATA payment or second term of FATA payment
    #FYI, in Citilink Indonesia the cockpit crew being paid into
    #2 terms of FATA payment
    #01st term is counted from 01st date to the 15th
    #02nd term is counted as the rest of the month
    #For cabin crew are being paid in one term of payment
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

    #A condition to extract ID number for any crew in the dataset
    #The raw data is like -(CPT) 303421
    #Its about how to extract "303421" into 
    #our field called "Crew" 
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

#After the data being cleaned up and formatted 
#Next step is aggregation based on YEAR,MONTH,fataPayment
#Rank,Crew
#to determine the total flight hours 
#Which this will calculate about how much anycrew would get 
#for its allowance
df2 = df[["YEAR","MONTH","fataPayment","RANK","Crew","blockDec"]]
df2 = df2.groupby(["YEAR","MONTH","fataPayment","RANK","Crew"]).agg(
    totalFlightHour = ("blockDec","sum")
).reset_index()

#Converting the format of flight hour
#from decimal form to something like 52h 12m (52 hours 12 minutes)
df2["totalFlightHour"] = df2["totalFlightHour"].round(2)
total_minutes = (df2["totalFlightHour"] * 60).round()
hours = (total_minutes // 60).astype("Int64")
minutes = (total_minutes % 60).astype("Int64")
df2["totalFlightHour HR:MM"] = hours.astype(str) + "h " + minutes.astype(str).str.zfill(2) + "m"

#Exporting files from the pipeline to a folder called "output"
df.to_csv(save_at2,sep=";",index=False)
df2.to_csv(save_at,sep=";",index=False)
