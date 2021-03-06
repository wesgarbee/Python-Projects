# Script written to import documents from MongoDB to SQL Server

import time
from pprint import pprint
import pypyodbc as pyodbc
from pymongo import MongoClient


# Establish connection with SQL Server
connsql = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
                         "SERVER=localhost,1401;"
                         "DATABASE=Cava_Wine_List;"
                         "UID=SA;"
                         "PWD=Swizzle1984wg!;"
                         "TrustServerCertificate=no;"
                         "Connection Timeout=60")

cursor = connsql.cursor()

# Establish connection with mongodb
try:
    connmon = MongoClient()
    print("Connected successfully!")
except Exception as e:
    print("Could not connect to MongoDB. Error: ", e)

# Connect to db
db = connmon.database
collection = db.cava_wine_list

# Create index
collection.create_index([("wine_id", 1)], unique=True)

count = 1

wines = collection.find()

for wine in wines:
    try:
        cursor.execute("INSERT INTO dbo.Wines "
                       "(Name, wine_id, Vintage, varietal, color, CountryOfOrigin, ImageURL) "
                       "VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (wine['name'], wine['wine_id'], wine['vintage'], wine['varietal'],
                        wine['color'], wine['origin'], wine['label_image']))
        cursor.commit()
        # cursor.execute("INSERT INTO dbo.WineRatings")
        print("SQL wine ID:", wine['wine_id'])

        count += 1
        if count % 100 == 0:
            time.sleep(5)
    except pyodbc.IntegrityError as e:
        print(wine['wine_id'], "already exists in the DB")
    except Exception as e:
        print(wine['name'])
