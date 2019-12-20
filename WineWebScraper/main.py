# A web scraper specifically for wine.com
# Built by Wes Garbee

import os
import time
import random
import urllib.request
import requests
import re
from http import client
from pymongo import MongoClient
import pypyodbc as pyodbc
from bs4 import BeautifulSoup as bSoup
from Classes.console_colors import Concolors as color


def service_function():
    # Checks if txt file for saving page exists
    if not os.path.exists('page_progress.txt'):
        with open('page_progress.txt', 'w') as file:
            file.write('https://www.wine.com/list/wine/7155/1')

    with open('page_progress.txt', 'r') as file:
        data = file.read()

    # Passes the URL read from the save file to address file to be used elsewhere in the script
    address = data
    # Strips the page number for use later when rebuilding the URL for save after incrementing the page number
    main_url = address.rsplit('/', 1)[0]
    # Increments the page number to scrape
    page_number = int(data.rsplit('/', 1)[-1])

    req = requests.head(address)
    code = req.status_code

    # Select which db to persist to- commented out to as MongoDB is being deprecated for this task
    # print("Select which db to add to")
    # print("1. SQL Server")
    # print("2. MongoDB")
    selection = '1'

    # Runs if
    if selection == '1':
        # Counts number of entries added to db
        added_to_SQL_db = 0

        try:
            # Connects to local SQL server
            connsql = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
                                     "SERVER=localhost,1401;"
                                     "DATABASE=Cava_Wine_List;"
                                     "UID=SA;"
                                     "PWD=HandleTempDB!;"
                                     "TrustServerCertificate=no;"
                                     "Connection Timeout=120")
            cursor = connsql.cursor()
            print("Connected to local SQL DB successfully!")
        except Exception as e:
            print("Could not connect to SQL DB. Error: ", e)

        try:
            # Runs as long as the server code returned is 200
            while code is 200:
                req = requests.get(address)

                soup = bSoup(req.text, "html.parser")
                wines = soup.findAll("li", {"class": "prodItem"})

                for wine in wines:
                    wine_sql_id = 0
                    rating_sql_id = 0
                    list_rating_sql_id = []

                    # This checks that the wine isn't a gift set or collection
                    if not wine.find("div", {"class": "prodItemInfo_origin js-is-hidden"}):
                        # Gets the line of text for the name and vintage
                        wine_text = wine.find("span", {"class": "prodItemInfo_name"}).text.replace("/", " ")

                        # Gets the id from wine.com for later use
                        wine_id = int(wine.find("meta")['content'])

                        # Gets the wine vintage, if no vintage, sets 0 which will output 'NV' later on
                        wine_vintage = wine_text[-4:]
                        try:
                            wine_vintage = int(wine_vintage)
                        except ValueError:
                            wine_vintage = 0

                        # If there is a vintage for the wine, removes the vintage from the end.
                        # Else the wine name is the wine_text
                        if wine_vintage is not 0:
                            wine_name = wine_text[:-4].strip()
                        else:
                            wine_name = wine_text

                        # Gets all of the ratings for the wine and adds them to
                        wine_ratings = []
                        try:
                            wine_ratings_elements = wine.find_all("li", {"class": "wineRatings_listItem"})
                            if wine_ratings_elements:
                                for wre in wine_ratings_elements:
                                    individual_rating = []
                                    rating = wre['title']  # Gets the string of the rating from the element
                                    rater = rating.rsplit(' ', 3)[0].strip()  # Strips the points value
                                    rating_value = re.search(r'\d+', rating).group()  # Grabs point value from the string
                                    individual_rating = ([rater, rating_value])  # Adds the rater and point to the list
                                    wine_ratings.append(individual_rating)  # Adds rating to list
                        except Exception as e:
                            print("No ratings to add:", e)

                        # Get the varietal/blend
                        wine_varietal_text = wine.find("span", {"class": "prodItemInfo_varietal"}).text

                        # Gets the color of the wine: Red, White, Champagne/Sparkling, or other colors
                        try:
                            wine_color = wine.find("li", {"class": "prodAttr_icon"})['title']
                        except TypeError:
                            # This stores the wine varietal from the data as there is now attrs 'title' for these
                            if (wine_varietal_text == "Other Dessert") or \
                                    (wine_varietal_text == "Sherry") or \
                                    (wine_varietal_text == "Port"):
                                wine_color = wine_varietal_text

                        # Creates the path where images will be stored
                        try:
                            if os.path.isdir("./images/" + wine_color.replace(" ", "_")):
                                dir_name = ("./images/" + wine_color.replace(" ", "_") + "/" +
                                            wine_varietal_text.replace(" ", "_").replace("/", "_"))
                            else:
                                dir_name = ("./images/Other/" + wine_varietal_text.replace(" ", "_").replace("/", "_"))
                            # If that dir does not exist, creates it.
                            if not os.path.isdir(dir_name):
                                os.mkdir(dir_name)
                        except FileNotFoundError as e:
                            print(color.FAIL + color.BOLD + e + color.ENDC)

                        wine_origin = wine.find("span", {"class": "prodItemInfo_originText"}).text
                        wine_img_element = wine.find('img')
                        wine_img = ("https://www.wine.com/" + wine_img_element.get('src'))
                        wine_img_path = (dir_name + "/" + wine_text.replace(" ", "_") + "_label.jpg")
                        urllib.request.urlretrieve(wine_img, wine_img_path)

                        try:
                            # Inserts a new wine, fails if there is already a wine in the DB with same wine_id
                            cursor.execute('INSERT INTO dbo.Wines '
                                           '(Name, wine_id, Vintage, Varietal, color, CountryOfOrigin, ImageURL) '
                                           'VALUES (?, ?, ?, ?, ?, ?, ?);',
                                           (wine_name, wine_id, wine_vintage, wine_varietal_text,
                                            wine_color, wine_origin, wine_img_path.encode('utf-8')))
                            cursor.commit()

                            # Gets the newly created sql db auto increment id
                            cursor.execute('SELECT Id FROM dbo.Wines WHERE wine_id = ?', [wine_id])
                            wine_sql_id = cursor.fetchone()[0]

                            # Prints the inserted ID
                            print(color.OKBLUE + color.BOLD + "\t\t\t" + wine_name + " inserted with SQL id "
                                  + color.FAIL + str(wine_sql_id) + color.ENDC)
                            # Increments count of number of entries added to the db
                            added_to_SQL_db += 1
                        except pyodbc.IntegrityError as e:
                            print("Wine ID", color.BOLD + color.OKBLUE + str(wine_id) + color.ENDC,
                                  "already exists in the SQL DB")

                        # Loops through the ratings and adds to db
                        if wine_ratings:
                            for wr in wine_ratings:
                                try:
                                    cursor.execute('INSERT INTO dbo.Ratings'
                                                   '(Reviewer, Rating)'
                                                   'VALUES (?, ?);',
                                                   wr)
                                    cursor.commit()
                                    print(color.OKGREEN + color.BOLD + "\t\t\tRating inserted with SQL ID "
                                          + str(rating_sql_id) + color.ENDC)
                                except pyodbc.IntegrityError as e:
                                    print("Rating already exists in the SQL DB")

                                # Gets the SQL ID for the last entered row and adds to a list for later user
                                cursor.execute('SELECT Id FROM dbo.Ratings WHERE Reviewer = ? AND Rating = ?', wr)
                                rating_sql_id = cursor.fetchone()[0]
                                list_rating_sql_id.append(rating_sql_id)

                        # Gets the SQL ID for the current wine from the db
                        # in the event that one was not added in current loop
                        if not wine_sql_id:
                            cursor.execute('SELECT Id FROM dbo.Wines WHERE wine_id = ?', [wine_id])
                            wine_sql_id = cursor.fetchone()[0]

                        # Adds the Wine ID and Rating ID to the junction table
                        if list_rating_sql_id:
                            for x in list_rating_sql_id:
                                try:
                                    cursor.execute('INSERT INTO dbo.WineRatings'
                                                   '(WineId, RatingId)'
                                                   'VALUES (?, ?);',
                                                   (wine_sql_id, x))
                                    cursor.commit()
                                    print(color.OKGREEN + color.BOLD + "\t\t\tJunction table updated" + color.ENDC)
                                except pyodbc.IntegrityError as e:
                                    print("This relationship already exists")
                    else:
                        continue

                page_number += 1

                # Prints the number of new entries added to the SQL db
                print(str(added_to_SQL_db), "new entries added to the SQL db")
                added_to_SQL_db = 0

                # Writes the latest URL to the save file
                print("Page number:", page_number)
                address = main_url + "/" + str(page_number)
                with open('page_progress.txt', 'w') as file:
                    file.write(address)

                # Gets the number of wines in the db
                cursor.execute('SELECT COUNT(Id) FROM Wines')
                number_of_entries = cursor.fetchone()[0]
                print("There are currently " + str(number_of_entries) + " wines in the SQL database.")

                # Sets random delay in seconds so as not to overload the server
                seconds = random.randrange(45, 60)
                if page_number % 3 is 0:
                    seconds += 30
                print("Paused for", seconds, "seconds.")
                time.sleep(seconds)

                # Gets status code
                code = requests.head(address).status_code
        except client.HTTPException as e:
            code = requests.head(address).status_code
            if code != 402 or code != 403:
                print("Status code:", code)
                print("Stopped with the following error:", e)
                service_function()
            else:
                print("Status code:", code)
                print("Stopped with the following error:", e)
                connsql.close()
                quit()

    # Adds to the MongoDB
    # May deprecate
    elif selection == '2':
        # Establish connection
        try:
            conn = MongoClient()
            print("Connected successfully!")
        except Exception as e:
            print("Could not connect to MongoDB. Error: ", e)

        # Connect to db
        db = conn.database
        collection = db.cava_wine_list

        # Create index
        collection.create_index([("wine_id", 1)], unique=True)

        while code is 200:
            req = requests.get(address)

            soup = bSoup(req.text, "html.parser")
            wines = soup.findAll("li", {"class": "prodItem"})
            for wine in wines:
                # This checks that the wine isn't a gift set or collection
                if not wine.find("div", {"class": "prodItemInfo_origin js-is-hidden"}):
                    # Gets the line of text for the name and vintage
                    wine_text = wine.find("span", {"class": "prodItemInfo_name"}).text.replace("/", " ")

                    # Gets the id from wine.com for later use
                    wine_id = int(wine.find("meta")['content'])

                    # Gets the wine vintage, if no vintage, sets 0 which will output 'NV' later on
                    wine_vintage = wine_text[-4:]
                    try:
                        wine_vintage = int(wine_vintage)
                    except ValueError:
                        wine_vintage = 0

                    # If there is a vintage for the wine, removes the vintage from the end.
                    # Else the wine name is the wine_text
                    if wine_vintage is not 0:
                        wine_name = wine_text[:-4].strip()
                    else:
                        wine_name = wine_text

                    # Gets all of the ratings for the wine
                    try:
                        wine_ratings_elements = wine.find_all("li", {"class": "wineRatings_listItem"})
                        wine_ratings = []
                        for wre in wine_ratings_elements:
                            wine_ratings.append(wre['title'])
                    except Exception as e:
                        print("No ratings to add:", e)

                    # Get the varietal/blend
                    wine_varietal_text = wine.find("span", {"class": "prodItemInfo_varietal"}).text

                    # Gets the color of the wine: Red, White, or Champagne/Sparkling
                    try:
                        wine_color = wine.find("li", {"class": "prodAttr_icon"})['title']
                    except TypeError:
                        # This stores the wine varietal from the data as there is now attrs 'title' for these
                        if (wine_varietal_text == "Other Dessert") or \
                                (wine_varietal_text == "Sherry") or \
                                (wine_varietal_text == "Port"):
                            wine_color = wine_varietal_text

                    # Creates the path where images will be stored
                    try:
                        if os.path.isdir("./images/" + wine_color.replace(" ", "_")):
                            dir_name = ("./images/" + wine_color.replace(" ", "_") + "/" +
                                        wine_varietal_text.replace(" ", "_").replace("/", "_"))
                        else:
                            dir_name = ("./images/Other/" + wine_varietal_text.replace(" ", "_").replace("/", "_"))
                        # If that dir does not exist, creates it.
                        if not os.path.isdir(dir_name):
                            os.mkdir(dir_name)
                    except FileNotFoundError as e:
                        print(color.FAIL + color.BOLD + e + color.ENDC)

                    wine_origin = wine.find("span", {"class": "prodItemInfo_originText"}).text
                    wine_img_element = wine.find('img')
                    wine_img = ("https://www.wine.com/" + wine_img_element.get('src'))
                    wine_img_path = (dir_name + "/" + wine_text.replace(" ", "_") + "_label.jpg")
                    urllib.request.urlretrieve(wine_img, wine_img_path)

                    try:
                        wine = {"name": wine_name,
                                "wine_id": wine_id,
                                "vintage": wine_vintage,
                                "ratings": wine_ratings,
                                "varietal": wine_varietal_text,
                                "color": wine_color,
                                "origin": wine_origin,
                                "label_image": wine_img_path}
                        rec_id = collection.insert_one(wine).inserted_id
                        print(color.OKGREEN, color.BOLD, "Data inserted with record id", rec_id, color.ENDC)
                    except KeyError as e:
                        print(color.OKBLUE, "Duplicate key: ", e.args[0], color.ENDC)
                    except Exception as e:
                        print(color.FAIL, color.BOLD, "Error: ", e, color.ENDC)
                else:
                    continue

            page_number += 1

            # Writes the latest URL to the save file
            print("Page number:", page_number)
            address = main_url + "/" + str(page_number)
            with open('page_progress.txt', 'w') as file:
                file.write(address)

            # Gets the latest db entry count
            print("There are currently", collection.count(), "wines in the MongoDB database.")

            # Sets random delay in seconds so as not to overload the server
            seconds = random.randrange(45, 60)
            if page_number % 3 is 0:
                seconds += 30
            print("Paused for", seconds, "seconds.")
            time.sleep(seconds)

            # Gets status code.
            code = requests.head(address).status_code
            if code != 200:
                print("Status code:", code)

    # Intended to add to both dbs
    # May deprecate
    else:
        # TODO

        # Sets random delay in seconds so as not to overload the server
        seconds = random.randrange(45, 60)
        if page_number % 3 is 0:
            seconds += 30
        print("Paused for", seconds, "seconds.")
        time.sleep(seconds)

        # Gets status code.
        code = requests.head(address).status_code
        if code != 200:
            print("Status code:", code)


service_function()
