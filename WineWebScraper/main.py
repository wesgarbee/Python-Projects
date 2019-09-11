# A web scraper specifically for wine.com
# Built by Wes Garbee

import os
import time
import random
import urllib.request
import requests
from pymongo import MongoClient, errors
from bs4 import BeautifulSoup as bSoup
from Classes.console_colors import Concolors as color


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

# Checks if txt file for saving page exists
if not os.path.exists('page_progress.txt'):
    with open('page_progress.txt', 'w') as file:
        file.write('https://www.wine.com/list/wine/7155/1')

with open('page_progress.txt', 'r') as file:
    data = file.read()

# Prompts user to enter the wine.com url to scrape
address = data
main_url = address.rsplit('/', 1)[0]
# Increments the page number to scrape
page_number = int(data.rsplit('/', 1)[-1])

req = requests.head(address)
code = req.status_code

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
    print("There are currently", collection.count(), "wines in the database.")

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
