# A web scraper specifically for wine.com
# Built by Wes Garbee

import os
import time
import random
import urllib.request
import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup as bSoup


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
        file.write('1')

with open('page_progress.txt', 'r') as file:
    data = file.read()

# Prompts user to enter the wine.com url to scrape
address = input("Enter wine.com URL: ").strip()
# Increments the page number to scrape
page_number = int(data)

req = requests.head(address)
code = req.status_code

while code is 200:
    page_to_pull = address + "/" + str(page_number)

    req = requests.get(page_to_pull)

    soup = bSoup(req.text, "html.parser")
    wines = soup.findAll("li", {"class": "prodItem"})

    for wine in wines:
        # This checks that the wine isn't a gift set or collection
        if not wine.find("div", {"class": "prodItemInfo_origin js-is-hidden"}):
            # Gets the line of text for the name and vintage
            wine_text = wine.find("span", {"class": "prodItemInfo_name"}).text

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
            wine_ratings_elements = wine.find_all("li", {"class": "wineRatings_listItem"})
            wine_ratings = []
            for wre in wine_ratings_elements:
                wine_ratings.append(wre['title'])

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

            try:
                # Path name where the images for this search will be stored
                dir_name = ("./images/" + wine_color.replace(" ", "_") + "/" +
                            wine_varietal_text.replace(" ", "_").replace("/", "_"))
                # If that dir does not exist, creates it.
                if not os.path.isdir(dir_name):
                    os.mkdir(dir_name)
            except FileNotFoundError as e:
                print(e)

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
                print("Data inserted with record id", rec_id)
            except Exception as e:
                print("Error: ", e)
        else:
            continue

    page_number += 1
    print("Page number:", page_number)

    with open('page_progress.txt', 'w') as file:
        file.write(str(page_number))

    # # Sets random delay in seconds so as not to overload the server
    seconds = random.randrange(45, 60)
    time.sleep(seconds)

    # Gets status code.
    code = requests.head(address).status_code
    if code != 200:
        print("Status code:", code)
