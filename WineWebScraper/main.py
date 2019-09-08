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

# Increments the page number to scrape
page_number = 1

# Prompts user to enter the wine.com url to scrape
address = input("Enter wine.com URL: ").strip()
req = requests.head(address)
code = req.status_code

while code is 200:
    page_to_pull = address + "/" + str(page_number)

    req = requests.get(page_to_pull)

    soup = bSoup(req.text, "html.parser")
    wines = soup.findAll("li", {"class": "prodItem"})

    for wine in wines:
        wine_text = wine.find("span", {"class": "prodItemInfo_name"}).text
        wine_id = wine.find("meta")['content']
        wine_vintage = wine_text[-4:]
        wine_varietal_text = wine.find("span", {"class": "prodItemInfo_varietal"}).text

        try:
            # Path name where the images for this search will be stored
            dir_name = ("./images/" + wine_varietal_text.replace(" ", "_").replace("/", "_").lower())
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
            wine = {"name": wine_text[:-4].strip(),
                    "wine_id": wine_id,
                    "vintage": wine_vintage,
                    "varietal": wine_varietal_text,
                    "origin": wine_origin,
                    "label_image": wine_img_path}
            rec_id = collection.insert_one(wine).inserted_id
            print("Data inserted with record id", rec_id)
        except Exception as e:
            print("Error: ", e)

    page_number += 1

    # # Sets delay between 90 and 120 seconds so as not to overload the server
    seconds = random.randrange(90, 120)
    time.sleep(seconds)

    print(seconds)

    # Sets delay between 90 and 120 seconds so as not to overload the server
    # seconds = random.randrange(1, 5)
    # time.sleep(seconds)

    # Gets status code.
    code = requests.head(address).status_code
    if code != 200:
        print("Status code:", code)
