from bs4 import BeautifulSoup as bs
from prefect import task, flow, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import requests
import sqlite3
import os
import pandas as pd
import numpy as np
import re
import datetime

pages = 1
name_of_place = []
location = []
category = []
number_likes = []
description = []
urls = []


base_url = "https://hotels.ng"

url = f"https://hotels.ng/places/nigeria-1/{pages}"



@task(  name="extract_task",
        log_prints=True, retries=3, cache_key_fn=task_input_hash,
        cache_expiration=timedelta(days=1)
)
def extract(url: str=url):
    global pages
    logger = get_run_logger()
    logger.info("Extraction Phase Started")

    for page in range(3):
        response = requests.get(url).text
        soup = bs(response, "html.parser")
        # response.raise_for_status()

        print(f"Extracting data from page{pages}")
        logger.info(f"Extraction for {pages} initiated...")

        name_tags = soup.select("a > h2")
        for i in name_tags:
            places = i.text.strip()
            name_of_place.append(places)
        print("fininshed populating the names")
        logger.info(f"fininshed extracting the names")


        location_tags = soup.find_all("div", {"class":"category-de01"})
        for loc in location_tags:
            location_p_tags = loc.find_all("p")
            locations = location_p_tags[1].text.strip()
            location.append(locations)
        logger.info("Finished extracting the locations")


        categories_placeholder = soup.findAll("div", {"class":"category-de01"})
        for categories in categories_placeholder:
            category_tags = categories.find_all("p")
            category_label = category_tags[0].text.strip()
            category.append(category_label)
        logger.info("finished extracting the categories")

        description_tag = soup.find_all("p", {"class":"sub-details"})
        for desc in description_tag:
            descriptions = desc.text.strip()
            descriptions = descriptions.replace("\n", "")
            descriptions = descriptions.replace("\r", "")
            description.append(descriptions.strip())
        logger.info("finished extracting the descriptions")


        # #scraping data for the number of likes
        likes_tags = soup.find_all("div", {"class":"head_right"})
        for like in likes_tags:
            likes = like.find_all("span")[1].text.strip()
            number_likes.append(likes)
        logger.info("finished extracting the number of likes")

        # #scraping data for the urls of places
        url_tags = soup.find_all("div", {"class":"head_left"})
        for links in url_tags:
            places_url = links.find_all("a")
            places_urls = base_url + places_url[0]["href"]
            # url = []
            urls.append(places_urls)
        logger.info("finished extracting the urls")


        logger.info(f"Extraction for page{pages} done...")
        pages += 1  





@task()
def load_to_csv():
    pass



@task()
def load_to_sql():
    pass



@task()
def transform():
    pass


@flow(name="Interesting places extraction pipeline")
def main_flow():
    extract()


if __name__ == "__main__":
    main_flow()