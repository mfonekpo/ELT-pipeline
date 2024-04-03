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


base_url = "https://hotels.ng"

names = []
locations = []
categories = []
descriptions = []
likes = []
urls = []


@task(
    name="fetch_page",
    log_prints=True,
    retries=3
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1)
)
def fetch_page(url, page):
    logger = get_run_logger()
    logger.info("Fetching Site Data")
    try:
        response = requests.get(f"{url}{page}")
        response.raise_for_status()
        soup = bs(response.text, "html.parser")
        return soup
    except Exception as e:
        logger.error(e)
        raise

@task(
    name="parse_names",
    log_prints=True,
    retries=3
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1)
)
def parse_names(soup, name_tag:str):
    logger = get_run_logger()
    logger.info("parsing phase started >> Parsing Names")
    try:
        for name in soup.select(name_tag):
            names.append(name.text.strip())
        return names
    except Exception as e:
        logger.error(e)
        raise

@task(
    name="parse_locations",
    log_prints=True,
    retries=3
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(minutes=20)
)
def parse_locations(soup, location_tag:str):
    logger = get_run_logger()
    logger.info("Parsing Locations from the website")
    try:
        tag_name, tag_attrs = location_tag
        for loc in soup.find_all(tag_name, **tag_attrs):
            locations.append(loc.find_all("p")[1].text.strip())
        return locations
    except Exception as e:
        logger.error(e)
        raise

@task(
    name="parse_categories",
    log_prints=True,
    retries=3
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1)
)
def parse_categories(soup, category_tag:str):
    logger = get_run_logger()
    logger.info("Parsing Categories from the website")
    try:
        tag_name, tag_attrs = category_tag
        for cat in soup.find_all(tag_name, **tag_attrs):
            categories.append(cat.find_all("p")[0].text.strip())
        return categories
    except Exception as e:
        logger.error(e)
        raise

@task(
    name="parse_descriptions",
    log_prints=True,
    retries=3
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1)
)
def parse_descriptions(soup, description_tag:str):
    logger = get_run_logger()
    logger.info("Parsing Descriptions from the website")
    try:
        tag_name, tag_attrs = description_tag
        for desc in soup.find_all(tag_name, **tag_attrs):
            descriptions.append(desc.text.strip().replace("\n", "").replace("\r", ""))
        return descriptions
    except Exception as e:
        logger.error(e)
        raise

@task(
    name="parse_likes",
    log_prints=True,
    retries=3
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1)
)
def parse_likes(soup, like_tag:str):
    logger = get_run_logger()
    logger.info("Parsing Likes from the website")
    try:
        tag_name, tag_attrs = like_tag
        for like in soup.find_all(tag_name, **tag_attrs):
            likes.append(like.find_all("span")[1].text.strip())
        return likes
    except Exception as e:
        logger.error(e)
        raise

@task(
    name="parse_urls",
    log_prints=True,
    retries=3
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1)
)
def parse_urls(soup, url_tag:str):
    logger = get_run_logger()
    logger.info("Parsing urls from the website")
    try:
        tag_name, tag_attrs = url_tag
        for url in soup.find_all(tag_name, **tag_attrs):
            urls.append(base_url+url.find_all("a")[0]["href"])
        return urls
    except Exception as e:
        logger.error(e)
        urls.append(None)


@task(
    name="add_data_to_dict",
    log_prints=True,
    retries=3
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1)
)
def add_data_to_dict(names, locations, categories, descriptions, likes, urls):
    logger = get_run_logger()
    logger.info(" performing data aggregate")
    try:
        dict_data = {
            "name_of_sight": names,
            "location": locations,
            "category": categories,
            "description": descriptions,
            "likes_count": likes,
            "location_url": urls
        }
        logger.info("Data aggregated successfully")
        return dict_data
    except Exception as e:
        logger.error(e)
        raise


@task(
    name="load_to_db",
    log_prints=True,
    retries=3
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1)
)
def load_to_db(dict_data):
    logger = get_run_logger()
    try:
        csv_path = "./datafiles/data.db"
        dir_name = os.path.dirname(csv_path)

        # Create the directory if it doesn't exist
        if not os.path.exists(dir_name):
            logger.info(f"Creating directory: {dir_name}")
            os.makedirs(dir_name)

        logger.info("Loading data to database")
        conn = sqlite3.connect("./datafiles/data.db")
        df = pd.DataFrame(dict_data)
        df.to_sql("data", conn, if_exists="replace", index = False)
        conn.close()
        logger.info("Data loaded successfully")
    except Exception as e:
        # logger.error(f"Failed to load data to database {e}")
        logger.error(e)
        raise



@task(
    name="load_to_csv",
    log_prints=True,
    retries=3
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1)
)
def load_to_csv(dict_data):
    logger = get_run_logger()
    print(f"environment_directory: {os.getcwd()}")
    print(f"List directories from current directory: {os.listdir()}")
    print(f"list of directories one level up: {os.listdir('../')}")

    csv_path = "./datafiles/data.csv"
    dir_name = os.path.dirname(csv_path)

    # Create the directory if it doesn't exist
    if not os.path.exists(dir_name):
        logger.info(f"Creating directory: {dir_name}")
        os.makedirs("./datafiles/data.csv")
    logger.info("Loading data to csv")
    df = pd.DataFrame(dict_data)
    df.to_csv(dir_name, index = False)
    logger.info("Data loaded successfully")
    return df



@flow(name = "Interesting places extraction pipeline", log_prints=True)
def main_flow(url = "https://hotels.ng/places/nigeria-1/", pages = 3):
    # logger = get_run_logger()
    name_tag = "a > h2"
    location_tag = ("div", {"class":"category-de01"})
    category_tag = "div", {"class":"category-de01"}
    description_tag = "p", {"class":"sub-details"}
    like_tag = "div", {"class":"head_right"}
    url_tag = "div", {"class":"head_left"}


    for page in range(1, pages+1):
        print(f"Extracting data from page{page}")
        soup = fetch_page(url, page)
        names = parse_names(soup, name_tag)
        locations = parse_locations(soup, location_tag)
        categories = parse_categories(soup, category_tag)
        descriptions = parse_descriptions(soup, description_tag)
        likes = parse_likes(soup, like_tag)
        urls = parse_urls(soup, url_tag, return_state=True)
        dict_data = add_data_to_dict(names, locations, categories, descriptions, likes, urls)
        load_to_csv(dict_data)
        load_to_db(dict_data)


    return dict_data


if __name__ == "__main__":
    main_flow()