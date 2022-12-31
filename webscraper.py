from bs4 import BeautifulSoup
from celery import Celery
import requests
import datetime

app = Celery("scraper", broker="redis://localhost:9090")


@app.task()
def scrape(url, elements=None):
    """
    Scrape the given URL and return the results.
    """
    r = requests.get(url)  # get the response from the server

    soup = BeautifulSoup(r.text, "html.parser")

    scraped_data = {}

    # get the specific element from the html
    k = []
    v = []
    if elements is not None:
        for element in elements:
            # grabs all the data from the element
            fetched_data = soup.find_all(element)

            if fetched_data is not None:
                # extract the Gym name
                name = [_data.get_text().strip(
                ) for _data in fetched_data if _data.get_text().strip().startswith(title)]
                if len(name) > 0:
                    k = name

                # get the percentages of people
                percentage = [_data.get_text().strip(
                ) for _data in fetched_data if _data.get_text().strip().endswith(num)]
                if len(percentage) > 0:
                    v = percentage

    scraped_data = dict(zip(k, v))
    print(scraped_data)


url = "https://www.lemongym.ee/en/club-vacancy"
elements = ['h3', 'h1']
title = "Lemon"
num = "%"

data = scrape(url, elements)
date_time = datetime.datetime.now()
# format to YY:MM:DD HH:MM:SS
date_time = date_time.strftime("%Y-%m-%d %I:%M:%S")

print(date_time)
