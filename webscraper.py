from bs4 import BeautifulSoup
from celery import Celery
import requests
import datetime
import csv
import os
from celery.schedules import crontab


app = Celery("webscraper", broker="redis://localhost:6379/0")

#  configure the periodic schedule 
app.conf.beat_schedule = {
"scrape-LemonGym": {
    "task": "webscraper.run_scraper",
    "schedule": crontab(minute='*/30'),
    }}



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

    if len(scraped_data) > 0:
        return scraped_data
    else:
        raise ValueError("No data was scraped")


def save_data(data, time, filename=None):
    """
    Save the data to a CSV.
    """

    if filename is not None:
        # setup the save location and open file
        filename = filename+'.csv'
        file_path = os.path.join(os.getcwd(), filename)

        if os.path.exists(file_path):
            # if file already exists
            with open(file_path, 'a+', newline='') as csvfile:

                writer = csv.DictWriter(csvfile, fieldnames=['Name', 'Percentage', 'Time'])
                
                # Write the data as rows
                for name, amount in data.items():
                    writer.writerow(
                        {'Name': name, 'Percentage': amount.strip('%'), 'Time': time})
        else:
            # create a new file
            with open(file_path, 'w', newline='') as csvfile:
                
                writer = csv.DictWriter(csvfile, fieldnames=['Name', 'Percentage', 'Time'])
                writer.writeheader()  # writes the header colum names

                # Write the data as rows
                for name, amount in data.items():
                    writer.writerow(
                        {'Name': name, 'Percentage': amount.strip('%'), 'Time': time})
    else:
        raise NotImplementedError(
            "Enter a filename to save data.")




@app.task
def run_scraper():
    global title, num

    url = "https://www.lemongym.ee/en/club-vacancy"
    elements = ['h3', 'h1']
    title = "Lemon"
    num = "%"
    success = False
    try:
        data = scrape(url, elements)
        date_time = datetime.datetime.now()
        # format to YY:MM:DD HH:MM:SS
        date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
        # print(data)
        save_data(data, date_time, filename="gymdata")
        success = True
    except Exception as e:
        print(e)
    finally:
        if success:
            return f"Success! Scraped data on {date_time}"
        else:
            return f"Task failed! {date_time}"
    








