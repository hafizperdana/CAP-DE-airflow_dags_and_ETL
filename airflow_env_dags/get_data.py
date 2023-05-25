import requests
from bs4 import BeautifulSoup as bs


def get_data():

    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    result = requests.get(url)
    soap = bs(result.content, 'html.parser')

    for data in soap.findAll('div', {'id':'faq2022'})[0].findAll('ul'):
        link = data.findAll('li')[1].a['href']
        data = requests.get(link)
        name = link.split('/')[-1]
        with open('/home/pablo/data/taxi_data/' + name, 'wb') as file:
            file.write(data.content)

get_data()