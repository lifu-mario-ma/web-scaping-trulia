#Import necessay libraries
import requests
import time
from bs4 import BeautifulSoup
import re
import json
import pymongo
import pandas as pd
import os
import base64
import http.client, urllib.parse
from dotenv import dotenv_values


def neighbourhood_process_request(response_type_is_xml, latitude, longitude, level_hint):
    api_url = f"place/bylocation?latitude={latitude}&longitude={longitude}&levelHint={level_hint}"
    headers = {AUTH_HEADER: accessToken}
    headers["Accept"] = "application/json"
    page = requests.get(PRECISELY_API_URL + api_url, headers=headers)
    # print(PRECISELY_API_URL + api_url)
    # print(page.status_code)
    if page.status_code == 200:
        doc = BeautifulSoup(page.content, 'html.parser')
        json_dict = json.loads(str(doc))
        # Fetch latitude and longitude
        if json_dict:
            neighborhood = json_dict['location'][0]['place']['name'][0]['value']

        else:
            neighborhood = " "

    else:
        neighborhood = " "
    return neighborhood


def acquire_auth_token():
    #Enter your API key and secret key by signing on this website "https://www.precisely.com/product/precisely-addresses/precisely-addresses"
    API_KEY2 = "qWZSyAT0zqaFpKZR7AeQvWNyeGiD2rXu"
    SECRET = "poAdOkh8n53mSQtH"
    auth_header = BASIC + base64.b64encode(f"{API_KEY2}:{SECRET}".encode()).decode()
    client = requests.Session()
    headers = {AUTH_HEADER: auth_header}
    form_data = {GRANT_TYPE: CLIENT_CREDENTIALS}
    response = client.post(OAUTH2_TOKEN_URL, headers=headers, data=form_data)
    json_response = response.content.decode()
    access_token = json.loads(json_response)[ACCESS_TOKEN]
    global accessToken
    accessToken = BEARER + access_token

def load_and_store_contents(url):
    states = ["CA"]
    cities = ["San_Francisco", "Los_Angeles"]
    for state in states:
        for city in cities:
            for i in range(1, 19):
                new_url = url + state + "/" + city + "/" + str(i) + "_p/"
                print(new_url)
                page = requests.get(new_url, headers=headers)
                # Loading the content of page.
                with open("trulia" + city + str(i) + ".htm", "w", encoding="utf-8") as f:
                    #write contents to file
                    f.write(page.text)
                    time.sleep(10)

def create_collection():
    config = dotenv_values(".env")
    client = pymongo.MongoClient(config["ATLAS_URI"])
    #Create db
    mydb = client["trulia_db"]
    #Create collection
    mycol = mydb["trulia_col"]
    return(mycol)

def merged_neighbourhood():
    #Read existing houses file
    house = pd.read_excel("houses_final.xlsx")
    #Read demogrpahy file
    med_demo = pd.read_excel("demography_LA&SF.xlsx")
    house['zip_code']=house['zip_code'].astype(str)
    med_demo['zip_code']=med_demo['zip_code'].astype(str)

    #Merge both codes
    meg_house = pd.merge(house, med_demo, on="zip_code")
    #Calculate avg price per neighbourhood
    grouped_mean = meg_house.groupby("neighbourhood")["price"].mean()
    avg_price = pd.DataFrame(grouped_mean)
    avg_price = avg_price.rename(columns={"price": "avg_price_per_neighborhood"})
    meg_house = pd.merge(meg_house, avg_price, on="neighbourhood")
    #Calculate house payment for one year
    i = 0.04125
    ratio = 0.2
    year = 30

    def year_payment(price):
        house_payment_1yr = round((price * (1 - ratio) * i * (1 + i) ** year) / ((1 + i) ** year - 1), 2)
        return (house_payment_1yr)
    #calcualte ratio of house payment over median income
    meg_house["house_payment_1yr"] = meg_house["price"].apply(year_payment)
    meg_house["house_payment-income ratio"] = round(meg_house["house_payment_1yr"] / meg_house["Median Income"], 2)
    #Update the data in xlsx
    meg_house.to_excel("merged_house.xlsx", index=False)

def update_db(mycol):
        df = pd.read_excel('merged_house.xlsx')

        # Loop through each row in the DataFrame and update MongoDB collection
        for index, row in df.iterrows():
            # Find document in MongoDB with matching value in the specified column
            document = mycol.find_one({'address': row['address']})

            # If document exists, update it with all columns from the Excel file
            if document:
                mycol.update_one({'_id': document['_id']}, {'$set': row.to_dict()})


def main(mycol):
    #Intialize necessary variables
    acquire_auth_token()
    global rel_path
    global latitude
    global longitude
    rel_path = os.path.dirname(__file__)
    house_list=[]
    neigh = ""
    sqft_value=""
    states = ["CA"]
    cities = ["San_Francisco", "Los_Angeles"]
    for state in states:
        for city in cities:
            for i in range(1,18):
                time.sleep(10)
                try:
                    with open(f"trulia"+city+str(i)+".htm", "r", encoding="utf-8") as f:
                        #Read content of page
                        contents = f.read()
                        #Load bs4 object
                        soup2 = BeautifulSoup(contents, "html.parser")
                        #Read all hosue cards
                        #It will read 7 house cards due to bot limitations but the
                        # with downloaded files(downloaded before limitations added) we can fetch 40 house cards
                        house_card = soup2.find_all("div", attrs={"data-testid": "home-card-sale"})
                        for card in house_card:
                            #fetch price
                            property_price = card.find("div", attrs={"data-testid": "property-price"}).text
                            if(property_price):
                                #use regex to fetch integer value
                                property_price = (re.sub("([\$])([\d]+)([,])([\d]+)(,?)(\d+)", r'\2\4\6', property_price))
                                property_price = re.sub("(\d+)(\+?)", r'\1', property_price)
                                price=int(property_price)
                            else:
                                price=""
                            #Fetch no of beds
                            no_of_beds = card.find("div", attrs={"data-testid": "property-beds"})
                            if (no_of_beds):
                                no_of_beds = no_of_beds.text
                                #Use regex to fetch intger value
                                bed_room = re.sub("(\d+)(bd)", r'\1', no_of_beds)
                                if(bed_room=="Studio"):
                                    bed_room=0
                                else:
                                    bed_room=int(bed_room)
                            else:
                                bed_room = ""
                            #Fetch no of bath rooms
                            no_of_bath = card.find("div", attrs={"data-testid": "property-baths"})
                            if (no_of_bath):
                                no_of_bath = no_of_bath.text
                                # Use regex to fetch intger value
                                bath_room = re.sub("(\d+)(ba)", r'\1', no_of_bath)
                                bath_room=int(bath_room)
                            else:
                                bath_room = 0

                            #Fetch sqft
                            no_of_sqft = card.find("div", attrs={"data-testid": "property-floorSpace"})
                            if (no_of_sqft):
                                no_of_sqft = no_of_sqft.text
                                # Use regex to fetch intger value
                                sqft=re.sub(r"(\d+)(,?)(\d+)(.*)",r"\1\3",no_of_sqft)
                                sqft_value=int(sqft)
                            else:
                                sqft_value=0

                            #Fetch address
                            address = card.find("div", attrs={"data-testid": "property-address"}).text
                            address = re.sub("(\d+)(ba)", r'\1', address)
                            if address !="":
                                #Fetch zip code from address using regex
                                zip_code = re.sub("([a-zA-Z0-9\s,]+)(\#?)([a-zA-Z0-9,\s]+)(CA )(\d+)", r"\5", address)
                            else:
                                zip_code=""
                            #Code to fetch latitude and longitude using address
                            MAX_RETRIES = 3
                            retry_count = 0
                            while retry_count < MAX_RETRIES:
                                try:
                                    if address != "":
                                        params = urllib.parse.urlencode({
                                            "access_key": API_KEY,
                                            "query": address,
                                            "region": "California",
                                            "limit": 1})
                                        conn.request('GET', '/v1/forward?{}'.format(params))
                                        res = conn.getresponse()
                                        result = res.read()
                                        res_data = result.decode('utf-8')
                                        if res.code == 200:
                                            json_data = json.loads(res_data)
                                            # Fetch latitude and longitude
                                            if json_data["data"]:
                                                latitude = json_data["data"][0]["latitude"]
                                                longitude = json_data["data"][0]["longitude"]
                                            else:
                                                latitude = ""
                                                longitude = ""
                                            break
                                        else:
                                            latitude = ""
                                            longitude = ""
                                            break  # this break statement may not be necessary here

                                except Exception as e:
                                    print("Error occurred: ", e)
                                    retry_count += 1
                                    print("Retrying after 5 seconds...")
                                    time.sleep(5)
                            else:
                                print("Maximum retries exceeded. Could not fetch data.")
                                latitude = ""
                                longitude = ""

                            if(latitude and longitude):
                                neigh = neighbourhood_process_request(False, latitude, longitude, "6")

                            house_list.append({ "address":address,
                                                "city":city,
                                                "zip_code":zip_code,
                                                "latitude": latitude,
                                                "longitude": longitude,
                                                "price": price,
                                                "bed_room" : bed_room,
                                                "bath_room": bath_room,
                                                 "sqft" :sqft_value,
                                                 "neighbourhood":neigh
                                                 })
                            housedict = {      "address":address,
                                                "city":city,
                                                "zip_code":zip_code,
                                                "latitude": latitude,
                                                "longitude": longitude,
                                                "price": price,
                                                "bed_room" : bed_room,
                                                "bath_room": bath_room,
                                                 "sqft" :sqft_value,
                                                 "neighbourhood":neigh
                                      }
                            #Insert data into db
                            mycol.insert_one(housedict)
                            # Create a pandas DataFrame from the list of dictionaries
                        house_df = pd.DataFrame(house_list)
                        # Write the DataFrame to an Excel file
                        house_df.to_excel("houses_final.xlsx", index=False)
                except FileNotFoundError:
                    continue

if __name__ == "__main__":
    # Setting variables
    url = "https://www.trulia.com/"
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }

    #API deatils to fetch latitude and longitude
    #Enter your API key by signing on this website https://positionstack.com
    API_KEY = "<<APILEY>>"
    BASE_URL = "http://api.positionstack.com/v1/forward"
    conn = http.client.HTTPConnection('api.positionstack.com')

    #Api details to fetch neigbourhood
    OAUTH2_TOKEN_URL = "https://api.precisely.com/oauth/token"
    ACCESS_TOKEN = "access_token"
    BEARER = "Bearer "
    BASIC = "Basic "
    CLIENT_CREDENTIALS = "client_credentials"
    GRANT_TYPE = "grant_type"
    AUTH_HEADER = "Authorization"
    COLON = ":"
    API_FRAGMENT = "neighborhoods/v1/"
    PRECISELY_API_URL = "https://api.precisely.com/" + API_FRAGMENT

    #Load and store contents from site
    load_and_store_contents(url)
    # #Create a mongodb collection
    collection=create_collection()
    # #Create DB collection
    main(collection)
    #Merge Data of neighbourhood
    merged_neighbourhood()
    #update in Mongo Db
    update_db(collection)