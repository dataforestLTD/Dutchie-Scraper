"""
   ___ _____                _       
  / _ \\_   \     _ __ __ _| |_ ___ 
 / /_)/ / /\/____| '__/ _` | __/ _ \
/ ___/\/ /_|_____| | | (_| | ||  __/
\/   \____/      |_|  \__,_|\__\___|
                                    
                       _    _       
    __      _____  ___| | _| |_   _ 
    \ \ /\ / / _ \/ _ \ |/ / | | | |
     \ V  V /  __/  __/   <| | |_| |
      \_/\_/ \___|\___|_|\_\_|\__, |
                              |___/                                                         
"""

### imports ###

from re import S
import grequests
import requests
from bs4 import BeautifulSoup
import json
import mysql.connector
import time
import random
from datetime import datetime
import logging
import pandas as pd
import itertools
import smtplib

### functions ###


def db_ids():

    c.execute("SELECT * FROM dispensaries")
    dispos = c.fetchall()

    for id in dispos:
        d_ids.append(
            "https://dutchie.com/graphql?operationName=ConsumerDispensaries&variables=%7B%22dispensaryFilter%22%3A%7B%22type%22%3A%22Dispensary%22%2C%22activeOnly%22%3Afalse%2C%22cNameOrID%22%3A%22"
            + str(id[0])
            + '%22%2C%22city%22%3A%22Reno%22%2C%22nearLat%22%3A39.5296329%2C%22nearLng%22%3A-119.8138027%2C%22destinationTaxState%22%3A%22NV%22%7D%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash"%3A"'
            + hash_3
            + '"%7D%7D'
        )
    print(str(len(dispos)) + " stores ready to scrape")


def scrape_ids():

    print("scraping IDs")
    keepgoing = True
    fails = 0
    back_door = (
        "https://dutchie.com/graphql?operationName=DispensarySearch&variables=%7B%22dispensaryFilter%22%3A%7B%22type%22%3A%22Dispensary%22%2C%22activeOnly%22%3Atrue%2C%22nearLat%22%3A39.6478653%2C%22nearLng%22%3A-104.9877587%2C%22destinationTaxState%22%3A%22CO%22%2C%22recreational%22%3Atrue%2C%22medical%22%3Atrue%2C%22creditCards%22%3Afalse%2C%22distance%22%3A"
        + str(search_area)
        + '%7D%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash"%3A"'
        + hash_1
        + '"%7D%7D'
    )

    while keepgoing == True:

        try:
            session = requests.Session()
            time.sleep(15)
            response = session.get(back_door)            
            soup = BeautifulSoup(response.text, "lxml")
            parsed = json.loads(soup.text)
            ids = [
                d.get("id")
                for d in parsed["data"]["filteredDispensaries"]
                if d.get("id")
            ]
            print(str(len(ids)) + " ID's added")
            for id in ids:
                d_ids.append(
                    "https://dutchie.com/graphql?operationName=ConsumerDispensaries&variables=%7B%22dispensaryFilter%22%3A%7B%22type%22%3A%22Dispensary%22%2C%22activeOnly%22%3Afalse%2C%22cNameOrID%22%3A%22"
                    + str(id)
                    + '%22%2C%22city%22%3A%22Reno%22%2C%22nearLat%22%3A39.5296329%2C%22nearLng%22%3A-119.8138027%2C%22destinationTaxState%22%3A%22NV%22%7D%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash"%3A"'
                    + hash_3
                    + '"%7D%7D'
                )
            keepgoing = False

        except Exception as e1:
            fails += 1
            time.sleep(random.randrange(1, 5))
            print("trying again")
            print(e1)
            if fails >= 20:
                keepgoing = False
                print("we tried")
                db_ids()


def scrape_dispo_info():

    print("scraping store info")
    h3_er = []
    db_er = []
    reqs = (grequests.get(link) for link in d_ids)
    resp = grequests.imap(reqs, grequests.Pool(128))
    dispo_info = []
    print(len(d_ids))
    current = 1
    start = len(d_ids)

    for r in resp:
        keepgoing = True
        fails = 0

        while keepgoing == True:
            try:
                soup = BeautifulSoup(r.text, "lxml")
                parsed = json.loads(soup.text)
                dispo_id = parsed.get("data").get("filteredDispensaries")[0].get("id")
                dispo_name = (
                    parsed.get("data")
                    .get("filteredDispensaries")[0]
                    .get("name")
                    .encode("ascii", "ignore")
                    .decode()
                )
                dispo_cname = (
                    parsed.get("data")
                    .get("filteredDispensaries")[0]
                    .get("cName")
                    .encode("ascii", "ignore")
                    .decode()
                )
                dispo_st = (
                    (parsed.get("data").get("filteredDispensaries")[0].get("location"))
                    .get("ln1")
                    .encode("ascii", "ignore")
                    .decode()
                )
                dispo_st_2 = (
                    (parsed.get("data").get("filteredDispensaries")[0].get("location"))
                    .get("ln2")
                    .encode("ascii", "ignore")
                    .decode()
                )
                dispo_city = (
                    (parsed.get("data").get("filteredDispensaries")[0].get("location"))
                    .get("city")
                    .encode("ascii", "ignore")
                    .decode()
                )
                dispo_state = (
                    (parsed.get("data").get("filteredDispensaries")[0].get("location"))
                    .get("state")
                    .encode("ascii", "ignore")
                    .decode()
                )
                dispo_country = (
                    (parsed.get("data").get("filteredDispensaries")[0].get("location"))
                    .get("country")
                    .encode("ascii", "ignore")
                    .decode()
                )
                dispo_gps = str(
                    parsed.get("data")
                    .get("filteredDispensaries")[0]
                    .get("location")
                    .get("geometry")
                    .get("coordinates")
                )
                med = int(
                    parsed.get("data")
                    .get("filteredDispensaries")[0]
                    .get("medicalDispensary")
                )
                rec = int(
                    parsed.get("data")
                    .get("filteredDispensaries")[0]
                    .get("recDispensary")
                )
                delivery = int(
                    parsed.get("data")
                    .get("filteredDispensaries")[0]
                    .get("offerDelivery")
                )
                quantity_limit = str(
                    parsed.get("data")
                    .get("filteredDispensaries")[0]
                    .get("storeSettings")
                    .get("quantityLimit")
                )
                dispo_info.append(
                    [
                        dispo_id,
                        dispo_name,
                        dispo_cname,
                        dispo_gps,
                        dispo_st,
                        dispo_st_2,
                        dispo_city,
                        dispo_state,
                        dispo_country,
                        med,
                        rec,
                        delivery,
                        quantity_limit,
                    ]
                )
                current += 1
                print(f"{current}/{start} stores scraped", end="\r")
                keepgoing = False
            except Exception as e:
                fails += 1
                time.sleep(0.1)

                if fails >= 25:
                    print(f"{e}", end="\r")
                    keepgoing = False
                    h3_er.append(e)

    try:
        print("add to db attempt")
        sql2 = "INSERT INTO dispensaries (d_id, name, c_name, gps, street, street_2, city, state, country, med, rec, delivery, quantity_limit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE d_id=Values(d_id), c_name=Values(c_name), gps=Values(gps), street=Values(street), street_2=Values(street_2), city=Values(city), state=Values(state), country=Values(country), med=Values(med), rec=Values(rec), delivery=Values(delivery), quantity_limit=Values(quantity_limit)"
        c.executemany(sql2, dispo_info)
        db.commit()
        print(
            str(len(dispo_info))
            + " items added to database, "
            + str(len(h3_er))
            + " hash 3 errors"
        )
    except Exception as e2:
        db_er.append(e2)
        print(e2)

    email(h3_er, db_er)


def database(command):

    if command == "connect":
        global db
        db = mysql.connector.connect(
            user="",
            password="",
            host="",
            database="dutchie",
        )
        global c
        c = db.cursor()
        print("connected to database")
    if command == "disconnect":
        c.close()
        db.close()
        print("database connection closed")


def email(hash_errors, db_errors):

    gu = "" # Gmail Username
    gp = "" # Gmail Password
    sent_from = "Weekly D-Boi Report"
    to = "" # Send to address
    if len(hash_errors) <= 200 and len(db_errors) <= 50:
        subject = "Scrape Results Normal"
        body = (
            str(len(hash_errors))
            + " hash errors, "
            + str(len(db_errors))
            + " database errors"
        )
    else:
        subject = "Huston, we have a problem."
        body = "HASH ERRORS \n" + str(hash_errors) + "DB ERRORS \n" + str(db_errors)
    email_text = """From: %s\nTo: %s\nSubject: %s\n\n%s""" % (
        sent_from,
        to,
        subject,
        body,
    )
    try:
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.ehlo()
        server.login(gu, gp)
        server.sendmail(sent_from, to, email_text)
        server.close()

    except Exception as e:
        print("email error")
        print(e)


def run():
    database("connect")
    scrape_ids()
    scrape_dispo_info()
    database("disconnect")


### set initial variables ###

# dispensary search hash
hash_1 = "705e9d913c37b5f85ef56e7977d2a8b3c21bccd42e3431a91ed93def1b8adcf7"

# store info hash
hash_3 = "80df31222d3308a44abdb0c9196758af79068b37362b32f258c14926f37cf16e"

search_area = 35000
logger = logging.Logger("catch_all")
# dispensary_ids = []
d_ids = []
ids = []

### run code ###

run()
