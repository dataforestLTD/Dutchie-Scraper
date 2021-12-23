"""
   ___ _____                _       
  / _ \\_   \     _ __ __ _| |_ ___ 
 / /_)/ / /\/____| '__/ _` | __/ _ \
/ ___/\/ /_|_____| | | (_| | ||  __/
\/   \____/      |_|  \__,_|\__\___|
                                    
              _       _ _           
           __| | __ _(_) |_   _     
          / _` |/ _` | | | | | |    
         | (_| | (_| | | | |_| |    
          \__,_|\__,_|_|_|\__, |    
                          |___/ 
                                              _..  
                                          .qd$$$$bp.
                                        .q$$$$DF$$$$m.
                                       .$$$$L$T$D$$$$$
                                     .q$$$$$$$$$$$$$$$$
                                    .$$$$$$$$$$$$P\$$$$;
                                  .q$$$$$$$$$P^"_.`;$$$$
                                 q$$$$$$$P;\   ,  /$$$$P
                               .$$$P^::Y$/`  _  .:.$$$/
                              .P.:..    \ `._.-:.. \$P
                              $':.  __.. :   :..    :'
                             /:_..::.   `. .:.    .'|
                           _::..          T:..   /  :
                        .::..             J:..  :  :
                     .::..          7:..   F:.. :  ;
                 _.::..             |:..   J:.. `./
            _..:::..               /J:..    F:.  : 
          .::::..                .T  \:..   J:.  /
         /:::...               .' `.  \:..   F_o'
        .:::...              .'     \  \:..  J ;
        ::::...           .-'`.    _.`._\:..  \:
        ':::...         .'  `._7.-'_.-  `\:.   \.
         \:::...   _..-'__.._/_.--' ,:.   b:.   \._ 
          `::::..-"_.'-"_..--"      :..   /):.   `.\   
            `-:/"-7.--""            _::.-'P::..    \} 
 _....------            _..--".-'   \::..     `. 
(::..              _...----  _.-'       `---:..  `-.
 \::..      _.-""" "   `" """---""             `::...___)
  `\:._.-  tread softly, because you tread on my dreams
  '''                                                                                                                           
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
        dispensary_ids.append(
            "https://dutchie.com/graphql?operationName=FilteredProducts&variables=%7B%22productsFilter%22%3A%7B%22dispensaryId%22%3A%22"
            + str(id[0])
            + '"%2C"bypassOnlineThresholds"%3Atrue%7D%2C"useCache"%3Afalse%7D&extensions=%7B"persistedQuery"%3A%7B"version"%3A1%2C"sha256Hash"%3A"'
            + hash_2
            + '"%7D%7D'
        )

    print(str(len(dispos)) + " stores ready to scrape")


def scrape_stores():
    print("scraping")
    h2_er = []
    db_er = []
    price_info = []
    item_info = []
    start = len(dispensary_ids)
    current = 0
    items_scraped = 0
    reqs = (grequests.get(link) for link in dispensary_ids)
    resp = grequests.imap(reqs, grequests.Pool(128))

    for r in resp:
        current += 1
        # print(f"{current}/{start} stores scraped", end="\r")
        products = []
        brands = []
        product_types = []
        strain_types = []

        keep_going = True
        attempts = 0

        while keep_going == True:
            try:

                soup = BeautifulSoup(r.text, "lxml")
                parsed = json.loads(soup.text)
                products = parsed.get("data").get("filteredProducts").get("products")
                keep_going = False
            except Exception as e:
                attempts += 1
                if attempts >= 10:
                    keep_going = False
                    h2_er.append(e)
                    print("Store Error, check Hash 2")
                    print(e)

            time_string = str(datetime.now())

        for product in products:
            try:
                name = str(product.get("Name")).encode("ascii", "ignore").decode()
                price_list = product.get("Prices")
                med_price_list = product.get("medicalPrices")
                rec_price_list = product.get("recPrices")
                brand = str(product.get("brandName")).encode("ascii", "ignore").decode()
                options = product.get("Options")
                strain_type = (
                    str(product.get("strainType")).encode("ascii", "ignore").decode()
                )
                type_ = str(product.get("type")).encode("ascii", "ignore").decode()
                d_id = (
                    str(product.get("DispensaryID")).encode("ascii", "ignore").decode()
                )
                product_id = str(product.get("id")).encode("ascii", "ignore").decode()
                prices = (
                    str(list(zip(options, price_list)))
                    .encode("ascii", "ignore")
                    .decode()
                )
                rec_prices = (
                    str(list(zip(options, rec_price_list)))
                    .encode("ascii", "ignore")
                    .decode()
                )
                med_prices = (
                    str(list(zip(options, med_price_list)))
                    .encode("ascii", "ignore")
                    .decode()
                )

                try:
                    threshold = int(product.get("isBelowThreshold"))
                    kiosk_threshold = int(product.get("isBelowKioskThreshold"))
                except:
                    threshold = 3
                    kiosk_threshold = 3
                try:
                    thc = str(
                        product.get("THCContent").get("range")
                        + " "
                        + str(product.get("THCContent").get("unit"))
                    )
                except:
                    thc = "not available"
                try:
                    quantity = (
                        str(product.get("POSMetaData").get("children"))
                        .encode("ascii", "ignore")
                        .decode()
                    )
                except:
                    quantity = "None Found"
                try:
                    man_quantity = (
                        str(product.get("manualInventory"))
                        .encode("ascii", "ignore")
                        .decode()
                    )
                except:
                    man_quantity = "None Found"

                item_info.append(
                    [
                        product_id,
                        name,
                        brand,
                        type_,
                        strain_type,
                        thc,
                        d_id,
                        prices,
                        rec_prices,
                        med_prices,
                    ]
                )
                price_info.append(
                    [
                        product_id,
                        str(options),
                        prices,
                        rec_prices,
                        med_prices,
                        quantity,
                        man_quantity,
                        threshold,
                        kiosk_threshold,
                        time_string,
                    ]
                )
                items_scraped += 1
                print(
                    f"{current}/{start} stores scraped, {items_scraped} items scraped",
                    end="\r",
                )

            except Exception as e:
                print("scrape error")
                print(e)

        if len(item_info) >= 50000:
            try:
                sql_1 = "INSERT INTO items (p_id, i_name, i_brand_name, i_type, i_strain_type, i_thc, d_id, i_current_prices, i_current_rec_prices, i_current_med_prices) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE p_id=Values(p_id), i_name=Values(i_name), i_brand_name=Values(i_brand_name), i_type=Values(i_type), i_strain_type=Values(i_strain_type), i_thc=Values(i_thc), d_id=Values(d_id), i_current_prices=Values(i_current_prices), i_current_rec_prices=Values(i_current_rec_prices), i_current_med_prices=Values(i_current_med_prices)"
                c.executemany(sql_1, item_info)
                db.commit()
                sql_2 = "INSERT INTO price_history (p_id, weights, prices, rec_prices, med_prices, inventory, man_inventory, threshold, kiosk_threshold, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                c.executemany(sql_2, price_info)
                db.commit()
                price_info = []
                item_info = []
            except Exception as e2:
                db_er.append(e2)
                print("db error")
                print(e2)
    try:
        sql_1 = "INSERT INTO items (p_id, i_name, i_brand_name, i_type, i_strain_type, i_thc, d_id, i_current_prices, i_current_rec_prices, i_current_med_prices) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE p_id=Values(p_id), i_name=Values(i_name), i_brand_name=Values(i_brand_name), i_type=Values(i_type), i_strain_type=Values(i_strain_type), i_thc=Values(i_thc), d_id=Values(d_id), i_current_prices=Values(i_current_prices), i_current_rec_prices=Values(i_current_rec_prices), i_current_med_prices=Values(i_current_med_prices)"
        c.executemany(sql_1, item_info)
        db.commit()
        sql_2 = "INSERT INTO price_history (p_id, weights, prices, rec_prices, med_prices, inventory, man_inventory, threshold, kiosk_threshold, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        c.executemany(sql_2, price_info)
        db.commit()

    except Exception as e2:
        db_er.append(e2)
        print("db error")
        print(e2)

    email(h2_er, db_er)


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
    sent_from = "Daily D-Boi Report"
    to = "" # Send to address
    if len(hash_errors) <= 50 and len(db_errors) <= 50:
        subject = "Scrape Results Normal"
        body = (
            "Scrape results normal. \n HASH ERRORS \n\n"
            + str(hash_errors)
            + "DB ERRORS \n\n"
            + str(db_errors)
        )
    else:
        subject = "Huston, we have a problem."
        body = (
            "Huston we have a problem. \n HASH ERRORS \n\n"
            + str(hash_errors)
            + "DB ERRORS \n\n"
            + str(db_errors)
        )
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

    print(str(datetime.now()))
    database("connect")
    db_ids()
    scrape_stores()
    database("disconnect")
    print(str(datetime.now()))


### set initial variables ###

# store menu hash, basic flower search works

hash_2 = "524b25c69553267798a34b10b2175aa111551accc81d932a96f2bf996ca3a469"

logger = logging.Logger("catch_all")
dispensary_ids = []


### run code ###

run()
