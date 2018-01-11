# -*- coding: utf-8 -*-
import sys
sys.path.append("./static/py")
import os
import db
import codecs
import cPickle as pickle
import numpy as np
import common as cm
import constants as CONST
import re
import nltk
import time
import requests
import string
from flask import *
from lxml import html


app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = CONST.UPLOAD_FOLDER
app.secret_key = 'super secret key'
app.config['SESSION_TYPE'] = 'filesystem'

# Common variables
db = db.Database()
cmc_url = "https://coinmarketcap.com"

@app.route("/")
def index():
    return render_template("index.html")


def getCoinList():
    page = requests.get(cmc_url + "/all/views/all/")
    tree = html.fromstring(page.content)
    rows = tree.xpath("//tbody/tr")
    for row in rows:
        url = row[1][2].get("href")
        name = row[1][2].text
        symbol = row[2].text
        cap = row[3].text.strip().replace("$", "").replace(",","")
        price = row[4][0].text.replace("$","").replace(",","")
        cs = row[5][0].text.strip().replace(",","")

        cap = re.sub('[^\d\.]', '', cap)
        price = re.sub('[^\d\.]', '', price)
        cs = re.sub('[^\d\.]', '', cs)
        
        if cs == "": 
            cs = 0
        if cap == "": 
            cap = 0
        if price == "": 
            price = 0

        db.execUpdate("""
            insert into coin(coin, sym, marketcap, price, cs, url)
            values(%s,%s,%s,%s,%s,%s)"""
            , (name, symbol, cap, price, cs, url))
    return rows


def saveMarkets():
    results = db.execQuery("select id, url from coin")
    for result in results:
        print result[0]
        page = requests.get(cmc_url + result[1] + "#markets")
        tree = html.fromstring(page.content)
        mkts = tree.xpath("//table[@id='markets-table']/tbody/tr")

        try:
            ts = tree.xpath(
                    """//div[@class='coin-summary-item-detail details-text-medium']"""
                )[3].text.strip()
            ts = re.sub('[^\d\.]', '', ts)
            db.execUpdate("update coin set ts=%s where id=%s", (ts, result[0]))
        except:
            pass

        for mkt in mkts:
            src = mkt[1][0].text
            pair = mkt[2][0].text
            vol = mkt[3][0].text.strip()
            price = mkt[4][0].text.strip()
            dist = mkt[5].text.strip()
        
            vol = re.sub('[^\d\.]', '', vol)
            price = re.sub('[^\d\.]', '', price)
            dist = re.sub('[^\d\.]', '', dist)

            if vol == "": 
                vol = 0
            if price == "": 
                price = 0
            if dist == "": 
                dist = 0

            db.execUpdate("""
                insert into market(coinid, src, pair, vol, price, dist)
                values(%s,%s,%s,%s,%s,%s)"""
                , (result[0], src, pair, vol, price, dist))

getCoinList()
saveMarkets()

if __name__ == "__main__":
    sess.init_app(app)
    app.run(debug=True)
