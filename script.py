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
import time
import tweepy
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from lxml import html

db = db.Database()
cmc_url = "https://coinmarketcap.com"
twitter_token = "HtuEQkaaUwaZksAaKRKDpSrIC"
twitter_secret = "BeI6QYGD9OwzRpkiTYXVKzYs2GMIVelj0cu7ynqJViK9jqpK8d"
twitter_access_token = "712132794-gnAMHJdrcoTK5ZcajEssFdEMb7ZUqXp3T3RzIV8P"
twitter_access_secret = "GBBPuVCzt4iOXmYGOB0YibiVrlPcOu2skrt4JDpYXb5fs"

def main():
    # print("getting coin list...")
    # getCoinList()
    # print("saving markets...")
    # saveMarkets()
    print("getting historic price...")
    getAllCoinTicks()
    print("done.")


def getCoinList():
    page = requests.get(cmc_url + "/all/views/all/")
    tree = html.fromstring(page.content)
    rows = tree.xpath("//tbody/tr")
    ts = time.time()
    timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    for row in rows:
        rank = row[0].text.strip()
        url = row[1][2].get("href")
        name = row[1][2].text
        symbol = row[2].text
        cap = row[3].text.strip().replace("$", "").replace(",","")
        price = row[4][0].text.replace("$","").replace(",","")
        cs = row[5][0].text.strip().replace(",","")
        vol = row[6][0].text.replace("$","").replace(",","")

        cap = re.sub('[^\d\.]', '', cap)
        price = re.sub('[^\d\.]', '', price)
        cs = re.sub('[^\d\.]', '', cs)
        vol = re.sub('[^\d\.]', '', vol)
        
        if cs == "": 
            cs = 0
        if cap == "": 
            cap = 0
        if price == "": 
            price = 0
        if vol == "":
            vol = 0

        results = db.execQuery("select id from coin where url=%s", (url,))
        if len(results) < 1:
            db.execUpdate("""
                insert into coin(coin, sym, marketcap, price, cs, url, rank, volume)
                values(%s,%s,%s,%s,%s,%s,%s,%s)"""
                , (name, symbol, cap, price, cs, url, rank, vol))
            results = db.execQuery("select id from coin where url=%s", (url,))                
        else:
            db.execUpdate("""
                update coin set price=%s, marketcap=%s, volume=%s, cs=%s, rank=%s 
                where url=%s"""
                , (price, cap, vol, cs, rank, results[0][0]))

        db.execUpdate("""
            insert into coindata(created, coinid, price, marketcap, rank, volume, cs)
            values(%s, %s, %s, %s, %s, %s, %s)"""
            , (timestamp, results[0][0], price, cap, rank, vol, cs))
    return rows


def saveMarkets():
    results = db.execQuery("select id, url from coin")
    for result in results:
        page = requests.get(cmc_url + result[1] + "#markets")
        tree = html.fromstring(page.content)
        mkts = tree.xpath("//table[@id='markets-table']/tbody/tr")

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
                values(%s,%s,%s,%s,%s,%s);"""
                , (result[0], src, pair, vol, price, dist))


def getTweets():
    auth = tweepy.OAuthHandler(twitter_token, twitter_secret)
    auth.set_access_token(twitter_access_token, twitter_access_secret)
    tweepy_api = tweepy.API(auth)
    tweets = []
    for tweet in tweepy.Cursor(tweepy_api.search,
        q="bitcoin OR btc", 
        result_type="mixed", 
        count=100, lang="en").items():
        tweets.append(tweet.text)
    print tweets


def getRedditHeadlines():
    page = 0
    base_url = "https://www.reddit.com/r/bitcoin/.json?"
    tp_url = "http://text-processing.com/api/sentiment/"
    after = ""
    count = 0
    bDone = False
    dataset = []
    stopwords = set(nltk.corpus.stopwords.words("english"))
    total_score = 0
    while not bDone:
        url = base_url+"after="+after+"&count="+str(count)
        reddit = requests.get(url, headers={"User-agent":"cmcscrape"})
        listing = json.loads(reddit.content)
        
        if listing["data"]["after"]:
            after = listing["data"]["after"]
        else:
            bDone = True
            
        for heading in listing["data"]["children"]:
            temp = {}
            created = datetime.fromtimestamp(heading["data"]["created"])
            temp["date"] = created.strftime('%c')
            if datetime.now()-timedelta(hours=24) >= created:
                continue
            words = nltk.tokenize.word_tokenize(heading["data"]["title"])
            clean_words = [word.lower() for word in words if word not in stopwords]
            temp["title"] = " ".join(clean_words)   
            tp = requests.post(tp_url, {"text":temp["title"]})
            temp["prob"] = json.loads(tp.content)
            temp["score"] = heading["data"]["score"]
            total_score += heading["data"]["score"]
            dataset.append(temp)
            count += 1
        page += 1
    
    pos = 0.0
    neg = 0.0
    neutral = 0.0
    for data in dataset:
        pos += (data["prob"]["probability"]["pos"] * (data["score"]/float(total_score)))
        neg += (data["prob"]["probability"]["neg"] * (data["score"]/float(total_score)))
        neutral += (data["prob"]["probability"]["neutral"] * (data["score"]/float(total_score)))
    db.execUpdate("""
        insert into sentiment(pos, neutral, neg, posts, score)
        values(%s, %s, %s, %s, %s);
        """, (pos, neutral, neg, count, total_score))


def getCoinTicks(coin="bitcoin", reset=False):
    jump_start = 0
    e_start = 0
    e_end = 999999999999999
    if reset:
        db.execUpdate("delete from historic where coin=%s", (coin,))
    else:
        max_epoch = db.execQuery("""
            select max(epoch) from historic where coin=%s
            """, (coin,))
        if max_epoch[0][0] is not None:
            jump_start = int(max_epoch[0][0])

    base_url = "https://graphs2.coinmarketcap.com/currencies/" + coin + "/"
    url = base_url + str(e_start) + "/" + str(e_end)
    page = requests.get(url)
    data = json.loads(page.content)
    days = [int(tick[0]) for tick in data["price_btc"]]
    for i, day in enumerate(days):
        if i+1 != len(days):
            if jump_start > days[i+1]:
                continue
        e_start = day
        if i+1 == len(days):
            e_end = e_start + 86400000
        else:
            e_end = days[i+1]
        url = base_url + str(e_start) + "/" + str(e_end)
        page = requests.get(url)
        data = json.loads(page.content)
        for mc, price, vol in zip(data["market_cap_by_available_supply"], data["price_usd"], data["volume_usd"]):
            epoch = mc[0]
            db.execUpdate("""
                insert ignore into historic(coin, epoch, marketcap, price, vol)
                values(%s,%s,%s,%s,%s)
                """, (coin, epoch, mc[1], price[1], vol[1]))
    db.execUpdate("update historic set dt=from_unixtime(epoch/1000.) where dt is null")


def getAllCoinTicks():
    results = db.execQuery("select url from coin where id > 18 order by marketcap desc")
    for url in results:
        coin = url[0].replace("currencies", "").replace("/", "")
        print coin
        try:
            getCoinTicks(coin)
        except:
            pass

def getCorrelation():
    results = db.execQuery("""
        select epoch, price from historic where coin='bitcoin' 
        and epoch >= (UNIX_TIMESTAMP(STR_TO_DATE('Jan 1 2014', '%%M %%d %%Y')) * 1000) 
        and epoch < (UNIX_TIMESTAMP(STR_TO_DATE('Mar 29 2014', '%%M %%d %%Y')) * 1000)
        limit 10000;
        """)
    data = [(tick[0],float(tick[1])) for tick in results]
    price = pd.DataFrame.from_records(data, columns=["epoch", "2014"])
    price['2015'] = getPriceSeries("2015").values
    price['2016'] = getPriceSeries("2016").values
    price['2017'] = getPriceSeries("2017").values
    price['2018'] = getPriceSeries("2018").values
    c = price.corr()
    sns.heatmap(c, xticklabels=c.columns.values, yticklabels=c.columns.values)
    plt.show()


def getPriceSeries(year):
    results = db.execQuery("""
        select epoch, price from historic where coin='bitcoin' 
        and epoch >= (UNIX_TIMESTAMP(STR_TO_DATE('Jan 1 """ + year + """', '%%M %%d %%Y')) * 1000) 
        and epoch < (UNIX_TIMESTAMP(STR_TO_DATE('Mar 29 """ + year + """', '%%M %%d %%Y')) * 1000)
        limit 10000;
        """)
    data = [float(tick[1]) for tick in results]
    return pd.Series(data)



if __name__ == '__main__': getCorrelation()