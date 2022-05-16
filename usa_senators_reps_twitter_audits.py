"""
d:
cd D:\2020\coding\twitter_bot_tweepy
sqlite3 usa_fed_politician_tweets_2.db
dir *.db


#use this to run
d:
cd D:\2020\coding\twitter_bot_tweepy
env_may2022\Scripts\activate.bat
python


pip install xlrd
https://xlrd.readthedocs.io/en/latest/


https://ucsd.libguides.com/congress_twitter/home
congress_twitter_040722.xls

#sql audit queries
select count(*) from user_completed;
select count(*) from err_userid;
select count(*) from users_liking_dict;
select count(*) from err_info_in_tweets;
select count(*) from tweet_dict;
select count(*) from user_completed;
select count(*) from tweet_likes_completed;

select userID, count(*) as count from completed group by userID;
select userID, count(*) as count from err_info_in_tweets group by userID;
select userID, count(*) as count from err_userid group by userID;
select userID, count(*) as count from completed group by userID;
select userID, tweet_id, count(*) as count from tweet_likes_completed group by userID, tweet_id;

"""
import urllib.request
import json
import xlrd
import os
import datetime
import time
import tweepy as tw
import pandas as pd

import sqlite3
from sqlite3 import Error
from datetime import datetime

import requests
#nb: should have these keys loading from environment variables configured outside source code in repo.
#omitting the keys is a temporary fix. 
bearer_token=""
consumer_key=""
consumer_secret=""
access_token=""
access_token_secret=""
#nb: this uses an older method. lazy reuse.
auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)
#
client = tw.Client( bearer_token=bearer_token,
                        consumer_key=consumer_key,
                        consumer_secret=consumer_secret,
                        access_token=access_token,
                        access_token_secret=access_token_secret,
                        return_type = requests.Response,
                        wait_on_rate_limit=True)

db_filename="usa_fed_politician_tweets_2.db"
conn = sqlite3.connect(db_filename)



twitter_names = []

wb = xlrd.open_workbook('congress_twitter_040722.xls')
sheet_names = wb.sheet_names()
for sheet_name in sheet_names:
    sheet = wb.sheet_by_name(sheet_name)
    print("\n")
    print("Number of Rows: ", sheet.nrows)
    print("Number of Columns: ",sheet.ncols)
    for j in range(2,sheet.nrows):
        twitter_name = sheet.cell(j,4).value
        twitter_name
        if len(twitter_name)>0:
            twitter_names.append(twitter_name)

num_accounts = len(twitter_names)
print("Number of twitter accounts :", num_accounts)


def connect(host='http://google.com'):
    try:
        urllib.request.urlopen(host) #Python 3.x
        return True
    except:
        return False

# test
print( "connected" if connect() else "no internet!" )


for i in range(13, num_accounts):
    try:
        userID = twitter_names[i]
        print("userID:", userID)
        tweets = api.user_timeline(screen_name=userID,
                                   # 200 is the maximum allowed count
                                   count=200,
                                   include_rts = False,
                                   # Necessary to keep full_text
                                   # otherwise only the first 140 words are extracted
                                   tweet_mode = 'extended'
                                   )
        num_tweets = len(tweets)
        print("# of tweets:", num_tweets)
        #for info in tweets[:1]:
        #for info in tweets:
        for j in range(num_tweets):
            info = tweets[j]
            try:
                #dir(info)
                print("userID:", userID)
                print("ID: {}".format(info.id))
                print("created_at:", info.created_at)
                print("full_text:", info.full_text)
                print("retweet_count:", info.retweet_count)
                print("favorite_count:", info.favorite_count)
                print("\n")
                liking_users = client.get_liking_users(str(info.id))
                record_time = datetime.now()
                users_liking = json.loads(liking_users.content.decode("utf-8") )['data']
                num_liking_tweet = len(users_liking)
                print("num_liking:", num_liking_tweet)
                tweet_dict = {
                    "userID":userID,
                    'tweet_id':str(info.id),
                    "created_at":info.created_at.strftime("%m/%d/%Y, %H:%M:%S"),
                    "record_time":record_time,
                    "full_text":info.full_text,
                    "retweet_count":info.retweet_count,
                    "favorite_count":info.favorite_count,
                    "num_liking_tweet":num_liking_tweet,
                }
                print("tweet_dict:", tweet_dict)
                df_temp = pd.DataFrame(tweet_dict, index=[0])
                df_temp.to_sql("tweet_dict", conn, schema=None, index=False, if_exists='append')
                num_users_linking = len(users_liking)
                #for user_liking in users_liking:
                for k in range(num_users_linking):
                    user_liking = users_liking[k]
                    try:
                        record_time = datetime.now()
                        users_liking_dict = {
                            "tweet_id":user_liking['id'],
                            "name":user_liking['name'],
                            "username":user_liking['username'],
                            "record_time":record_time,
                        }
                        print("users_liking_dict:", users_liking_dict)
                        df_temp = pd.DataFrame(users_liking_dict, index=[0])
                        df_temp.to_sql("users_liking_dict", conn, schema=None, index=False, if_exists='append')
                    except Exception as e:
                        record_time = datetime.now()
                        err_dict = {
                            "userID":userID,
                            "user_liking":user_liking,
                            "error":str(e),
                            "record_time":record_time,
                        }
                        print("err_user_liking:", err_dict)
                        df_err = pd.DataFrame(err_dict, index=[0])
                        df_err.to_sql("err_user_liking", conn, schema=None, index=False, if_exists='append')
                        if not connect():
                            print("internet not connected.")
                            df_err = pd.DataFrame(err_dict, index=[0])
                            df_err.to_sql("no_internet", conn, schema=None, index=False, if_exists='append')
                            sleep(60)
                #
                record_time = datetime.now()
                completed_dict = {
                    "userID":userID,
                    "tweet_id":str(info.id),
                    "num_likes":num_users_linking,
                    "record_time":record_time,
                }
                print("completed_dict:", completed_dict)
                df_completed = pd.DataFrame(completed_dict, index=[0])
                df_completed.to_sql("tweet_likes_completed", conn, schema=None, index=False, if_exists='append')
            except Exception as e:
                record_time = datetime.now()
                err_dict = {
                    "userID":userID,
                    "info_id":str(info.id),
                    "error":str(e),
                    "record_time":record_time,
                }
                print("err_info_in_tweets:", err_dict)
                df_err = pd.DataFrame(err_dict, index=[0])
                df_err.to_sql("err_info_in_tweets", conn, schema=None, index=False, if_exists='append')
                if not connect():
                    print("internet not connected.")
                    record_time = datetime.now()
                    df_err = pd.DataFrame(err_dict, index=[0])
                    df_err.to_sql("no_internet", conn, schema=None, index=False, if_exists='append')
                    ##move counter back one unit
                    sleep(60)
        record_time = datetime.now()
        completed_dict = {
            "userID":userID,
            "num_tweets":len(tweets),
            "record_time":record_time,
        }
        print("completed_dict:", completed_dict)
        df_completed = pd.DataFrame(completed_dict, index=[0])
        df_completed.to_sql("user_completed", conn, schema=None, index=False, if_exists='append')
    except Exception as e:
        record_time = datetime.now()
        err_dict = {
            "userID":userID,
            "error":str(e),
            "record_time":record_time,
        }
        print("err_userid:", err_dict)
        df_err = pd.DataFrame(err_dict, index=[0])
        df_err = pd.DataFrame(err_dict, index=[0])
        df_err.to_sql("err_userid", conn, schema=None, index=False, if_exists='append')
    #

completed_dict = {
    "record_time" = datetime.now(),
}
print("completed_dict:", completed_dict)
df_completed = pd.DataFrame(completed_dict, index=[0])
df_completed.to_sql("set_completed", conn, schema=None, index=False, if_exists='append')
