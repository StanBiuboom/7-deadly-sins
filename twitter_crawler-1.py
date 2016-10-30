#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'songjian'
import json
import sys
import time

import tweepy
from pymongo import MongoClient
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# Variables that contains the user credentials to access Twitter API
access_token = "722011495060471808-UBN8XgxujwuuWdgzp72mUIM9K4btGIh"
access_token_secret = "s6FTHy9ZWDcEaIWXxE90c8gm7OakW0DE60qlMVreKcY1A"
consumer_key = "2VP6A9p8t8XA48dLD8RUFubUr"
consumer_secret = "55A3K54NolROQDlqATzcIAQ4OeCpx1nd3icupthWlNv3gVsxgp"

API_KEY = "A6ZJ2Cd5yLkjP0UHgmOZ3EJl2"
API_SECRET = "cQZ7B2r94IqgAg71voCJQWgwbItnclNWACMbpuJSZXdhnXdMcq"

MAX_TWEETS = 5000000
TWEETS_PER_QRY = 100
TWEET_AMOUNT_PER_USER = 200

GEOBOX_MELBOURNE = [144.4427490234, -38.2338654156, 145.546875, -37.4835765504]
GEO_CIRCLE_INFO_MELB = "-37.950685,144.873759,60km"
GEO_CIRCLE_INFO_SYDNEY = "-33.836727,151.169669,60km"

file_path_streaming = './streaming_sydney.txt'
file_path_search = './tweet_id_sydney.txt'
file_path_names = './names_sydney.txt'
file_path_names_history = './names_history_sydney.txt'
file_path_output_tweets = './output_sydney.txt'

count = 0  # a global varaible for tweets number counting with streaming API chosen
doc_id = None  # a global varaible for recording the id of the doc in database
# tweets_doc = {}
tweets_set = set()
f_streaming = open(file_path_streaming, 'a')



# This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):
    def on_data(self, data):
        global count
        global f_streaming
        # f_streaming.write(data)
        # tweets_doc[count] = data
        #write_data_to_db(data)
        count += 1
        print count, data
        return True

    def on_error(self, status):
        if status == 420:
            print status
            # sys.exit()
            return False


def get_max_id():
    id_list = []
    try:
        f_r = open(file_path_search, 'r')
        for i in f_r.readlines():
            if "-" not in i:
                i = int(i)
                id_list.append(i)
        return id_list[-1]
    except:
        return -1


def get_last_name():
    try:
        file = open(file_path_names_history, 'r')
        i = -1
        while True:
            i -= 1
            file.seek(i, 2)
            if file.read(1) == '\n':
                break
        return file.readline().strip()
    except:
        return -1


def get_all_tweets_by_screen_name(screen_name, api, db_collection):
    coll = get_db(db_collection)
    tweet_count = 0
    print ('--Now begin to crawl %s\'s tweets...' % screen_name)
    all_tweets = []
    new_tweets = api.user_timeline(screen_name=screen_name, count=200)
    last_id = new_tweets[-1].id

    while len(new_tweets) > 0:
        new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=last_id)
        all_tweets.extend(new_tweets)
        for tweet in new_tweets:
            tweet_count += 1
            if tweet_count > TWEET_AMOUNT_PER_USER:
                return
            else:
                json_str = json.dumps(tweet._json)
                json_o = json.loads(json_str)
                coll.insert(json_o)
                print ("Download {0} tweets".format(tweet_count) + " from user: %s" % screen_name)
        last_id = all_tweets[-1].id - 1

    print ('--%s has %d tweets in all' % (screen_name, len(all_tweets)))
    print ('\n')


def get_db(collection):
    client = MongoClient("localhost", 27017)
    db = client['twitter_search']
    return db[collection]


def main():
    '''
    :param argv: argv[1]: which api will be chosen, streaming/search.
                 argv[2]: which city for this task, Melbourne or Sydney
    :return:
    '''
    # Initialization...
    api_type = sys.argv[1]
    city = sys.argv[2]

    if city == 'Melbourne':
        geo_circle_info = GEO_CIRCLE_INFO_MELB
        which_file_path = './tweet_id_melbourne.txt'
        db_collection = 'tweets_melbourne'
    elif city == 'Sydney':
        geo_circle_info = GEO_CIRCLE_INFO_SYDNEY
        which_file_path = './tweet_id_sydney.txt'
        db_collection = 'tweets_sydney'
    else:
        print ('You should choose either \'Melbourne\' or \'Sydney\' as the second para')
        return 0

    # This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()

    auth1 = OAuthHandler(consumer_key, consumer_secret)
    auth1.set_access_token(access_token, access_token_secret)

    auth = tweepy.AppAuthHandler(API_KEY, API_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    tweet_count = 0

    screen_names = set()

    if api_type == 'search':
        coll = get_db(db_collection)
        max_id = get_max_id()
        if not max_id:
            print "This is your first time to invoke this script ^_^"
        else:
            print "The task will start from tweet NO." + str(max_id)

        with open(which_file_path, 'a') as f:
            time_stamp = time.strftime('%Y-%m-%d-%H-%M', time.localtime(time.time()))
            f.write(str(time_stamp) + '\n')

            while tweet_count < MAX_TWEETS:
                try:
                    if (max_id <= 0):
                        new_tweets = api.search(q="*", geocode=geo_circle_info, count=TWEETS_PER_QRY)
                    else:
                        new_tweets = api.search(q="*", geocode=geo_circle_info, count=TWEETS_PER_QRY,
                                                max_id=str(max_id - 1))
                    if not new_tweets:
                        print("No more tweets found")
                    oldest_id = new_tweets[-1].id
                    f.write(str(oldest_id) + '\n')
                    global count
                    for tweet in new_tweets:
                        json_str = json.dumps(tweet._json)
                        json_o = json.loads(json_str)
                        coll.insert(json_o)

                        new_screen_name = json_o['user']['screen_name']
                        if new_screen_name not in screen_names:
                            screen_names.add(new_screen_name)
                            get_all_tweets_by_screen_name(new_screen_name, api, db_collection)

                        count += 1

                    tweet_count += len(new_tweets)
                    print ("Download {0} tweets".format(tweet_count))

                except tweepy.TweepError as e:
                    print ("ERROR FOUND: " + str(e))
            f.close()
    elif api_type == 'streaming':
        while True:
            try:
                stream = Stream(auth1, l)
                stream.filter(locations=GEOBOX_MELBOURNE)
            except Exception as e:
                print e
                continue
    elif api_type == 'username':
        last_name = get_last_name()
        if last_name == -1:
            print "No name history has been found, start from the first name ^_^"
        else:
            print "This task will start from name: [%s]" % last_name
        f_n_h = open(file_path_names_history, 'a')
        flag = False
        with open(file_path_names, 'r') as f:
            for screen_name in f.readlines():
                try:
                    if last_name == -1:
                        f_n_h.write(screen_name.strip() + '\n')
                        get_all_tweets_by_screen_name(screen_name.strip(), api)
                    elif not flag and last_name != -1:
                        if screen_name.strip() != last_name.strip():
                            pass
                        else:
                            flag = True
                            # f_n_h.write(screen_name.strip() + '\n')
                            # get_all_tweets_by_screen_name(screen_name.strip(), api)
                    else:
                        f_n_h.write(screen_name.strip() + '\n')
                        get_all_tweets_by_screen_name(screen_name.strip(), api)
                except Exception as e:
                    print e
                    continue
    else:
        print 'For the 1st para, neither \"streaming\" nor \"search\" has been correctly input'


if __name__ == '__main__':
    main()
