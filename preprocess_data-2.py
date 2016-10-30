#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'songjian'

import json
import re
import sys
import string

import nltk
from bson import json_util
from nltk.stem.porter import *
from nltk.tokenize import word_tokenize
from pymongo import MongoClient

# file_path_output_tweets = './output-single-thread.txt'
file_path_output_tweets_coordinate = './output-coordinate.txt'
file_path_boundings_melbourne = './bounding_9_melbourne.txt'
file_path_boundings_sydney = './bounding_13_sydney.txt'

re_url = re.compile(r'http(s)?\://[^ ]*')
re_at = re.compile(r'@[^\W]+')
re_hashtag = re.compile(r'#[^\W]+')
list_of_eng_word = set(nltk.corpus.words.words())

sent_segmenter = nltk.data.load('tokenizers/punkt/english.pickle')
word_tokenizer = nltk.tokenize.regexp.WordPunctTokenizer()
english_stop_words = set(nltk.corpus.stopwords.words('english'))
punctuation = set(string.punctuation)
stemmer = PorterStemmer()
lemmatizer = nltk.stem.wordnet.WordNetLemmatizer()

district_name_melbourne = ['Melbourne_Inner', 'Melbourne_Inner_East', 'Melbourne_Inner_South',
                           'Melbourne_North_East', 'Melbourne_North_West', 'Melbourne_Outer_East',
                           'Melbourne_South_East', 'Melbourne_West', 'Mornington_Peninsula',
                           'Outside']

district_name_sydney = ['Central Coast', 'Sydney-Baulkham', 'Sydney-Blacktown', 'Sydney-City and Inner South',
                        'Sydney-Eastern Suburbs', 'Sydney-Inner South West', 'Sydney-Inner West',
                        'Sydney-Northern Beaches', 'Sydney-North Sydney and Hornsby', 'Sydney-Outer South West',
                        'Sydney-Outer West and Blue Mountains', 'Sydney-Parrametta', 'Sydney-Ryde', 'Outside']


def lemmatize(word):
    lemma = lemmatizer.lemmatize(word, 'v')
    if lemma == word:
        lemma = lemmatizer.lemmatize(word, 'n')
    return lemma


def max_match(word, list_of_eng_word):
    res = []
    if len(word) == 0:
        return res
    for pos, letter in enumerate(word[::-1]):
        first_word = word[:len(word) - pos]
        remainder = word[len(word) - pos:]
        if lemmatize(first_word) in list_of_eng_word:
            res.append(first_word)
            return res + max_match(remainder, list_of_eng_word)
    first_word = word[0]
    remainder = word[1:]
    res.append(first_word)
    return res + max_match(remainder, list_of_eng_word)


def hashtag_tokenizer(hashtag_word_list):  # expecting a list of #xxx
    token_list = []
    for word in hashtag_word_list:
        word = word[1:]
        if word[0].isupper():
            pos = 0
            pos_list = []
            while pos != len(word):
                if word[pos].isupper():
                    pos_list.append(pos)
                pos += 1
            if len(pos_list) > 0:  # newly added
                i = 0
                while i != len(pos_list) - 1:
                    token_list.append(word[pos_list[i]:pos_list[i + 1]])
                    i += 1
                token_list.append(word[pos_list[-1]:])
        else:
            res_from_m_m = max_match(word, list_of_eng_word)
            if len(res_from_m_m) > 1:
                token_list += res_from_m_m

    return token_list


def tokenize(line, tokenizer=word_tokenize):
    utf_line = line.decode('utf-8').lower()
    return [token.encode('ascii', 'backslashreplace') for token in tokenizer(utf_line)]


def preprocess_docs(doc):
    tweet = []
    # doc = re.sub(r"[^0-9a-zA-Z]+", " ", doc).strip()  # remove strange symbols which are not included in punctuation set
    doc = re.sub(re_at, "", doc).strip()
    doc = re.sub(re_url, "", doc).strip()
    hashtag_word_list = re.findall(re_hashtag, doc)
    doc = re.sub(re_hashtag, "", doc).strip()
    doc = re.sub(r"[^0-9a-zA-Z]+", " ", doc).strip()
    if len(hashtag_word_list) != 0:
        hashtag_res = hashtag_tokenizer(hashtag_word_list)  # return a list of words after tokenization
        if len(hashtag_res) != 0:
            for word in hashtag_res:
                tweet.append(lemmatize(word))
                # print "hashtag_res in line %d is: " % i, hashtag_res

    doc = doc.lower()
    for j, sent in enumerate(sent_segmenter.tokenize(doc)):  # input a list of all the sents
        words = word_tokenizer.tokenize(sent)  # get a list of all the words in the sent
        if len(words) != 0:
            for word in words:
                tweet.append(lemmatize(word))

    return tweet


def get_db(collection):
    client = MongoClient("localhost", 27017)
    db = client['twitter_search']
    return db[collection]


def get_centre_point_in_district_syd(file_name):
    res = []
    with open(file_name, "r") as f:
        for i, line in enumerate(f):
            elements = line.strip('\n').split(',')
            longtitute = (float(elements[0]) + float(elements[2])) / float(2)
            latitute = (float(elements[1]) + float(elements[3])) / float(2)
            tuple_res = (district_name_sydney[i], latitute, longtitute)
            res.append(tuple_res)
    return res


def get_centre_point_in_district_melb(file_name):
    res = []
    with open(file_name, "r") as f:
        for i, line in enumerate(f):
            elements = line.strip('\n').split(',')
            print elements, i
            longtitute = (float(elements[0]) + float(elements[2])) / float(2)
            latitute = (float(elements[1]) + float(elements[3])) / float(2)
            tuple_res = (district_name_melbourne[i], latitute, longtitute)
            res.append(tuple_res)
    return res


def read_district(fileName):
    districts = []
    with open(fileName, "r") as f:
        for i, line in enumerate(f):
            elements = line.strip('\n').split(',')
            districts.append(elements)
        return districts


def comparing(bounding1, bouding2, data):
    if float(bounding1) < float(data) and float(data) < float(bouding2):
        return True
    if float(bouding2) < float(data) and float(data) < float(bounding1):
        return True
    return False


def check_bounding(longtitude, latitude, bounding_box):
    longtitude1 = bounding_box[0]
    longtitude2 = bounding_box[2]
    latitude1 = bounding_box[1]
    latitude2 = bounding_box[3]
    if comparing(longtitude1, longtitude2, longtitude) and comparing(latitude1, latitude2, latitude):
        return True
    else:
        return False


def distribute_district(longtitude, latitude, file_path_boundings):
    districts = read_district(file_path_boundings)
    for i, element in enumerate(districts):
        if check_bounding(longtitude, latitude, element):
            return i
    return len(districts)


def read_data_from_db(db_collection_source, db_collection_output, file_path_boundings, district_name):
    source_coll = get_db(db_collection_source)
    output_coll = get_db(db_collection_output)

    useful_info_set = {
        '_id': None,
        'tweet_id': None,
        'text': None,
        'BOW': None,
        'user_id': None,
        'screen_name': None,
        'geo': None,
        'district_index': None,
        'district': None,
    }

    cursor = source_coll.find()

    for index, doc in enumerate(cursor):
        j_str = json.dumps(doc, default=json_util.default)
        j_o = json.loads(j_str.replace('$oid', 'oid'))

        if str(j_o['user']['geo_enabled']) != 'False' and str(j_o['geo']) != 'None':
            # print index, j_o['user']['geo_enabled'], j_o['geo'], ': ', j_str
            # print index, j_o['user']['screen_name']
            # f.write(j_str + '\n')
            # print '*' * 100
            useful_info_set['_id'] = j_o['_id']
            print useful_info_set['_id']
            useful_info_set['tweet_id'] = j_o['id']
            print "tweet_id: " + str(useful_info_set['tweet_id'])
            useful_info_set['text'] = j_o['text']
            print "text: " + useful_info_set['text']

            sent_list = preprocess_docs(useful_info_set['text'])  #preprocess, turn a raw sentence into a bag_of_word
            useful_info_set['BOW'] = sent_list
            print "BOW: " + str(useful_info_set['BOW'])

            useful_info_set['user_id'] = j_o['user']['id']
            print "user_id: " + str(useful_info_set['user_id'])
            useful_info_set['screen_name'] = j_o['user']['screen_name']
            print "screen_name: " + useful_info_set['screen_name']
            if j_o['geo']['type'] == 'Point':
                print j_o['geo']['coordinates']
                useful_info_set['geo'] = j_o['geo']['coordinates']
            coordinate = useful_info_set['geo']
            district_index = distribute_district(coordinate[1], coordinate[0], file_path_boundings) #classify this point into a district
            useful_info_set['district_index'] = district_index
            print "district_index: " + str(useful_info_set['district_index'])
            useful_info_set['district'] = district_name[district_index]
            print "district: " + str(useful_info_set['district'])
            output_coll.insert(useful_info_set)


def main():
    '''
    print (get_centre_point_in_district_syd(file_path_boundings_sydney))
    print (get_centre_point_in_district_melb(file_path_boundings_melbourne))
    '''
    city = sys.argv[1]
    if city == 'Melbourne':
        db_collection_source = 'tweets'
        db_collection_output = 'after_preprocessing_melbourne'
        file_path_boundings = file_path_boundings_melbourne
        district_name = district_name_melbourne
        read_data_from_db(db_collection_source, db_collection_output, file_path_boundings, district_name)
    elif city == 'Sydney':
        db_collection_source = 'tweets_sydney'
        db_collection_output = 'after_preprocessing_sydney'
        file_path_boundings = file_path_boundings_sydney
        district_name = district_name_sydney
        read_data_from_db(db_collection_source, db_collection_output, file_path_boundings, district_name)
    else:
        print ('You should choose either \'Melbourne\' or \'Sydney\' as the second para')
        return 0


if __name__ == '__main__':
    main()
