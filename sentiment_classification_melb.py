from pymongo import MongoClient
from bson import json_util
import json

pride_file = './pride_list.txt'
wrath_file = './wrath_list.txt'
sloth_file = './sloth_list.txt'
envy_file = './envy_list.txt'
lust_file = './lust_list.txt'
gluttony_file = './gluttony_list.txt'
greed_file = './greed_list.txt'

pride_list = []
wrath_list = []
sloth_list = []
envy_list = []
lust_list = []
gluttony_list = []
greed_list = []
total_sentiment_seed_number = 0


def load_data(which_file):
    with open(which_file, 'r') as f_pride:
        json_dict = json.load(f_pride)
        which_list = json_dict["expanded_list"]
        f_pride.close()
        return which_list


pride_list = load_data(pride_file)
total_sentiment_seed_number += len(pride_list)
wrath_list = load_data(wrath_file)
total_sentiment_seed_number += len(wrath_list)
sloth_list = load_data(sloth_file)
total_sentiment_seed_number += len(sloth_list)
envy_list = load_data(envy_file)
total_sentiment_seed_number += len(envy_list)
lust_list = load_data(lust_file)
total_sentiment_seed_number += len(lust_list)
gluttony_list = load_data(gluttony_file)
total_sentiment_seed_number += len(gluttony_list)
greed_list = load_data(greed_file)
total_sentiment_seed_number += len(greed_list)


def get_db(collection):
    client = MongoClient("localhost", 27017)
    db = client['twitter_search']
    return db[collection]


def calculate_sentiment_scores(bow, sentiment_list):
    score = 0
    sentiment_list_set = set(sentiment_list)
    bow_set = set(bow)
    for word in bow_set:
        if word in sentiment_list_set:
            score += 1 / (len(sentiment_list) / float(total_sentiment_seed_number))
    return score


def get_max_values(scores):
    sentiment_dict = {
        'pride': scores[0],
        'wrath': scores[1],
        'sloth': scores[2],
        'envy': scores[3],
        'lust': scores[4],
        'gluttony': scores[5],
        'greed': scores[6]
    }

    sorted_sentiment_dict = sorted(sentiment_dict.items(), key=lambda d: d[1], reverse=True)
    seed = sorted_sentiment_dict[0][1]
    for i, j in enumerate(sorted_sentiment_dict):
        if seed > j[1]:
            return sorted_sentiment_dict[0:i]


def read_data_from_db():
    coll = get_db('after_preprocessing_melbourne')
    cursor = coll.find()
    for index, doc in enumerate(cursor):
        scores = []
        j_str = json.dumps(doc, default=json_util.default)
        j_o = json.loads(j_str)
        bow = j_o['BOW']
        pride_score = calculate_sentiment_scores(bow, pride_list)
        scores.append(pride_score)
        wrath_score = calculate_sentiment_scores(bow, wrath_list)
        scores.append(wrath_score)
        sloth_score = calculate_sentiment_scores(bow, sloth_list)
        scores.append(sloth_score)
        envy_score = calculate_sentiment_scores(bow, envy_list)
        scores.append(envy_score)
        lust_score = calculate_sentiment_scores(bow, lust_list)
        scores.append(lust_score)
        gluttony_score = calculate_sentiment_scores(bow, gluttony_list)
        scores.append(gluttony_score)
        greed_score = calculate_sentiment_scores(bow, greed_list)
        scores.append(greed_score)

        print bow
        print pride_score, wrath_score, sloth_score, envy_score, lust_score, gluttony_score, greed_score
        top_sentiments = get_max_values(scores)
        res = coll.update({'_id': j_o['_id']}, {'$set': {'sentiment': top_sentiments}})
        print top_sentiments
        print '-' * 50


print (read_data_from_db())
