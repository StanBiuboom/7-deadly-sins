from pymongo import MongoClient
from bson import json_util
import json
import sys
import csv


def get_db(collection):
    client = MongoClient("localhost", 27017)
    db = client['twitter_search']
    return db[collection]


def get_sin_rank_for_district(district, db_collection):
    sentiment_dict = {
        'pride': 0,
        'wrath': 0,
        'sloth': 0,
        'envy': 0,
        'lust': 0,
        'gluttony': 0,
        'greed': 0
    }
    coll = get_db(db_collection)
    cursor = coll.find()
    for index, doc in enumerate(cursor):
        j_str = json.dumps(doc, default=json_util.default)
        j_o = json.loads(j_str)
        if j_o['district'] == district:
            if j_o['sentiment'] is not None:
                sentiment_list = j_o['sentiment']
                for i in sentiment_list:
                    sentiment_dict[i[0]] += 1
    return sorted(sentiment_dict.items(), key=lambda d: d[1], reverse=True), sentiment_dict


def get_typical_tweets_for_sin(top_x, db_collection):
    sentiment_dict = {
        'pride': [],
        'wrath': [],
        'sloth': [],
        'envy': [],
        'lust': [],
        'gluttony': [],
        'greed': []
    }
    coll = get_db(db_collection)
    cursor = coll.find()
    for index, doc in enumerate(cursor):
        j_str = json.dumps(doc, default=json_util.default)
        j_o = json.loads(j_str)
        if j_o['sentiment'] is not None:
            sent_tweet_tuple = (j_o['sentiment'][0][0], float(j_o['sentiment'][0][1]), j_o['text'])
            sentiment_dict[j_o['sentiment'][0][0]].append(sent_tweet_tuple)
    for k, v in sentiment_dict.iteritems():
        sorted_v = sorted(v, key=lambda x: x[1], reverse=True)
        if top_x > len(sorted_v):
            sentiment_dict[k] = sorted_v
        else:
            sentiment_dict[k] = sorted_v[:top_x]

    return sentiment_dict


def get_data_in_a_row(district, district_dict):
    return (
        district, district_dict['pride'], district_dict['wrath'], district_dict['sloth'], district_dict['envy'],
        district_dict['lust'], district_dict['gluttony'], district_dict['greed'])


def get_sin_rank_for_all_districts_Melbourne(db_collection):
    ranked_res_Melbourne_Inner, Melbourne_Inner_dict = get_sin_rank_for_district('Melbourne_Inner',
                                                                                 db_collection)
    ranked_res_Melbourne_Inner_East, Melbourne_Inner_East_dict = get_sin_rank_for_district('Melbourne_Inner_East',
                                                                                           db_collection)
    ranked_res_Melbourne_Inner_South, Melbourne_Inner_South_dict = get_sin_rank_for_district('Melbourne_Inner_South',
                                                                                             db_collection)
    ranked_res_Melbourne_North_East, Melbourne_North_East_dict = get_sin_rank_for_district('Melbourne_North_East',
                                                                                           db_collection)
    ranked_res_Melbourne_North_West, Melbourne_North_West_dict = get_sin_rank_for_district('Melbourne_North_West',
                                                                                           db_collection)
    ranked_res_Melbourne_Outer_East, Melbourne_Outer_East_dict = get_sin_rank_for_district('Melbourne_Outer_East',
                                                                                           db_collection)
    ranked_res_Melbourne_South_East, Melbourne_South_East_dict = get_sin_rank_for_district('Melbourne_South_East',
                                                                                           db_collection)
    ranked_res_Melbourne_West, Melbourne_West_dict = get_sin_rank_for_district('Melbourne_West',
                                                                               db_collection)
    ranked_res_Mornington_Peninsula, Mornington_Peninsula_dict = get_sin_rank_for_district('Mornington_Peninsula',
                                                                                           db_collection)
    ranked_res_Outside, Outside_dict = get_sin_rank_for_district('Outside', db_collection)

    district_statistic = {
        'Melbourne_Inner': ranked_res_Melbourne_Inner,
        'Melbourne_Inner_East': ranked_res_Melbourne_Inner_East,
        'Melbourne_Inner_South': ranked_res_Melbourne_Inner_South,
        'Melbourne_North_East': ranked_res_Melbourne_North_East,
        'Melbourne_North_West': ranked_res_Melbourne_North_West,
        'Melbourne_Outer_East': ranked_res_Melbourne_Outer_East,
        'Melbourne_South_East': ranked_res_Melbourne_South_East,
        'Melbourne_West': ranked_res_Melbourne_West,
        'Mornington_Peninsula': ranked_res_Mornington_Peninsula,
        'Outside': ranked_res_Outside
    }
    file_path = db_collection + '.csv'
    csvfile = file(file_path, 'wb')
    writer = csv.writer(csvfile)
    writer.writerow(['', 'pride', 'wrath', 'sloth', 'envy', 'lust', 'gluttony', 'greed'])
    data_in_rows = []
    data_in_rows.append(get_data_in_a_row('Melbourne_Inner', Melbourne_Inner_dict))
    data_in_rows.append(get_data_in_a_row('Melbourne_Inner_East', Melbourne_Inner_East_dict))
    data_in_rows.append(get_data_in_a_row('Melbourne_Inner_South', Melbourne_Inner_South_dict))
    data_in_rows.append(get_data_in_a_row('Melbourne_North_East', Melbourne_North_East_dict))
    data_in_rows.append(get_data_in_a_row('Melbourne_North_West', Melbourne_North_West_dict))
    data_in_rows.append(get_data_in_a_row('Melbourne_Outer_East', Melbourne_Outer_East_dict))
    data_in_rows.append(get_data_in_a_row('Melbourne_South_East', Melbourne_South_East_dict))
    data_in_rows.append(get_data_in_a_row('Melbourne_West', Melbourne_West_dict))
    data_in_rows.append(get_data_in_a_row('Mornington_Peninsula', Mornington_Peninsula_dict))
    data_in_rows.append(get_data_in_a_row('Outside', Outside_dict))

    writer.writerows(data_in_rows)
    csvfile.close()
    return district_statistic


def get_sin_rank_for_all_districts_Sydney(db_collection):
    ranked_res_Central_Coast, Central_Coast_dict = get_sin_rank_for_district('Central Coast', db_collection)
    ranked_res_Sydney_Baulkham, Sydney_Baulkham_dict = get_sin_rank_for_district('Sydney-Baulkham', db_collection)
    ranked_res_Sydney_Blacktown, Sydney_Blacktown_dict = get_sin_rank_for_district('Sydney-Blacktown', db_collection)
    ranked_res_Sydney_City_and_Inner_South, Sydney_City_and_Inner_South_dict = get_sin_rank_for_district(
            'Sydney-City and Inner South', db_collection)
    ranked_res_Sydney_Eastern_Suburbs, Sydney_Eastern_Suburbs_dict = get_sin_rank_for_district('Sydney-Eastern Suburbs',
                                                                                               db_collection)
    ranked_res_Sydney_Inner_South_West, Sydney_Inner_South_West_dict = get_sin_rank_for_district(
            'Sydney-Inner South West', db_collection)
    ranked_res_Sydney_Inner_West, Sydney_Inner_West_dict = get_sin_rank_for_district('Sydney-Inner West', db_collection)
    ranked_res_Sydney_Northern_Beaches, Sydney_Northern_Beaches_dict = get_sin_rank_for_district(
            'Sydney-Northern Beaches', db_collection)
    ranked_res_Sydney_North_Sydney_and_Hornsby, Sydney_North_Sydney_and_Hornsby_dict = get_sin_rank_for_district(
            'Sydney-North Sydney and Hornsby', db_collection)
    ranked_res_Sydney_Outer_South_West, Sydney_Outer_South_West_dict = get_sin_rank_for_district(
            'Sydney-Outer South West', db_collection)
    ranked_res_Sydney_Outer_West_and_Blue_Mountains, Sydney_Outer_West_and_Blue_Mountains_dict = get_sin_rank_for_district(
            'Sydney-Outer West and Blue Mountains', db_collection)
    ranked_res_Sydney_Parrametta, Sydney_Parrametta_dict = get_sin_rank_for_district('Sydney-Parrametta', db_collection)
    ranked_res_Sydney_Ryde, Sydney_Ryde_dict = get_sin_rank_for_district('Sydney-Ryde', db_collection)
    ranked_res_Outside, Outside_dict = get_sin_rank_for_district('Outside', db_collection)

    district_statistic = {
        'Central Coast': ranked_res_Central_Coast,
        'Sydney-Baulkham': ranked_res_Sydney_Baulkham,
        'Sydney-Blacktown': ranked_res_Sydney_Blacktown,
        'Sydney-City and Inner South': ranked_res_Sydney_City_and_Inner_South,
        'Sydney-Eastern Suburbs': ranked_res_Sydney_Eastern_Suburbs,
        'Sydney-Inner South West': ranked_res_Sydney_Inner_South_West,
        'Sydney-Inner West': ranked_res_Sydney_Inner_West,
        'Sydney-Northern Beaches': ranked_res_Sydney_Northern_Beaches,
        'Sydney-North Sydney and Hornsby': ranked_res_Sydney_North_Sydney_and_Hornsby,
        'Sydney-Outer South West': ranked_res_Sydney_Outer_South_West,
        'Sydney-Outer West and Blue Mountains': ranked_res_Sydney_Outer_West_and_Blue_Mountains,
        'Sydney-Parrametta': ranked_res_Sydney_Parrametta,
        'Sydney-Ryde': ranked_res_Sydney_Ryde,
        'Outside': ranked_res_Outside
    }
    file_path = db_collection + '.csv'
    csvfile = file(file_path, 'wb')
    writer = csv.writer(csvfile)
    writer.writerow(['', 'pride', 'wrath', 'sloth', 'envy', 'lust', 'gluttony', 'greed'])
    data_in_rows = []
    data_in_rows.append(get_data_in_a_row('Central Coast', Central_Coast_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-Baulkham', Sydney_Baulkham_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-Blacktown', Sydney_Blacktown_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-City and Inner South', Sydney_City_and_Inner_South_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-Eastern Suburbs', Sydney_Eastern_Suburbs_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-Inner South West', Sydney_Inner_South_West_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-Inner West', Sydney_Inner_West_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-Northern Beaches', Sydney_Northern_Beaches_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-North Sydney and Hornsby', Sydney_North_Sydney_and_Hornsby_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-Outer South West', Sydney_Outer_South_West_dict))
    data_in_rows.append(
            get_data_in_a_row('Sydney-Outer West and Blue Mountains', Sydney_Outer_West_and_Blue_Mountains_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-Parrametta', Sydney_Parrametta_dict))
    data_in_rows.append(get_data_in_a_row('Sydney-Ryde', Sydney_Ryde_dict))
    data_in_rows.append(get_data_in_a_row('Outside', Outside_dict))

    writer.writerows(data_in_rows)
    csvfile.close()
    return district_statistic


def count_tweets_number_in_districts_melb(db_collection):
    district_statistic = {
        'Melbourne_Inner': 0,
        'Melbourne_Inner_East': 0,
        'Melbourne_Inner_South': 0,
        'Melbourne_North_East': 0,
        'Melbourne_North_West': 0,
        'Melbourne_Outer_East': 0,
        'Melbourne_South_East': 0,
        'Melbourne_West': 0,
        'Mornington_Peninsula': 0,
        'Outside': 0
    }
    coll = get_db(db_collection)
    cursor = coll.find()
    for index, doc in enumerate(cursor):
        j_str = json.dumps(doc, default=json_util.default)
        j_o = json.loads(j_str)
        district_statistic[j_o['district']] += 1
    return district_statistic


def count_tweets_number_in_districts_syd(db_collection):
    district_statistic = {
        'Central Coast': 0,
        'Sydney-Baulkham': 0,
        'Sydney-Blacktown': 0,
        'Sydney-City and Inner South': 0,
        'Sydney-Eastern Suburbs': 0,
        'Sydney-Inner South West': 0,
        'Sydney-Inner West': 0,
        'Sydney-Northern Beaches': 0,
        'Sydney-North Sydney and Hornsby': 0,
        'Sydney-Outer South West': 0,
        'Sydney-Outer West and Blue Mountains': 0,
        'Sydney-Parrametta': 0,
        'Sydney-Ryde': 0,
        'Outside': 0
    }
    coll = get_db(db_collection)
    cursor = coll.find()
    for index, doc in enumerate(cursor):
        j_str = json.dumps(doc, default=json_util.default)
        j_o = json.loads(j_str)
        district_statistic[j_o['district']] += 1
    return district_statistic


def main():
    city = sys.argv[1]
    top_x = sys.argv[2]
    if city == 'Melbourne':
        db_collection = 'after_preprocessing_melbourne'
        print (get_sin_rank_for_all_districts_Melbourne(db_collection))
        sentiment_dict = get_typical_tweets_for_sin(int(top_x), db_collection)
        sent_dict_file_path = './melbourne_sentiment_dict.txt'
        with open(sent_dict_file_path, 'w') as f:
            json_dict = {}
            json_dict["melbourne_sentiment_dict"] = sentiment_dict
            f.write(json.dumps(json_dict))
            f.close()
    elif city == 'Sydney':
        db_collection = 'after_preprocessing_sydney'
        print (get_sin_rank_for_all_districts_Sydney(db_collection))
        sentiment_dict = get_typical_tweets_for_sin(int(top_x), db_collection)
        sent_dict_file_path = './sydney_sentiment_dict.txt'
        with open(sent_dict_file_path, 'w') as f:
            json_dict = {}
            json_dict["sydney_sentiment_dict"] = sentiment_dict
            f.write(json.dumps(json_dict))
            f.close()
    else:
        print ('You should choose either \'Melbourne\' or \'Sydney\' as the second para')
        return 0


if __name__ == '__main__':
    main()
