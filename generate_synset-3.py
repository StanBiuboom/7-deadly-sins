import gensim
import nltk
from nltk.corpus import brown
import math
import string
from pymongo import MongoClient
from bson import json_util
import json
import os
import sys

THRESHOLD = 7.0
json_file_word_dict_path = './word_dict.json'
json_file_co_occurrence_dict_path = './co_occurrence_dict.json'

pride_file = 'pride_list.txt'
wrath_file = 'wrath_list.txt'
sloth_file = 'sloth_list.txt'
envy_file = 'envy_list.txt'
lust_file = 'lust_list.txt'
gluttony_file = 'gluttony_list.txt'
greed_file = 'greed_list.txt'

# positive_seeds = ["good", "nice", "excellent", "positive", "fortunate", "correct", "superior", "great"]
# negative_seeds = ["bad", "nasty", "poor", "negative", "unfortunate", "wrong", "inferior", "awful"]
pride_seeds = ["pride", "delight", "dignity", "ego", "happiness", "honor", "joy", "pleasure", "satisfaction", "face"]
wrath_seeds = ["wrath", "displeasure", "fury", "angry", "hate", "offense", "rage", "storm", "temper", "distaste",
               "dislike", "annoying"]
sloth_seeds = ["lazy", "bum", "idleness", "inactivity", "indolence", "inertia", "slack", "slow", "idler"]
envy_seeds = ["envy", "envious", "jealousy", "spite", "opposition", "prejudice", "malice", "hatred", "resentment",
              "rivalry"]
lust_seeds = ["lust", "desire", "longing", "hunger", "libido", "thirst", "itch", "excitement", "sexy", "appetite",
              "ardor", "passion"]
gluttony_seeds = ["hungry", "taste", "stomach", "demand", "chocolate", "burger", "pizza", "chips", "cheese",
                  "delicious", "tasty", "starve"]
greed_seeds = ["greed", "liking", "avarice", "excess", "crave", "desire", "longing", "hunger", "eager", "deluxe",
               "luxury"]

word_dict = {}
co_occurrence_dict = {}
total_count = 0

expand_pride_list_word_count = 0
expand_wrath_list_word_count = 0
expand_sloth_list_word_count = 0
expand_envy_list_word_count = 0
expand_lust_list_word_count = 0
expand_gluttony_list_word_count = 0
expand_greed_list_word_count = 0

lemmatizer = nltk.stem.wordnet.WordNetLemmatizer()
english_stop_words = set(nltk.corpus.stopwords.words('english'))
punctuation = set(string.punctuation)


def lemmatize(word):
    lemma = lemmatizer.lemmatize(word, 'v')
    if lemma == word:
        lemma = lemmatizer.lemmatize(word, 'n')
    return lemma


def get_db(collection):
    client = MongoClient("localhost", 27017)
    db = client['twitter_search']
    return db[collection]


def turn_sentence_into_worddict(sent):
    sent = [word.lower() for word in sent if word not in english_stop_words and word not in punctuation]
    for i in range(len(sent)):
        if word_dict.has_key(sent[i].lower()):  # if the word has already appeared in word_dict, then add its value by 1
            word_dict[sent[i].lower()] += 1
        else:
            word_dict[sent[
                i].lower()] = 1  # if the word is not in word_dict, add a new item in the dict with word's value equals to 1
        if sent[i].lower() in co_occurrence_dict:  # if the word has already appeared in co_occurrence_dict
            if i < len(sent) - 1:  # if the word is not the last word in this sentence
                if sent[i + 1].lower() in co_occurrence_dict[sent[i].lower()]:
                    co_occurrence_dict[sent[i].lower()][sent[i + 1].lower()] += 1
                else:
                    co_occurrence_dict[sent[i].lower()][sent[i + 1].lower()] = 1
                if i < len(sent) - 2:
                    if sent[i + 2].lower() in co_occurrence_dict[sent[i].lower()]:
                        co_occurrence_dict[sent[i].lower()][sent[i + 2].lower()] += 1
                    else:
                        co_occurrence_dict[sent[i].lower()][sent[i + 2].lower()] = 1
            if i > 0:  # if the word is not the first word in this sentence
                if sent[i - 1].lower() in co_occurrence_dict[sent[i].lower()]:
                    co_occurrence_dict[sent[i].lower()][sent[i - 1].lower()] += 1
                else:
                    co_occurrence_dict[sent[i].lower()][sent[i - 1].lower()] = 1
                if i > 1:
                    if sent[i - 2].lower() in co_occurrence_dict[sent[i].lower()]:
                        co_occurrence_dict[sent[i].lower()][sent[i - 2].lower()] += 1
                    else:
                        co_occurrence_dict[sent[i].lower()][sent[i - 2].lower()] = 1

        else:  # if the word is not in co_occurrence_dict
            if i < len(sent) - 1 and i > 0:
                co_occurrence_dict[sent[i].lower()] = {}
                co_occurrence_dict[sent[i].lower()][sent[i + 1].lower()] = 1
                co_occurrence_dict[sent[i].lower()][sent[i - 1].lower()] = 1
                if i < len(sent) - 2:
                    co_occurrence_dict[sent[i].lower()][sent[i + 2].lower()] = 1
                if i > 1:
                    co_occurrence_dict[sent[i].lower()][sent[i - 2].lower()] = 1

            elif i == 0 and len(sent) > 1:
                co_occurrence_dict[sent[i].lower()] = {}
                co_occurrence_dict[sent[i].lower()][sent[i + 1].lower()] = 1
            elif i == len(sent) - 1 and len(sent) > 1:
                co_occurrence_dict[sent[i].lower()] = {}
                co_occurrence_dict[sent[i].lower()][sent[i - 1].lower()] = 1


def generate_worddict_in_db(db_collection):
    coll = get_db(db_collection)
    cursor = coll.find()
    for index, doc in enumerate(cursor):
        j_str = json.dumps(doc, default=json_util.default)
        j_o = json.loads(j_str)
        bow = j_o['BOW']
        turn_sentence_into_worddict(bow)


def generate_worddict_in_brown():  # generate a word_dict for all the words in brown.sents() in their lower_case, key: word, value: word_freq
    for sent in brown.sents():
        turn_sentence_into_worddict(sent)


def start_preparing(db_collection):
    generate_worddict_in_db(db_collection)
    generate_worddict_in_brown()
    with open(json_file_word_dict_path, 'w') as f1:
        json_word_dict = {}
        json_word_dict["word_dict"] = word_dict
        f1.write(json.dumps(json_word_dict))
        f1.close()
    with open(json_file_co_occurrence_dict_path, 'w') as f2:
        json_co_occurrence_dict = {}
        json_co_occurrence_dict["co_occurrence_dict"] = co_occurrence_dict
        f2.write(json.dumps(json_co_occurrence_dict))
        f2.close()
    global total_count
    for k, v in word_dict.iteritems():
        total_count += v


def load_saved_data():
    with open(json_file_word_dict_path, 'r') as f1:
        json_word_dict = json.load(f1)
        global word_dict
        word_dict = json_word_dict['word_dict']
    with open(json_file_co_occurrence_dict_path, 'r') as f2:
        json_co_occurrence_dict = json.load(f2)
        global co_occurrence_dict
        co_occurrence_dict = json_co_occurrence_dict['co_occurrence_dict']

    global total_count
    for k, v in word_dict.iteritems():
        total_count += v


def check_file_loaded(db_collection):
    if os.path.isfile(json_file_word_dict_path) and os.path.isfile(json_file_co_occurrence_dict_path):
        load_saved_data()
    else:
        print "first time to invoke this part"
        start_preparing(db_collection)


def get_PMI_for_collocation_brown(word1, word1_count, word2, word2_count, total_count):
    try:
        if word2 in co_occurrence_dict[word1]:
            res = math.log(
                    (co_occurrence_dict[word1][word2] / float(total_count)) /
                    ((word1_count / float(total_count)) * (word2_count / float(total_count))),
                    2)
            if res >= 0:  # only possitive values will be returned
                return res
            else:
                return 0
        else:
            return 0
    except Exception, e:
        # print Exception, ":", e
        return 0


def expand_seed_list(which_seeds, which_seeds_file):
    expanded_list = []
    for i in range(len(which_seeds)):
        expanded_list.append(which_seeds[i])

    for key, value in word_dict.iteritems():
        PMI_score = 0
        for seed in which_seeds:
            if word_dict.has_key(seed):
                pmi = get_PMI_for_collocation_brown(key, value, seed, word_dict[seed], total_count)
                PMI_score += pmi
                if pmi > 0:
                    print "seed: ", seed
                    print "word: ", key
                    print "pmi: ", pmi
            else:
                print seed + " not in word_dict!"
        if PMI_score > THRESHOLD:
            print "PMI_SCORE = ", PMI_score
            print "-" * 50
            expanded_list.append(lemmatize(key))

    with open(which_seeds_file, 'w') as f:
        json_dict = {}
        json_dict["expanded_list"] = expanded_list
        json_dict["word_count"] = len(expanded_list)
        f.write(json.dumps(json_dict))
        f.close()
    return expanded_list


def main():
    city = sys.argv[1]
    if city == 'Melbourne':
        db_collection = 'after_preprocessing_melbourne'
    elif city == 'Sydney':
        db_collection = 'after_preprocessing_sydney'
    else:
        print ('You should choose either \'Melbourne\' or \'Sydney\' as the second para')
        return 0

    check_file_loaded(db_collection)
    print len(word_dict), len(co_occurrence_dict), total_count
    pride_list = expand_seed_list(pride_seeds, pride_file)
    expand_pride_list_word_count = len(pride_list)
    print "pride: " + str(len(pride_list))

    wrath_list = expand_seed_list(wrath_seeds, wrath_file)
    expand_wrath_list_word_count = len(wrath_list)
    print "wrath: " + str(len(wrath_list))

    sloth_list = expand_seed_list(sloth_seeds, sloth_file)
    expand_sloth_list_word_count = len(sloth_list)
    print "sloth: " + str(len(sloth_list))

    envy_list = expand_seed_list(envy_seeds, envy_file)
    expand_envy_list_word_count = len(envy_list)
    print "envy: " + str(len(envy_list))

    lust_list = expand_seed_list(lust_seeds, lust_file)
    expand_lust_list_word_count = len(lust_list)
    print "lust: " + str(len(lust_list))

    gluttony_list = expand_seed_list(gluttony_seeds, gluttony_file)
    expand_gluttony_list_word_count = len(gluttony_list)
    print "gluttony: " + str(len(gluttony_list))

    greed_list = expand_seed_list(greed_seeds, greed_file)
    expand_greed_list_word_count = len(greed_list)
    print "greed: " + str(len(greed_list))



if __name__ == '__main__':
    main()

