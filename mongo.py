import json
import pymongo

from pymongo import MongoClient
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client['twitter']
print("Database is created !!")
print(db)

collection = db["retweets"]
print("List of databases after creating new one")
print(client.list_database_names())
cursor = collection.find()




with open("./corona-out-2.txt", "r") as f1:
    for line in f1:
        try:
            data = json.loads(line)

            if (data['text'].startswith('RT')):
                data_retweet = {'text': data['text'], 'timestamp_ms': data['timestamp_ms'],
                                'created_at': data['created_at'], 'name': data['user']['name'],
                                'Followers_count': data['user']['followers_count'],
                                'Retweet_count': data['retweeted_status']['retweet_count']}
                collection.insert_one(data_retweet)
            else:
                continue
        except:
            continue

with open("./corona-out-2.txt", "r") as f1:
    for line in f1:
        try:
            data = json.loads(line)
            if not (data['text'].startswith('RT')):
                data_tweet = {'text': data['text'], 'timestamp_ms': data['timestamp_ms'],
                              'created_at': data['created_at'], 'name': data['user']['name'],
                              'Followers_count': data['user']['followers_count']}
                collection.insert_one(data_tweet)
            else:
                continue
        except:
            continue



def search_by_string(text, start_year_para, end_year_para):
    import datetime
    new_list = []
    new_list_with_timerange = []
    new_list_withtimerange_relevance = []
    for doc in db.my_tweet_project.find():
        if (text in doc['text']):
            new_list.append(doc)

    for doc in new_list:
        doc["timestamp_ms_ts"] = datetime.datetime.fromtimestamp((int(doc["timestamp_ms"])) / 1000).strftime(
            '%Y-%m-%d %H:%M:%S')
        doc["timestamp_ms_ts_year"] = datetime.datetime.fromtimestamp((int(doc["timestamp_ms"])) / 1000).strftime('%Y')

    # Time range search
    for doc in new_list:
        if (int(doc["timestamp_ms_ts_year"]) in range(start_year_para, end_year_para)):
            new_list_with_timerange.append(doc)

        # Notion of relevance to rank the search results
    new_list_with_timerange_relevance = sorted(new_list_with_timerange, key=lambda x: x['Followers_count'],
                                               reverse=True)
    return new_list_with_timerange_relevance


def search_by_hashtag(name, start_year_para, end_year_para):
    import datetime
    my_new_list = []
    my_new_list_with_timerange = []
    for doc in collection.find({'hashtags': name}, {"text": 1, "timestamp_ms": 1}):
        my_new_list.append(doc)

    for doc in my_new_list:
        doc["timestamp_ms_ts"] = datetime.datetime.fromtimestamp((int(doc["timestamp_ms"])) / 1000).strftime(
            '%Y-%m-%d %H:%M:%S')
        doc["timestamp_ms_ts_year"] = datetime.datetime.fromtimestamp((int(doc["timestamp_ms"])) / 1000).strftime('%Y')

    # Time range search
    for doc in my_new_list:
        if (int(doc["timestamp_ms_ts_year"]) in range(start_year_para, end_year_para)):
            my_new_list_with_timerange.append(doc)

    return my_new_list_with_timerange


def insert_data():
    with open("./corona-out-2", "r") as f1:
        for line in f1:
            try:
                data = json.loads(line)

                if (data['text'].startswith('RT')):
                    data_retweet_hashtag = {'text': data['text'], 'timestamp_ms': data['timestamp_ms'],
                                            'created_at': data['created_at'], 'name': data['user']['name'],
                                            'Followers_count': data['user']['followers_count'],
                                            'hashtags': data['entities']['hashtags'][0]['text']}
                    collection.insert_one(data_retweet_hashtag)
                else:
                    continue
            except:
                continue

    with open("./corona-out-2", "r") as f1:
        for line in f1:
            try:
                data = json.loads(line)
                if not (data['text'].startswith('RT')):
                    data_tweet_hashtag = {'text': data['text'], 'timestamp_ms': data['timestamp_ms'],
                                          'created_at': data['created_at'], 'name': data['user']['name'],
                                          'Followers_count': data['user']['followers_count'],
                                          'hashtags': data['entities']['hashtags'][0]['text']}
                    collection.insert_one(data_tweet_hashtag)
                else:
                    continue
            except:
                continue


def search_by_hashtag_using_index(name_hashtag_index, start_year_para, end_year_para):
    import datetime
    my_new_list = []
    my_new_list_with_timerange = []
    my_new_list_withtimerange_relevance = []

    # Create text index
    # collection.create_index([('hashtags', 'text')], name='Hashtags_index')

    # Find hashtags with the hashtags that user searched for
    for doc in collection.find({"$text": {"$search": name_hashtag_index}},
                               {"text": 1, "timestamp_ms": 1, "Followers_count": 1}):
        my_new_list.append(doc)

    # Execution statistics for the query
    print(collection.find({"$text": {"$search": name_hashtag_index}},
                          {"text": 1, "timestamp_ms": 1, "Followers_count": 1}).explain()["executionStats"])

    for doc in my_new_list:
        doc["timestamp_ms_ts"] = datetime.datetime.fromtimestamp((int(doc["timestamp_ms"])) / 1000).strftime(
            '%Y-%m-%d %H:%M:%S')
        doc["timestamp_ms_ts_year"] = datetime.datetime.fromtimestamp((int(doc["timestamp_ms"])) / 1000).strftime('%Y')

    # Time range search
    for doc in my_new_list:
        if (int(doc["timestamp_ms_ts_year"]) in range(start_year_para, end_year_para)):
            my_new_list_with_timerange.append(doc)

    # Notion of relevance to rank the search results
    my_new_list_with_timerange_relevance = sorted(my_new_list_with_timerange, key=lambda x: x['Followers_count'],
                                                  reverse=True)

    return my_new_list_with_timerange_relevance

if __name__ == '__main__':





    # insert_data()
    # Enter Hashtag
    print("Enter Twitter HashTag to search for")
    words_hashtag = input()
    # Enter string
    print("Enter string to search for")
    words_string = input()
    print("Enter start year search")
    start_year = int(input())
    print("Enter end year search")
    end_year = int(input())
    output_for_hashtag = search_by_hashtag_using_index(words_hashtag, start_year, end_year)
    # output_for_string = search_by_string(words_string, start_year, end_year)
    #
    print(output_for_hashtag)



