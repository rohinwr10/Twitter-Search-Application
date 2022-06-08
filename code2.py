import json
import mysql.connector
from datetime import datetime
from datetime import timedelta

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="12345678",
  database= "twitter"
)

def main():
    lines = 0
    num_tweets = 0
    num_retweets = 0
    print_tweet = 0
    print_retweet = 0
    tweets = {}

    users = []

    # insert path and replace name of the file below as needed
    with open("corona-out-2", "r") as f1:
        print("database connection completed")
        mycursor = mydb.cursor()

        for line in f1:

            try:
                data = json.loads(line)
                lines = lines + 1
                val = (
                        data['user']['id'], data['user']['name'], data['user']['screen_name'], data['user']['location'],
                        data['user']['url'], data['user']['description'], data['user']['translator_type'],
                        data['user']['protected'],
                        data['user']['verified'], data['user']['followers_count'], data['user']['friends_count'],
                        data['user']['listed_count'], data['user']['favourites_count'], data['user']['statuses_count'], datetime.strptime(data['user']['created_at'], '%a %b %d %H:%M:%S %z %Y'))
                users.append(val)
                if (data['text'].startswith('RT')):
                    num_retweets += 1
                    # print out some fields of one retweet
                    # note that you should look at other fields too
                    if (print_retweet < 2):
                        print_retweet += 1
                else:
                    num_tweets += 1
                    # print out some fields of one tweet
                    # note that you should look at other fields too
                    if (print_tweet < 2):
                        print_tweet += 1
                        # print('TWEET\n', 'id--', data['id'], 'text--', data['text'], '\n')
                        # print('User-- ', data['user'], '\n')

                if (data['id_str'] not in tweets):
                    tweets[data['id_str']] = data
            except:
                # if there is an error loading the json of the tweet, skip
                continue

    with open("corona-out-3", "r") as f1:
        for line in f1:

            try:
                data = json.loads(line)
                lines = lines + 1
                val = (
                        data['user']['id'], data['user']['name'], data['user']['screen_name'], data['user']['location'],
                        data['user']['url'], data['user']['description'], data['user']['translator_type'],
                        data['user']['protected'],
                        data['user']['verified'], data['user']['followers_count'], data['user']['friends_count'],
                        data['user']['listed_count'], data['user']['favourites_count'], data['user']['statuses_count'], datetime.strptime(data['user']['created_at'], '%a %b %d %H:%M:%S %z %Y'))
                users.append(val)
                if (data['text'].startswith('RT')):
                    num_retweets += 1
                    if (print_retweet < 2):
                        print_retweet += 1
                else:
                    num_tweets += 1
                    if (print_tweet < 2):
                        print_tweet += 1

                if (data['id_str'] not in tweets):
                    tweets[data['id_str']] = data
            except:
                # if there is an error loading the json of the tweet, skip
                continue

        print("Tweers",num_tweets, "retweets",num_retweets)
        userMap = {}
        for user in users:
            if user[0] in userMap:
                if userMap[user[0]][14] < user[14]:
                    userMap[user[0]] = user
            else:
                userMap[user[0]] = user

        print(len(userMap))
        for id in userMap:
            user = userMap[id]
            try:
                # print("Insertion started")
                sql = "INSERT INTO user (id, name, screen_name,location, url , description, translator_type ,protected,verified," \
                      "followers_count, friends_count,listed_count,favourites_count,statuses_count, created_at) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s, %s)"


                val = (user[0], user[1], user[2], user[3], user[4], user[5], user[6], user[7], user[8], user[9], user[10], user[11]
                       , user[12], user[13], user[14])
                # print("val:", val)
                mycursor.execute(sql, val)
            except mysql.connector.Error as err:
                print(" 2   Something went wrong: {}".format(err))


        print("Commiting")
        mydb.commit()
    # print('num of lines=', lines, 'num of tweets=', num_tweets, 'num of retweets=', num_retweets)
    # print('num of unique tweets/retweets=', len(tweets.keys()))


# select id,name,screen_name,verified from user
# where name like ("j%") or screen_name like ("j%") group by 1,2,3,4;
#
# select location,id,name,screen_name,verified from user group by 1,2,3,4,5;
#
# select id,name,screen_name,verified from user where verified = 1;

def search_by_name(name):
    print("Opening database connection")
    mycursor = mydb.cursor()
    try:
        print("Select by name or screen name starting")
        sql = "SELECT id,name,screen_name,verified from USER where name like CONCAT(%s,'%') OR screen_name like CONCAT(%s,'%') group by 1,2,3,4"
        val = (name,name,)
        mycursor.execute(sql, val)
        results = mycursor.fetchall()
        print(results)
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))
    print("Search Commited")
    return results

def search_by_location(location):
    print("Opening database connection")
    mycursor = mydb.cursor()
    try:
        print("Select by location starting")
        sql = "SELECT location,id,name,screen_name,verified from USER where location like CONCAT(%s,'%') group by 1,2,3,4,5"
        val = (location,)
        mycursor.execute(sql, val)
        results = mycursor.fetchall()
        print(results)
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))
    print("Search Commited")
    return results

def search_by_verified(status):
    print("Opening database connection")
    mycursor = mydb.cursor()
    try:
        print("Select by location starting")
        sql = "SELECT id,name,screen_name,verified from USER where verified = %s"
        val = (1 if status else 0,)
        mycursor.execute(sql, val)
        results = mycursor.fetchall()
        print(results)
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))
    print("Search Commited")
    return results


# def search_by_hash_tag(hashtag):
#     name = hashtag[1:]
#     return search_by_name(name)
#

if __name__ == '__main__':
    # while True:
    #     print("Please see the search options below")
    #     print("1 for search by user name, 2 for search by hashtag, 3 to quit")
    #     choice = int(input())
    #     if choice == 1:
    #         print("Please enter the search string:")
    #         name = input()
    #         results = search_by_name(name)
    #         for r in results:
    #             print(r)
    #         print("****************Search complete****************")
    #     elif choice == 2:
    #         print("Please enter the hashtag:")
    #         hashtag = input()
    #         results = search_by_hash_tag(hashtag)
    #         for r in results:
    #             print(r)
    #         print("****************Search complete****************")
    #     elif choice == 3:
    #         break
    #     else:
    #         print("Please enter correct choice !")
    # # search_by_name("j")
    # # search_by_location("i")
    # # search_by_verified(True)
    # # search_by_hash_tag("#j")

    main()


    #sudo mongod --dbpath /System/Volumes/Data/data/db