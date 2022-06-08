import json
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="12345678",
  database= "twitter"
)

keys = []

def main():
    lines = 0
    num_tweets = 0
    num_retweets = 0
    print_tweet = 0
    print_retweet = 0
    tweets = {}
    users = 0
    global keys

    mycursor = mydb.cursor()
    # mycursor.execute("CREATE TABLE USER (id BIGINT, name VARCHAR(255), screen_name VARCHAR(255), location VARCHAR(255), " \
    #                           "url VARCHAR(255), description VARCHAR(255), translator_type VARCHAR(255), protected boolean default 0, verified boolean default 0," \
    #                           "followers_count BIGINT, friends_count BIGINT,listed_count BIGINT,favourites_count BIGINT,statuses_count BIGINT)")

    # insert path and replace name of the file below as needed
    with open("corona-out-2", "r") as f1:
        print("database connection completed")
        for line in f1:
            try:
                data = json.loads(line)
                lines = lines + 1
                if data['user']:
                    try:
                        users+=1
                        sql = "INSERT INTO User (id, name, screen_name, location, " \
                              "url, description, translator_type, protected, verified," \
                              "followers_count, friends_count,listed_count,favourites_count,statuses_count)" \
                              "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                        val = (data['user']['id'], data['user']['id_str'], data['user']['name'], data['user']['screen_name'], data['user']['location'],
                               data['user']['url'], data['user']['description'],data['user']['translator_type'], data['user']['protected'],
                               data['user']['verified'], data['user']['followers_count'],
                               data['user']['friends_count'], data['user']['listed_count'], data['user']['favourites_count'],
                               data['user']['statuses_count'])

                        print(int(data['user']['id']), int(data['user']['id_str']), data['user']['name'], data['user']['screen_name'], data['user']['location'],
                               data['user']['url'], data['user']['description'],data['user']['translator_type'], data['user']['protected'],
                               data['user']['verified'], int(data['user']['followers_count']),
                               int(data['user']['friends_count']), int(data['user']['listed_count']), int(data['user']['favourites_count']),
                               int(data['user']['statuses_count']))

                        mycursor.execute(sql, val)
                        print("Insertion sCompletef:")
                    except mysql.connector.Error as err:
                        print("Something went wrong: {}".format(err))


            #     if (data['text'].startswith('RT')):
            #         num_retweets += 1
            #         if (print_retweet < 2):
            #             print_retweet += 1
            #             # print('User-- ', data['user ']['id'], '\n')
            #
            #             # mycursor.execute("CREATE TABLE user (id BIGINT, name VARCHAR(255), screen_name VARCHAR(255),location VARCHAR(255),"
            #             #                  " url VARCHAR(255), description VARCHAR(255), translator_type VARCHAR(255),protected boolean default 0,verified boolean default 0,"
            #             #                   "followers_count int, friends_count int,listed_count int,favourites_count int,statuses_count int)")
            #
            #
            #     else:
            #         num_tweets += 1
            #         # if (print_tweet < 2):
            #         #     print_tweet += 1
            #         #     print('TWEET\n', 'id--', data['id'], 'text--', data['text'], '\n')
            #         #     print(data.keys())
            #         #     print('User-- ', data['user'], '\n')
            #         #
            #
            except:
                # if there is an error loading the json of the tweet, skip
                continue
        print("Commiting")
        mydb.commit()

    print("number of users", users)

if __name__ == '__main__':
    main()