"""
Authors: Andres Rivera & Shreya Modi
Purpose: Twitter API that accesses relational database (SQL)

Functions:
    - Post data (post_tweet)
    - Generate random user (gen_rand_user)
    - Getters
        - Get timeline
        - Get followers
        - Get Followees
        - Get Tweets
"""

import csv
import random
from datetime import datetime

from Assignment.DB.dbutils import DBUtils
from Assignment.timer.timer import timer  # timer wrapper


class TwitterAPI:
    """Twitter API - Make requests and post data"""

    def __init__(self, user, password, database, host="localhost"):
        self.dbu = DBUtils(user, password, database, host)

    @timer
    def post_tweet(self):
        """Read data from tweet file and upload to Tweet table"""
        # file handler - read Twitter csv and load tweets into DB

        with open("twitter_data/tweet.csv") as file:

            reader = csv.reader(file, delimiter=",")

            batch = []  # initialize batch list
            tweet_id = 0  # Tweet id auto-increment
            # SQL insert query
            sql = "INSERT INTO Tweet(tweet_id, user_id,tweet_ts, tweet_text) VALUES (%s, %s, %s, %s)"
            # Skip header
            next(reader)

            for row in reader:
                tweet_id += 1  # increment tweet_id for each row
                # Extract data from file
                date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')  # create date for each tweet
                user_id = row[0]  # unique id for each user
                text = row[1]  # user tweet
                values = (tweet_id, user_id, date, text)  # create row of data
                batch.append(values)
                # insert 5 tweets
                if len(batch) == 5:
                    self.dbu.insert_many(sql, batch)
                    batch = []
            # insert remainder
            if len(batch) > 0:
                self.dbu.insert_many(sql, batch)

    def gen_rand_user(self):
        """Generate random user given the available users in Tweet db"""
        # select query gets user_ids
        query = "SELECT user_id from Tweet;"
        # run query
        db = self.dbu.get_rows(query)
        # return first value of the tuple - an int
        return random.choice(db)[0]

    # Getter methods

    @timer
    def get_timeline(self, frame=False):
        """Return timeline of 10 users - sorted by timestamp"""
        # generate random user
        rand_user = str(self.gen_rand_user())
        print(f"Timeline for user: {rand_user}")
        # SQL query
        query = f"""
                    SELECT tweet.user_id as post_id, tweet.tweet_text as text, tweet_ts
                        FROM Follows LEFT JOIN Tweet on Follows.follows_id = tweet.user_id
                        WHERE tweet.user_id IN (
                        SELECT follows.user_id 
                        FROM follows
                        WHERE follows.follows_id = 39) 
                        ORDER BY Tweet.tweet_ts limit 10;"""

        # return dataframe if chosen
        if frame:
            cols = ["User_id", "Tweet", "Timestamp"]
            return self.dbu.get_frame(query, cols)
        data = self.dbu.get_data(query)  # run query and fetch data
        return data

    @timer
    def get_followers(self, follows_id, frame=False):
        """returns a twitter object of followers from a given user"""
        sql = f"SELECT user_id FROM Follows WHERE follows_id = {follows_id} "

        # return dataframe if chosen
        if frame:
            cols = ['User_id']
            return self.dbu.get_frame(sql, cols)
        return self.dbu.get_data(sql)

    # noinspection SpellCheckingInspection
    @timer
    def get_followees(self, user_id, frame=False):
        """returns a twitter object of followees from a given user"""
        sql = f"SELECT follows_id FROM follows WHERE user_id = {user_id}"

        # return dataframe if chosen
        if frame:
            cols = ["follows_id"]
            return self.dbu.get_frame(sql, cols)
        return self.dbu.get_data(sql)  # return raw data (twitter object)

    @timer
    def get_tweet(self, user_id, frame=False):
        sql = f"SELECT tweet_text, tweet_ts FROM Tweet WHERE user_id = {user_id}"

        # return dataframe if chosen
        if frame:
            cols = ["Tweet", "Timestamp"]
            return self.dbu.get_frame(sql, cols)
        return self.dbu.get_data(sql)  # return raw data (twitter object)
