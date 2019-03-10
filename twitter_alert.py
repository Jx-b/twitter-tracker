from tweepy import API
from tweepy import Cursor
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

import twitter_credentials
import twitter_search
import mailgun_credentials

import pandas as pd

import requests

import os
PATH = os.path.dirname(os.path.realpath(__file__))+'/'

### TWITTER AUTHENTICATOR ###
class TwitterAuthenticator():

    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.API_KEY, twitter_credentials.API_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_SECRET)
        return auth

### TWITTER CLIENT ###
class TwitterClient():
    def __init__(self, twitter_user= None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.api = API(self.auth)
        self.twitter_user = twitter_user

    #def get_twitter_client_api(self):
    #    return self.api

    #def get_user_timeline_tweets(self, num_tweets):
    #    tweets = []
    #    for tweet in Cursor(self.api.user_timeline, id= self.twitter_user).items(num_tweets):
    #        tweets.append(tweet)
    #   return tweets

    #def get_user_home_timeline_tweets(self, num_tweets):
    #    home_tweets = []
    #    for tweet in Cursor(self.api.home_timeline, id= self.twitter_user).items(num_tweets):
    #        home_tweets.append(tweet)
    #    return home_tweets

### TWITTER STREAMER ###
class TwitterStreamer():
    """
    Class for streaming and processing live tweets
    """
    def __init__(self):
        self.twitter_authenticator = TwitterAuthenticator()
    def stream_tweets(self, fetched_tweets_filename, hashtag_list):
        #This handles Twitter authentification and the connection to the Twitter Streaming API
        listener = TwitterListener(fetched_tweets_filename)
        #Authentificate
        auth = self.twitter_authenticator.authenticate_twitter_app()
        #Set stream
        stream = Stream(auth,listener)
        # Add filter
        stream.filter(track=hashtag_list)

### TWITTER LISTENER ###
class TwitterListener(StreamListener):
    """
    This is a basic listener class that just prints received tweets to stdout
    """
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print(data)
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))

    def on_error(self, status):
        if status == 420:
            # Returning False on_data method in case rate limit occurs.
            return False
        print(status)

### TWEET TRACKER ###
class TweetTracker():
    """
    Functionality for tracking number of new tweets and adding them to the records
    """
    def __init__(self):
        self.filename = 'tweet_search_results.xlsx'
        try:
            self.old_tweets = pd.read_excel(PATH+self.filename, converters={'id_str':str}) #converter to avoid pd to read id_str as int64
        except FileNotFoundError:
            self.old_tweets = None

    def get_new_tweets(self, statuses):
        tweets_json = [tweet._json for tweet in statuses]
        df = pd.DataFrame(data=tweets_json)
        if self.old_tweets is None:
            return df
        else:
            df = df[~df['id_str'].isin(self.old_tweets['id_str'])]
            return df

    def add_new_tweets(self, statuses):
        new_tweets = self.get_new_tweets(statuses)
        if self.old_tweets is None:
            df = new_tweets
        else:
            df = pd.concat([new_tweets, self.old_tweets], sort=False, ignore_index=False)
        df.to_excel(PATH+self.filename)
        return df

### NOTIFIER ###
class Notifier():
    """
    Class for sending email alerts via Mailgun services
    """
    def send_alert(self, query, num_tweets, tweets):
        return requests.post(
                "https://api.mailgun.net/v3/"+mailgun_credentials.DOMAIN+"/messages",
                auth=("api", mailgun_credentials.API),
                data={"from": "Twitter tracker <mailgun@"+mailgun_credentials.DOMAIN+">",
                  "to": [mailgun_credentials.DEST_EMAIL],
                  "subject": str(num_tweets)+" new tweets for query: "+ str(query),
                  "text":'\n'.join([str(ind+1)+'\n'+tweet for ind, tweet in enumerate(tweets.full_text)])
                  })

if __name__ == "__main__":

    #### Stream tweets ###
    #Specify keywords
    #hashtag_list = twitter_search.HASHTAGS
    #name of file to write tweets
    #fetched_tweets_filename = "tweet.json"
    #twitter_streamer = TwitterStreamer()
    #twitter_streamer.stream_tweets(fetched_tweets_filename, hashtag_list)

    ### Search tweets ###
    twitter_client = TwitterClient()
    api = twitter_client.api
    # from user
    #statuses = api.user_timeline(screen_name= "realDonaldTrump", count= 10)
    # by query
    query = twitter_search.QUERY
    max_tweets = twitter_search.MAX_TWEETS
    statuses = [status for status in Cursor(api.search, q=query, tweet_mode='extended').items(max_tweets)]

    ### Analyze tweets ###
    tweet_tracker = TweetTracker()
    new_tweets = tweet_tracker.get_new_tweets(statuses)
    tweet_tracker.add_new_tweets(statuses)

    if not new_tweets.empty:
        #add new tweets to the record
        tweet_tracker.add_new_tweets(statuses)
        
        ### Send alert ###
        notifier = Notifier()
        notifier.send_alert(query= query, num_tweets= len(new_tweets), tweets= new_tweets)
    else:
        print("no new tweets")