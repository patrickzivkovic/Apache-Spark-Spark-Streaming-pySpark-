#!/usr/bin/env python

import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

# request to get credentials at http://apps.twitter.com
consumer_key    = 'RYGP65g3D3GDblpdwR1dBcE6c'
consumer_secret = 'C95LQZl4gsJpkyPj1C9llsDJgMhpgky8451cp3aoQdIuKZ3Mug'
access_token    = '1261996151655579649-VkIn6e59IniEFT4zwMERTCE5ifF5gA'
access_secret   = 'g3hSvEszY7PLa6lLN77RaUVUJSKREHM3Hiixt3IKK7UYu'

# we create this class that inherits from the StreamListener in tweepy StreamListener
class TweetsListener(StreamListener):

    # we override the on_data() function in StreamListener
    def on_data(self, data):
        try:
            message = json.loads(data)
            print(message["text"].encode('utf-8'))
            #print(message["user"]["location"])
            #print(message["user"]["followers_count"])
            #message = json.loads(data)
            #print(message["user"]["location"])
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
    
twitter_stream = Stream(auth, TweetsListener())
twitter_stream.filter(languages=["en"], track = ["car"])
