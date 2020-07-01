#!/usr/bin/env python
# coding: utf-8

 # In[ ]:
 
import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

#import var
#search = var.var1
error = 0
 
#request to get credentials at http://developer.twitter.com
consumer_key    = 'RYGP65g3D3GDblpdwR1dBcE6c'
consumer_secret = 'C95LQZl4gsJpkyPj1C9llsDJgMhpgky8451cp3aoQdIuKZ3Mug'
access_token    = '1261996151655579649-VkIn6e59IniEFT4zwMERTCE5ifF5gA'
access_secret   = 'g3hSvEszY7PLa6lLN77RaUVUJSKREHM3Hiixt3IKK7UYu'
 
#we create this class that inherits from the StreamListener in tweepy StreamListener
class TweetsListener(StreamListener):
 
    def __init__(self, csocket):
        self.client_socket = csocket
    #we override the on_data() function in StreamListener
    def on_data(self, data):
        try:
            message = json.loads( data )
            print( message['text'].encode('utf-8'))
            print(message['geo']['coordinates'] if message['geo'] else None)
            print(message['user']['screen_name'].encode('utf-8'))
            print([x['text'].encode('utf-8') for x in message['entities']['hashtags']])
            
            message["geoData"] = message['geo']['coordinates'] if message['geo'] else None,
            message["textData"] = message['text'].encode('utf-8'),
            message["usernameData"] = message['user']['screen_name'].encode('utf-8'),
            message["hashtagData"] = [x['text'].encode('utf-8') for x in message['entities']['hashtags']]
            
            self.client_socket.send(message['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            error += 1
            if(error == 50):
                exit
        return True
 
    def if_error(self, status):
        print(status)
        return True

def send_tweets(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['b'])  #this is the topic we are interested in



if __name__ == "__main__":
    new_skt = socket.socket()          #initiate a socket object
    host = "127.0.0.1"      #local machine address
    port = 5555                  #specific port for your service.
    new_skt.bind((host, port))         #Binding host and port

    print("Now listening on port: %s" % str(port))

    new_skt.listen(5)                   #waiting for client connection.
    c, addr = new_skt.accept()         #Establish connection with client. it returns first a socket object,c, and the address bound to the socket

    print("Received request from: " + str(addr))
    #and after accepting the connection, we can send the tweets through the socket
    send_tweets(c)
 
# In[ ]:




