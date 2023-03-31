#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key= "SjnqAuGbRAkZoCmu5rfWxNbuz"
consumer_secret= "6MSXAIxVkdLilxfD6AJRYd4LD8apOwJtcNfdWJow1rqwnOEqsb"
access_token= "2607308140-mD2H1rsI0QHMMy8FIJfyGtDRutlsBgIwTlPPwNW"
access_secret= "Jg23ZGjgVWRtceaBiYi3ErB5yCjkkG2hTrqxcM4M5s5nl" 

class TweetsListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket
 
    def on_data(self, data):
        try:
            print(data.split('\n'))
            byt=data.encode()
            self.client_socket.send(byt)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True
 
def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
 
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(languages=["en"],track=['Atlanta'])
 
if __name__ == "__main__":
    s = socket.socket()     # Create a socket object
    host = "localhost"      # Get local machine name
    port = 5555             # Reserve a port for your service.
    s.bind((host, port))    # Bind to the port
 
    print("Listening on port: %s" % str(port))
 
    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.
 
    print("Received request from: " + str(addr))

    sendData(c)
