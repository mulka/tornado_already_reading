import os
import time
import urllib
import logging
from datetime import timedelta

import tornado
import tornado.ioloop
import tornado.web
from tornado.options import define, options, parse_command_line

import tweetstream

define("port", default=8888, help="run on the given port", type=int)

def create_tweetstream_config():
    return {
        "twitter_consumer_key": os.environ["TWITTER_CONSUMER_KEY"],
        "twitter_consumer_secret": os.environ["TWITTER_CONSUMER_SECRET"],
        "twitter_access_token": os.environ["TWITTER_ACCESS_TOKEN"],
        "twitter_access_token_secret": os.environ["TWITTER_ACCESS_TOKEN_SECRET"],
    }

class TweetIngester(object):
    rooms = set()
    stream = tweetstream.TweetStream(create_tweetstream_config())
    def tweetstream_callback(self, tweet):
        time.sleep(1)

    def restart_stream(self):
        track = ','.join(['#' + room for room in self.rooms])
        self.stream.fetch(
            "/1.1/statuses/filter.json?" + urllib.urlencode({'track': track}),
            callback=lambda tweet: self.tweetstream_callback(tweet)
        )

    def init_room_stream(self, room):
        if room in self.rooms:
            return

        self.rooms.add(room)

        self.restart_stream()
    
tweet_ingester = TweetIngester()

def main():
    parse_command_line()

    ioloop = tornado.ioloop.IOLoop.instance()

    ioloop.add_timeout(timedelta(seconds=1), lambda: tweet_ingester.init_room_stream('follow'))
    ioloop.add_timeout(timedelta(seconds=7), lambda: tweet_ingester.init_room_stream('retweet'))

    ioloop.start()


if __name__ == "__main__":
    main()
