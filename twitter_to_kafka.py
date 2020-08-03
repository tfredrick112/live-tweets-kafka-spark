from kafka import KafkaProducer, KafkaClient
import tweepy
import settings
import json
import string

class StdOutListener(tweepy.StreamListener):
    def on_status(self, status):
        # We do not want to analyze the retweeted version of a Tweet.
        if status.retweeted:
            return

        json_tweet = status._json
        us_state = json_tweet['place']['full_name'][-2:]
        if us_state in ['NY', 'CA']:
            try:
                #data = ''.join((filter(lambda x: x in printable, json_tweet['extended_tweet']['full_text'].replace("’", "'")))).strip()
                data = ''.join((filter(lambda x: x in printable, json_tweet['text'].replace("’", "'")))).strip()
                producer.send(us_state, data.encode('utf-8'))
                #producer.send(us_state, json_tweet['place']['full_name'].encode('utf-8'))
            except:
                pass
        return True

    def on_error(self, status_code):
        print(status_code)
        # returning False in on_data disconnects the stream
        if status_code == 420:
            return False

#kafka = KafkaClient("localhost:9092")
# producer = KafkaProducer(kafka)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
# Authorization
auth = tweepy.OAuthHandler(settings.TWITTER_APP_KEY, settings.TWITTER_APP_SECRET)
auth.set_access_token(settings.TWITTER_KEY, settings.TWITTER_SECRET)
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

printable = string.printable
stream_listener = StdOutListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(locations=[-124.409591,32.534156,-114.131211,42.009518,
                        -79.762152,40.496103,-71.856214,45.01585], languages=['en'])
