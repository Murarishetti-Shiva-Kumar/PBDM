from __future__ import absolute_import, print_function
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

consumer_key="------------"
consumer_secret="------------"
access_token="---------------"
access_token_secret="-----------------"

class StdOutListener(StreamListener):
    def on_data(self, data):
        try:
            with open('data10.json', 'a') as outfile:
                json.dump(data,outfile)
            with open('data20.json','a') as outputj:
                outputj.write(data)
            with open('tweetsdata.txt', 'a') as tweets:
                tweets.write(data)
                tweets.write('\n')
            outfile.close()
            tweets.close()
            outputj.close()
        except BaseException as e:
            print('problem collecting tweet',str(e))
        return True
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['coronavirus','chinese virus','COVID19','COVID-19','novel coronavirus','lockdown','COVIDIDIOTS','social distance','pandemic','WHO','hygenic','quarantine','isolation','sanitizers','contagious','virus','toilet rolles','community spread','spread','economy','recession','Covid19Out','gocorona',])