import json


with open("California.json") as json_file:
    data = json.load(json_file)

print(data)
try:
    hashtags = ["#" + hashtag["text"] for hashtag in data['entities']['hashtags']]
        # Tweets without hashtags are a waste of time
    if len(hashtags) != 0:
        print(hashtags)
except KeyError:
        print("error")
