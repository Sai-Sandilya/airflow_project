import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
import time

api_key = "ZbuhMpCJ6B6Bcei1N3TfegAIH"
api_secret_key = "RCsjMKlFhgMpEFhLBjMPGwDbsEavOpvUDbFzjGRUs6dsIUvQxo"
access_token = "1411877788324270082-AMxSgZqfuiYaCfI9GigmxgK0SimCKX"
access_token_secret = "95EVmuHy54KIqSznFM6tWnFwLfyuGs5bxQkTOEkNJgl0c"
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAGeKxAEAAAAAd9aHgJnQTFTv6EK65sUYd7yR5pM%3DhIOZ0ht9G0YTlknCeluApnHxygbeQjoXsLtbHbiwqxsdESxyJ8'

client = tweepy.Client(bearer_token=bearer_token, consumer_key=api_key, consumer_secret=api_secret_key,
                       access_token=access_token, access_token_secret=access_token_secret)

# Get user ID from username
username = 'SaiSandilya10'
user = client.get_user(username=username)
user_id = user.data.id

while True:
    try:
        # Fetch tweets
        response = client.get_users_tweets(id=user_id, tweet_fields=['created_at', 'text'], max_results=100)
        # Print tweets
        tweets = response.data if response.data else []
        for tweet in tweets:
            print(f"{tweet.created_at}: {tweet.text}")
        # Optionally, you can store tweets into a DataFrame or upload them to S3
        data = [(tweet.created_at, tweet.text) for tweet in tweets]
        df = pd.DataFrame(data, columns=['created_at', 'text'])
        print(df)
        break  # Exit the loop if successful
    except tweepy.errors.TooManyRequests as e:
        # Extract rate limit reset time from response headers
        reset_time = int(e.response.headers.get('x-rate-limit-reset'))
        wait_time = max(reset_time - int(time.time()), 0)
        print(f"Rate limit hit. Waiting for {wait_time} seconds")
        time.sleep(wait_time)

tweet_list = []
for tweet in tweets:
    refined_text = {
        "user": username,
        'text': tweet.text,
        'created_at': tweet.created_at
    }
    tweet_list.append(refined_text)

df = pd.DataFrame(tweet_list)
df.to_csv("saisandilya_twitter_data.csv", index=False)
print("hello")