import praw
import requests
import json
import pandas
from pymongo import MongoClient, ASCENDING
import time
from alpha_vantage.timeseries import TimeSeries


subreddits = ["stocks", "investing"]

client = MongoClient('localhost', 27017)
db = client["text_mining"]
mongo_posts = db["posts"]
mongo_comments = db["comments"]

WEEK_IN_SECONDS= 604800


def setup_database():
    mongo_comments.create_index([("post_id", ASCENDING), ("id", ASCENDING)], unique=True)


def download_wsb_daily_discussion(start_timestamp, end_timestamp, increment):

    for i in range(start_timestamp, end_timestamp, increment):
        url = f"https://api.pushshift.io/reddit/submission/search/?q=flair_name%3A'Daily%20Discussion'&after={str(i)}&before={str(i+increment)}&sort_type=created_utc&sort=asc&subreddit=wallstreetbets"
        print("Downloading posts...")
        response = requests.get(url)

        json_response = json.loads(response.text)
        print(f"Done downloading, found {len(json_response['data'])} posts")
        for post in json_response["data"]:
            mongo_posts.delete_many({"id": post["id"]})

        if json_response["data"]:
            mongo_posts.insert_many(json_response["data"])


        """
        for post in json_response["data"]:
            print("-"*20)
            print(f"processing post: {post['full_link']}")
            print("Downloading comments...")
            print(f"Expect {post['num_comments']} comments")
            comments_json = requests.get(f"https://api.pushshift.io/reddit/comment/search/?link_id={post['id']}&limit={post['num_comments']}")
            comments = json.loads(comments_json.text)
            post_id = post["id"]

            # Delete all comments from this post if they exist to prevent duplicates
            mongo_comments.delete_many({"post_id": post_id})

            print(f"Found {len(comments['data'])} comments")

            for comment in comments["data"]:
                comment["post_id"] = post_id
            if comments["data"]:
                mongo_comments.insert_many(comments["data"])
"""

def praw_download():
    reddit = praw.Reddit(
        client_id="SB_FU-9Jegya9Q",
        client_secret="tiAJicBztlq-pEBSdmsBWZUDJhGMqA",
        user_agent="my user agent"
    )
    for submission in reddit.subreddit("wallstreetbets").search("what are your moves"):
        all_comment = getAll(reddit, submission.id)
        print(all_comment)
        #print(submission.title)
        break


def getAll(r, submissionId, verbose=True):
  submission = r.submission(submissionId)
  comments = submission.comments
  commentsList = []
  for comment in comments:
    getSubComments(comment, commentsList, verbose=verbose)
  return commentsList


def getSubComments(comment, allComments, verbose=True):
  allComments.append(comment)
  if not hasattr(comment, "replies"):
    replies = comment.comments()
    if verbose: print("fetching (" + str(len(allComments)) + " comments fetched total)")
  else:
    replies = comment.replies
  for child in replies:
    getSubComments(child, allComments, verbose=verbose)



def download_alpha_vantage():
    ts = TimeSeries(key="M8RF5RFRF8PG32I1", output_format='pandas')
    res = ts.get_daily_adjusted(symbol="SPY", outputsize="full")[0]

    res = res.drop(["1. open", "2. high", "3. low", "4. close", "6. volume", "7. dividend amount", "8. split coefficient"], axis=1)
    res = res.rename(columns={'5. adjusted close': "close"})
    print(res.columns)
    print(res)


def main():
    from alpha_vantage.timeseries import TimeSeries
    import pandas
    ts = TimeSeries(key="M8RF5RFRF8PG32I1", output_format='pandas')
    res = ts.get_daily_adjusted(symbol="SPY", outputsize="full")[0]
    res = res.drop(
        ["1. open", "2. high", "3. low", "4. close", "6. volume", "7. dividend amount", "8. split coefficient"], axis=1)
    res = res.rename(columns={'5. adjusted close': "close"})
    from datetime import datetime, timedelta

    one_day_offset = timedelta(days=1)
    post_ids = mongo_posts.distinct('id')
    for post_id in post_ids:
        comments = mongo_comments.find({"post_id": post_id})
        countr = 0

        for comment in comments:
            countr += 1
            start_day = datetime.fromtimestamp(comment["created_utc"]).date()

            date = datetime.fromtimestamp(comment["created_utc"]).date() + one_day_offset
            while res.loc[res.index == str(date)].empty:
                date = date + one_day_offset
            next_trading_day = date

            date = start_day
            while res.loc[res.index == str(date)].empty:
                date = date - one_day_offset
            previous_trading_day = date
            next_close = float(res.loc[res.index == str(next_trading_day)]["close"])
            prev_close = float(res.loc[res.index == str(previous_trading_day)]["close"])

            if next_close >= prev_close:
                positive_day = True
            else:
                positive_day = False
            if countr % 1000 == 0:
                print(f"comment day: {start_day}, next trading day: {next_trading_day}, positive_day: {positive_day}")

            mongo_comments.update_one({"id": comment["id"], "post_id": post_id},
                                      {"$set": {"next_trading_day": str(next_trading_day)}})

            """mongo_comments.update_one({"id": comment["id"], "post_id": post_id},
                                      {"$set": {"spy_closing_price": next_close}})

            mongo_comments.update_one({"id": comment["id"], "post_id": post_id},
                                      {"$set": {"next_trading_day_positive": positive_day}})
            """

if __name__ == '__main__':
    main()
