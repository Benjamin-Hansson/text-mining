import praw
import requests
import json
from pymongo import MongoClient, ASCENDING
import time

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




def main():

    start_date = 1546300800 # 2019/01/01
    start_date = 1596844800

    end_date = int(time.time())
    #praw_download()
    download_wsb_daily_discussion(start_date, end_date, WEEK_IN_SECONDS)


if __name__ == '__main__':
    main()
