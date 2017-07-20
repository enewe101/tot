import sys
import json
import gzip

#start and end time for timestamp normalization
START_TIME = 1464539640 #2016-05-29
END_TIME = 1467390840 #2016-07-01

def read_reddit(path):
	TIME_INTERVAL = END_TIME - START_TIME
	with gzip.open(path, 'rb') as f:
		posts_raw = f.read().decode('utf-8')
	posts = json.loads(posts_raw)

	#filter for only title + self-text and timestamp
	filtered_posts = []
	for post in posts:
		tokens = post["title"] + post["selftext"]
		timestamp_fractionized = (post["created_utc"] - START_TIME)/float(TIME_INTERVAL)
		filtered_posts.append((timestamp_fractionized, tokens))

	return filtered_posts
