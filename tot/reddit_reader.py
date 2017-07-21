import sys
import json
import gzip


def read_reddit(path):
	with gzip.open(path, 'rb') as f:
		posts_raw = f.read().decode('utf-8')
	posts = json.loads(posts_raw)

	#filter for only title + self-text and timestamp
	filtered_posts = []
	for post in posts:
		tokens = post["title"] + post["selftext"]
		filtered_posts.append((post["created_utc"], tokens))

	return filtered_posts
