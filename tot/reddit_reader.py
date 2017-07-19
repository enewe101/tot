import sys
import json
import gzip

def read_reddit(path):
	with gzip.open(path, 'rb') as f:
		posts_raw = f.read()
	posts = json.loads(posts_raw)

	#filter for only title + self-text and timestamp
	filtered_posts = []
	for i,post in enumerate(posts):
		print(i)
		tokens = post["title"] + post["selftext"]
		filtered_posts.append((post["created_utc"], tokens))

	return filtered_posts
