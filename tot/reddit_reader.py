import bz2
import t4k
import cjson
from datetime import datetime
from tokenizer import Tokenizer


DEFAULT_START_TIME = 1451606400
DEFAULT_END_TIME = 1462060799


class RedditReader(object):


    def __init__(
        self, 
        start_time_utc=DEFAULT_START_TIME, 
        stop_time_utc=DEFAULT_END_TIME,
        tokenize=Tokenizer().split,
        subreddits=None,
        limit=None
    ):
        self.subreddits = set(subreddits) if subreddits is not None else None
        self.start_time_utc = start_time_utc
        self.stop_time_utc = stop_time_utc
        self.timespan = float(stop_time_utc - start_time_utc)
        self.tokenize = tokenize
        self.limit = limit


    def read(self, path, limit=None):
        limit = limit or self.limit
        documents = []
        for lineno, line in enumerate(bz2.BZ2File(path)):

            if limit is not None and lineno >= limit:
                break

            # Parse the post json.
            post = cjson.decode(line)

            # Only consider posts from subs that we care about.  None means all.
            if (
                self.subreddits is not None 
                and post[subreddit] not in self.subreddits
            ):
                continue

            # Parse the datetime into a fraction, and tokenize the tweet text
            timestamp_as_fraction = (
                (post['created_utc'] - self.start_time_utc) / self.timespan
            )

            # Take both the title and selftext as the text
            post_text = post['title'] + ' ' + post['selftext']
            tokens = self.tokenize(post_text)

            # Each "docuement" is a timestamp and list of tokens.
            documents.append((timestamp_as_fraction, tokens))

        return documents


