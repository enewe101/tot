import gzip
from datetime import datetime
import t4k
from tokenizer import Tokenizer

TWITTER_DATE_FORMAT = '%a %b %d %H:%M:%S %Y'
DEFAULT_START_TIME = datetime(2012,01,01,0,0,0)
DEFAULT_END_TIME = datetime(2017,01,01,0,0,0)


class TwitterCSVReader(object):


    def __init__(
        self, 
        start_time=DEFAULT_START_TIME, 
        stop_time=DEFAULT_END_TIME,
        tokenize=Tokenizer().split
    ):
        self.start_time = start_time
        self.stop_time = stop_time
        self.tokenize = tokenize


    def read(self, path):
        documents = []
        for line in t4k.skipfirst(gzip.open(path)):

            # Parse the line, and check if the language is english
            fields = line.strip().split('\t')
            try:
                lang, user_lang = fields[1], fields[14]
            except IndexError:
                print len(fields)
                print fields

            if lang != 'en' or user_lang != 'en':
                continue

            # Parse the datetime into a fraction, and tokenize the tweet text
            as_datetime = datetime.strptime(
                fields[0].replace(' +0000', ''), TWITTER_DATE_FORMAT)
            timestamp_as_fraction = (
                (as_datetime - self.start_time).total_seconds() / 
                (self.stop_time - self.start_time).total_seconds()
            )
            tokens = self.tokenize(fields[3])

            # Each "docuement" is a timestamp and list of tokens.
            documents.append((timestamp_as_fraction, tokens))

        return documents




