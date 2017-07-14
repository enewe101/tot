import re
from tlds import tlds


EYES = r'[\#:;Xx8]'
HAT = r'[<>~]?'
NOSE = r'[\'-o\*]?'
MOUTH_F = r'[\[\]\|\)\(\/\\DSspPQ><xoO\*]'
MOUTH_B = r'[\[\]\|\)\(\/\\DSsqQ><xoO\*]'
TEAR = r'\'?' 

class Tokenizer(object):

    SPLITTER = re.compile(
        '((?:' + ')|(?:'.join([

            # Emoticons
            HAT + EYES + TEAR + NOSE + MOUTH_F,   # <:'-(
            MOUTH_B + NOSE + TEAR + EYES + HAT,   # )-':>
            r'[oO\^\*][_.oOv][oO\^\*]',  # o_O

            # Whitespace
            r'\s+',

            # Elipsis
            r'\.\.+'

            # Contractions
            #r"\w+n't",
            #r"\w+'ll",
            #r"\w+'ve",
            #r"\w+'re",
            #r"\w+'d",
            #r"\w+'s",
            #r"I'm",
            #r"y'all",
            r"\w+'\w+",
            r"\w+n'",
            r"o'\w*",
            r"ol'",
            r"'tis",
            r"'twas",

            # Punctuation
            r'[?!,.;:"\'\(\)\]\[]',

            # Hyperlinks
            r'https?\://\w+(?:\.\w+)+(?:/\S+)*/?',
            r'(?:\w+\.)+(?:' + '|'.join(tlds) + ')(?:/\S+)*/?(?:\s|$)',

        ]) + '))')

    ALL_WHITE = re.compile(r'\s*$')


    def split(self, text):
        return [
            s.lower() for s in self.SPLITTER.split(text) 
            if not self.ALL_WHITE.match(s)
        ]


