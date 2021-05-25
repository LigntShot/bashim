from json import dump, load
from rutermextract import TermExtractor
from utilities import compare_phrase
import re
from time import sleep
import requests
from bs4 import BeautifulSoup

te = TermExtractor()
trigrams = load(open('./data/trigrams.json', 'r'))
in_data = load(open('data/pstu_qa_11554.json', 'r', encoding='utf-8'))
th = None
t = None

def compare(S1, S2):
    ngrams = [S1[i:i+3] for i in range(len(S1))]
    count = 0
    for ngram in ngrams:
        count += S2.count(ngram)

    return count/max(len(S1), len(S2))


def speak(msg: str):
    q_message = msg
    q_terms = set(te(q_message, strings=1, nested=1))

    candidates = []

    for ngram in trigrams:
        for part in ngram:
            message, terms = part[1], set(part[1])
            # l1 = compare(message.lower(), q_message.lower())
            similarity = compare_phrase(message, q_message)
            # if message == 'можно':
            #     pass
            # try:
            #     l2 = len(terms & q_terms) / max(len(terms), len(q_terms))
            # except ZeroDivisionError:
            #     l2 = 0
            # print()
            candidates += [(similarity, message, ngram)]

    out = sorted(candidates, key = lambda x: x[0])[-1]
    return out[1]


def check(reply: str):
    global t
    global th
    out = str(reply)
    print("bashim: " + out + '\n')


if __name__ == "__main__":
    for dialog in in_data:
        print("user: ", dialog['question'])
        reply = speak(dialog['question'])
        print("answer: ", dialog['answer'])
        check(reply)

