from json import dump, load
from rutermextract import TermExtractor
# from utilities import compare_phrase
import re
from time import sleep
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from logging import basicConfig, debug, info, error, INFO, DEBUG
import pymorphy2  # $ pip install pymorphy2
from multiprocessing.pool import Pool
from functools import partial


basicConfig(stream=open('log.txt', 'w', encoding='utf-8'), filemode='w', level=DEBUG)

te = TermExtractor()
trigrams = load(open('./data/trigrams.json', 'r', encoding='utf-8'))
in_data = load(open('data/pstu_qa_11554.json', 'r', encoding='utf-8'))

stopterms = {'республика', 'город', 'край'}

global session
global dataset

functors_pos = {'INTJ', 'PRCL', 'CONJ', 'PREP'}  # function words


def is_valid_term(token) -> bool:
    def is_russian(token) -> bool:
        try:
            rez = re.match(r'[а-яё ]+', token).group() == token
        except AttributeError as e:
            rez = False
        return rez

    def pos(word, morth=pymorphy2.MorphAnalyzer()):
        "Return a likely part of speech for the *word*."""
        return morth.parse(word)[0].tag.POS

    try:
        assert token
        assert is_russian(token)
        assert len(token) > 3
        assert pos(token) not in functors_pos
    except AssertionError:
        return False
    return True


def expand_term(input_term):
    try:
        request = session.post(
            'https://html.duckduckgo.com/html/',
            data={'q': input_term.replace(' ', '+')},
            headers=
            {
                'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,mn;q=0.6',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, '
                              'like Gecko) Chrome/50.0.2661.102 Safari/537.36 '
            }
        )

        x = request.content
        dom = BeautifulSoup(x, features='lxml')
        x = dom.text
        rez = [
            t for t in te(x, strings=1) if t.count(' ') > 0 and t not in stopterms and is_valid_term(t)
        ]
    except (KeyError, AssertionError):
        return None
    return tuple(rez)


def filter_dataset(dataset: list):
    for thread, i in zip(dataset, range(len(dataset))):
        for post, j in zip(thread, range(len(thread))):
            tokens = post[0]
            new_term = [token for token in tokens if is_valid_term(token[1])]
            dataset[i][j][0] = new_term
    return dataset


def filter_thread(thread: list):
    # for thread, i in zip(dataset, range(len(dataset))):
    for post, j in zip(thread, range(len(thread))):
        tokens = post[0]
        new_term = [token for token in tokens if is_valid_term(token[1])]
        # dataset[i][j][0] = new_term
        thread[j][0] = new_term
    return thread


if __name__ == "__main__":
    # pass
    # dataset = trigrams
    # filter_partial = partial(filter_dataset, trigrams)
    with Pool(12) as p:
        ds = p.map(filter_thread, trigrams)
    debug("[{}] Dataset filtered".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
    dump(ds, open('./data/ds.json', 'w'), indent=4, ensure_ascii=False)
    session = requests.Session()
    for thread, i in zip(ds, range(len(ds))):
        for post, j in zip(thread, range(len(thread))):
            tokens = post[0]
            for term in tokens:
                result = expand_term(term[1])
                # if result is None:
                #     debug("[{}] Term '{}' skipped".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                #                                           term[1]))
                #     continue
                # li = []
                search_terms_str = ""
                for search_term in result:
                    search_terms_str += search_term + "\n"
                    trigrams[i][j][0].append([0.5, search_term])
                debug("[{}] Term '{}' expanded with:\n{}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                                                                 term[1], search_terms_str))
    dump(trigrams, open('./data/trigrams.json', 'w'), indent=4, ensure_ascii=False)
