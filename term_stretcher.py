import multiprocessing
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
from multiprocessing import Pool, Queue
from queue import Empty
from functools import partial


# basicConfig(stream=open('log.txt', 'w', encoding='utf-8'), filemode='w', level=DEBUG)

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
        print("Post {} has been processed".format(j))
    print("\nTHREAD PROCESSED\n")
    return thread


out_queue = Queue()


def filter_threads(q: Queue, out: Queue):
    while True:
        try:
            thread = q.get(True, 3)
        except Empty:
            return
        if thread == "\0":
            q.put(thread)
            return

        for post, j in zip(thread, range(len(thread))):
            tokens = post[0]

            new_term = [token for token in tokens if is_valid_term(token[1])]
            # dataset[i][j][0] = new_term
            thread[j][0] = new_term
            print("Post {} has been processed".format(j))
        print("\nTHREAD PROCESSED\n")
        out.put(thread, True, 3)


if __name__ == "__main__":

    queue = Queue()
    ds = []
    pool = Pool(3, filter_threads, (queue, out_queue, ))
    for thread in trigrams:
        queue.put(thread)
    queue.put("\0")
    while not out_queue.empty():
        ds.append(out_queue.get())

    # ds = pool.map(filter_threads, queue)
    # with Pool(3) as p:
    #     ds = p.map(filter_threads, queue)
    print("[{}] Dataset filtered".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
    dump(ds, open('./data/ds.json', 'w'), indent=4, ensure_ascii=False)
    # session = requests.Session()
    # for thread, i in zip(ds, range(len(ds))):
    #     for post, j in zip(thread, range(len(thread))):
    #         tokens = post[0]
    #         for term in tokens:
    #             result = expand_term(term[1])
    #             # if result is None:
    #             #     debug("[{}] Term '{}' skipped".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    #             #                                           term[1]))
    #             #     continue
    #             # li = []
    #             search_terms_str = ""
    #             for search_term in result:
    #                 search_terms_str += search_term + "\n"
    #                 trigrams[i][j][0].append([0.5, search_term])
    #             debug("[{}] Term '{}' expanded with:\n{}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    #                                                              term[1], search_terms_str))
    # dump(trigrams, open('./data/trigrams.json', 'w'), indent=4, ensure_ascii=False)
