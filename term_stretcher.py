import gc
import multiprocessing
import os
import sys
import time
from json import dump, load

import path
from rutermextract import TermExtractor
import re
from time import sleep
from datetime import datetime
from logging import basicConfig, debug, info, error, INFO, DEBUG
import pymorphy2  # $ pip install pymorphy2
from multiprocessing import Pool, Queue
from queue import Empty
#from memory_profiler import profile

basicConfig(stream=open('log.txt', 'w', encoding='utf-8'), filemode='w', level=DEBUG)

te = TermExtractor()

stopterms = {'республика', 'город', 'край'}

# parser = argparse.ArgumentParser(description="Filters the terms in ./data/trigrams.json")
# parser.add_argument('--n_jobs', action='store', type=int, help='processes')
n_jobs = 8

functors_pos = {'INTJ', 'PRCL', 'CONJ', 'PREP'}  # function words

cache_dir = 'cache/'


def is_valid_term(token, forbidden_words_dict: dict) -> bool:
    def is_russian(token) -> bool:
        try:
            rez = re.match(r'[а-яё ]+', token).group() == token
        except AttributeError as e:
            rez = False
        return rez

    def pos(word, morth=pymorphy2.MorphAnalyzer()):
        "Return a likely part of speech for the *word*."""
        try:
            return forbidden_words_dict[word]
        except KeyError:
            x = morth.parse(word)[0].tag.POS
            forbidden_words_dict.update({word: x})
            if n_jobs is not None:
                filename = cache_dir + 'word_cache_{}.json'.format(os.getpid())
            else:
                filename = cache_dir + 'word_cache.json'
            dump(forbidden_words_dict, open(filename.format(os.getpid()), 'w'), indent=4, ensure_ascii=False)
            return x

    try:
        assert token
        assert is_russian(token)
        assert len(token) > 3
        assert pos(token) not in functors_pos
    except AssertionError:
        return False
    return True


# def filter_dataset(dataset: list):
#     for thread, i in zip(dataset, range(len(dataset))):
#         for post, j in zip(thread, range(len(thread))):
#             tokens = post[0]
#             new_term = [token for token in tokens if is_valid_term(token[1])]
#             dataset[i][j][0] = new_term
#     return dataset

# @profile
def filter_thread(thread: list, thread_idx: int, forbidden_words_dict: dict):
    for post, j in zip(thread, range(len(thread))):
        tokens = post[0]
        thread[j][0] = [token for token in tokens if is_valid_term(token[1], forbidden_words_dict)]
        print("Post {} of thread {} has been processed".format(j, thread_idx))
    print("\nTHREAD {} PROCESSED\n".format(thread_idx))
    # return thread


# @profile
def filter_threads(q: Queue, out: Queue, forbidden_words_dict: dict):
    while True:
        try:
            tup = q.get(True, 3)
        except Empty:
            break
        if tup == "\0":
            q.put(tup)
            out.put(tup)
            break

        thread = tup[1]
        thread_idx = tup[0]
        filter_thread(thread, thread_idx, forbidden_words_dict)
        out.put(tup)
        gc.collect()
        #time.sleep(0.1)
    dump(forbidden_words_dict, open((cache_dir + 'word_cache_{}.json'.format(os.getpid())).format(os.getpid()), 'w'),
         indent=4, ensure_ascii=False)


# @profile
def dump_queue(out: Queue, temp_list: list, checkpoint_idx: int):
    # temp_list = []
    while True:
        try:
            tup = out.get()
        except Empty:
            return
        if tup == '\0':
            current_ds = load(open('./data/ds.json', 'r', encoding='utf-8'))
            current_ds.append(temp_list)
            dump(current_ds, open('./data/ds.json', 'w'), indent=4, ensure_ascii=False)
            temp_list.clear()
            current_ds = None
            print("/////////////////////{} REMAINING THREADS DUMPED!////////////////////////".format(len(temp_list)))
            return
        
        thread = tup[1]
        thread_idx = tup[0]
        temp_list.append(thread)
        if len(temp_list) >= 10:
            current_ds = load(open('./data/ds.json', 'r', encoding='utf-8'))
            current_ds.append(temp_list)
            dump(current_ds, open('./data/ds.json', 'w'), indent=4, ensure_ascii=False)
            temp_list.clear()
            current_ds = None
            checkpoint_idx = thread_idx
            with open('checkpoint.txt', 'w', encoding='utf-8') as file:
                file.write(str(checkpoint_idx))
            print("/////////////////////10 THREADS DUMPED!////////////////////////")
            
            
#def restart(start_time):
#    while True:
#        if (time.time() - start_time) >= 20: # 12 hours
#            print("[{}] Rebooting...".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
#            os.execv(__file__, sys.argv)
#            exit(0)


# @profile
def main():
#    start_time = time.time()
    # global trigrams
    trigrams = load(open('./data/trigrams.json', 'r', encoding='utf-8'))
    forbidden_words_dict = {}

    for filename in os.listdir(cache_dir):
        current_dict = load(open(cache_dir + filename, 'r', encoding='utf-8'))
        forbidden_words_dict = {**forbidden_words_dict, **current_dict}  # join
        # os.remove(cache_dir + filename)

    num_trigrams = [(i, thread) for i, thread in zip(range(len(trigrams)), trigrams)]
    trigrams = None
    temp_list = []
    gc.collect()
    
    checkpoint = 0
    try:
        with open('checkpoint.txt', 'r', encoding='utf-8') as file:
            checkpoint = int(file.read())
    except FileNotFoundError:
        pass
    
    if checkpoint != 0:
        print("[{}] Restarting from checkpoint {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), checkpoint))
    else:
        dump([], open('./data/ds.json', 'w'), indent=4, ensure_ascii=False)
    
    if n_jobs is not None:
        in_queue, out_queue = Queue(), Queue()

        pool = Pool(n_jobs, filter_threads, (in_queue, out_queue, forbidden_words_dict, ))
        out_proc = multiprocessing.Process(target=dump_queue, args=(out_queue, temp_list, checkpoint))
#        restart_proc = multiprocessing.Process(target=restart, args=(start_time, ))
        out_proc.start()
#        restart_proc.start()
        for tup in num_trigrams[checkpoint:]:
            in_queue.put(tup)
            #if tup[0] == 22:
            #    break
            while in_queue.qsize() > 12:
                time.sleep(1)
        in_queue.put("\0")
        pool.close()
        pool.join()
        out_proc.join()
        out_proc.close()

    else:
        for tup in num_trigrams:
            filter_thread(tup[1], tup[0], forbidden_words_dict)
            #if tup[0] == 22:
            #    break
        dump(forbidden_words_dict,
             open((cache_dir + 'word_cache.json'.format(os.getpid())).format(os.getpid()), 'w'),
             indent=4, ensure_ascii=False)
        # while in_queue.qsize() > 10:
        #     time.sleep(10)

    print("[{}] Dataset filtered".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))


if __name__ == "__main__":
    main()
    # dump(ds, open('./data/ds.json', 'w'), indent=4, ensure_ascii=False)
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
