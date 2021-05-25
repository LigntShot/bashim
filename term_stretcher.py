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

stopterms = {'республика', 'город', 'край'}

def expand_term(input_term):
    def is_russian(term):
        try:
            rez = re.match(r'[а-яё ]+', term).group() == term
        except AttributeError as e:
            rez = False
        return rez

    try:
        # rez = trigrams[input_term]
        assert input_term
        assert is_russian(input_term)
        assert len(input_term) > 3

        sleep(1)
        x = requests.post(
            'https://html.duckduckgo.com/html/',
            data={'q': input_term.replace(' ', '+')},
            headers={
                'accept-language': 'ru-RU,en;q=0.9',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:32.0) Gecko/20100101 Firefox/32.0', }
        ).content
        dom = BeautifulSoup(x, features='lxml')
        x = dom.text
        open('/tmp/%s.html' % input_term[0:30], 'w').write(dom.prettify())
        rez = [
            t for t in te(x, strings=1) if t.count(' ') > 0 and t not in stopterms and is_russian(t)
        ]
    except (KeyError, AssertionError):
        return None
    return tuple(rez)


if __name__ == "__main__":
    # pass
    for thread, i in zip(trigrams, range(len(trigrams))):
        for post, j in zip(thread, range(len(thread))):
            tokens = post[0]
            result = []
            for term, k in zip(tokens, range(len(tokens))):
                result = expand_term(post[1])
            if result is None:
                continue
                # li = []
            for search_term in result:
                # li.append([0.5, search_term])
                trigrams[i][j][0].append([0.5, search_term])
            pass
                # trigrams[i][j][0].append(li)
                # trigrams.update({input_term: rez})

    dump(trigrams, open('./data/trigrams.json', 'w'), indent=4, ensure_ascii=False)