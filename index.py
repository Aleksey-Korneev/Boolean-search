from simple9 import Simple9
from search import decode_data
from sys import argv
from collections import defaultdict
from copy import deepcopy
import json
import gzip
import os

def data_preprocessing(path):
    """
    Предобработка сырых данных.
    """
    if not os.path.exists(path):
        raise RuntimeError('Указанной директории не существует.')
    if len(os.listdir(path)) == 0:
        raise RuntimeError('Указанная директория пуста.')
    dumps = list(filter(lambda file : file.endswith('.gz'), os.listdir(path)))
    data = []
    if len(dumps) == 0:
        raise RuntimeError('В указанной директории нет файлов с расширением .gz.')
    for dump in dumps:
        filename = path + dump
        with gzip.open(filename) as file:
            data.extend(file.read().decode('utf-8', errors = 'ignore').lower().split())

    words = set(data) # список всех слов в корпусе текстов
    urls = dict() # список всех сайтов
    buf = []

    def __parse_url(url):
        """
        Удаление лишних символов.
        """

        clear_url = ''
        ord_point = 46 # = ord('.')
        for c in url:
            clear_url += c
            if len(clear_url) == 4 and clear_url != 'http':
                clear_url = clear_url[1:] # удаляем первый символ (лишний)
            if ord(clear_url[-1]) < ord_point and 'http' in clear_url: # url кончился, появился лишний символ
                break
        return clear_url[:-1]

    for word in data:
        if 'http://lenta' in word: # ссылки на lenta.ru могут находиться на самих страницах lenta.ru
            if len(buf) > 2:
                urls[__parse_url(buf[0])] = buf[1:]
                words.remove(buf[0])
            buf.clear()
        buf.append(word)

    return words, urls


def encode_data(word_to_id, id_to_url, url_to_id, reversed_index,
                index_encoding = 'simple9', dict_encoding = 'json',
                path = 'temp_dumps/', check_safety = False):
    """
    Сохранение обратного индекса и дополнительных структур данных \
    по указанному пути.
    """

    if path[-1] != '/':
        path += '/'
    if not os.path.exists(path):
        os.mkdir(path)

    if index_encoding == 'simple9':
        simple9_encoder_decoder = Simple9()
        reversed_index_prepared = []
        for key, value in reversed_index.items():
            list_copy = deepcopy(value)
            for i in range(len(list_copy) - 1, 0, -1):
                list_copy[i] -= list_copy[i - 1]
            tmp_list = [key, len(list_copy)] + list_copy # [ id, n, 1st item, 2nd item, ..., nth item ]
            reversed_index_prepared.extend(tmp_list) # id, n, 1st item, 2nd item, ..., nth item, id, n, 1st item, 2nd item, ..., nth item, ...
        data_compressed = simple9_encoder_decoder.encode(reversed_index_prepared)
        with gzip.open(path + 'reversed_index.gz', 'w') as file:
            file.write(data_compressed)




    if dict_encoding == 'json':
        data_compressed = json.dumps(word_to_id).encode('utf-8')
        with gzip.open(path + 'word_to_id.gz', 'w') as file:
            file.write(data_compressed)

        data_compressed = json.dumps(id_to_url).encode('utf-8')
        with gzip.open(path + 'id_to_url.gz', 'w') as file:
            file.write(data_compressed)

        data_compressed = json.dumps(url_to_id).encode('utf-8')
        with gzip.open(path + 'url_to_id.gz', 'w') as file:
            file.write(data_compressed)


def make_index(path):
    """
    Создание обратного индекса и дополнительных структур данных.
    """

    words, urls = data_preprocessing(path)

    word_to_id = { word : id_ for (id_, word) in enumerate(words) }    # словарь { слово : id слова}
    id_to_url = { id_ : url for (id_, url) in enumerate(urls.keys()) } # словарь { id документа : url сайта }
    url_to_id = { url : id_ for (id_, url) in enumerate(urls.keys()) } # словарь { url сайта : id документа }

    reversed_index = defaultdict(list) # словарь { id слова : список id документов }
    for id_, (url, words_list) in enumerate(urls.items()):
        for word in words_list:
            reversed_index[word_to_id[word]].append(url_to_id[url])

    return word_to_id, id_to_url, url_to_id, reversed_index

def make_path(argv):
    """
    Обработка пути до директории с дампом lenta.ru
    """

    if len(argv) > 1:
        path = argv[1]
        if path[-1] != '/':
            path += '/'
    else:
        path = 'dumps/'
    return path


def main(argv):
    path = make_path(argv)
    word_to_id, id_to_url, url_to_id, reversed_index = make_index(path)
    encode_data(word_to_id, id_to_url, url_to_id, reversed_index,
                path = 'temp_dumps/', check_safety = True)

if __name__ == '__main__':
    main(argv)
