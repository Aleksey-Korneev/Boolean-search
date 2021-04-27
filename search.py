from simple9 import Simple9
from random import randint
from collections import defaultdict
from copy import deepcopy
from sys import stdin
import gzip
import json

nodes_idx = set() # множество индексов вершин дерева поиска


def boolean_search(query, word_to_id, id_to_url, url_to_id, reversed_index):
    """
    Поиск документов, удовлетворяющих булевому запросу.
    """

    search_tree = dict()

    def __streaming_tree_processing(search_tree, root, reversed_index, word_to_id):
        """
        Потоковая обработка дерева.
        """

        def __binary_search(list_, key, left = 0, right = -100): # можно еще подумать над выбором константы,
                                                                 # 100 кажется оптимальным значением
            """
            Бинарный поиск.
            """
            if right == -100:
                right = len(list_) - 1
            if left == len(list_):
                return left
            if left > right:
                return left
            mid = ((left + right) // 2)
            if list_[mid] < key:
                return __binary_search(list_, key, mid + 1, right)
            elif list_[mid] > key:
                return __binary_search(list_, key, left, mid - 1)
            return mid

        idx_res = -1
        res = []

        def __run_search(ptr, idx_res):
            """
            Рекурсивный обход дерева.
            """
            node = search_tree[ptr]
            if node[0] == 'binary':
                if node[1] == '&':
                    return max(
                        __run_search(node[2], idx_res),
                        __run_search(node[3], idx_res)
                    )
                if node[1] == '|':
                    return min(
                        __run_search(node[2], idx_res),
                        __run_search(node[3], idx_res)
                    )

            elif node[0] == 'unary':
                if node[1] == '!':
                    tmp = __run_search(node[2], idx_res)
                    return (tmp + 1) if tmp == idx_res else idx_res

            elif node[0] == 'operand':
                docs = reversed_index[node[1]]
                node[2] = __binary_search(docs, idx_res, left = node[2])
                if node[2] == len(docs):
                    return float('inf')
                elif docs[node[2]] >= idx_res:
                    return docs[node[2]]

        while idx_res < max_idx_res:
            query_out = __run_search(root, idx_res)
            if idx_res == query_out:
                res.append(idx_res)
                idx_res += 1
            else:
                if idx_res < query_out:
                    idx_res = query_out
        return res

    def __create_node(stack, item, item_type):
        new_idx = __get_unique_idx()
        if item_type == 'binary':
            node = { new_idx : [ item_type, item, stack[-2], stack[-1] ] }
            del stack[-2:]
        elif item_type == 'unary':
            node = { new_idx : [ item_type, item, stack[-1] ] }
            stack.pop()
        elif item_type == 'operand':
            node = { new_idx : [ item_type, item, 0 ] }

        stack.append(new_idx)
        search_tree.update(node)

        return stack


    def __transform_query(query):
        """
        Преобразование запроса в список строк трех типов: \
        скобки, операнды и операции над множествами (унарные и бинарные).
        """
        res = []
        token = ""

        for c in query.lower():
            if c.isalpha(): # часть операнда
                token += c
            else:
                if token.strip(): # операнд считан целиком, лишние символы удалены
                    res.append(token)
                token = ""
                if c.strip(): # операция над множествами
                    res.append(c)
        if token.strip(): # последний/-ая операнд / операция над множеством
            res.append(token)
        return res

    def __reverse_polish_notation(query_list):
        """
        Перевод запроса в обратную польскую нотацию.
        """
        rpn = []
        stack = []
        priorities = {
            '(' : 1,
            '|' : 2,
            '&' : 3,
            '!' : 4
        } # приоритеты операций над множествами

        for item in query_list:
            if item == '(':
                stack.append('(')
            elif item in priorities.keys():
                while len(stack) > 0 and priorities[stack[-1]] >= priorities[item]:
                    rpn.append(stack[-1])
                    stack.pop()
                stack.append(item)
            elif item == ')':
                while stack[-1] != '(':
                    rpn.append(stack[-1])
                    stack.pop()
                stack.pop()
            else:
                rpn.append(item)

        rpn.extend(reversed(stack))

        return rpn

    def __get_unique_idx():
        global nodes_idx

        new_idx = randint(0, len(word_to_id))
        while new_idx in nodes_idx:
            new_idx = randint(0, len(word_to_id))
        nodes_idx.add(new_idx)
        return new_idx

    # получаем query_list
    query_list = __transform_query(query)

    rpn = __reverse_polish_notation(query_list)

    stack = []

    for item in rpn:
        if item == '!':
            stack = __create_node(stack, item, 'unary')
        elif item == '&' or item == '|':
            stack = __create_node(stack, item, 'binary')
        else:
            stack = __create_node(stack, word_to_id[item], 'operand')

    root = stack[0]

    max_idx_res = max(url_to_id.values())
    doc_idx = __streaming_tree_processing(search_tree, root, reversed_index, word_to_id)
    output = []
    for id_ in doc_idx:
        if id_ >= 0:
            output.append(id_to_url[id_])

    return output


def decode_data(index_encoding = 'simple9', dict_encoding = 'json', path = 'temp_dumps/'):
    """
    Извлечение обратного индекса и дополнительных структур данных \
    по указанному пути.
    """

    if index_encoding == 'simple9':
        simple9_encoder_decoder = Simple9()
        with gzip.open(path + 'reversed_index.gz', 'r') as file:
            reversed_index_prepared = simple9_encoder_decoder.decode(file.read())
        reversed_index = defaultdict(list)
        flag = 0 # тип декодируемого элемента:
                 # 0 - id слова
                 # 1 - n
                 # 2 - список id документов
        for item in reversed_index_prepared:
            if flag == 0:
                key = item
                flag += 1
            elif flag == 1:
                n = item
                cur_list = []
                flag += 1
            else:
                n -= 1
                if len(cur_list) > 0:
                    cur_list.append(item + cur_list[-1])
                else:
                    cur_list.append(item)

                if n == 0:
                    reversed_index[key] = deepcopy(cur_list)
                    flag = 0

    if dict_encoding == 'json':
        with gzip.open(path + 'id_to_url.gz', 'r') as file:
            data_decompressed = json.loads(file.read().decode('utf-8'))
            id_to_url = { int(key) : value for key, value in data_decompressed.items() }
        with gzip.open(path + 'url_to_id.gz', 'r') as file:
            data_decompressed = json.loads(file.read().decode('utf-8'))
            url_to_id = { key : int(value) for key, value in data_decompressed.items() }
        with gzip.open(path + 'word_to_id.gz', 'r') as file:
            data_decompressed = json.loads(file.read().decode('utf-8'))
            word_to_id = { key : int(value) for key, value in data_decompressed.items() }

    return word_to_id, id_to_url, url_to_id, reversed_index


def run_boolean_search(word_to_id, id_to_url, url_to_id, reversed_index):
    for query in stdin:
        documents = boolean_search(query.strip(), word_to_id, id_to_url, url_to_id, reversed_index)
        print(query.strip())
        print(len(documents))
        print(*documents, sep = '\n', end = '\n\n')


def main():
    word_to_id, id_to_url, url_to_id, reversed_index = decode_data()
    run_boolean_search(word_to_id, id_to_url, url_to_id, reversed_index)


if __name__ == '__main__':
    main()
