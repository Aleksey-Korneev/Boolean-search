import search
import index
from sys import argv
import os

if __name__ == "__main__":
    if len(argv) < 2:
        raise ValueError('Expected at least one command line argument.')
    if argv[1] == 'index': # построение индекса
        if len(argv) > 2:
            index.main(['index.py', argv[2]])
        else:
            index.main(['index.py'])
    elif argv[1] == 'search': # булев поиск
        search.main()
    elif argv[1] == 'clean': # очистка временныъ данных
        path = 'temp_dumps/' if len(argv) < 3 else argv[2]
        if path[-1] != '/':
            path += '/'
        flag = False
        for file in ('reversed_index.gz', 'word_to_id.gz', 'id_to_url.gz', 'url_to_id.gz'):
            if os.path.exists(f'{path}{file}'):
                os.system(f'rm {path}{file}')
                flag = True
        if not flag:
            raise ValueError('The directory does not contain temporary files.')
    else:
        raise ValueError('Unexpected command line argument.')
