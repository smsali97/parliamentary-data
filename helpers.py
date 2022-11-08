import logging
import urllib
import urllib.request
from socket import timeout
from urllib.error import HTTPError, URLError
from threading import Thread
from random import randint
from time import sleep
    

import re
def format_text(text):
    pat1 = r'\n'
    pat2 = r'\t'
    pat3 = r'§'
    pat4 = r'[ \t]{2,}'
    patterns = [pat1,pat2,pat3,pat4]
    combined_pat = r'|'.join(patterns)
    text = re.sub(combined_pat, '', text)
    return text

def loop_and_apply_function(store, inputs, func):
    for input in inputs:
        store[input] = func(input)


def get_response_from_url(url,timeout=10,retries=10):
    response = None
    if retries == 0:
        return response # Cannot retry anomore
    try:
        sleep(randint(0, 3))
        response = urllib.request.urlopen(url,timeout=timeout).read().decode('utf-8')
    except Exception as e:
        print('Got this error',e)
        print('Exiting..')
        exit(1)
    except HTTPError as error:
        logging.error('HTTP Error: Data of not retrieved because %s\nURL: %s', error, url)
    except URLError as error:
        logging.error('URL Error: Data not retrieved because %s\nURL: %s', error, url)
        logging.info('Retrying %s after sleeping with %d retries\n' % (url,retries))
        sleep(randint(10,15))
        return get_response_from_url(url,timeout,retries - 1)
    except Exception as e:
        print(e)
        sleep(randint(10,15))
        return get_response_from_url(url,timeout,retries - 1)
    return response

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def parallelize_function_calls_to_map(inputs,func,nthreads=50):
    """parallelizes inputs for a function, returns a store"""
    store = {}
    threads = []
    # create the threads
    for i in range(nthreads):
        inputs_per_thread = inputs[i::nthreads]
        t = Thread(target=loop_and_apply_function, args=(store,inputs_per_thread,func))
        threads.append(t)

    # start the threads
    [ t.start() for t in threads ]
    # wait for the threads to finish
    [ t.join() for t in threads ]

    return store