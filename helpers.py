import logging
import urllib
import urllib.request
from socket import timeout
from urllib.error import HTTPError, URLError
from threading import Thread

def loop_and_apply_function(store, inputs, func):
    for input in inputs:
        store[input] = func(input)


def get_response_from_url(url,timeout=10,retries=10):
    response = None
    if retries == 0:
        return response # Cannot retry anomore
    try:
        from random import randint
        from time import sleep
        sleep(randint(10,100))
        response = urllib.request.urlopen(url).read().decode('utf-8')
    except HTTPError as error:
        logging.error('HTTP Error: Data of not retrieved because %s\nURL: %s', error, url)
    except URLError as error:
        if isinstance(error.reason, timeout):
            logging.error('Timeout Error: Data of %s not retrieved because %s\nURL: %s',error, url)
            logging.info('Retrying %s after sleeping with %d retries\n' % (url,retries))
            sleep(randint(10,100))
            get_response_from_url(url,timeout,retries - 1)
        else:
            logging.error('URL Error: Data not retrieved because %s\nURL: %s', error, url)
    else:
        logging.info('Access successful.')
    return response

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