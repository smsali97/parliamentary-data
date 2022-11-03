
import csv
from bs4 import BeautifulSoup as bs
from dateparser.search import search_dates
import urllib.request as ur
from threading import Thread
import pandas as pd

from helpers import get_response_from_url



def get_data_from_url(url):
    """Given a url will extract the content from it"""
    res = get_response_from_url(url)
    if res == None:
        print('No Response, Skipping %s...' % url)
    soup = bs(res,features='html.parser')
    soup = soup.find("div", {"id": "content"})
    date_section=soup.find('cite',attrs={'class':'section'})
    date = None
    dates = search_dates(date_section.text)
    if len(dates) >= 1: date = dates[0][0]
    contents = soup.findAll(['cite','p'])
    # cite has the speaker name, p has the dialogue
    article_text = ''
    paragraph = list()
    for content in contents:
        paragraph.append(content.text.lstrip())
    article_text += ' '.join(paragraph)
    return {'date':date, 'text':article_text}

def process_urls_in_range(ids):
    for i in ids:
        yr,month,day,title,url  = input[i]
        data = get_data_from_url(url)
        print('Fetched %s from %s' % (title,url))
        date = data['date']
        article_text = data['text']
        row = [i+1,title,date,article_text]
        write_rows.append(row)


def process_articles_in_parallel(id_range,nthreads=75):
    """process the id range in a specified number of threads"""
    store = {}
    threads = []
    # create the threads
    for i in range(nthreads):
        ids = id_range[i::nthreads]
        t = Thread(target=process_urls_in_range, args=(ids,))
        threads.append(t)

    # start the threads
    [ t.start() for t in threads ]
    # wait for the threads to finish
    [ t.join() for t in threads ]




START_YEAR=1920
END_YEAR=1921
df = pd.read_csv('urls.csv',header=None,names=['yr','month','day','title','url'],low_memory=True)
for year in range(START_YEAR,END_YEAR+1):
    df_per_year = df[df['yr'] == 1920]
    content_file = open('parliament_articles_%d.csv' % year, 'a')
    writer = csv.writer(content_file,delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['ID','Title','Date','Text'])
    uniq_months = set(df_per_year['month'])
    for mon in uniq_months:
        input = [[row['yr'],row['month'],row['day'],row['title'],row['url']] for _, row in df_per_year[df_per_year['month'] == mon].iterrows()]
        print('%d of Urls Read' % len(input))
        write_rows = []
        process_articles_in_parallel(range(len(input)))
        write_rows = sorted(write_rows)
        writer.writerows(write_rows)
    content_file.close()


