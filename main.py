import sqlite3
import time
import os

from a_parser import AParser
from dotenv import load_dotenv

load_dotenv()

aparser = AParser(os.getenv('APARSER_ENDPOINT'), os.getenv('APARSER_PASSWORD'))
conn = sqlite3.connect(os.getenv('DATABASE_NAME'))
cursor = conn.cursor()


def parse_urls(urls):
    print('parsing', urls)

    response = aparser.bulkRequest('Net::HTTP', 'default', 'default', 5, urls, rawResults=1)
    parsed_urls = []

    for item in response['data']['results']:
        if not item['success']:
            continue

        url = item['query']['query']

        parsed_urls.append(url)
        cursor.execute('INSERT INTO htmls (url, code, html) VALUES (?, ?, ?);', (url, item['code'], item['data'],))
        conn.commit()

    return parsed_urls


def get_urls_for_parsing():
    results = cursor.execute(
        "SELECT url FROM parsing_queue WHERE deadline_at < datetime('now') OR deadline_at IS NULL LIMIT 5")
    return [result[0] for result in results.fetchall()]


def remove_from_queue(urls):
    formatted_urls = ', '.join(['?' for _ in urls])

    cursor.execute(f'DELETE FROM parsing_queue WHERE url IN ({formatted_urls});', urls)
    conn.commit()


if __name__ == '__main__':
    while True:
        queue = get_urls_for_parsing()

        if not queue:
            print('empty queue, wait 60 sec for new item')
            time.sleep(60)
            continue

        successfully_parsed_urls = parse_urls(queue)
        remove_from_queue(successfully_parsed_urls)
