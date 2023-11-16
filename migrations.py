import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()

queries = []


def prepare_queries():
    queries.append('''
        CREATE TABLE IF NOT EXISTS parsing_queue (
            id INTEGER PRIMARY KEY,
            url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            deadline_at TIMESTAMP DEFAULT NULL
        );
        ''')

    queries.append('''
        CREATE TABLE IF NOT EXISTS htmls (
            id INTEGER PRIMARY KEY,
            url TEXT,
            code INTEGER,
            html TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ''')


def migrate():
    conn = sqlite3.connect(os.getenv('DATABASE_NAME'))
    cursor = conn.cursor()

    prepare_queries()

    for query in queries:
        cursor.execute(query)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    migrate()
