import json
import os
import re
import sqlite3
import string
from datetime import datetime
from typing import Optional, Any, Dict

import requests


class ElectricityOutages:
    """
    Gather the details of the petitions.
    """

    usage_url = 'https://www.energex.com.au/static/Energex/Network%20Demand/networkdemand.txt'
    outage_summary_url = 'https://www.energex.com.au/api/outages/v0.3/summary'
    outage_councils_url = 'https://www.energex.com.au/api/outages/v0.3/council?council='
    outage_suburbs_url = 'https://www.energex.com.au/api/outages/v0.3/suburb?council=&suburb='
    outage_council_suburbs_url = 'https://www.energex.com.au/api/outages/v0.3/suburb?council={}&suburb='
    outage_suburb_url = 'https://www.energex.com.au/api/outages/v0.3/search?suburb={}'

    sqlite_db_file = 'data.sqlite'
    iso_datetime_format = '%Y-%m-%dT%H:%M:%S+10:00'
    regex_collapse_newline = re.compile(r'(\n|\r)+')
    regex_collapse_whitespace = re.compile(r'\s{2,}')

    allowed_chars = string.digits + string.ascii_letters + string.punctuation

    cache_chars = string.digits + string.ascii_letters
    local_cache_dir = 'cache'
    use_cache = False

    def run(self):
        current_time = datetime.today()

        db_conn = None
        try:
            db_conn = self.get_sqlite_db()
            self.create_sqlite_database(db_conn)

            demand = {'demand': None, 'rating': None, 'retrieved_at': current_time.strftime(self.iso_datetime_format)}
            summary = {'retrieved_at': current_time.strftime(self.iso_datetime_format), 'updated_at': None,
                       'total_cust': None}
            data = []

            print('Reading usage')
            usage_page = self.download_text(self.usage_url)
            demand['demand'] = usage_page
            demand['rating'] = self.demand_rating(usage_page)

            print('Reading outage summary')
            outage_summary_page = self.download_json(self.outage_summary_url)
            total_cust = outage_summary_page['data']['totalCustomersAffected']
            updated_at = datetime.strptime(outage_summary_page['data']['lastUpdated'], '%d %B %Y %I:%M %p').strftime(
                self.iso_datetime_format)
            summary['total_cust'] = total_cust
            summary['updated_at'] = updated_at

            print('Reading Councils list')
            outage_councils_page = self.download_json(self.outage_councils_url)
            outage_councils = outage_councils_page['data']

            print('Reading Suburbs list')
            outage_suburbs_page = self.download_json(self.outage_suburbs_url)
            outage_suburbs = outage_suburbs_page['data']

            for council in outage_councils:
                outage_council_suburbs_page = self.download_json(
                    self.outage_council_suburbs_url.format(council['name']))
                suburbs = outage_council_suburbs_page['data']

                for suburb in suburbs:
                    outage_suburb_page = self.download_json(self.outage_suburb_url.format(suburb['name']))
                    events = outage_suburb_page['data']

                    for event in events:
                        # event_item = {
                        #     'id': event['event'],
                        #     'cause': event['cause'],
                        #     'status': event['status'],
                        #     'council': event['council'],
                        #     'postcode': event['postcode'],
                        #     'suburb': event['suburb'].title(),
                        #     'cust': event['customersAffected'],
                        #     'restore_time': event['restoreTime'],
                        #     'streets': str.join(',', [s.title() for s in event['streets']]),
                        # }

                        data.append({
                            'title': event['council'],
                            'region': event['council'],
                            'suburb': event['suburb'].title(),
                            'cust': event['customersAffected'],
                            'cause': event['cause'],
                            'retrieved_at': current_time.strftime(self.iso_datetime_format),
                        })

            # insert data
            self.sqlite_demand_row_insert(db_conn, demand)
            self.sqlite_summary_row_insert(db_conn, summary)

            for item in data:
                self.sqlite_data_row_insert(db_conn, item)

            db_conn.commit()

            print('Added demand, summary, and {} data item(s).'.format(len(data)))
            print('Completed successfully.')

        finally:
            if db_conn:
                db_conn.close()

    def demand_rating(self, demand: str):
        demand = int(demand)

        # demand min: 0, demand max: 5500
        # found in: https://www.energex.com.au/__data/assets/js_file_folder/0011/653996/main.js?version=0.3.59

        # divided into 4 equal parts: low, moderate, high, extreme
        # then into 3 parts = approx 458.3 per smallest part
        demand_min = 0
        demand_max = 5500
        rating_min = 1
        rating_max = 12

        demand_part = demand_max / 4 / 3
        rating = int(demand / demand_part)

        if rating < rating_min:
            rating = rating_min

        if rating > rating_max:
            rating = rating_max

        return rating

    # ---------- SQLite Database -------------------------

    def sqlite_demand_row_insert(self, db_conn, row: Dict[str, Any]) -> int:
        c = db_conn.execute(
            'INSERT INTO demand '
            '(demand, rating, retrieved_at) '
            'VALUES (?, ?, ?)',
            (row['demand'], row['rating'], row['retrieved_at'],))

        row_id = c.lastrowid

        return row_id

    def sqlite_summary_row_insert(self, db_conn, row: Dict[str, Any]) -> int:
        c = db_conn.execute(
            'INSERT INTO summary '
            '(retrieved_at, updated_at, total_cust) '
            'VALUES (?, ?, ?)',
            (row['retrieved_at'], row['updated_at'], row['total_cust'],))

        row_id = c.lastrowid

        return row_id

    def sqlite_data_row_insert(self, db_conn, row: Dict[str, Any]) -> int:
        c = db_conn.execute(
            'INSERT INTO data '
            '(title, region, suburb, cust, cause, retrieved_at) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (row['title'], row['region'], row['suburb'], row['cust'],
             row['cause'], row['retrieved_at'],))

        row_id = c.lastrowid

        return row_id

    def get_sqlite_db(self):
        conn = sqlite3.connect(self.sqlite_db_file)
        return conn

    def create_sqlite_database(self, db_conn):
        db_conn.execute(
            'CREATE TABLE IF NOT EXISTS data '
            '('
            'title TEXT, '
            'region TEXT, '
            'suburb TEXT, '
            'cust TEXT, '
            'cause TEXT, '
            'retrieved_at TEXT UNIQUE'
            ')')

        db_conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS data_retrieved_at '
            'ON data (retrieved_at)')

        db_conn.execute(
            'CREATE TABLE IF NOT EXISTS demand '
            '('
            'demand TEXT, '
            'rating TEXT, '
            'retrieved_at TEXT UNIQUE'
            ')')

        db_conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS retrieved_at '
            'ON demand (retrieved_at)')

        db_conn.execute(
            'CREATE TABLE IF NOT EXISTS summary'
            '('
            'retrieved_at unique, '
            'updated_at TEXT, '
            'total_cust TEXT'
            ')')

        db_conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS summary_retrieved_at '
            'ON summary (retrieved_at)')

    # ---------- Downloading -----------------------------

    def download_text(self, url: str):
        content = self.load_page(url)

        if content:
            content = content.decode('utf-8')

        if not content:
            page = requests.get(url)
            if page.is_redirect or page.is_permanent_redirect or page.status_code != 200:
                content = None
            else:
                content = page.text
                self.save_page(url, content.encode('utf-8'))

        if not content:
            return None

        return content

    def download_json(self, url: str) -> Optional[Dict]:
        content = self.load_page(url)

        if content:
            content = json.loads(content.decode('utf-8'))

        if not content:
            page = requests.get(url)
            if page.is_redirect or page.is_permanent_redirect or page.status_code != 200:
                content = None
            else:
                content = page.json()
                self.save_page(url, json.dumps(content).encode('utf-8'))

        if not content:
            return None

        return content

    # ---------- Local Cache -----------------------------

    def cache_item_id(self, url):
        item_id = ''.join(c if c in self.cache_chars else '' for c in url).strip()
        return item_id

    def save_page(self, url, content) -> None:
        if not self.use_cache:
            return

        os.makedirs(self.local_cache_dir, exist_ok=True)
        item_id = self.cache_item_id(url)
        file_path = os.path.join(self.local_cache_dir, item_id + '.txt')

        with open(file_path, 'wb') as f:
            f.write(content)

    def load_page(self, url) -> Optional[bytes]:
        if not self.use_cache:
            return None

        os.makedirs(self.local_cache_dir, exist_ok=True)
        item_id = self.cache_item_id(url)
        file_path = os.path.join(self.local_cache_dir, item_id + '.txt')

        if not os.path.isfile(file_path):
            return None

        with open(file_path, 'rb') as f:
            return f.read()


outages = ElectricityOutages()
outages.run()
