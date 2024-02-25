import json
import requests
from bs4 import BeautifulSoup
import psycopg2
import time
def findWaitTime():
    waitTimesUrl = 'https://www.thrill-data.com/waittimes/fiesta-texas'
    response = requests.get(waitTimesUrl)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', class_ = 'data-table')

    ridesTimes = {}
    for row in table.find_all('tr'):
      columns = row.find_all('td')
      if(len(columns)==4):
        ridesTimes[columns[0].text.strip()] = columns[-1].text.strip()
    return(ridesTimes)

class dataBaseWriter():
    def __init__(self):
        self.conn = psycopg2.connect(host="localhost", dbname="amusmentPark", user="postgres",
                                password="Wack3yW8v37??", port=5432)

    def __enter__(self):
        self.cur = self.conn.cursor()
        return self.cur

    def __exit__(self, *args):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

class dataBaseEditor():
    def check_table_exists(cursor, table_name, schema_name='public'):
        query = """
               SELECT EXISTS (
                   SELECT 1 
                   FROM information_schema.tables 
                   WHERE table_schema = %s
                   AND table_name = %s
               ); """
        cursor.execute(query, (schema_name, table_name))
        table_exists = cursor.fetchone()[0]
        return table_exists

    def create_table(cursor, table_name, schema_name='public'):
        sql_create_table = """
                CREATE TABLE ride_wait_times (
                    ride_name VARCHAR(100) PRIMARY KEY,
                    wait_time INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
        cursor.execute(sql_create_table)

    def dump_table_rows(cursor, table_name, data):
        dump_rows = f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"
        cursor.execute(dump_rows)
    def update_table(cursor, table_name, data, columns):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))

        query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
            """

        cursor.execute(query)


if __name__ == '__main__':
    #interval_seconds = 60 * 5  # Example: Scrape every 5 minutes
    # while True:
    #     rides_Times = findWaitTime()
    wait_times = findWaitTime()
    with dataBaseWriter() as db_writer:


        #time.sleep(interval_seconds

