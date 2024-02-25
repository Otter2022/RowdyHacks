import json
import requests
from bs4 import BeautifulSoup
import psycopg2
import DataConversion
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

    @staticmethod
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

    @staticmethod
    def create_table(cursor, table_name, schema_name='public'):
        sql_create_table = """
                CREATE TABLE ride_wait_times (
                    ride_name VARCHAR(100) PRIMARY KEY,
                    current_wait_time REAL,
                    average_wait_time INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
        cursor.execute(sql_create_table)

    @staticmethod
    def dump_table_rows(cursor, table_name, data):
        dump_rows = f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"
        cursor.execute(dump_rows)
    @staticmethod
    def update_table(cursor, table_name, data):
        # Construct the SQL query dynamically
        columns = ', '.join(["\"{}\"".format(col) for col in data.keys()])
        placeholders = ', '.join(['%s'] * len(data))
        # values = tuple(data.values())
        #
        # for key, values in data.items():
        #     if values is None:
        #         data[key] = 0
        values = tuple(value if value else None for value in data.values())
        print(values)


        query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders}) 
--                 ON CONFLICT ("Ride Name") DO UPDATE 
--                 SET 
--                     "Current Wait" = EXCLUDED."Current Wait",
--                     "Average Wait" = EXCLUDED."Average Wait" 
                """

        # Execute the query
        cursor.execute(query, values)


if __name__ == '__main__':
    interval_seconds = 60 * 5  # Example: Scrape every 5 minutes
    # while True:
    #     rides_Times = findWaitTime()
    # wait_times = findWaitTime()
    listOfDicts = DataConversion.makeDictFromCSV("output.csv")

    with dataBaseWriter() as db_writer:
        if not dataBaseEditor.check_table_exists(db_writer, 'ride_wait_times'):
            dataBaseEditor.create_table(db_writer, 'RideTimes')
            for dictionary in listOfDicts:
                dataBaseEditor.update_table(db_writer, 'ride_wait_times', dictionary)
        else:
            dataBaseEditor.dump_table_rows(db_writer, 'ride_wait_times', listOfDicts)
            for dictionary in listOfDicts:
                dataBaseEditor.update_table(db_writer, 'ride_wait_times', dictionary)

        #time.sleep(interval_seconds

