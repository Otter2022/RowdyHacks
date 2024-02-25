import json
import requests
from bs4 import BeautifulSoup
import psycopg2
import pandas as pd
import DataConversion
import time

class Ride:
    def __init__(self, name, current_wait_time, average_wait_time):
        self.name = name
        self.current_wait_time = current_wait_time
        self.average_wait_time = average_wait_time

    def update_wait_time(self, new_wait_time):
        self.current_wait_time = new_wait_time

    def update_average_wait_time(self, new_average_wait_time):
        self.average_wait_time = new_average_wait_time

    def display_info(self):
        print(f"Ride Name: {self.name}")
        print(f"Current Wait Time: {self.current_wait_time} minutes")
        print(f"Average Wait Time: {self.average_wait_time} minutes")

def findWaitTime():
    # waitTimesUrl = 'https://www.thrill-data.com/waittimes/fiesta-texas'
    # response = requests.get(waitTimesUrl)
    # soup = BeautifulSoup(response.text, 'html.parser')
    #
    # table = soup.find('table', class_ = 'data-table')
    #
    # ridesTimes = {}
    # for row in table.find_all('tr'):
    #   columns = row.find_all('td')
    #   if(len(columns)==4):
    #     ridesTimes[columns[0].text.strip()] = columns[-1].text.strip()
    # return(ridesTimes)
    # parkUrl = 'https://queue-times.com/parks/39/queue_times.json'
    waitTimesUrl = 'https://www.thrill-data.com/waittimes/fiesta-texas'
    averageWaitsUrl = 'https://queue-times.com/en-US/parks/39/stats'
    response = requests.get(waitTimesUrl)
    # from _typeshed import SupportsAllComparisons
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', class_='data-table')
    ridesTimes = {}
    for row in table.find_all('tr'):
        columns = row.find_all('td')
        # print(len(columns))
        if (len(columns) == 4):
            ridesTimes[columns[0].text.strip()] = columns[-1].text.strip()
    request2 = requests.get(averageWaitsUrl)
    soup2 = BeautifulSoup(request2.text, 'html.parser')
    table2 = soup2.find('table', class_='table is-fullwidth')
    rideAvg = {}
    for row in table2.find_all('tr'):
        columns = row.find_all('td')
        if (len(columns) == 2):
            if ('[Archived]' in columns[0].text.strip()):
                result_string = columns[0].text.strip().replace('[Archived] ', "")
                rideAvg[result_string] = columns[1].text.strip()
            else:
                rideAvg[columns[0].text.strip()] = columns[1].text.strip()
            # print([column.text.strip() for column in columns])
    # for key,val in rideAvg.items():
    #  if('[Archived]' not in key):
    #    print(key,val)
    filteredAvgs = {key: value for key, value in rideAvg.items() if '[Archived]' not in key}
    keys_set1 = set(ridesTimes.keys())
    keys_set2 = set(filteredAvgs.keys())

    # Keys present in both dictionaries
    common_keys = keys_set1.intersection(keys_set2)

    # Keys present in either dictionary
    all_keys = keys_set1.union(keys_set2)

    # Keys present in dict1 but not in dict2
    keys_only_in_dict1 = keys_set1.difference(keys_set2)

    # Keys present in dict2 but not in dict1
    keys_only_in_dict2 = keys_set2.difference(keys_set1)

    # print("\nCommon Keys:", sorted(common_keys))
    # print("\nAll Keys:", sorted(all_keys))
    # print("\nKeys only in Current Times List:", sorted(keys_only_in_dict1))
    # print("\nKeys only in Average Times List:", sorted(keys_only_in_dict2))
    ridesMaster = []
    for key in sorted(all_keys):
        if key in ridesTimes and key in filteredAvgs:
            temp = [key, ridesTimes[key], filteredAvgs[key]]
            ridesMaster.append(temp)
            # print(f'{key} Current Wait: {ridesTimes[key]} Average Wait: {filteredAvgs[key]}')
        elif key in ridesTimes:
            ridesMaster.append([key, ridesTimes[key], None])
            # print(f'{key} Current Wait: {ridesTimes[key]} Average Wait: null')
        else:
            ridesMaster.append([key, None, filteredAvgs[key]])
            # print(f'{key} Current Wait: null Average Wait: {filteredAvgs[key]}')
    df = pd.DataFrame(ridesMaster, columns=["ride_name", "current_wait_time", "average_wait_time"])

    # Write the DataFrame to a CSV file
    df.to_csv("output.csv", index=False)
    # table
    # soup

class dataBaseWriter():
    def __init__(self):
        self.conn = psycopg2.connect(host="localhost", dbname="amusmentPark", user="postgres",
                                password="???", port=5432)

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
    interval_seconds = 60  # Example: Scrape every 1 minutes

    while True:
        findWaitTime()
        listOfDicts = DataConversion.makeDictFromCSV("output.csv")
        print(listOfDicts)
        with dataBaseWriter() as db_writer:
            if not dataBaseEditor.check_table_exists(db_writer, 'ride_wait_times'):
                dataBaseEditor.create_table(db_writer, 'RideTimes')
                for dictionary in listOfDicts:
                    dataBaseEditor.update_table(db_writer, 'ride_wait_times', dictionary)
            else:
                dataBaseEditor.dump_table_rows(db_writer, 'ride_wait_times', listOfDicts)
                for dictionary in listOfDicts:
                    dataBaseEditor.update_table(db_writer, 'ride_wait_times', dictionary)

        time.sleep(interval_seconds)

