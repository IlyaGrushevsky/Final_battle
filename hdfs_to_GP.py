import datetime
import csv
import time
import pg8000
# import pandas as pd
import os


dbname = 'postgres'
user = 'grushevskiy'
password = 'd2rz7'
host = '192.168.77.21'
port = 5432

count = 0
i = 0

try:
        conn = pg8000.connect(database=dbname, user=user,
                              password=password, host=host, port=port)
        cur = conn.cursor()

        # нужно доделать выборку для файлов за прошедний день
        filename = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        open(filename+'.csv') as file:
        data = csv.reader(file, delimiter=';')
        a = 1
        b = 5
        # Недоделан процессинг для записи в GP
        for row in list(data)[1:]:

             if row[a] == :
                query = "INSERT INTO fb_client (id_client, gender, age,location) VALUES (%s,%s,%s,%s)"
             elif row[a]:
                query = "INSERT INTO fb_service (id_service, title_service, service_type) VALUES (%s,%s,%s)"
                query = "INSERT INTO fb_subs (id_subs, title_subs, type_subs,start_date,end_date) VALUES (%s,%s,%s,%s.%s)"
             else:
                query = "INSERT INTO fb_events (id_event, event_type, event_title,date_event) VALUES (%s,%s,%s,%s)"
             print(str(val))
             cur.execute(query, val)
             count += 1

   finally:
       conn.commit()
       conn.close()
