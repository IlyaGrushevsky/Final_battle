from kafka import KafkaConsumer
from datetime import datetime, timedelta
import time
from hdfs import InsecureClient
import pandas as pd
import json

host = 'hdfs://vm-dlake2-m-1.test.local'

path = '/user/grushevskiy/fb_data/'

consumer = KafkaConsumer('fb_grushevskiy', bootstrap_servers='vm-strmng-s-1.test.local:9092',
                         group_id='group_src_1', value_deserializer=lambda m: json.loads(m.decode('utf-8')))

client = InsecureClient(host, user='grushevskiy')

i = 0
for message in consumer:
    val = []
    dict = json.loads(message.value)
    for key in dict:
        val.append(dict[key])
    i = +1
    if i == 480000:
        i = 0
        df = pd.DataFrame(val)
        with client.write(path + str(datetime.now())+'.csv') as writer:
            df.to_csv(writer, index=False, sep=';')

consumer.close()
