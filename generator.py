from kafka import KafkaProducer
from datetime import datetime, timedelta
import json
import time
import random

num_message = 1
a = 1
id_event = 0

producer = KafkaProducer(bootstrap_servers='vm-strmng-s-1.test.local:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def generate_message():

        names_fp = ["Свято", "Яро", "Влади", "Изя", "Вяче", "Мечи", "Бори", "Гори", "Бого",
                    "Любо", "Мило", "Свето", "Миро", "Добро", "Брони"]

        names_sp = ["слав", "мир", "полк", "люб", "рад", "мил", "зар",
                    "мысл", "дан", "гор", "яр", "вед", "бор", "мысл", "свет"]

        cities = ["Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург", "Нижний Новгород",
                 "Казань", "Челябинск", "Омск", "Самара", "Ростов-на-Дону"]

        services = ["free", "pay", "pre_pay"]

        service_titles_free = ["Запись на прием", "Консультация", "Самодиагностика", "Вакцинация", "Получить выписку из медкарты",
                 "Дневник самочувствия", "Дневник лекарств"]

        service_titles_pay = ["Консультация", "Запись на сдачу анализов",
            "Вызов врача", "Запись на прием", "ПЦР-тест"]

        subs = [1, 3, 6, 12]

        subs_titles = ["Промо", "Квартал не заболевал",
            "Полгода пищевода", "Год без забот"]

        events = ["create", "cancel", "update", "done", "input",
                 "output", "payed"]

        def gen_num(a):
                n = round(random.random()*a)
                return n

        def gen_gender():
                n = random.choice([1, 0])
                return n

        def name_client(gender):
                fp = round(random.random()*len(names_fp))
                sp = round(random.random()*len(names_sp))
                s1 = names_fp[fp-1]+names_sp[sp-1]
                s2 = s1+"а"
                if gender == 1:
                    s = s1
                else:
                    s = s2
                return s

        def gen_city():
                n = round(random.random()*len(cities))
                val = cities[n-1]
                return val

        def gen_type_service():
                n = round(random.random()*len(services))
                val = services[n-1]
                return val

        def gen_title_service(id_service):
                val = service_titles_free[id_service-1]
                return val

        def gen_title_subs():
                n = round(random.random()*len(subs_titles))
                val = subs_titles[n-1]
                return val

        def gen_type_subs():
                n = round(random.random()*len(subs))
                val = subs[n-1]
                return val

        def gen_title_event():
                n = round(random.random()*len(events))
                val = events[n-1]
                return val

        def gen_type_event():
                n = random.random()
                if n < 0.8:
                    val = "success"
                elif n < 0.9:
                    val = "unsuccess"
                else:
                    val = "error"
                 return val

        def gen_date():
                val=(datetime.now() - timedelta(days=random.randint(0,365),minutes=random.randint(0,59),hours=random.randint(0,23))).strftime('%Y-%m-%d %H:%M:%S')
                return val

        def gen_end_date():
                val=(datetime.strptime(start_date,'%Y-%m-%d %H:%M:%S') + timedelta(days=random.randint(0,365),minutes=random.randint(0,59),hours=random.randint(0,23))).strftime('%Y-%m-%d %H:%M:%S')
                return val

        #client
        id_client=gen_num(10000)
        gender=gen_gender()
        client_name=name_client(gender)
        age=gen_num(150)
        location=gen_city()

        #service
        id_service=gen_num(len(service_titles_free))
        title_service=gen_title_service(id_service)
        service_type=gen_type_service()
        price=gen_num(1000)

        #subscription
        id_subs=gen_num(4)
        title_subs=gen_title_subs()
        type_subs=gen_type_subs()
        start_date=gen_date()
        end_date=gen_end_date()

        #event
        id_event=id_event+1
        event_title=gen_title_event()
        event_type=gen_type_event()
        date_event=gen_date()

        

        message={
                 'client':
                          {
                           'id': id_client,
                           'name':client_name,
                           'gender':gender,
                           'age': age,
                           'location': location
                          },
                  'service':
                          {
                           'id': id_service,
                           'title': title_service,
                           'type': service_type,
                           'price': price
                          },
                 'subscription':
                          {
                           'id': id_subs,
                           'title':title_subs,       
                           'type':type_subs,
                           'start_date':start_date,
                           'end_date': end_date
                          },
                  'event':
                          {
                            'id':id_event,
                            'type': event_type,
                            'title':event_title,
                            'date': date_event
                          }

                }
        return  json.dumps(message)

for i in range(num_message):

        message=generate_message()
        producer.send('fb_grushevskiy',value=message)

producer.close()
