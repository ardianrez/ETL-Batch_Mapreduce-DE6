#!python3

import os
import json
import pandas
from sqlalchemy import create_engine
import model
import connection
import pickle
from hdfs import InsecureClient
from datetime import datetime

def insert_data():
    path = os.getcwd() + "\\" + "dataset" + "\\"

    for dic in [("TR_OrderDetails.csv","fact_orderdetails"),
                ("TR_Products.csv","dim_products"),
                ("TR_PropertyInfo.csv","dim_location"),
                ("TR_UserInfo.csv","dim_users")]:
        
        df = pandas.read_csv(path + dic[0])
        # engine = create_engine('postgresql://username:password@localhost:5432/digitalskola')
        engine = create_engine('postgresql://postgres:root@localhost:5432/digitalskola')
        df.to_sql(dic[1], engine, if_exists='replace', index=False)


def ingestion_data():
    conf_postgresql = connection.param_config("postgresql")
    conf_hadoop = connection.param_config("hadoop")["ip"]

    conn = connection.postgres_conn(conf_postgresql)
    cur = conn.cursor()

    client = InsecureClient(conf_hadoop)

    list_tables = model.list_tables()
    for table in list_tables:
        sql = table[1]
        cur.execute(sql)
        data = cur.fetchall()
        
        time = datetime.now().strftime("%Y%m%d")
        df = pandas.DataFrame(data, columns=[col[0] for col in cur.description])
        with client.write(f'/DigitalSkola/{time}/{table[0]}_{time}.csv', encoding='utf-8') as writer:
            df.to_csv(writer, index=False)


def transform_data():
    time = datetime.now().strftime("%Y%m%d")

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/dwh_digitalskola')

    conf_hadoop = connection.param_config("hadoop")["ip"]
    client = InsecureClient(conf_hadoop)

    with client.read(f'/DigitalSkola/{time}/user_{time}.csv', encoding='utf-8') as writer:
        df = pandas.read_csv(writer)
    df.to_sql("dwh_dim_users", engine, if_exists='replace', index=False)

    conf_postgresql = connection.param_config("postgresql")
    conn = connection.postgres_conn(conf_postgresql)
    cur = conn.cursor()

    sql = model.dwh_fact_orders()
    cur.execute(sql)
    data = cur.fetchall()
    df = pandas.DataFrame(data, columns=[col[0] for col in cur.description])
    df.to_sql("dwh_fact_orders", engine, if_exists='replace', index=False)

def map(data):
    mapped = []
    for index,row in data.iterrows():
        mapped.append((row['OrderDate'],row['Quantity']))        
    return mapped


def mapper():
    time = datetime.now().strftime("%Y%m%d")

    conf_hadoop = connection.param_config("hadoop")["ip"]
    client = InsecureClient(conf_hadoop)

    with client.read(f'/DigitalSkola/{time}/orders_{time}.csv', encoding='utf-8') as writer:
        df = pandas.read_csv(writer)

    #Slicing Data
    slice1 = df.iloc[0:1000,:]
    slice2 = df.iloc[1000:,:]

    map1 = map(slice1)
    map2 = map(slice2)

    shuffled = {}
    for i in [map1,map2]:
        for j in i:
            if j[0] not in shuffled:
                shuffled.update({j[0]:[]})
                
            shuffled[j[0]].append(j[1])
    print(shuffled)

    file = open('shuffled.pkl','ab')
    pickle.dump(shuffled,file)
    file.close()
    print("Data has been mapped. Now, run reducer.py to reduce the contents in shuffled.pkl file.")


def reduce(shuffled_dict):
    reduced = {}
    
    for i in shuffled_dict: 
        reduced[i] = sum(shuffled_dict[i])
    return reduced


def reducer():
    time = datetime.now().strftime("%Y%m%d")

    file= open('shuffled.pkl','rb')
    shuffled = pickle.load(file)

    conf_hadoop = connection.param_config("hadoop")["ip"]
    client = InsecureClient(conf_hadoop)

    final = reduce(shuffled)
    df = pandas.DataFrame(final, index=[0])
    with client.write(f'/DigitalSkola/{time}/db_mart_quantity_day_{time}.csv', encoding='utf-8') as writer:
            df.to_csv(writer, index=False)

    
    print("Quantity Transaction... ")