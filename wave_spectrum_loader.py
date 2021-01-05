import pymongo
import os
import sqlite3
import time
import datetime

client = pymongo.MongoClient(host='localhost', port=27017)
db = client['wpcms-data']


def load_eigenvalue_to_sqlite(type, db_name, sensor, file):
    if type == '04':
        table_name = 'zhe'
    elif type == '05':
        table_name = 'zle'
    conn = sqlite3.connect(db_name)

    curs = conn.cursor()
    create = 'CREATE TABLE ' + table_name + \
        '(time LONG PRIMARY KEY,' + \
        'type INTEGER NOT NULL,' + \
        'voltage DECIMAL(10,5) NOT NULL,' + \
        'rssi INTEGER NOT NULL,' + \
        'rstp INTEGER NOT NULL' + \
        'temperature' + \
        ')'
    print(create)
    curs.execute(create)

    fin = open(file, 'r')
    while True:
        line = fin.readline()
        if not line:
            break
        # Write line data to sqlite as row
        new_line = line.split('\t')

        print(tuple(new_line))
        curs.execute("insert into " + table_name + " values (?, ?, ?, ?, ?)",  tuple(new_line)[0:5])
        # curs.execute("insert into " + table_name + " values (?, ?, ?, ?, ?, ?)", (None, '2020-05-23 09:03:00', 5, 3608.9373, -59, -60))

    conn.commit()

    fin.close()


def load(target):
    client.drop_database('wpcms-data')
    for root, dirs, files in os.walk(target):
        # handling dirs
        if len(dirs) != 0:
            for dir in dirs:
                if dir.startswith('s'):  # sensor name
                    # write sensor data to mysql
                    print('mysql: insert sensor info into sensor table')
        # handling files
        files = [f for f in files if not f[0] == '.']
        if len(files) != 0:
            collection = db['temp']

            print('the raw data in current directory:')
            base_data = root.split('/')
            if len(base_data) == 2:  # Eigenvalue data
                db_name = 'data/' + base_data[1][-2:] + '.db'
                print(db_name)
                if os.path.exists(db_name):
                    os.remove(db_name)
            else:  # Wave-form or Spectrum data
                if base_data[2].startswith('01'):  # Z-Axis High Frequency Wave-Form data
                    collection = db['data-zhw']
                elif base_data[2].startswith('02'):  # Z-Axis Low Frequency Wave-Form data
                    collection = db['data-zlw']
                elif base_data[2].startswith('03'):  # Z-Axis High Frequency Spectrum data
                    collection = db['data-zls']

            i = 10
            j = 1
            for file in files:
                if file.endswith('.txt'):
                    if file.startswith('04'):        # Z-Axis High Frequency Eigenvalue
                        # load_eigenvalue_to_sqlite('04', db_name, base_data[1], os.path.join(root, file))
                        pass
                    elif file.startswith('05'):      # Z-Axis Low Frequency Eigenvalue
                        # load_eigenvalue_to_sqlite('05', db_name, base_data[1], os.path.join(root, file))
                        pass
                    else:
                        file_full_name = os.path.join(root, file)
                        print(file_full_name)
                        file_object = open(file_full_name, 'r')
                        record_time = os.path.splitext(file)[0].replace("_", ":")
                        print(time)
                        # convert to time array
                        time_array = time.strptime(record_time, "%Y-%m-%d %H:%M:%S")
                        # 转换成时间戳
                        timestamp = int(time.mktime(time_array))
                        data = {
                            'sensor': base_data[1],
                            'checkpoint': i // 10,
                            'time': timestamp,
                            'data': file_object.read()
                        }
                        i = i + 1
                        result = collection.insert(data)
                        print(result)
                        file_object.close()


load('data')

'''
    wave = {
        'id': '20170101',
        'name': 'jordan',
        'age': 20,
        'gender': 'male'
    }
    result = collection.insert(wave)
    print(result)
'''
