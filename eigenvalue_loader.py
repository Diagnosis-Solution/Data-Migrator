import pymongo
import os
import sqlite3
import datetime


def load_eigenvalue_to_sqlite(type, db_name, sensor, file):
    if type == '04':
        table_name = 'zhe' + sensor[-2:]
    elif type == '05':
        table_name = 'zle' + sensor[-2:]

    conn = sqlite3.connect(db_name)

    curs = conn.cursor()
    create = 'CREATE TABLE IF NOT EXISTS ' + table_name + \
        '(time VARCHAR(24) PRIMARY KEY,' + \
        'type INTEGER NOT NULL,' + \
        'voltage DECIMAL(10,5) NOT NULL,' + \
        'rssi INTEGER NOT NULL,' + \
        'rstp INTEGER NOT NULL' + \
        ')'

    curs.execute(create)
    print(create)

    fin = open(file, 'r')
    while True:
        line = fin.readline()
        if not line:
            break
        # Write line data to sqlite as row
        new_line = line.split('\t')

        print(tuple(new_line))
        curs.execute("insert into " + table_name + " values (?, ?, ?, ?, ?)",  tuple(new_line)[0:5])

    conn.commit()
    curs.close();

    fin.close()


def load(target):
    for root, dirs, files in os.walk(target):
        # handling files
        files = [f for f in files if not f[0] == '.']
        if len(files) != 0:
            base_data = root.split('/')
            if len(base_data) == 3:  # Eigenvalue data
                db_name = 'data/' + base_data[1] + '.db'
                print(db_name)
                # if os.path.exists(db_name):
                #     os.remove(db_name)

            for file in files:
                if file.endswith('.txt'):
                    if file.startswith('04'):        # Z-Axis High Frequency Eigenvalue
                        load_eigenvalue_to_sqlite('04', db_name, base_data[2], os.path.join(root, file))

                    elif file.startswith('05'):      # Z-Axis Low Frequency Eigenvalue
                        load_eigenvalue_to_sqlite('05', db_name, base_data[2], os.path.join(root, file))


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
