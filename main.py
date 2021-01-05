# Read Oscillate data from folder and save to MySQL

import os
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from entity.entities import Enterprise


def migrate(target):

    conn = sa.create_engine(
        "mysql+pymysql://xdadmin:123456@localhost:3306/wpcms",
        encoding="utf-8",
        echo=True
    )

    for root, dirs, files in os.walk(target):
        files = [f for f in files if not f[0] == '.']
        if len(files) != 0:
            # parse enterprise, department, sensor, data type from path and save to database
            print('the raw data in current directory:')
            base_data = root.split('/')
            save_enterprise_to_db(conn, base_data[1])

            for file in files:
                if file.endswith('.txt'):
                    file_full_name = os.path.join(root, file)
                    print(file_full_name)
                    file_object = open(file_full_name, 'r')
                    print(file_object)


def save_enterprise_to_db(connection, name):
    ent = Enterprise(name)
    Session = sessionmaker(bind=connection)
    session = Session()
    session.add(ent)
    session.flush()
    session.commit()



if __name__ == "__main__":
    migrate('data')
