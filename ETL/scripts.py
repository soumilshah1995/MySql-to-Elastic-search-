
try:
    import mysql
    from mysql.connector import connect
    import json
    import os
    import sys
    import  pandas as pd
    import threading

    import elasticsearch
    from elasticsearch import Elasticsearch


    import json
    from ast import literal_eval
    import datetime
    import os
    import sys
    from elasticsearch import helpers

    print("Loaded  .. . . . . . . .")
except Exception as e:
    print("Error : {} ".format(e))


class Settings():

    def __init__(self,
                 mysqlhost='localhost',
                 mysqlport=3308,
                 mysqluser='root',
                 mysqlpassword='password',
                 mysqldataBase='mydb',
                 mysqltableName='netflix',
                 mysqlquery='',
                 elkhost="localhost",
                 elkport='9200'):

        self.mysqlhost=mysqlhost
        self.mysqlport = mysqlport
        self.mysqluser = mysqluser
        self.mysqlpassword = mysqlpassword
        self.mysqldataBase = mysqldataBase
        self.mysqltableName = mysqltableName
        self.mysqlquery =mysqlquery
        self.elkhost =elkhost
        self.elkport =elkport
        self.elkhost = "http://{}:{}".format(self.elkhost, self.elkport)

class MySql(object):

    def __init__(self, settings=None):
        self.settings=settings

    def execute(self):
        try:

            self.db = connect(
                host     =      self.settings.mysqlhost,
                port     =      self.settings.mysqlport,
                password =      self.settings.mysqlpassword,
                user     =      self.settings.mysqluser,
                database =      self.settings.mysqldataBase,
            )

            self.cursor = self.db.cursor()
            self.cursor.execute("{}".format(self.settings.mysqlquery))
            myresult = self.cursor.fetchall()
            yield myresult
        except Exception as e:
            print("Error : {} ".format(e))
            return "Invalid Query : {} ".format(e)


class ELK(object):
    def __init__(self, settings=None):
        self.settings =settings
        self.es = Elasticsearch(timeout=600, hosts=self.settings.elkhost)

    def upload(self, records):

        try:
            res = helpers.bulk(self.es,records )
        except Exception as e:
            print("{}".format(e))


def main():
    # Step 1: Create a Settings

    BATCH_SIZE      = 10
    TABLE_NAME      = "netflix"
    DATABASE_NAME   = 'mydb'
    TOTAL_RECORDS   = 0

    # Count Total number of Records
    _settings = Settings(mysqltableName=TABLE_NAME,
                         mysqldataBase=DATABASE_NAME,
                         mysqlquery='SELECT COUNT(*) from {}.{} '.format(DATABASE_NAME, TABLE_NAME))


    # Create a MySQL Class
    _helper = MySql(settings=_settings)
    res = _helper.execute()

    TOTAL_RECORDS = next(res)[0][0]

    # ===========================================================================================
    # Pagination system to pull records in a efficient way
    queries = ['SELECT * FROM {}.{} limit {},{}'.format(DATABASE_NAME, TABLE_NAME, page, BATCH_SIZE)
               for page in range(0,TOTAL_RECORDS, BATCH_SIZE)]

    # ==============================================================================================
    columnQuery = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{}'   AND  TABLE_NAME = '{}' ".format(DATABASE_NAME, TABLE_NAME)
    _settings.mysqlquery = columnQuery
    res = _helper.execute()

    # List of All column Names
    columnNames = [name[0] for name in next(res)]

    for query in queries:
        _settings.mysqlquery = query
        res = _helper.execute()
        res = next(res)
        df = pd.DataFrame(data=res, columns=columnNames)
        df1 = df.to_dict("records")
        records = [
            {
                '_index': '{}'.format(TABLE_NAME),
                '_type': '_doc',
                '_id': c,
                '_source':x
            }
            for c, x in enumerate(df1)
        ]

        eshelper = ELK(settings=_settings)
        eshelper.upload(records=records)







