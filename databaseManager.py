#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@description:
1. class to manager mysql: DatabaseManager
2. methods:
    connect(self, host, user, passwd, name): connect to database
    table_exists(self, table): test if a table exits
    exists(self, table): test duplicate
    insert_dict(self, my_dict, table_name): insert a dict into table
    close(self): close connection
"""

import sys
import pymysql
import re


class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cur = None

    def connect(self, host='localhost', user='root', passwd='Aa1999513', name='mysql', char="utf8"):
        db_host = host
        db_user = user
        db_pass = passwd
        db_name = name
        db_char = char
        try:
            self.conn = pymysql.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name,
                                        charset=db_char)
            self.cur = self.conn.cursor()
            print("Database connection succeeded!")
        except Exception as e:
            print(e)

    # test if a table exists
    def table_exists(self, table):
        sql = "show tables"
        self.cur.execute(sql)
        tables = self.cur.fetchall()
        # print(tables)
        tables_list = re.findall('(\'.*?\')', str(tables))
        # print(tables_list)
        tables_list = [re.sub("'", '', each) for each in tables_list]
        # print(tables_list)
        if table in tables_list:
            print("Table exists")
            return True
        else:
            print("Table not exists")
            return False

    # test duplicate
    def exists(self, table, url):
        query = ("select count(*) from {} where pdf_url='{}'".format(table, url))
        self.cur.execute(query)
        self.conn.commit()
        count = self.cur.fetchone()
        if count[0] > 0:
            return True
        else:
            return False

    def insert_dict(self, my_dict, table_name):
        data_values = "(" + "%s," * (len(my_dict)) + ")"
        data_values = data_values.replace(',)', ')')

        dbField = my_dict.keys()
        dataTuple = tuple(my_dict.values())
        dbField = str(tuple(dbField)).replace("'", '')
        sql = """ insert into %s %s values %s """ % (table_name, dbField, data_values)
        params = dataTuple
        try:
            num = self.cur.execute(sql, params)
            self.conn.commit()
            return num
        except:
            print("SQL error:", sys.exc_info()[1])
            return 0

    def close(self):
        self.cur.close()
        self.conn.close()
        print('Database connection closed!')


# test
if __name__=='__main__':

    db = DatabaseManager()
    db.connect(host='localhost', user='root', passwd='Aa1999513', name='test')
    db.close()
