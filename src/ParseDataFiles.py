#!/usr/bin/env python
import pandas as pd
import os
from sqlalchemy import create_engine
from io import StringIO
import csv
import threading


class readAndInsertDataFileThread(threading.Thread):
    def __init__(self, filepath, engine):
        threading.Thread.__init__(self)
        self.filepath = filepath
        self.engine = engine

    def run(self):
        # print("Starting " + str(self.y) + " " + str(self.q)+"")
        print("Reading " + self.filepath)
        fileDf = pd.read_csv(self.filepath, index_col='ItinID')
        # fileDf = readCSV(filepath)
        # dfs.append(fileDf)
        print("Deleting " + self.filepath)
        os.remove(self.filepath)
        print("Inserting " + self.filepath + " to DB ")
        fileDf.to_sql('testdata', self.engine, method=psql_insert_copy, if_exists='append')
        print("Done Inserting " + self.filepath)
        # downloadAndUnZipAction(self.currentYearPath, self.y, self.q)
        # print("Exiting " + str(self.y) + " " + str(self.q)+"")


def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def getCSVFiles():
    datapath = os.path.join(os.path.dirname(os.path.abspath(__file__)) + "/../data/")
    threads = []
    print("Connecting to DB ")
    engine = create_engine(
        'postgresql+psycopg2://postgres:PASSWORD@aviationdbcluster.cluster-czl7eyzwjcyy.us-east-1.rds.amazonaws.com:5432/faa_database')
    for subdir, dirs, files in os.walk(datapath):
        for filename in files:
            filepath = subdir + os.sep + filename
            if filepath.endswith(".csv"):
                newThread = readAndInsertDataFileThread(filepath, engine)
                newThread.start()
                # threads.append(newThread)
                newThread.join()
    # for proc in threads:


    print("Done")
