#!/usr/bin/python
# -*- mode: python; coding: utf-8 -*-

import psycopg2
from dateutil.relativedelta import *
import datetime
import csv
import numpy
import sys
import time
import textwrap
import contextlib
import psycopg2
import dateutil.parser
import datetime
import time
import os, os.path
import threading
import concurrent, concurrent.futures
import traceback
import tarfile
import io
import subprocess

year = 2019
needed_data = [[y,x] for y in [12] for x in range(27, 32)]
table = 'bigdata.all_checks_mo_2018'



CONNECT_PARAMS = 'host= dbname= user= password='

DEMAND_SQL_ITERATION = \
'''\
select
 time_check::date as date_check,
 tr_check_id,
 time_check,
 camera_id,
 regno,
 speed,
 gps_x,
 gps_y,
 camera_place,
 azimut,
 direction
from gibdd_traffic.tr_checks_%(year)s_{month_check} c
where time_check between %(start_time)s::timestamp without time zone and %(end_time)s::timestamp without time zone

    \
'''

def queryInsert(csv_name, table = table):
    subprocess.call(f'clickhouse-client -h 10.177.3.34 -u auditplus --password Rawk0knec+ --format_csv_delimiter="," --query="INSERT INTO {table} FORMAT CSV" < {csv_name}', shell=True)


csv_header = (
             'tr_check_id',
             'time_check',
             'camera_id',
             'regno',
             'speed',
             'gps_x',
             'gps_y',
             'camera_place',
             'azimut',
             'direction'
           )

def execute_on_recovery(cur, *args, **kwargs):
    error = None
    
    for i in range(5):
        try:
            return cur.execute(*args, **kwargs)
        except psycopg2.extensions.TransactionRollbackError as e:
            print(f'warning: {type(e)}: {str(e)}', file=sys.stderr)
            
            error = e
    
    raise error


def main():
    with contextlib.ExitStack() as stack:
        local = threading.local()
        io_lock = threading.RLock()
        workers = 3
        interval = 300 
        
              
        def process(month, day, start_time):
            try:
                    
                try:
                    con = local.con
                except AttributeError:
                    with io_lock:
                        print('*** new connection ***')
                    
                    con = stack.enter_context(psycopg2.connect(CONNECT_PARAMS))
                    
                    con.autocommit = True
                    cur = stack.enter_context(con.cursor())
                    
                    local.con = con
                    local.cur = cur
                else:
                    cur = local.cur

                execute_on_recovery(cur, DEMAND_SQL_ITERATION.format(month_check=str('%02d' % month)), {
                        'year':  year,
                        'start_time': str(start_time),
                        'end_time': str(start_time + datetime.timedelta(0, interval - 1))
                })
                
                row = cur.fetchall()
                if row is None:
                    return
                
                with io_lock:
                    with open(f'{year}-{month}-{day}.csv', 'a') as csv_file:
                        csv_writer = csv.writer(csv_file)
                        len_csv = 0
                        for r in row:
                            len_csv += 1
                            csv_writer.writerow(r)

                    print('start_time:', start_time, 'end_time:', start_time + datetime.timedelta(0,interval - 1), datetime.datetime.now(), f' done!    len_csv = {len_csv}')
                    
            except:
                with io_lock:
                    traceback.print_exc()


        executor = stack.enter_context(
            concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        )





        for month,day in needed_data:
            with open(f'{year}-{month}-{day}.csv', 'w') as csv_file:
                csv_writer = csv.writer(csv_file)
#                csv_writer.writerow(csv_header)

            start_time = datetime.datetime(year,month, day, 0, 0, 0)
            finish_time = datetime.datetime(year, month, day, 23, 59, 59)
            while start_time < finish_time:
                executor.submit(process, month, day, start_time)
                start_time += datetime.timedelta(0, interval)

        executor.shutdown()


        

if __name__ == '__main__':
    main()
    for month,day in needed_data:
        print(f'{year}-{month}-{day}.csv has started to insert')
        queryInsert(f'{year}-{month}-{day}.csv')
    print('done!')
