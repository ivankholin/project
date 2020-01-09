#!/usr/bin/python
# -*- mode: python; coding: utf-8 -*-

assert str is not bytes

import sys
import textwrap
import contextlib
import psycopg2
import dateutil.parser
import datetime
import time
import os, os.path
import csv
import threading
import concurrent, concurrent.futures
import traceback
import tarfile
import io

CONNECT_PARAMS = 'host= dbname= user= password='

TS_IMAGE_SQL = \
'''\

   \
'''

def strftime(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')

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
    mod_num = 0

    csv_path = f'detect_train_load_new.csv'
    out_dir_path_ts = f'train/auto.out'  # example tstypes
    out_dir_path_grz = f'train/regno.out'  # example tstypes

    workers = 2
    csv_head = {}

        
    with contextlib.ExitStack() as stack:
        local = threading.local()
        io_lock = threading.RLock()

        
        executor = stack.enter_context(
            concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        )
        
        def process(csv_row_i, csv_row):
            
            try:
                
                
                tr_check_id = int(csv_row[csv_head['tr_check_id']])
                regno = csv_row[csv_head['regno']]
                time_check = csv_row[csv_head['time_check']]

                
                image_name = '{tr_check_id}_{regno}.jpg'.format(
                                    csv_row_i = csv_row_i,
                                    tr_check_id = int(tr_check_id),
                                    regno = regno
                                )
                
                ts_image_path = os.path.join(
                    out_dir_path_ts,
                    image_name,
                )
                grz_image_path = os.path.join(
                    out_dir_path_grz,
                    image_name,
                )
               
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
                
                execute_on_recovery(cur, TS_IMAGE_SQL, {
                    'tr_check_id': tr_check_id,
                })
                
                row = cur.fetchone()
                
                if row is None:
                    return
                
                ts_image, grz_image = row
                
                with io_lock:
                    print(f'csv_row_i={csv_row_i} ts_image {repr(tr_check_id)}: {repr(len(ts_image))} bytes')
                    
                    
                    
                    # Write to sepparate image files
                    os.makedirs(out_dir_path_ts, exist_ok=True)
                    with open(ts_image_path, mode='wb') as fd:
                        fd.write(ts_image)
                    
                    
                    # Write to sepparate image files
                    os.makedirs(out_dir_path_grz, exist_ok=True)
                    with open(grz_image_path, mode='wb') as fd:
                        fd.write(grz_image)
                    

            except:
                with io_lock:
                    traceback.print_exc()


        
        with open(csv_path, 'r') as csw_file:
            csv_reader = csv.reader(csw_file)
            iterator = 0
            head = next(csv_reader)
            csv_head = {x: y for x,y in zip(head, range(len(head)))}
            
            for csv_i in csv_reader:
                iterator += 1
                executor.submit(process, iterator, csv_i)


        executor.shutdown()
        
        print('done!')

if __name__ == '__main__':
    main()
