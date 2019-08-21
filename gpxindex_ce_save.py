"""
gpxindex_ce_save.py - job saver part of celery version of
gpxindex.py.

Start with

celery -A gpxindex_ce_save worker --hostname w2@%h --loglevel=info --autoscale 1,1

Terry N. Brown terrynbrown@gmail.com Tue Aug 20 21:28:54 CDT 2019
"""

import time
from celery import Celery

app = Celery('gpxindex_ce_save', broker='pyamqp://guest@localhost//')
app.conf.task_default_queue = 'saver'


@app.task
def save(thing):
    time.sleep(1)
    print("SAVED", thing)
