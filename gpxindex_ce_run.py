"""
gpxindex_ce_gen.py - job runner part of celery version of
gpxindex.py.

Start with
celery -A gpxindex_ce_run worker --hostname w1@%h --loglevel=info

Terry N. Brown terrynbrown@gmail.com Tue Aug 20 21:28:54 CDT 2019
"""

import time
from celery import Celery

app = Celery('gpxindex_ce_run', broker='pyamqp://guest@localhost//')
app.conf.task_default_queue = 'runner'


from gpxindex_ce_save import save


@app.task
def run(thing):
    time.sleep(7)
    print(thing)
    save.delay(thing)
