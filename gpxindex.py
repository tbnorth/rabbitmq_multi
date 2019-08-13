"""
rabbit mq based task distribution, perhaps should be celery instead

gpxindex.py - index may GPS track files

Terry N. Brown terrynbrown@gmail.com Mon Aug 12 19:09:17 CDT 2019
"""

import argparse
import json
import multiprocessing
import os
import pika
import time
from pathlib import Path
from uuid import uuid4

QUEUE = 'gpxindex'


def make_parser():

    parser = argparse.ArgumentParser(
        description="""Index may GPS track files""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--load-queue",
        help="Queue files to index from folder",
        metavar='FOLDER',
    )
    parser.add_argument(
        "--spawn", help="Spawn N workers", metavar='N', type=int
    )
    parser.add_argument(
        "--show-queue", action='store_true', help="Show files queued to index"
    )
    parser.add_argument(
        "--clear-queue",
        action='store_true',
        help="Clear files queued to index",
    )
    parser.add_argument(
        "--exit",
        action='store_true',
        help="With --clear-queue, leave an exit task on the queue "
        "after clearing",
    )
    return parser


def get_options(args=None):
    """
    get_options - use argparse to parse args, and return a
    argparse.Namespace, possibly with some changes / expansions /
    validatations.

    Client code should call this method with args as per sys.argv[1:],
    rather than calling make_parser() directly.

    Args:
        args ([str]): arguments to parse

    Returns:
        argparse.Namespace: options with modifications / validations
    """
    opt = make_parser().parse_args(args)

    # modifications / validations go here

    return opt


def get_connection_channel(**kwargs):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost', **kwargs)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE)
    return connection, channel


def enc(task):
    return json.dumps(task)


def dec(task):
    return json.loads(task.decode('utf-8'))


def load_queue(opt):
    connection, channel = get_connection_channel()
    for path, dirs, files in os.walk(opt.load_queue):
        for filename in [i for i in files if i.lower().endswith('.gpx')]:
            filepath = Path(path).joinpath(filename)
            print('+', filepath)
            task = dict(filepath=str(filepath))
            channel.basic_publish(
                exchange='', routing_key=QUEUE, body=enc(task)
            )
    channel.basic_publish(
        exchange='', routing_key=QUEUE, body=enc({'EXIT': "EXIT"})
    )
    connection.close()


def show_item_cb(ch, method, properties, body):
    print('*', dec(body).get('filepath'))


def show_queue(opt):
    connection, channel = get_connection_channel()
    channel.basic_consume(queue=QUEUE, on_message_callback=show_item_cb)
    connection.process_data_events(time_limit=0)
    connection.close()


def clear_queue(opt):
    connection, channel = get_connection_channel()
    channel.basic_consume(
        queue=QUEUE, on_message_callback=show_item_cb, auto_ack=True
    )
    connection.process_data_events(time_limit=0)
    if opt.exit:
        channel.basic_publish(
            exchange='', routing_key=QUEUE, body=enc({'EXIT': "EXIT"})
        )
    connection.close()


def proc_item_cb(ch, method, properties, body):
    body = dec(body)
    if body.get('EXIT'):
        return False
    print("X:", body['filepath'])
    time.sleep(5)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    return True


def proc_queue(opt):
    connection, channel = get_connection_channel()
    channel.basic_qos(prefetch_count=1)
    tag = str(uuid4())

    def cb(ch, method, properties, body, tag=tag):
        if not proc_item_cb(ch, method, properties, body):
            print("Cancelling")
            ch.basic_cancel(tag)

    channel.basic_consume(
        queue=QUEUE, on_message_callback=cb, consumer_tag=tag
    )
    print(tag)
    channel.start_consuming()
    # connection.process_data_events(time_limit=10)
    connection.close()


def spawn(opt):
    p = multiprocessing.Pool(opt.spawn)
    for i in range(opt.spawn):
        p.apply_async(proc_queue, (opt,))
    p.close()
    p.join()  # doesn't work because


def main():
    opt = get_options()
    for queue_op in 'load_queue', 'show_queue', 'clear_queue', 'spawn':
        if getattr(opt, queue_op):
            globals()[queue_op](opt)
            break
    else:
        proc_queue(opt)


if __name__ == "__main__":
    main()
