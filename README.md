# RabbitMQ experiment.

Using a folder with 500
[`.gpx`](https://en.wikipedia.org/wiki/GPS_Exchange_Format)
files in it as practice for distributing work
with [RabbitMQ](https://www.rabbitmq.com/).  Probably a case for using
[Celery](http://www.celeryproject.org/), or just
[multiprocessing](https://docs.python.org/3/library/multiprocessing.html), but
a useful learning exercise for multi-node setups.

 - You have to `return`, not `exit()`, from a Python `multiprocessing`
 subprocess in a `Pool`, if you want `pool.join()` to exit.
 - So you need to have the RabbitMQ callback cancel the `.start_consuming()`
 call using `.basic_cancel()` with a pre-shared consumer tag.
 - Also there's no easy way to list a RabbitMQ queue without consuming it, in
 this case `--show-queue` just doesn't acknowledge the messages to leave them
 on the queue.
 - Although clearly doc'ed in the tutorial, it's east to miss the crucial
 `.basic_qos()` call needed for consumers to get one task at a time.
