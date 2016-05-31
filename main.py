import os
from random import randint
import logging
import csv
import contextlib
from uuid import uuid4
from datetime import datetime, timedelta
from tornado.options import define, options
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.queues import Queue

import models

define('host', default='localhost:5432', help='Database host')
define('db_name', default='postgres', help='Name of database to connect')
define('db_user', default='postgres', help='User for database')
define('db_password', default='123456', help='Database password')
define('test_time', default=60, help='Time of test running in seconds')
define('clients_number', default=8, help='Number of clients for concurrent testing')
define('queries_number', default=60, help='Number of queries to database per client')
define('conf_file', default=None, help='Path to configuration file')
define('rw_ratio', default=0.2, help='Ratio read_req/write_req')
define('debug', default=False, help='Show debug messages')
define('log_suffix', default=None, help='Suffix for log file')
define('log_params', default=True, help='Add run params to log name [test_time, clients_number, queries_number]')
define('truncate', default=True, help='Remove all data from table before start')
define('batch_size', default=10000, help='Divide request in chunks')


logger = logging.getLogger()
options.parse_command_line()
if options.conf_file is not None and os.path.exists(options.conf_file):
    options.parse_config_file(options.conf_file)

if options.debug:
    logger.setLevel(logging.DEBUG)

csv_file = None
chunc_start = 0


def log_func(func_tp):
    def innter(func_):
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            success = True
            try:
                func_(*args, **kwargs)
            except:
                success = False
            stop_time = datetime.now()
            runtime = stop_time - start_time
            csv_file.writerow((start_time, stop_time, runtime, success, func_tp))
        return wrapper
    return innter


class Worker(object):
    def __init__(self, id_):
        self.id_ = id_

    @gen.coroutine
    @log_func('w')
    def write_worker(self):
        logger.debug('write start {}'.format(self.id_ if self.id_ else ''))
        engine = models.get_engine(**options.as_dict())
        with contextlib.closing(models.get_session(engine)) as session:
            models.TestTable.random(session)
            session.commit()
        logger.debug('write stop {}'.format(self.id_ if self.id_ else ''))

    @gen.coroutine
    @log_func('r')
    def read_worker(self):
        logger.debug('read start {}'.format(self.id_ if self.id_ else ''))
        engine = models.get_engine(**options.as_dict())
        with contextlib.closing(models.get_session(engine)) as session:
            query = session.query(models.TestTable)\
                .order_by(models.TestTable.value)\
                .filter(models.TestTable.value > models.TestTable._rand_lim)
            for inst in query:
                inst
        logger.debug('read stop {}'.format(self.id_ if self.id_ else ''))


class Client(object):
    def __init__(self, stop_time, register, name=None):
        self._queue = Queue()
        self._queue.join()
        self.queries_number = options.queries_number
        self.stop_time = stop_time
        self.register = register
        if register is not None:
            register.add(self)
        if name is None:
            self.name = uuid4()
        else:
            self.name = name
        IOLoop.current().spawn_callback(self.produce)

    @gen.coroutine
    def exit(self):
        logger.debug('client {} exiting'.format(self.name))
        self.register.remove(self)
        if not self.register:
            IOLoop.instance().stop()

    @gen.coroutine
    def produce(self):
        logger.debug('producer start')
        for i in xrange(self.queries_number):
            if datetime.now() > self.stop_time:
                break
            p = randint(0, 100)
            w = Worker(self.name)
            if float(p) / 100. <= options.rw_ratio:
                w.run = w.write_worker
            else:
                w.run = w.read_worker
            yield self._queue.put(w.run())
        yield self.exit()
        logger.debug('producer stop')


def init():
    engine = models.get_engine(**options.as_dict())
    models.Base.metadata.create_all(engine)
    session = models.get_session(engine)
    models.TestTable.random(session)
    session.commit()
    session.close()
    if options.truncate:
        models.TestTable.truncate(engine)
    logger.debug('initiated')


def get_log_name(*args):
    suffix_array = [datetime.now()]
    delim = '_'
    splitter = ','
    prefix = os.path.join('logs', 'log' + delim)
    if options.log_suffix:
        suffix_array.extend(options.suffix_array.split(splitter))
    if options.log_params:
        suffix_array.extend([
            options.test_time,
            options.clients_number,
            options.queries_number
        ])
    if args:
        suffix_array.extend(*args)
    suffix = delim.join(map(str, suffix_array))
    ext = '.csv'
    return prefix + suffix + ext


@gen.coroutine
def main():
    init()
    global csv_file
    register = set()
    if not os.path.exists('logs'):
        os.mkdir('logs')
    log_file = open(get_log_name(), 'wb')
    csv_file = csv.writer(log_file)
    stop_time = datetime.now() + timedelta(seconds=options.test_time)
    for i in xrange(options.clients_number):
        register.add(
            Client(
                stop_time=stop_time,
                register=register,
                name=str(i)
            )
        )
    IOLoop.current().start()
    IOLoop.current().stop()


if __name__ == '__main__':
    main()
