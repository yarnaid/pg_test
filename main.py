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
from tornado.queues import PriorityQueue

import models

define('host', default='localhost:5432', help='Database host')
define('db_name', default='postgres', help='Name of database to connect')
define('db_user', default='postgres', help='User for database')
define('db_password', default='123456', help='Database password')
define('test_time', default=60, help='Time of test running in seconds')
define('clients_number', default=100, help='Number of clients for concurrent testing')
define('queries_number', default=60, help='Number of queries to database per client')
define('conf_file', default=None, help='Path to configuration file')
define('rw_ratio', default=0.2, help='Ratio read_req/write_req')
define('debug', default=False, help='Show debug messages')
define('log_suffix', default=None, help='Suffix for log file')
define('log_params', default=True, help='Add run params to log name [test_time, clients_number, queries_number]')
define('truncate', default=True, help='Remove all data from table before start')
define('init_records', default=10000, help='Initiate test table with number of records')


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
            t = models.TestTable.random(session)
            session.add(t)
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
    def __init__(self, register, name=None):
        self._queue = PriorityQueue(maxsize=options.queries_number)
        self.queries_number = options.queries_number
        self.stop_time = datetime.now() + timedelta(seconds=options.test_time)
        self.register = register
        self.__count = 0
        self.__processed = 0
        if register is not None:
            register.add(self)
        if name is None:
            self.name = uuid4()
        else:
            self.name = name
        io_loop = IOLoop.instance()
        io_loop.spawn_callback(self.consume)
        io_loop.spawn_callback(self.start)

    @gen.coroutine
    def start(self):
        yield self.produce()
        yield self._queue.join()

    @gen.coroutine
    def exit(self):
        logger.debug('client {} exiting'.format(self.name))
        self.register.remove(self)
        if not self.register:
            IOLoop.instance().stop()
            logger.debug('all clients exited')
        else:
            logger.debug('{} clients left'.format(len(self.register)))

    @gen.coroutine
    def produce(self):
        logger.debug('producer {} start'.format(self.name))
        while True:
            if self.__count >= self.queries_number or datetime.now() > self.stop_time:
                break
            p = randint(0, 100)
            res = 'r'
            if float(p) / 100. <= options.rw_ratio:
                res = 'w'
            yield self._queue.put((randint(0, 10), res))
            self.__count += 1
        logger.debug('producer {} finished'.format(self.name))
        self.stop_time = datetime.now() + timedelta(seconds=options.test_time)

    @gen.coroutine
    def consume(self):
        logger.debug('consumer {} start'.format(self.name))
        while True:
            if datetime.now() > self.stop_time or self.__processed >= self.queries_number:
                logger.debug('{}'.format(datetime.now() > self.stop_time))
                break
            pr, tp = yield self._queue.get()
            w = Worker(self.name)
            # if tp == 'r':
            #     yield w.read_worker()
            # elif tp == 'w':
            #     yield w.write_worker()
            yield gen.sleep(0.00000)  # hack
            if tp == 'w':
                yield w.write_worker()
            elif tp == 'r':
                yield w.read_worker()
            self.__processed += 1
        self._queue.task_done()
        logger.debug('consumer {} finished'.format(self.name))
        yield gen.sleep(0.00000)  # hack
        yield self.exit()


def init():
    engine = models.get_engine(**options.as_dict())
    models.Base.metadata.create_all(engine)
    if options.truncate:
        with contextlib.closing(models.get_session(engine)) as session:
            models.TestTable.truncate(session)
    if options.init_records:
        with contextlib.closing(models.get_session(engine)) as session:
            session.bulk_save_objects([models.TestTable.random(session) for i in xrange(options.init_records)])
            session.commit()
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
    csv_file.writerow(['start', 'finish', 'runtime', 'success', 'r/w'])
    for i in xrange(options.clients_number):
        register.add(
            Client(
                register=register,
                name=str(i)
            )
        )
    IOLoop.current().start()
    IOLoop.current().stop()


if __name__ == '__main__':
    main()
