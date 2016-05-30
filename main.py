import os
from random import randint
from datetime import datetime, timedelta
from tornado.options import define, options
from tornado import gen, ioloop

import models

define('host', default='localhost:5432', help='Database host')
define('db_name', default='postgres', help='Name of database to connect')
define('db_user', default='postgres', help='User for database')
define('db_password', default='123456', help='Database password')
define('test_time', default=60, help='Time of test running in seconds')
define('clients_number', default=8, help='Number of clients for concurrent testing')
define('freq', default=60, help='Frequency of requests generation for each client (items/sec)')
define('conf_file', default=None, help='Path to configuration file')
define('rw_ratio', default=0.2, help='Ratio read_req/write_req')


options.parse_command_line()
if options.conf_file is not None and os.path.exists(options.conf_file):
    options.parse_config_file(options.conf_file)


@gen.coroutine
def write_worker():
    engine = models.get_engine(**options.as_dict())
    session = models.get_session(engine)
    models.TestTable.random(session)
    session.commit()
    session.close()


@gen.coroutine
def read_worker():
    engine = models.get_engine(**options.as_dict())
    session = models.get_session(engine)
    query = session.query(models.TestTable)\
        .order_by(models.TestTable.value)\
        .filter(models.TestTable.value > models.TestTable._rand_lim)
    for inst in query:
        inst
    session.close()


@gen.coroutine
def run():
    start_time = datetime.now()
    stop_time = start_time + timedelta(seconds=options.test_time)
    pause = 60. / options.freq
    while datetime.now() < stop_time:
        gen.sleep(pause)
        p = randint(0, 100)
        if float(p) / 100. <= options.rw_ratio:
            yield write_worker()
        else:
            yield read_worker()


def main():
    engine = models.get_engine(**options.as_dict())
    models.Base.metadata.create_all(engine)
    session = models.get_session(engine)
    models.TestTable.random(session)
    session.commit()
    session.close()
    import logging
    logging.basicConfig()
    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(run)


if __name__ == '__main__':
    main()
