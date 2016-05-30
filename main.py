import os
from tornado.options import define, options

import models

define('host', default='localhost:5432', help='Database host')
define('db_name', default='postgres', help='Name of database to connect')
define('db_user', default='postgres', help='User for database')
define('db_password', default='123456', help='Database password')
define('test_time', default=60, help='Time of test running in seconds')
define('clients_number', default=8, help='Number of clients for concurrent testing')
define('requests_number', default=60, help='Number of request for session for one client')
define('conf_file', default=None, help='Path to configuration file')


options.parse_command_line()
if options.conf_file is not None and os.path.exists(options.conf_file):
    options.parse_config_file(options.conf_file)


def main():
    engine = models.get_engine(**options.as_dict())
    models.Base.metadata.create_all(engine)
    raise NotImplementedError


if __name__ == '__main__':
    main()
