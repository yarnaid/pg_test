from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy import create_engine

Base = declarative_base()


def get_engine(**kwargs):
    return create_engine('postgresql://{db_user}:{db_password}@{host}/{db_name}'.format(**kwargs))


class TestTable(Base):
    __tablename__ = 'test_table'

    id = Column(Integer, primary_key=True)
    str_field = Column(String)
    dt_field = Column(Date)
