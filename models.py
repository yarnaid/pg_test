from datetime import date, timedelta
from uuid import uuid4
from random import randint

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

Base = declarative_base()


def get_engine(**kwargs):
    return create_engine('postgresql://{db_user}:{db_password}@{host}/{db_name}'.format(**kwargs))


def get_session(engine):
    Session = sessionmaker(bind=engine)
    return scoped_session(Session)()


class TestTable(Base):
    __tablename__ = 'test_table'

    _rand_lim = 100000
    _day_shift = 365

    id = Column(Integer, primary_key=True)
    str_field = Column(String)
    dt_field = Column(Date)
    value = Column(Integer)

    @classmethod
    def random(cls, session):
        day = date.today() - timedelta(randint(-cls._day_shift, cls._day_shift))
        str_ = uuid4()
        val = randint(-cls._rand_lim, cls._rand_lim)
        res = cls(str_field=str_, dt_field=day, value=val)
        session.add(res)
        return res
