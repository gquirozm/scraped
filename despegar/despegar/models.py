from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy.engine.url import URL

import settings


DeclarativeBase = declarative_base()


def db_connect():
    """Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine('postgresql://dpa-user@localhost:5432/scrape')

def drop_despegar_table(engine):
    """"""
    DeclarativeBase.metadata.drop_all(engine)


def create_despegar_table(engine):
    """"""
    DeclarativeBase.metadata.create_all(engine)


class Despegar(DeclarativeBase):
    """Sqlalchemy deals model"""
    __tablename__ = "despegar"

    id = Column('id',Integer, primary_key=True)
    city = Column('city', String, nullable=True)
    name = Column('name', String, nullable=True)
    price = Column('price', String, nullable=True)
    address = Column('address', String, nullable=True)
    link = Column('link', String, nullable=True)
    latitud = Column('latitud', String, nullable=True)
    longitud = Column('longitud', String, nullable=True)
    source = Column('source', String, nullable=True)	
    date = Column('date', DateTime, nullable=True)
