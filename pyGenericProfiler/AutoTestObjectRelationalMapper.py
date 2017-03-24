# coding: utf-8
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import Session
from sqlalchemy import *
import urllib

import datetime
Base = declarative_base()
metadata = Base.metadata



# """
# https://pypi.python.org/pypi/sqlacodegen

# C:\Users\gcrowell\Source\Repos\GenericProfiles\pyGenericProfiler\PyGenericProfil
# er>C:\Users\gcrowell\AppData\Local\Programs\Python\Python35\Scripts\sqlacodegen
# mssql+pyodbc:///?odbc_connect=DRIVER%3D%7BODBC+Driver+11+for+Sql+Server%7D%3BSER
# VER%3DSTDBDECSUP01%3BDATABASE%3DGenericProfiles%3BTrusted_Connection%3DYes%3B --
# outfile "out.py"

# """

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 11 for Sql Server};SERVER=STDBDECSUP01;DATABASE=AutoTest_TEST;Trusted_Connection=Yes;")
# params = urllib.parse.quote_plus("DRIVER={ODBC Driver 13 for Sql Server};SERVER=localhost;DATABASE=AutoTest;UID=sa;PWD=2and2is5")

class server_info(Base):
    __tablename__ = 'server_info'
    __table_args__ = (
        Index('ix_server_info', 'server_name', 'server_type', unique=True),
    )

    server_info_id = Column(Integer, primary_key=True)
    server_name = Column(String(100), nullable=False)
    server_type = Column(String(100))
    
    def get_primary_key_value(self):
        return self.server_info_id


class database_info(Base):
    __tablename__ = 'database_info'

    database_info_id = Column(Integer, primary_key=True)
    server_info_id = Column(ForeignKey('server_info.server_info_id'), nullable=False)
    database_name = Column(String(100), nullable=False)

    server_info = relationship('server_info')
    
    def get_primary_key_value(self):
        return self.database_info_id


class table_info(Base):
    __tablename__ = 'table_info'

    table_info_id = Column(Integer, primary_key=True)
    database_info_id = Column(ForeignKey('database_info.database_info_id'), nullable=False)
    ansi_full_table_name = Column(String(100), nullable=False, unique=False)
    schema_name = Column(String(100), nullable=True, unique=False)
    table_name = Column(String(100), nullable=True, unique=False)
    logical_path = Column(String(100))
    table_type = Column(String(100))

    database_info = relationship('database_info')
    
    def get_primary_key_value(self):
        return self.table_info_id
    

class table_profile(Base):
    __tablename__ = 'table_profile'
    __table_args__ = (
        Index('ix_table_profile_id', 'table_profile_id', 'table_row_count_datetime', unique=True),
    )

    table_profile_id = Column(Integer, primary_key=True)
    table_info_id = Column(ForeignKey('table_info.table_info_id'), nullable=False)
    table_row_count = Column(Integer)
    table_row_count_datetime = Column(DateTime)
    table_row_count_execution_seconds = Column(Numeric(precision=5, scale=1), nullable=True)
    
    table_info = relationship('table_info')
    
    def get_primary_key_value(self):
        return self.table_profile_id


class column_info(Base):
    __tablename__ = 'column_info'
    __table_args__ = (
        Index('ix_column_info', 'column_info_id', unique=True),
    )

    column_info_id = Column(Integer, primary_key=True)
    table_info_id = Column(ForeignKey('table_info.table_info_id'), nullable=False)
    ansi_full_column_name = Column(String(100))
    ansi_full_table_name = Column(String(100))
    column_name = Column(String(100))
    
    table_info = relationship('table_info')
    
    def get_primary_key_value(self):
        return self.column_info_id


class column_profile(Base):
    __tablename__ = 'column_profile'
    __table_args__ = (
        Index('ix_column_profile', 'column_profile_id', unique=True),
    )

    column_profile_id = Column(Integer, primary_key=True)
    column_info_id = Column(ForeignKey('column_info.column_info_id'), nullable=False)
    table_profile_id = Column(ForeignKey('table_profile.table_profile_id'), nullable=False)
    column_distinct_count = Column(Integer)
    column_distinct_count_execution_seconds = Column(Numeric(precision=5, scale=1), nullable=True)
    column_distinct_count_datetime = Column(DateTime)

    column_info = relationship('column_info')
    table_profile = relationship('table_profile')
    
    def get_primary_key_value(self):
        return self.column_profile_id


class column_histogram(Base):
    __tablename__ = 'column_histogram'
    __table_args__ = (
        Index('ix_column_histogram', 'column_histogram_id', unique=True),
    )

    column_histogram_id = Column(Integer, primary_key=True)
    column_profile_id = Column(ForeignKey('column_profile.column_profile_id'), nullable=False)
    column_info_id = Column(ForeignKey('column_info.column_info_id'), nullable=False)
    column_value_count = Column(Integer)
    column_value_string = Column(String(100))
    
    column_info = relationship('column_info')
    column_profile = relationship('column_profile')
    
    def get_primary_key_value(self):
        return self.column_histogram_id

    # def __init__(self,table_profile_id,table_info_id,profile_datetime,table_row_count):
    #     self.table_profile_id = table_profile_id
    #     self.table_info_id = table_info_id
    #     self.profile_datetime = profile_datetime
    #     self.table_row_count = table_row_count



def insert_if_not_exists(session, orm_class, **kwargs):
    # print(kwargs)
    # print(orm_class)
    # print(orm_class.__dict__)
    # print(orm_class.__dict__.keys())
    # kwargs = kwargs.fromkeys(orm_class.__dict__.keys())
    row = session.query(orm_class).filter_by(**kwargs).first()
    if row:
        # print(row.get_primary_key_value())
        return row.get_primary_key_value()
    else:
        row = orm_class(**kwargs)
        session.add(row)
        session.commit()
        # print(row.get_primary_key_value())
        return row.get_primary_key_value()

def insert(session, orm_class, **kwargs):
    # print(kwargs)
    row = orm_class(**kwargs)
    session.add(row)
    session.commit()
    return row.get_primary_key_value()
def insert_all(session, orm_class, argv):
    print('insert_all: {}'.format(len(argv)))
    rows = [orm_class(**kwargs) for kwargs in argv]
    session.add_all(rows)
    session.commit()

class AutoTestOrm(object):
    def __init__(self):
        # params = urllib.parse.quote_plus("DRIVER={ODBC Driver 11 for Sql Server};SERVER=STDBDECSUP01;DATABASE=GenericProfiles;Trusted_Connection=Yes;")
        # params = urllib.parse.quote_plus("DRIVER={ODBC Driver 13 for Sql Server};SERVER=localhost;DATABASE=AutoTest;UID=sa;PWD=2and2is5")
        # engine = create_engine('mssql+pyodbc:///?odbc_connect='+params)
        # params = urllib.parse.quote_plus("DRIVER={MySql ODBC 5.3 Unicode Driver};SERVER=localhost;DATABASE=generic_profiles;UID=sa;PWD=2and2is5")
        # engine = create_engine('mysql+pyodbc:///?odbc_connect='+params)
        # params = urllib.parse.quote_plus("DRIVER={ODBC Driver 11 for Sql Server};SERVER=STDBDECSUP01;DATABASE=AutoTest;Trusted_Connection=Yes;")
        engine = create_engine('mssql+pyodbc:///?odbc_connect='+params)
        self.session = Session(bind=engine)
    def log_server_info(self, **kwargs):
        return insert_if_not_exists(self.session, server_info, **kwargs)
    def log_database_info(self, **kwargs):
        return insert_if_not_exists(self.session, database_info, **kwargs)
    def log_table_info(self, **kwargs):
        return insert_if_not_exists(self.session, table_info, **kwargs)
    def log_table_profile(self, **kwargs):
        return insert(self.session, table_profile, **kwargs)
    def log_column_info(self, **kwargs):
        return insert(self.session, column_info, **kwargs)
    def log_column_profile(self, **kwargs):
        return insert(self.session, column_profile, **kwargs)
    def log_column_histogram(self, argv):
        return insert_all(self.session, column_histogram, argv)


def deploy_sql_alchemy_model_database():
    print('recreating database backend...')
    # params = urllib.parse.quote_plus("DRIVER={ODBC Driver 13 for Sql Server};SERVER=localhost;DATABASE=AutoTest;UID=sa;PWD=2and2is5")
    # params = urllib.parse.quote_plus("DRIVER={ODBC Driver 11 for Sql Server};SERVER=STDBDECSUP01;DATABASE=AutoTest;Trusted_Connection=Yes;")
    engine = create_engine('mssql+pyodbc:///?odbc_connect='+params)
    # params = urllib.parse.quote_plus("DRIVER={MySql ODBC 5.3 Unicode Driver};SERVER=localhost;DATABASE=generic_profiles;UID=sa;PWD=2and2is5")
    # engine = create_engine('mysql+pyodbc:///?odbc_connect='+params)
    print('\tLogging Engine (SqlAlchemy): {}'.format(engine))
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


# deploy_sql_alchemy_model_database()

# from pyGenericProfiler import *


# sqlserver = SqlServerProfiler('STDBDECSUP03', 'CommunityMart')
# sqlserver.create_meta_dicts()
# sqlserver.execute_profile()
# save_profile(sqlserver.full_meta)

# sqlserver = SqlServerProfiler('SPDBDECSUP04', 'CommunityMart')
# sqlserver.create_meta_dicts()
# sqlserver.execute_profile()
# save_profile(sqlserver.full_meta)

# denodo = DenodoProfiler('foo', 'boo')
# denodo.create_meta_dicts()
# denodo.execute_profile()
# save_profile(denodo.full_meta)

# sqlserver = SqlServerProfiler('SPDBDECSUP04', 'DSDW')
# sqlserver.create_meta_dicts()
# sqlserver.execute_profile()
# save_profile(sqlserver.full_meta)
