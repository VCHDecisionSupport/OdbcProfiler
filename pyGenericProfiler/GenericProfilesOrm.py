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

class server_info(Base):
    __tablename__ = 'server_info'
    __table_args__ = (
        Index('ix_server_info', 'server_name', 'server_type', unique=True),
    )

    server_info_id = Column(Integer, primary_key=True)
    server_name = Column(String(100))
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


class view_table_info(Base):
    __tablename__ = 'view_table_info'

    view_table_info_id = Column(Integer, primary_key=True)
    database_info_id = Column(ForeignKey('database_info.database_info_id'), nullable=False)
    ansi_view_table_name = Column(String(100), nullable=False, unique=False)
    schema_name = Column(String(100), nullable=True, unique=False)
    view_table_name = Column(String(100), nullable=True, unique=False)
    logical_path = Column(String(100))

    database_info = relationship('database_info')
    
    def get_primary_key_value(self):
        return self.view_table_info_id
    

class view_table_profile(Base):
    __tablename__ = 'view_table_profile'
    __table_args__ = (
        Index('ix_view_table_profile_id', 'view_table_profile_id', 'view_table_row_count_date', unique=True),
    )

    view_table_profile_id = Column(Integer, primary_key=True)
    view_table_info_id = Column(ForeignKey('view_table_info.view_table_info_id'), nullable=False)
    view_table_row_count = Column(Integer)
    view_table_row_count_date = Column(DateTime)
    view_table_row_count_execution_time = Column(Numeric(precision=5, scale=1), nullable=True)
    view_table_info = relationship('view_table_info')
    
    def get_primary_key_value(self):
        return self.view_table_profile_id


class column_info(Base):
    __tablename__ = 'column_info'
    __table_args__ = (
        Index('ix_column_info', 'column_info_id', unique=True),
    )

    column_info_id = Column(Integer, primary_key=True)
    # view_table_profile_id = Column(ForeignKey('view_table_profile.view_table_profile_id'), nullable=False)
    view_table_info_id = Column(ForeignKey('view_table_info.view_table_info_id'), nullable=False)
    ansi_column_name = Column(String(100))
    view_table_info = relationship('view_table_info')
    
    def get_primary_key_value(self):
        return self.column_info_id


class column_profile(Base):
    __tablename__ = 'column_profile'
    __table_args__ = (
        Index('ix_column_profile', 'column_profile_id', unique=True),
    )

    column_profile_id = Column(Integer, primary_key=True)
    column_info_id = Column(ForeignKey('column_info.column_info_id'), nullable=False)
    view_table_profile_id = Column(ForeignKey('view_table_profile.view_table_profile_id'), nullable=False)
    column_distinct_count = Column(Integer)
    column_distinct_count_execution_time = Column(Numeric(precision=5, scale=1), nullable=True)
    column_distinct_count_date = Column(DateTime)

    column_info = relationship('column_info')
    
    def get_primary_key_value(self):
        return self.column_profile_id


class column_histogram(Base):
    __tablename__ = 'column_histogram'
    __table_args__ = (
        Index('ix_column_histogram', 'column_histogram_id', unique=True),
    )

    column_histogram_id = Column(Integer, primary_key=True)
    column_profile_id = Column(ForeignKey('column_profile.column_profile_id'), nullable=False)
    column_value_count = Column(Integer)
    column_value_string = Column(String(100))
    
    column_profile = relationship('column_profile')
    
    def get_primary_key_value(self):
        return self.column_histogram_id

    # def __init__(self,view_table_profile_id,view_table_info_id,profile_date,view_table_row_count):
    #     self.view_table_profile_id = view_table_profile_id
    #     self.view_table_info_id = view_table_info_id
    #     self.profile_date = profile_date
    #     self.view_table_row_count = view_table_row_count


def insert_if_not_exists(session, orm_class, **kwargs):
    print(kwargs)
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
    print(kwargs)
    row = orm_class(**kwargs)
    session.add(row)
    session.commit()
    return row.get_primary_key_value()

class GenericProfiles(object):
    def __init__(self):
        # params = urllib.parse.quote_plus("DRIVER={ODBC Driver 11 for Sql Server};SERVER=STDBDECSUP01;DATABASE=GenericProfiles;Trusted_Connection=Yes;")
        # params = urllib.parse.quote_plus("DRIVER={ODBC Driver 13 for Sql Server};SERVER=localhost;DATABASE=AutoTest;UID=sa;PWD=2and2is5")
        # engine = create_engine('mssql+pyodbc:///?odbc_connect='+params)
        # params = urllib.parse.quote_plus("DRIVER={MySql ODBC 5.3 Unicode Driver};SERVER=localhost;DATABASE=generic_profiles;UID=sa;PWD=2and2is5")
        # engine = create_engine('mysql+pyodbc:///?odbc_connect='+params)
        params = urllib.parse.quote_plus("DRIVER={ODBC Driver 11 for Sql Server};SERVER=STDBDECSUP01;DATABASE=AutoTest;Trusted_Connection=Yes;")
        engine = create_engine('mssql+pyodbc:///?odbc_connect='+params)
        self.session = Session(bind=engine)

    def log_server_info(self, **kwargs):
        return insert_if_not_exists(self.session, server_info, **kwargs)

    def log_database_info(self, **kwargs):
        return insert_if_not_exists(self.session, database_info, **kwargs)

    def log_view_table_info(self, **kwargs):
        return insert_if_not_exists(self.session, view_table_info, **kwargs)
    def log_view_table_profile(self, **kwargs):
        return insert(self.session, view_table_profile, **kwargs)

    def log_column_info(self, **kwargs):
        return insert(self.session, column_info, **kwargs)
    def log_column_profile(self, **kwargs):
        return insert(self.session, column_profile, **kwargs)


def deploy_sql_alchemy_model_database():
    # params = urllib.parse.quote_plus("DRIVER={ODBC Driver 13 for Sql Server};SERVER=localhost;DATABASE=AutoTest;UID=sa;PWD=2and2is5")
    params = urllib.parse.quote_plus("DRIVER={ODBC Driver 11 for Sql Server};SERVER=STDBDECSUP01;DATABASE=AutoTest;Trusted_Connection=Yes;")
    engine = create_engine('mssql+pyodbc:///?odbc_connect='+params)
    # params = urllib.parse.quote_plus("DRIVER={MySql ODBC 5.3 Unicode Driver};SERVER=localhost;DATABASE=generic_profiles;UID=sa;PWD=2and2is5")
    # engine = create_engine('mysql+pyodbc:///?odbc_connect='+params)
    print(engine)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


deploy_sql_alchemy_model_database()

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
