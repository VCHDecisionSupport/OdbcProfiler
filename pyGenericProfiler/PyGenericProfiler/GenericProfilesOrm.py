# coding: utf-8
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import Session
from sqlalchemy import *
import urllib

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

class ServerInfo(Base):
    __tablename__ = 'ServerInfo'
    __table_args__ = (
        Index('CI_ServerInfo', 'ServerName', 'ServerType', unique=True),
    )

    
    ServerInfoID = Column(Integer, primary_key=True)
    ServerName = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'))
    ServerType = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'))
    
    # def __init__(self, ServerInfoID, ServerName, ServerType):
    #     self.ServerInfoID = ServerInfoID
    #     self.ServerName = ServerName
    #     self.ServerType = ServerType

class DatabaseInfo(Base):
    __tablename__ = 'DatabaseInfo'

    
    DatabaseInfoID = Column(Integer, primary_key=True)
    ServerInfoID = Column(ForeignKey('ServerInfo.ServerInfoID'), nullable=False)
    DatabaseName = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'), unique=True)

    ServerInfo = relationship('ServerInfo')

    # def __init__(self, DatabaseInfoID, ServerInfoID, DatabaseName):
    #     self.DatabaseInfoID = DatabaseInfoID
    #     self.ServerInfoID = ServerInfoID
    #     self.DatabaseName = DatabaseName


class ViewTableInfo(Base):
    __tablename__ = 'ViewTableInfo'

    
    ViewTableInfoID = Column(Integer, primary_key=True)
    DatabaseInfoID = Column(ForeignKey('DatabaseInfo.DatabaseInfoID'), nullable=False)
    PhysicalViewTableName = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'), nullable=False, unique=True)
    LogicalViewTablePath = Column(String(100, 'SQL_Latin1_General_CP1_CI_AS'))

    DatabaseInfo = relationship('DatabaseInfo')
    
    # def __init__(self,ViewTableInfoID,DatabaseInfoID,PhysicalViewTableName,LogicalViewTablePath):
    #     self.ViewTableInfoID = ViewTableInfoID
    #     self.DatabaseInfoID = DatabaseInfoID
    #     self.PhysicalViewTableName = PhysicalViewTableName
    #     self.LogicalViewTablePath = LogicalViewTablePath


class ViewTableProfile(Base):
    __tablename__ = 'ViewTableProfile'
    __table_args__ = (
        Index('CI_ViewTableProfile', 'ViewTableProfileID', 'ProfileDate', unique=True),
    )

    
    ViewTableProfileID = Column(Integer, primary_key=True)
    ViewTableInfoID = Column(ForeignKey('ViewTableInfo.ViewTableInfoID'), nullable=False)
    ProfileDate = Column(DateTime)
    ViewTableRowCount = Column(Integer)

    ViewTableInfo = relationship('ViewTableInfo')

    # def __init__(self,ViewTableProfileID,ViewTableInfoID,ProfileDate,ViewTableRowCount):
    #     self.ViewTableProfileID = ViewTableProfileID
    #     self.ViewTableInfoID = ViewTableInfoID
    #     self.ProfileDate = ProfileDate
    #     self.ViewTableRowCount = ViewTableRowCount

def foo(orm_class):
    params = urllib.parse.quote_plus("DRIVER={ODBC Driver 13 for Sql Server};SERVER=localhost;DATABASE=AutoTest;UID=sa;PWD=2and2is5")
    params = urllib.parse.quote_plus("DRIVER={ODBC Driver 11 for Sql Server};SERVER=STDBDECSUP01;DATABASE=GenericProfiles;Trusted_Connection=Yes;")
    engine = create_engine('mssql+pyodbc:///?odbc_connect='+params)
    session = Session(bind=engine)
    server_info = orm_class(ServerName='unknown_server_name8', ServerType='Denodo')
    session.add(server_info)
    session.commit()
    print(server_info.ServerInfoID)

def main():
    # params = urllib.parse.quote_plus("DRIVER={ODBC Driver 11 for Sql Server};SERVER=STDBDECSUP01;DATABASE=GenericProfiles;Trusted_Connection=Yes;")
    # engine = create_engine('mssql+pyodbc:///?odbc_connect='+params)
    # session = Session(bind=engine)
    # server_info = ServerInfo(ServerName='unknown_server_name6', ServerType='Denodo')
    # # print(server_info.info)
    # session.add(server_info)
    # print(server_info.ServerInfoID)
    # # print(server_info.info)

    # com = session.commit()
    # print(server_info.ServerInfoID)

    # print(com)
    foo(ServerInfo)
if __name__ == '__main__':
    main()
