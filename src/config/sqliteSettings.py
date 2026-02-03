#! /usr/bin/env python3
#encoding: utf-8

#Filename: sqliteSettings.py  
#Author: Steven Lian's team
#E-mail:  / /steven.lian@gmail.com  
#Date: 2019-03-30
#Description:  SQL数据库地址,网络地址等

_VERSION="20260131"

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

from config import local_settings as local_settings

from common import  sqliteHandle as sqliteHandle

#生产环境 rss | 测试环境 dss
_SYS = local_settings._SYS
#_SYS = "server_01"


#sqlite数据库信息  begin
SQLITE_DB_DIR = {
    "local":"../data",
    "server_01":"/data/database", 
    "server_02":"/data/database", 
    "home":"../data", 
    }[_SYS]

#主库，写记录
SQLITE_WRITE_FILENAME = {
    "local":"database.sqlite",
    "server_01":"chin_hotel", 
    "server_02":"chin_hotel", 
    "home":"database.sqlite", 
    }[_SYS]
    
SQLITE_WRITE_USER = {
    "local":"",
    "server_01":"chdba", 
    "server_02":"chdba", 
    "home":"", 
    }[_SYS]

SQLITE_WRITE_PASSWD = {
    "local":"",
    "server_01":"", 
    "server_02":"", 
    "home":"", 
    }[_SYS]


#从库，读记录    
SQLITE_READ_FILENAME = {
    "local":"database.sqlite",
    "server_01":"chin_hotel", 
    "server_02":"chin_hotel", 
    "home":"database.sqlite", 
    }[_SYS]

SQLITE_READ_USER = {
    "local":"",
    "server_01":"chdba", 
    "server_02":"chdba", 
    "home":"", 
    }[_SYS]

SQLITE_READ_PASSWD = {
    "local":"",
    "server_01":"", 
    "server_02":"", 
    "home":"", 
    }[_SYS]

#主库，写记录
sqliteDBName = f"{SQLITE_DB_DIR}/{SQLITE_WRITE_FILENAME}"
# if not os.path.exists(sqliteDBName):
#     pass
sqlDBW = sqliteHandle.getSqlliteDB(sqliteDBName)

#从库，读记录
sqliteDBName = f"{SQLITE_DB_DIR}/{SQLITE_READ_FILENAME}"
sqlDBR = sqliteHandle.getSqlliteDB(sqliteDBName)

sqliteDB = sqliteHandle.sqliteHandle(dbW=sqlDBW,dbR=sqlDBR)
   

_DEBUG = True  #预设trace开关，禁止修改

if __name__ == "__main__":
    pass
    # import pdb
    # pdb.set_trace()
    print ("_SYS",_SYS)
    print ("SQLITE_WRITE_FILENAME",SQLITE_WRITE_FILENAME)
    print ("SQLITE_WRITE_USER",SQLITE_WRITE_USER)
    print ("SQLITE_WRITE_PASSWD",SQLITE_WRITE_PASSWD)

    print ("SQLITE_READ_FILENAME",SQLITE_READ_FILENAME)
    print ("SQLITE_READ_USER",SQLITE_READ_USER)
    print ("SQLITE_READ_PASSWD",SQLITE_READ_PASSWD)

    print ("sqlDBW",sqlDBW)
    print ("sqlDBR",sqlDBR)

    print ("sqliteDB",sqliteDB)
