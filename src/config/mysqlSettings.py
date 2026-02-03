#! /usr/bin/env python3
#encoding: utf-8

#Filename: mysqlSettings.py  
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

from common import  mysqlHandle as mysqlHandle

#生产环境 rss | 测试环境 dss
_SYS = local_settings._SYS
#_SYS = "server_01"


#mysql数据库信息  begin
#主库，写记录
MYSQL_WRITE_HOST = {
    "local":"127.0.0.1",
    "server_01":"127.0.0.1", 
    "server_02":"127.0.0.1", 
    "home":"192.168.100.100",
    }[_SYS]

MYSQL_WRITE_PORT = {
    "local":3306,
    "server_01":3306, 
    "server_02":3306, 
    "home":3306, 
    }[_SYS]

MYSQL_WRITE_DB = {
    "local":"stock_db",
    "server_01":"stock_db", 
    "server_02":"stock_db", 
    "home":"stock_db", 
    }[_SYS]
    
MYSQL_WRITE_USER = {
    "local":"stockdba",
    "server_01":"stockdba", 
    "server_02":"stockdba", 
    "home":"stockdba", 
    }[_SYS]

MYSQL_WRITE_PASSWD = {
    "local":"stock$123456",
    "server_01":"stock$123456", 
    "server_02":"stock$123456", 
    "home":"stock$123456", 
    }[_SYS]

#从库，读记录
MYSQL_READ_HOST = {
    "local":"127.0.0.1",
    "server_01":"127.0.0.1", 
    "server_02":"127.0.0.1", 
    "home":"192.168.100.100",
    }[_SYS]
    
MYSQL_READ_PORT = {
    "local":3306,
    "server_01":3306, 
    "server_02":3306, 
    "home":3306, 
    }[_SYS]

    
MYSQL_READ_DB = {
    "local":"stock_db",
    "server_01":"stock_db", 
    "server_02":"stock_db", 
    "home":"stock_db", 
    }[_SYS]

MYSQL_READ_USER = {
    "local":"stockdba",
    "server_01":"stockdba", 
    "server_02":"stockdba", 
    "home":"stockdba", 
    }[_SYS]

MYSQL_READ_PASSWD = {
    "local":"stock$123456",
    "server_01":"stock$123456", 
    "server_02":"stock$123456", 
    "home":"stock$123456", 
    }[_SYS]

#主库，写记录
mySqlW = mysqlHandle.getMysqlDB(MYSQL_WRITE_HOST ,MYSQL_WRITE_USER,MYSQL_WRITE_PASSWD,MYSQL_WRITE_DB)

#从库，读记录
mySqlR = mysqlHandle.getMysqlDB(MYSQL_READ_HOST ,MYSQL_READ_USER,MYSQL_READ_PASSWD,MYSQL_READ_DB)

mysqlDB = mysqlHandle.mysqlHandle(dbW=mySqlW,dbR=mySqlR)


def mysqlReconnect():
    global mySqlW, mySqlR, mysqlDB
    #主库，写记录
    mySqlW = mysqlHandle.getMysqlDB(MYSQL_WRITE_HOST ,MYSQL_WRITE_USER,MYSQL_WRITE_PASSWD,MYSQL_WRITE_DB)

    #从库，读记录
    mySqlR = mysqlHandle.getMysqlDB(MYSQL_READ_HOST ,MYSQL_READ_USER,MYSQL_READ_PASSWD,MYSQL_READ_DB)
#    mySqlR = mySqlW

    mysqlDB = mysqlHandle.mysqlHandle(dbW=mySqlW,dbR=mySqlR)
    

_DEBUG = True  #预设trace开关，禁止修改

if __name__ == "__main__":
    pass
    # import pdb
    # pdb.set_trace()
    print ("_SYS",_SYS)
    print ("MYSQL_WRITE_HOST",MYSQL_WRITE_HOST)
    print ("MYSQL_WRITE_USER",MYSQL_WRITE_USER)
    print ("MYSQL_WRITE_PASSWD",MYSQL_WRITE_PASSWD)
    print ("MYSQL_WRITE_DB",MYSQL_WRITE_DB)

    print ("MYSQL_READ_HOST",MYSQL_READ_HOST)
    print ("MYSQL_READ_USER",MYSQL_READ_USER)
    print ("MYSQL_READ_PASSWD",MYSQL_READ_PASSWD)
    print ("MYSQL_READ_DB",MYSQL_READ_DB)

    print ("mySqlW",mySqlW)
    print ("mySqlR",mySqlR)

    print ("mysqlDB",mysqlDB)
