#! /usr/bin/env python3
#encoding: utf-8

#Filename: dataBackup.py 
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com  
#Date: 2019-10-21
#Description: 服务器数据备份部分, 主要是备份用户数据


_VERSION="20260314"

_DEBUG=True

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import traceback

#from collections import Counter

# import requests
# import copy
import pathlib
import shutil

#global defintion/common var etc.
from common import globalDefinition as comGD

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#common functions(database operation)
from common import redisCommon as comDB

from common import mysqlCommon as comMysql


#common functions(funct operation)
from common import funcCommon as comFC

#from common import codingDecoding as comCD
# from common import aliyunOSS as OSS

#setting files
from config import basicSettings as settings

_processorPID = os.getpid()

HOME_DIR = settings._HOME_DIR

_DATA_DIR = settings._DATA_DIR


if "_LOG" not in dir() or not _LOG:
    logDir = os.path.join(HOME_DIR, "log")
    _LOG = misc.setLogNew("BACKUP", "databackuplog", logDir)
systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
_LOG.info(f"PID:{_processorPID}, python version:{systemVersion}, main code version:{_VERSION}")  


#mysql 默认数据存储目录
defaultMysqlDataDir = {
    "local":r"/var/lib/mysql",
    "suzhou_1":r"/var/lib/mysql",
    "suzhou_2":r"/var/lib/mysql",
    "cq_zjk_2":r"/var/lib/mysql",
    "cq_zjk_3":r"/var/lib/mysql",
    "home":r"/var/lib/mysql",
}[settings._SYS]


backupDataDir = {
    "local":r"/data/stockapp/data/backup",
    "suzhou_1":r"/data/stockapp/data/backup",
    "suzhou_2":r"/data/stockapp/data/backup",
    "cq_zjk_2":r"/data/stockapp/data/backup",
    "cq_zjk_3":r"/data/stockapp/data/backup",
    "home":r"/data/stockapp/data/backup",
} [settings._SYS]


#默认备份表名
backupTableNameList = [
    "USER_BASIC",
    "balance_sheets",
    "cash_flow_statements",
    "data_check_log",
    "hwinfo_report_record",
    "income_statements",
    "indicator_medians",
    "industry_history_data_day",
    "industry_history_data_day_hfq",
    "industry_history_data_day_qfq",
    "industry_history_data_month",
    "industry_history_data_month_hfq",
    "industry_history_data_month_qfq",
    "industry_history_data_week",
    "industry_history_data_week_hfq",
    "industry_history_data_week_qfq",
    "industry_info",
    "stock_dividend_data",
    "stock_history_data_day",
    "stock_history_data_day_hfq",
    "stock_history_data_day_qfq",
    "stock_history_data_month",
    "stock_history_data_month_hfq",
    "stock_history_data_month_qfq",
    "stock_history_data_week",
    "stock_history_data_week_hfq",
    "stock_history_data_week_qfq",
    "stock_info",
    "trade_day_record",
    "technical_indicators_day",
    "technical_indicators_day_hfq",
    "technical_indicators_day_qfq",
    "technical_indicators_month",
    "technical_indicators_month_hfq",
    "technical_indicators_month_qfq",
    "technical_indicators_week",
    "technical_indicators_week_hfq",
    "technical_indicators_week_qfq",
    "trade_day_record",
    "user_stock_list",
]

# command part

def checkMySqlDataBase():
    #user basic
    _LOG.info(f"checkMySqlDataBase begin ")

    comMysql.checkMySqlDataBase()
    
    _LOG.info(f"checkMySqlDataBase end ")


def dataFormatConvert(dataList):
    result = dataList
    for data in dataList:
        for k, v in data.items():
            if isinstance(v, int):
                data[k] = str(v)
            if isinstance(v, float):
                data[k] = str(v)
            if v == None:
                data[k] = ""
    result = dataList
    return result

'''
保存mysql数据
'''


#获取当前数据库,默认的csv数据备份路径
def  getDefaultMysqlBackupFileDir():
    global  defaultMysqlDataDir 
    try:
        sqlStr =  f"SHOW VARIABLES LIKE 'secure_file_priv';"
        valuesList = []
        rtn = comMysql.mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn:
            dataList = comMysql.mysqlDB.fetchAll()
            if dataList:
                currDataSet = dataList[0]
                defaultMysqlDataDir = currDataSet["Value"] 
                if _DEBUG:
                    _LOG.info(f"DEBUG: defaultMysqlDataDir:{defaultMysqlDataDir},rtn:{rtn}")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")    


'''
这个方法即使备份出来, 也由于权限问题,很难移动文件
'''
def mysqlCsvBackup(tableName, tempFilePath,backupFilePath):
    result = 0

    try:
        sqlStr =  f"SELECT * INTO OUTFIIE '{tempFilePath}' FIELDS TERMINATED BY ',' "
        sqlStr += f"ENCLOSED BY '\"' LINES TERMINATED BY '\n' FROM {tableName}"
        rtn = comMysql.mysqlDB.executeWrite(sqlStr) 
        if _DEBUG:
            _LOG.info(f"DEBUG: tableName:{tableName},rtn:{rtn}")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")    

    _LOG.info(f"backupOneTable: {tableName} end,result:{result} ")

    return result


'''
用读记录的方式备份, 虽然慢, 但是可以读出来
'''
def selfFetchCsvBackup(tableName,backupFilePath):
    result = 0

    batchNum = 1000
    splitChar = ","
    lineChar = "\n"
    textChar = "'"

    try:
        sqlStr =  f"DESC {tableName}"
        rtn = comMysql.mysqlDB.executeRead(sqlStr) 

        sqlStr =  f"SELECT * FROM {tableName}"
        rtn = comMysql.mysqlDB.executeRead(sqlStr) 

        if _DEBUG:
            _LOG.info(f"DEBUG: tableName:{tableName},rtn:{rtn}")

        if rtn:
            titleLineFlag = True
            with open(backupFilePath, 'w') as hFile:
                while True:
                    dataList = comMysql.mysqlDB.fetchMany(batchNum)
                    if titleLineFlag:
                        titleLineFlag = False
                        titleKeys = list(dataList[0].keys())
                        aList = []
                        for key in titleKeys:
                            aList.append(textChar + str(key) + textChar)
                            # aList.append(str(key))
                        strT = splitChar.join(aList)
                        hFile.write(strT)
                        hFile.write(lineChar)
                    for data in dataList:
                        aList = []
                        for key in titleKeys:
                            val = data[key]
                            if val == None:
                                val = ""
                            aList.append(textChar + str(val) + textChar)
                        strT = splitChar.join(aList)
                        hFile.write(strT)
                        hFile.write(lineChar)

                    #final
                    #如果取不到更多数据就退出
                    currDataLen = len(dataList)
                    result += currDataLen
                    if currDataLen < batchNum:
                        break
                misc.time.sleep(1)

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")    

    _LOG.info(f"backupOneTable: {tableName} end,result:{result} ")

    return result


#备份一个表
def backupOneTable(tableName):
    result = 0

    _LOG.info(f"backupOneTable: {tableName} begin ")

    try:
        #首先确认本地备份文件名称
        backupFileName = tableName + ".csv"
        preFileName = tableName + ".dat"
        tempFilePath = os.path.join(defaultMysqlDataDir, backupFileName)
        backupFilePath = os.path.join(backupDataDir, backupFileName)
        preFilePath = os.path.join(backupDataDir, preFileName)

        #首先判断文件是否存在, 如果存在就先备份
        if os.path.isfile(backupFilePath):
            shutil.move(backupFilePath, preFilePath)

        #使用mysql 自己的备份文件方法
        result = selfFetchCsvBackup(tableName,backupFilePath)

        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")    

    _LOG.info(f"backupOneTable: {tableName} end,result:{result} ")

    return result


def backupMysqlData():
    result = ""

    _LOG.info(f"backupMysqlData begin ")

    comFC.createDir(backupDataDir)

    try:
        for tableName in backupTableNameList:
            backupOneTable(tableName)
            misc.time.sleep(1)
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")    

    _LOG.info(f"backupMysqlData end ")

    return result


def main():

    checkMySqlDataBase()

    getDefaultMysqlBackupFileDir()

    backupMysqlData()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()

    main()    



