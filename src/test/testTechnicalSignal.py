#! /usr/bin/env python3
#encoding: utf-8

#Filename: testTechnicalSignal.py
#Author: Steven Lian's team/xie_frank@163.com
#E-mail:  steven.lian@gmail.com  
#Date: 2026-03-28
#Description: test technical signal functions 

_VERSION="20260328"

_DEBUG=True

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import traceback
import getopt
# import requests
import random

import pandas as pd

#global defintion/common var etc.
from common import globalDefinition as comGD  #modify here

#common functions(log,time,string, json etc)
from common import miscCommon as misc

# from common import redisCommon as comDB
# from common import mysqlCommon as comMysql

from common import funcCommon as comFC #常用函数
from common import stockCommon as comStock #股票数据
# from common import akshareCommon as comAK #akshare股票数据
from common import ylwzStockCommon as comYlwz #易联微众股票数据

from common import stockTechnicalIndicators as comTI #股票技术指标
from common import stockTechnicalSignal as comTS #股票指标信号

#setting files
from config import basicSettings as settings


_processorPID = os.getpid()

_DEBUG = True
_HOME_DIR =  settings._HOME_DIR

if "_LOG" not in dir() or not _LOG:
    logDir = os.path.join(_HOME_DIR, "log")
    _LOG = misc.setLogNew("SIGNAL", "testlog", logDir)

systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
_LOG.info(f"PID:{_processorPID}, python version:{systemVersion}, main code version:{_VERSION}")

comStock._LOG = _LOG
comYlwz._LOG = _LOG

#common function begin
def readSessionIDFromEnv():
    try:
        sessionID = os.getenv("YLWZ_SESSION_ID")
        if not sessionID:
            fileName = settings.STOCK_YLWZ_SESSION_ID_FILE
            filePath = os.path.join(settings.STOCK_CONFIG_DIR_NAME, fileName)
            sessionIDSet = misc.loadJsonData(filePath,"dict")
            if sessionIDSet:
                sessionID = sessionIDSet.get("sessionID","")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return sessionID


#创建股票服务器对象
host = host = settings.YLWZ_SERVER_HOST
sessionID = readSessionIDFromEnv()
ylwzStockServer = comYlwz.StockServer(host=host, sessionID=sessionID)

#common function end


#计算某个股票的技术指标信号
def calcOneTechnicalSignal(symbol,period,adjust):
    result = {}
    try:
        _LOG.info(f" - 计算股票: {symbol} 的技术信号(雷达指标),周期:{period},调整:{adjust}")
        currHistoryIndicators = ylwzStockServer.readHistoryTechnicalIndicators(symbol,period,adjust)
        if currHistoryIndicators:
            ts = comTS.StockTS()
            result = ts.calcTechnicalSignals(symbol,currHistoryIndicators)
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#更新技术指标信号
def calcTechnicalSignals():
    result = 0
    try:
        #获取所有用户股票列表
        allUserStockList = ylwzStockServer.getUniqueUserStockList()
        for symbol in allUserStockList:
            if symbol == comGD._DEF_STOCK_PORTFOLIO_CASH_NAME:
                #不更新现金(特殊类型)
                continue
            for period in ["day","week"]: #不计算月数据,只计算日数据和周数据
                for adjust in ["","hfq","qfq"]:
                    techSignal = calcOneTechnicalSignal(symbol,period,adjust)
                    pass
            
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#上传股票相关数据
def regularDataUpdater():
    result = {}
    try:

        _LOG.info(f"I: 计算技术指标信号 ... ")
        rtn = calcTechnicalSignals()
        _LOG.info(f"I: 计算技术指标信号 ...结束 ")
       
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hds:", ["help", "debug","sessionID="])
    except getopt.GetoptError:
        sys.exit()

    debugFlag = False
    sessionID = ""

    for name, value in opts:
        if name in ("-h", "--help"):
            # 打印帮助信息
            print("-d debug")
            sys.exit()

        elif name in ("-d", "--debug"):
            debugFlag = True
        elif name in ("-s", "--sessionID"):
            sessionID = value
        else:
            pass

    if debugFlag:
        import pdb
        pdb.set_trace()

    _LOG.info(f"I: PID:{_processorPID}, debug:{debugFlag}")

    regularDataUpdater()


if __name__ == "__main__":
    main()


