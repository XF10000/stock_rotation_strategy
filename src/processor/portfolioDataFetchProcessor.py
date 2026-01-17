#! /usr/bin/env python3
#encoding: utf-8

#Filename: portfolioDataFetchProcessor.py
#Author: Steven Lian's team/xie_frank@163.com
#E-mail:  steven.lian@gmail.com  
#Date: 2026-01-12
#Description: regular fetch portfolio data from server

_VERSION="20260117"

_DEBUG=True

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import traceback
import getopt
# import requests


#global defintion/common var etc.
from common import globalDefinition as comGD  #modify here

#common functions(log,time,string, json etc)
from common import miscCommon as misc

# from common import redisCommon as comDB
# from common import mysqlCommon as comMysql

from common import funcCommon as comFC
from common import stockCommon as comStock

#setting files
from config import basicSettings as settings


_processorPID = os.getpid()

_DEBUG = True
_HOME_DIR =  settings._HOME_DIR

if "_LOG" not in dir() or not _LOG:
    logDir = os.path.join(_HOME_DIR, "log")
    _LOG = misc.setLogNew("FETCH", comGD._DEF_LOG_STOCK_FETCH_DATA_NAME, logDir)

systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
_LOG.info(f"PID:{_processorPID}, python version:{systemVersion}, main code version:{_VERSION}")


#common function begin

#common function end


def dataFetchProcessor():
    result = {}
    try:
        #首先读取股票配置文件
        _LOG.info(f"I: 读取股票配置文件开始... ")

        portfolioList = comStock.readStockPortfolioConfig()
        stockConfigList = comStock.convertStockPortfolio2StockJson(portfolioList)
        comStock.saveStockPortfolioJson(stockConfigList)
        stockConfigList = comStock.readStockPortfolioJson()
        
        _LOG.info(f"I: 读取股票配置文件完成, 股票数量:{len(stockConfigList)}")
        
        if stockConfigList:
            #计算股票信息开始和结束日期
            currYMDHMS = misc.getTime()
            currYMD = currYMDHMS[0:8]
            startYMD = misc.getPassday(comGD._DEF_STOCK_KEEP_HISTORY_DATA_DAYS)

            _LOG.info(f"I: 开始检查股票信息是否存在... ")
        
            for stockConfig in stockConfigList:
                symbol = stockConfig["symbol"]
                comStock.checkReadStockFullData(symbol, startYMD, currYMD)

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:", ["help", "debug"])
    except getopt.GetoptError:
        sys.exit()

    debugFlag = False

    for name, value in opts:
        if name in ("-h", "--help"):
            # 打印帮助信息
            print("-d debug")
            sys.exit()

        elif name in ("-d", "--debug"):
            debugFlag = True

    if debugFlag:
        import pdb
        pdb.set_trace()

    _LOG.info(f"I: PID:{_processorPID}, debug:{debugFlag}")

    dataFetchProcessor()


if __name__ == "__main__":
    main()


