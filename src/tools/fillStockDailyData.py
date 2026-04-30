#! /usr/bin/env python3
#encoding: utf-8

#Filename: fillStockDailyData.py
#Author: Steven Lian's team/xie_frank@163.com
#E-mail:  steven.lian@gmail.com  
#Date: 2026-01-12
#Description: 填充股票日k线数据

_VERSION="20260402"

_DEBUG=True

import os
import sys
from turtle import up
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import traceback
import getopt
# import requests

import pandas as pd


#global defintion/common var etc.
from common import globalDefinition as comGD  #modify here

#common functions(log,time,string, json etc)
from common import miscCommon as misc

# from common import redisCommon as comDB
# from common import mysqlCommon as comMysql

# from common import funcCommon as comFC
from common import stockCommon as comStock
from common import ylwzStockCommon as comYlwz

#setting files
from config import basicSettings as settings


_processorPID = os.getpid()

_DEBUG = True
_HOME_DIR =  settings._HOME_DIR

if "_LOG" not in dir() or not _LOG:
    logDir = os.path.join(_HOME_DIR, "log")
    _LOG = misc.setLogNew("FILL", comGD._DEF_LOG_STOCK_UPLOAD_DATA_NAME, logDir)

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

sessionID = readSessionIDFromEnv()
host = "www.iottest.online"
ylwzStockServer = comYlwz.StockServer(host=host, sessionID=sessionID)

#common function end


#更新单只股票数据
def updateSingleStockCloseData(symbol,stockData,rightDate):
    result = 0
    try:
        _LOG.info(f"I: 更新单只股票当日数据开始... 股票代码:{symbol}, 日期:{rightDate}")
        # rightDate  = getRightStockDate()
        # #如果时间不是收盘时间,则不更新
        # if rightDate:
        if True:
            #格式转换
            saveSet = {}

            for k,v in stockData.items():    
                if pd.isna(v):
                    v = 0.0
                    stockData[k] = v
                if k == "change":
                    k = "price_change"
                if k == "last_price":
                    k = "close"
                if k in ["buy_price","sell_price","prev_close","datetime"]:
                    continue
                saveSet[k] = v

            #必须放到后面更新
            saveSet["symbol"] = symbol
            saveSet["period"] = "day"
            saveSet["adjust"] = ""
            if "date" not in saveSet: # 如果没有date, 则默认是当前日期, sino 数据
                saveSet["date"] = rightDate

            uploadSuccess = False
            cmd = "stockhistoryadd"
            rtnData = ylwzStockServer.query(cmd,saveSet)
            if rtnData:
                data = rtnData.get("data",[])
                recID = data.get("recID",0)
                if int(recID) > 0:
                    uploadSuccess = True

            if uploadSuccess:
                result += 1
                _LOG.info(f"I: 更新单只股票当日数据完成... 股票代码:{symbol}")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#获取当前日期的股票数据(本地服务器)
def getCurrentStockData(YMD,period="day",adjust=""):
    result = {}
    try:
        date = misc.YMD2HumanDate(YMD)
        stockDataList = ylwzStockServer.queryStockData(date=date, period=period, adjust=adjust)
        for stockData in stockDataList:
            symbol = stockData.get("stock_code","")
            date = stockData.get("date","")
            colName = f"{symbol}_{date}"
            result[colName] = stockData
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


def fillStockDailyData(YMD=""):
    result = 0
    try:
        _LOG.info(f"I: 上传股票配置文件开始... ")
        #获取当前日期服务器股票信息
        currStockDataList = getCurrentStockData(YMD)
        stockDataList = comStock.comTS.getStockDailyData(YMD)
        for stockData in stockDataList:
            symbol = stockData["symbol"]
            date = stockData["date"]
            colName = f"{symbol}_{date}"
            if colName in currStockDataList:
                _LOG.info(f"I: 股票数据已存在, 跳过上传... 股票代码:{symbol}, 日期:{date}")
                continue
            rtn = updateSingleStockCloseData(symbol,stockData,YMD)
            result += 1
            if rtn:
                _LOG.info(f"I: 上传股票数据完成... 股票代码:{symbol}")
            else:
                _LOG.warning(f"E: 上传股票数据失败... 股票代码:{symbol}")
            
            pass
        _LOG.info(f"I: 上传股票配置文件完成, 股票数量:{len(stockDataList)}")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


def dataFillProcessor(YMDList=[]):
    result = {}
    try:
        #读取股票配置文件
        for YMD in YMDList:
            fillStockDailyData(YMD)
        
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#获取交易日期列表, 默认获取最近几个月的交易日期
def getTradeYMDList():
    result = []
    try:
        startYMD = "20251201"
        currYMD = misc.getTime()[0:8]
        currYMD = "20260101"
        tradeDateList = ylwzStockServer.readStockTradeDateList(startYMD, currYMD)
        for data in tradeDateList:
            trade_day = data.get("trade_day","")
            tradeYMD = trade_day.replace("-","")
            result.append(tradeYMD)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hdi:", ["help", "debug", "input="])
    except getopt.GetoptError:
        sys.exit()

    debugFlag = False
    # inputYMDList = ["20260324","20260325","20260326","20260327","20260330","20260331","20260401"]
    # inputYMDList = ["20260202","20260205","20260225","20260226","20260227","20260316","20260317","20260318","20260319","20260320","20260323"]
    inputYMDList = ["20260105","20260106","20260107","20260108","20260109",
                    "20260112","20260113","20260114","20260115","20260116",
                    "20260119","20260120","20260121","20260122","20260123",
                    "20260126","20260127","20260128","20260129","20260130"]
    
    inputYMDList = getTradeYMDList()
   
    inputYMDString = misc.jsonDumps(inputYMDList)

    for name, value in opts:
        if name in ("-h", "--help"):
            # 打印帮助信息
            print("-d debug")
            sys.exit()

        elif name in ("-d", "--debug"):
            debugFlag = True

        elif name in ("-i", "--input"):
            inputYMDString = value

        elif name in ("-u", "--userID"):
            userID = value

    if debugFlag:
        import pdb
        pdb.set_trace()
    try:
        YMDList = misc.jsonLoads(inputYMDString)
    except Exception as e:
        pass

    _LOG.info(f"I: PID:{_processorPID}, debug:{debugFlag}, YMDList:{YMDList}")

    dataFillProcessor(YMDList)


if __name__ == "__main__":
    main()


