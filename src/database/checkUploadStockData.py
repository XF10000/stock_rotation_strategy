#! /usr/bin/env python3
#encoding: utf-8

#Filename: checkUploadStockData.py 
#Author: Steven Lian
#E-mail:  steven.lian@gmail.com  
#Date: 2026-1-31
#Description:  上传股票相关数据

_VERSION="20260224"

_DEBUG=True

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import traceback

#common functions(log,time,string, json etc)
from common import miscCommon as misc

from common import stockCommon as comStock
from common import ylwzStockCommon as comYlwz

from config import basicSettings as settings


_processorPID = os.getpid()

_DEBUG = True
_HOME_DIR =  settings._HOME_DIR

if "_LOG" not in dir() or not _LOG:
    logDir = os.path.join(_HOME_DIR, "log")
    _LOG = misc.setLogNew("SQLLITE","checkuploadstockdatalog", logDir,stdout=True)

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

#common function end

def uploadTradeDayInfo():
    try:
        _LOG.info(f"B: PID: {_processorPID}, upload trade day info begin")

        sessionID = readSessionIDFromEnv()
        if not sessionID:
            errMsg = f"PID: {_processorPID},errMsg:sessionID is empty"
            _LOG.error(f"{errMsg}, {traceback.format_exc()}")
            return result

        host = host = settings.YLWZ_SERVER_HOST
        ylwzStockServer = comYlwz.StockServer(host=host, sessionID=sessionID)

        tradeDayList = comStock.comAK.sinaGetTradeDate()
        for tradeDay in tradeDayList:
            uploadSuccess = False
            saveSet = {}
            saveSet["trade_day"] = tradeDay

            cmd = "tradedayadd"
            rtnData = ylwzStockServer.query(cmd,saveSet)
            if rtnData:
                data = rtnData.get("data",[])
                recID = data.get("recID",0)
                if int(recID) > 0:
                    uploadSuccess = True

            if uploadSuccess:
                _LOG.info(f"I: 上传股票交易日数据完成... 交易日:{tradeDay}")
            else:
                _LOG.error(f"E: 上传股票交易日数据失败... 交易日:{tradeDay}")
            
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")


def main():
    try:
        _LOG.info(f"B: PID: {_processorPID}, upload trade day info begin")
        uploadTradeDayInfo()
        _LOG.info(f"E: PID: {_processorPID}, upload trade day info end")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")


if __name__ == "__main__":
    main()
    
