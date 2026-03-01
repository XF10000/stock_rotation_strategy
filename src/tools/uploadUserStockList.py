#! /usr/bin/env python3
#encoding: utf-8

#Filename: uploadUserStockList.py
#Author: Steven Lian's team/xie_frank@163.com
#E-mail:  steven.lian@gmail.com  
#Date: 2026-01-12
#Description: regular fetch portfolio data from server

_VERSION="20260224"

_DEBUG=True

import os
import sys
from turtle import up
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
    _LOG = misc.setLogNew("UPLOAD", comGD._DEF_LOG_STOCK_UPLOAD_DATA_NAME, logDir)

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


#读取股票配置文件
def readStockPortfolioConfig(inputPortfolioFileName=""):
    result = []
    try:
        _LOG.info(f"I: 读取股票配置文件开始... ")

        portfolioList = comStock.readStockPortfolioConfig(inputPortfolioFileName)
        stockConfigList = comStock.convertStockPortfolio2StockJson(portfolioList)
        comStock.saveStockPortfolioJson(stockConfigList)
        stockConfigList = comStock.readStockPortfolioJson()
        
        _LOG.info(f"I: 读取股票配置文件完成, 股票数量:{len(stockConfigList)}")
   
        if stockConfigList:
            result = stockConfigList

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


def uploadStockPortfolio(userID="", stockPortfolio=[]):
    result = {}
    try:
        _LOG.info(f"I: 上传股票配置文件开始... ")
        sessionID = readSessionIDFromEnv()
        if not sessionID:
            errMsg = f"PID: {_processorPID},errMsg:sessionID is empty"
            _LOG.error(f"{errMsg}, {traceback.format_exc()}")
            return result

        host = "www.iottest.online"
        ylwzStockServer = comYlwz.StockServer(host=host, sessionID=sessionID)

        #上传股票配置信息
        for stockConfig in stockPortfolio:
            uploadSuccess = False
            saveSet = {}
            if userID:
                saveSet["userID"] = userID
                saveSet["username"] = userID
            symbol = stockConfig["symbol"]
            saveSet["user_plan"] = "default"
            saveSet["plan_status"] = "Y"
            saveSet["stock_code"] = stockConfig["symbol"]
            saveSet["stock_name"] = stockConfig["stock_name"]
            saveSet["initial_weight"] = stockConfig["Initial_weight"]

            cmd = "userstocklistadd"
            rtnData = ylwzStockServer.query(cmd,saveSet)
            if rtnData:
                data = rtnData.get("data",[])
                recID = data.get("recID",0)
                if int(recID) > 0:
                    uploadSuccess = True

            if uploadSuccess:
                _LOG.info(f"I: 上传股票数据完成... 股票代码:{symbol},userID:{userID}")
            else:
                _LOG.error(f"E: 上传股票数据失败... 股票代码:{symbol},userID:{userID}")
            
            pass
        _LOG.info(f"I: 上传股票配置文件完成, 股票数量:{len(stockPortfolio)}")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result

def dataUploadProcessor(inputPortfolioFileName="",userID=""):
    result = {}
    try:
        #读取股票配置文件
        stockPortfolio = readStockPortfolioConfig(inputPortfolioFileName)
        rtn = uploadStockPortfolio(userID, stockPortfolio)
        
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hdi:u:", ["help", "debug", "input=","userID="])
    except getopt.GetoptError:
        sys.exit()

    debugFlag = False
    inputPortfolioFileName = ""
    userID = ""

    for name, value in opts:
        if name in ("-h", "--help"):
            # 打印帮助信息
            print("-d debug")
            sys.exit()

        elif name in ("-d", "--debug"):
            debugFlag = True

        elif name in ("-i", "--input"):
            inputPortfolioFileName = value

        elif name in ("-u", "--userID"):
            userID = value

    if debugFlag:
        import pdb
        pdb.set_trace()

    _LOG.info(f"I: PID:{_processorPID}, debug:{debugFlag}, portfolio:{inputPortfolioFileName}, userID:{userID}")

    dataUploadProcessor(inputPortfolioFileName,userID)


if __name__ == "__main__":
    main()


