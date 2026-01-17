#!/usr/bin/env python3
#encoding: utf-8

#Filename: stockCommon.py  
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com/xie_frank@163.com  
#Date: 2026-01-16
#Description:   这个应用的股票相关的通用函数
#所有股票内容, symbol = 纯数字代码, 其他英文内容均采用小写,并用"_"连接

# 相关函数包括
# 1. 股票配置文件的读取
# 2. 股票基本信息的读取(包括,行业数据, 股票基本信息等)

_VERSION="20260117"


import os
import sys

from common.redisCommon import statSaveDataGeneral
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

import pathlib
import pandas as pd

from common import miscCommon as misc
# from common import funcCommon as comFC
from common import akshareCommon as comAK

from config import basicSettings as settings

#common begin
_processorPID = os.getpid()
#common end


def readStockPortfolioConfig():
    result = []
    try:
        fileName = settings.STOCK_PORTFOLIO_CONFIG_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"
        df = pd.read_csv(filePath)
        result = df.to_dict(orient='records')

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result 


def convertStockPortfolio2StockJson(portfolioList):
    result = []
    stockBasicInfoDict = readStockBasicInfo()
    for item in portfolioList:
        symbol = item.get("Stock_number","")
        if symbol in stockBasicInfoDict:
            stockInfo = stockBasicInfoDict[symbol]

        item["symbol"] = symbol
        item["stock_name"] = stockInfo.get("stock_name","")
        item["industry_name"] = stockInfo.get("industry_name","")
        item["industry_code"] = stockInfo.get("industry_code","")
        item["industry_name_sw"] = stockInfo.get("industry_name_sw","")
        item["industry_code_sw"] = stockInfo.get("industry_code_sw","")
        item["industry_name_em"] = stockInfo.get("industry_name_em","")
        item["industry_code_em"] = stockInfo.get("industry_code_em","")

        result.append(item)

    return result


#读取股票组合配置文件(json 格式)
def readStockPortfolioJson():
    result = []
    try:
        fileName = settings.STOCK_PORTFOLIO_CONFIG_JSON_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"
        result = misc.loadJsonData(filePath,"list")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result 


#保存股票组合配置文件(json 格式)
def saveStockPortfolioJson(data):
    result = False
    try:
        fileName = settings.STOCK_PORTFOLIO_CONFIG_JSON_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"
        rtn = misc.saveJsonData(filePath,data,indent=2,ensure_ascii=False)
        result = True

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result 


def readSWStockIndustryMapping():
    result = {}
    try:
        fileName = settings.STOCK_SW_STOCK_INDUSTRY_MAP_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"
        # 读取SW股票行业映射文件
        data = misc.loadJsonData(filePath,"dict")
        result = data.get("mapping",{})

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result

def getHistoryStockData(symbol, startYMD, endYMD):
    result = []
    try:
        if startYMD < endYMD:
            #获取股票历史数据(东方财富)
            result = comAK.gmGetHistroryData(symbol, startYMD, endYMD)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#获取所有股票的基本信息(包括,行业数据, 股票基本信息等,后续还需要增加其他数据)
#从网络获取股票基本信息
def getStockBasicInfo():
    result = {}
    try:
        #首先读取SW股票行业映射文件(申银万国)
        swIndustryMapping = readSWStockIndustryMapping()
        #其次获取所有股票的基本信息(东方财富)
        stockInfoDict = comAK.emGetStockInfoData()
        for symbol, stockInfo in stockInfoDict.items():
            #根据申银万国行业映射, 填充行业信息
            if symbol in swIndustryMapping:
                stockInfo["industry_name_sw"] = swIndustryMapping[symbol]["industry_name"]
                stockInfo["industry_code_sw"] = swIndustryMapping[symbol]["industry_code"]
            else:
                pass
                #pass
            result[symbol] = stockInfo
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#保存股票基本信息
def saveStockBasicInfo(data):
    result = False
    try:
        fileName = settings.STOCK_BASIC_INFO_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"
        rtn = misc.saveJsonData(filePath,data,indent=2,ensure_ascii=False)
        result = True
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#读取股票基本信息
def readStockBasicInfo():
    result = {}
    try:
        fileName = settings.STOCK_BASIC_INFO_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"
        # 读取股票基本信息文件
        result = misc.loadJsonData(filePath,"dict")
        
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#检查并读取股票完整数据, 包括, daily, weekly, monthly 数据, 包括,当前,前复权, 后复权等
# periodList = ["daily","weekly","monthly"]
periodList = ["daily","weekly"]
adjustList = ["","qfq","hfq"]
def checkReadStockFullData(symbol, startYMD, endYMD):
    result = False
    try:
        stockInfo = {}
        stockInfo["code"] = symbol
        stockInfo["symbol"] = symbol
        stockInfo["start_date"] = startYMD
        stockInfo["end_date"] = endYMD
        stockInfo["columns"] = ["open","high","low","close","volume","amount"]

        for period in periodList:
            for adjust in adjustList:
                if adjust:
                    stockJsonFileName = f"{period}/{symbol}_{adjust}.json"
                    stockDataFileName = f"{period}/{symbol}_{adjust}.csv"
                else:
                    stockJsonFileName = f"{period}/{symbol}.json"
                    stockDataFileName = f"{period}/{symbol}.csv"

                stockJsonFilePath = f"{settings.STOCK_DATA_SAVE_DIR_NAME}/{stockJsonFileName}"
                stockDataFilePath = f"{settings.STOCK_DATA_SAVE_DIR_NAME}/{stockDataFileName}"

                # 检查文件是否存在
                if os.path.exists(stockJsonFilePath) and os.path.exists(stockDataFilePath):
                    # 读取文件内容
                    savedStockInfo, savedStockDataList = readStockData(symbol, period, adjust)
                    savedStartDate = savedStockInfo.get("start_date","")
                    savedEndDate = savedStockInfo.get("end_date","")
                    newStartDate = savedEndDate
                    newStockDataList = getHistoryStockData(symbol, newStartDate, endYMD)
                    stockDataList = savedStockDataList + newStockDataList
                    stockDataList = filterStockData(stockDataList,startYMD,endYMD)
                else:
                    #读取网络数据
                    stockDataList = getHistoryStockData(symbol, startYMD, endYMD)
                    pass
                
                #保存数据
                stockInfo["YMDHMS"] = misc.getTime()
                stockInfo["save_time"] = misc.humanTime(stockInfo["YMDHMS"])
                saveStockData(symbol, period, adjust, stockDataList, stockInfo)

        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


# 保存股票数据, 包括, daily, weekly, monthly 数据, 包括,当前,前复权, 后复权等
def saveStockData(symbol, period, adjust, stockDataList,stockInfo):
    result = False
    try:
        if adjust:
            stockJsonFileName = f"{period}/{symbol}_{adjust}.json"
            stockDataFileName = f"{period}/{symbol}_{adjust}.csv"
        else:
            stockJsonFileName = f"{period}/{symbol}.json"
            stockDataFileName = f"{period}/{symbol}.csv"
        stockJsonFilePath = f"{settings.STOCK_DATA_SAVE_DIR_NAME}/{stockJsonFileName}"
        stockDataFilePath = f"{settings.STOCK_DATA_SAVE_DIR_NAME}/{stockDataFileName}"
        # 保存股票数据文件
        stockData = pd.DataFrame(stockDataList,columns=["date","open","high","low","close","volume","amount",\
            "symbol","range","pct_change","change","turnover"])
        stockData.to_csv(stockDataFilePath,index=False)

        # 保存股票基本信息文件
        rtn = misc.saveJsonData(stockJsonFilePath,stockInfo,indent=2,ensure_ascii=False)
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


# 读取股票数据, 包括, daily, weekly, monthly 数据, 包括,当前,前复权, 后复权等
def readStockData(symbol, period="", adjust=""):
    stockInfo = {}
    stockData = []
    try:
        if adjust:
            stockJsonFileName = f"{period}/{symbol}_{adjust}.json"
            stockDataFileName = f"{period}/{symbol}_{adjust}.csv"
        else:
            stockJsonFileName = f"{period}/{symbol}.json"
            stockDataFileName = f"{period}/{symbol}.csv"
        stockJsonFilePath = f"{settings.STOCK_DATA_SAVE_DIR_NAME}/{stockJsonFileName}"
        stockDataFilePath = f"{settings.STOCK_DATA_SAVE_DIR_NAME}/{stockDataFileName}"

        stockData = pd.read_csv(stockDataFilePath)

        stockData = stockData.to_dict(orient='records')

        stockInfo = misc.loadJsonData(stockJsonFilePath,"dict")
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return stockInfo,stockData


# 过滤股票数据, 只保留在startYMD和endYMD之间的数据
def filterStockData(stockDataList,startYMD,endYMD):
    result = []
    try:
        dateData = {}
        for stockData in stockDataList:
            date = stockData.get("date","")
            if date not in dateData:
                dateData[date] = date
                if date >= startYMD and date <= endYMD:
                    result.append(stockData)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def test():
    # 示例
    # data = getStockBasicInfo()
    # rtn = saveStockBasicInfo(data)
    # data = readStockBasicInfo()

    portfolioList = readStockPortfolioConfig()
    stockConfigList = convertStockPortfolio2StockJson(portfolioList)
    saveStockPortfolioJson(stockConfigList)
    stockConfigList = readStockPortfolioJson()
    pass


if __name__ == "__main__":
    if len(sys.argv) > 1:
        pass
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
    
    test()
