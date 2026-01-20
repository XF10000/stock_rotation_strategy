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

_VERSION="20260120"


import os
import sys

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

import pathlib

import pandas as pd
import pandas_ta as ta
import numpy as np

from common import globalDefinition as comGD
from common import miscCommon as misc
from common import akshareCommon as comAK

from config import basicSettings as settings

#common begin
_processorPID = os.getpid()

#建立目录
def createDir(dirName):
    if not os.path.exists(dirName):
        os.makedirs(dirName)

#common end


#读取股票组合配置文件(csv 格式)
def readStockPortfolioConfig(fileName=""):
    result = []
    try:
        if fileName == "":
            fileName = settings.STOCK_PORTFOLIO_CONFIG_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"
        df = pd.read_csv(filePath)
        result = df.to_dict(orient='records')

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result 


#将股票组合配置文件转换为股票json 格式
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

        # 检查目录是否存在, 如果不存在, 则创建
        dirName = os.path.split(filePath)[0]
        createDir(dirName)

        rtn = misc.saveJsonData(filePath,data,indent=2,ensure_ascii=False)
        result = True

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result 


#读取股票组合配置文件(csv 格式)
def readBacktestSetting(fileName=""):
    result = []
    try:
        if fileName == "":
            fileName = settings.STOCK_BACKTEST_SETTINGS_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"

        df = pd.read_csv(filePath)
        result = df.to_dict(orient='records')

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result 


#将股票组合配置文件转换为股票json 格式
def convertBacktestSetting2Json(backtestSettings):
    result = {}
    try:
        for item in backtestSettings:
            parameter = item.get("Parameter","")
            parameter = parameter.strip()
            if parameter.startswith("#") or parameter == "":
                # 跳过注释行和空行
                continue
            
            #真正的参数
            value = item.get("Value","")
            description = item.get("Description","")

            newItem = {}
            # 开始日期和结束日期, 格式为yyyy-mm-dd 跳过已有配置
            if parameter in ["start_date","end_date"]:
                startYMD = misc.getPassday(comGD._DEF_STOCK_BACKTEST_DAYS)
                endYMD = misc.getTime()[0:8]
                if parameter == "start_date":
                    # value = startYMD[0:4] + "-" + startYMD[4:6] + "-" + startYMD[6:8]
                    additionalItem = {}
                    additionalItem["parameter"] = "startYMD"
                    additionalItem["value"] = startYMD
                    additionalItem["description"] = "回测数据开始日期"
                    result["startYMD"] = additionalItem
                else:
                    # value = endYMD[0:4] + "-" + endYMD[4:6] + "-" + endYMD[6:8]                   
                    additionalItem = {}
                    additionalItem["parameter"] = "endYMD"
                    additionalItem["value"] = endYMD
                    additionalItem["description"] = "回测数据结束日期"
                    result["endYMD"] = additionalItem

            elif parameter in ["data_source","backup_data_source","tushare_token","data_fetch_strategy"]:
                # 数据来源, 备份数据来源, tushare token 等文本数据
                value = value.strip()
            elif parameter in ["historical_data_weeks","min_data_length"]:
                # 历史数据周数和最小数据长度, 必须为整数
                value = int(value)
            else:
                # 其他参数, 转换为浮点数
                value = float(value)
                
            newItem["parameter"] = parameter
            newItem["value"] = value
            newItem["description"] = description

            result[parameter] = newItem

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#读取股票组合配置文件(json 格式)
def readBacktestSettingJson():
    result = {}
    try:
        fileName = settings.STOCK_BACKTEST_SETTINGS_JSON_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"
        result = misc.loadJsonData(filePath,"dict")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result 


#保存回测配置文件(json 格式)
def saveBacktestSettingJson(data):
    result = False
    try:
        fileName = settings.STOCK_BACKTEST_SETTINGS_JSON_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"

        # 检查目录是否存在, 如果不存在, 则创建
        dirName = os.path.split(filePath)[0]
        createDir(dirName)

        rtn = misc.saveJsonData(filePath,data,indent=2,ensure_ascii=False)
        result = True

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result 


#读取股票行业映射文件(申银万国和东方财富)
def readStockIndustryMapping():
    result = {}
    try:
        fileName = settings.STOCK_SW_STOCK_INDUSTRY_MAP_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"
        # 读取SW股票行业映射文件
        data = misc.loadJsonData(filePath,"dict")
        result = data

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#保存股票行业映射文件(申银万国和东方财富)
def saveStockIndustryMapping(data):
    result = False
    try:
        fileName = settings.STOCK_SW_STOCK_INDUSTRY_MAP_FILE
        filePath = f"{settings._DATA_CONFIG_DIR}/{fileName}"
        # 保存股票行业映射文件
        rtn = misc.saveJsonData(filePath,data,indent=2,ensure_ascii=False)
        result = True

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def getHistoryStockData(symbol,startYMD,endYMD):
    result = []
    try:
        #排除周六和周日
        endYMDHMS = endYMD + "000000"
        weekDay = misc.weekDay(endYMDHMS)
        if weekDay.wday == 5:
            endYMD = misc.getPassday(1,endYMD)
        elif weekDay.wday == 6:
            endYMD = misc.getPassday(2,endYMD)

        if startYMD < endYMD:
            #获取股票历史数据(东方财富)
            result = comAK.swGetIndexHistory(symbol, period, endYMD)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#获取所有股票的基本信息(包括,行业数据, 股票基本信息等,后续还需要增加其他数据)
#从网络获取股票基本信息
def getStockBasicInfo():
    result = {}
    try:
        #首先获取申银万国股票的基本信息(申银万国)
        swStockInfoDict = comAK.swGetStockInfoData()
        #其次获取所有股票的基本信息(东方财富)
        emStockInfoDict = comAK.emGetStockInfoData()
        for symbol, stockInfo in swStockInfoDict.items():
            #根据东方财富数据, 填充东方财富行业信息
            if symbol in emStockInfoDict:
                stockInfo["industry_name_em"] = emStockInfoDict[symbol]["industry_name_em"]
                stockInfo["industry_code_em"] = emStockInfoDict[symbol]["industry_code_em"]
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
        stockInfo["columns"] = ["date","open","high","low","close","volume","amount",\
            "symbol","range","pct_change","change","turnover"]

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

                # 检查目录是否存在, 如果不存在, 则创建
                dirName = os.path.split(indexJsonFilePath)[0]
                createDir(dirName)

                # 检查文件是否存在
                changeFlag = False
                if os.path.exists(stockJsonFilePath) and os.path.exists(stockDataFilePath):
                    # 读取文件内容
                    savedStockInfo, savedStockDataList = readStockData(symbol, period, adjust)
                    # savedStartDate = savedStockInfo.get("start_date","")
                    savedEndDate = savedStockInfo.get("end_date","")
                    newStartDate = savedEndDate
                    newStockDataList = getHistoryStockData(symbol, newStartDate, endYMD)
                    if newStockDataList:
                        changeFlag = True
                    stockDataList = savedStockDataList + newStockDataList
                    stockDataList = filterStockData(stockDataList,startYMD,endYMD)
                else:
                    #读取网络数据
                    stockDataList = getHistoryStockData(symbol, startYMD, endYMD)
                    changeFlag = True
                    pass
                
                if changeFlag:
                    #保存数据
                    stockInfo["YMDHMS"] = misc.getTime()
                    stockInfo["save_time"] = misc.humanTime(stockInfo["YMDHMS"])
                    saveStockData(symbol, period, adjust, stockDataList, stockInfo)

                result = True
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


#rsi 计算 begin
#采用index 数据, 计算rsi
#检查并读取股票完整数据, 包括, day, week, month 数据等
# periodList = ["daily","weekly","monthly"]
indexPeriodList = ["day","week","month"]
def checkReadIndexFullData(symbol, startYMD,endYMD):
    result = False
    try:
        indexInfo = {}
        indexInfo["code"] = symbol
        indexInfo["symbol"] = symbol
        indexInfo["start_date"] = startYMD
        indexInfo["end_date"] = endYMD
        indexInfo["columns"] = ["date","symbol","open","high","low","close","volume","amount"]

        for period in indexPeriodList:
            indexJsonFileName = f"{period}/{symbol}.json"
            indexDataFileName = f"{period}/{symbol}.csv"

            indexJsonFilePath = f"{settings.INDEX_DATA_SAVE_DIR_NAME}/{indexJsonFileName}"
            indexDataFilePath = f"{settings.INDEX_DATA_SAVE_DIR_NAME}/{indexDataFileName}"

            # 检查目录是否存在, 如果不存在, 则创建
            dirName = os.path.split(indexJsonFilePath)[0]
            createDir(dirName)

            # 检查文件是否存在
            changeFlag = False
            if os.path.exists(indexJsonFilePath) and os.path.exists(indexDataFilePath):
                # 读取文件内容
                savedIndexInfo, savedIndexDataList = readIndexData(symbol, period)
                # savedStartDate = savedIndexInfo.get("start_date","")
                savedEndDate = savedIndexInfo.get("end_date","")
                newStartDate = savedEndDate
                newIndexDataList = getHistoryIndexData(symbol,period,newStartDate,endYMD)
                if newIndexDataList:
                    changeFlag = True
                indexDataList = savedIndexDataList + newIndexDataList
                indexDataList = filterIndexData(indexDataList,startYMD,endYMD)
            else:
                #读取网络数据
                indexDataList = getHistoryIndexData(symbol,period,startYMD,endYMD)
                changeFlag = True
                pass
            
            if changeFlag:
                #保存数据
                indexInfo["YMDHMS"] = misc.getTime()
                indexInfo["save_time"] = misc.humanTime(indexInfo["YMDHMS"])
                saveIndexData(symbol, period, indexDataList, indexInfo)

            result = True
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


# 保存行业数据, 包括, day, week, month数据
def saveIndexData(symbol, period, indexDataList,indexInfo):
    result = False
    try:
        indexJsonFileName = f"{period}/{symbol}.json"
        indexDataFileName = f"{period}/{symbol}.csv"

        indexJsonFilePath = f"{settings.INDEX_DATA_SAVE_DIR_NAME}/{indexJsonFileName}"
        indexDataFilePath = f"{settings.INDEX_DATA_SAVE_DIR_NAME}/{indexDataFileName}"

        # 保存股票数据文件
        indexData = pd.DataFrame(indexDataList,columns=["date","symbol","open","high","low","close","volume","amount"])
        indexData.to_csv(indexDataFilePath,index=False)

        # 保存行业基本信息文件
        rtn = misc.saveJsonData(indexJsonFilePath,indexInfo,indent=2,ensure_ascii=False)
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


# 读取行业数据, 包括, day, week, month 数据
def readIndexData(symbol, period=""):
    indexInfo = {}
    indexData = []
    try:
        indexJsonFileName = f"{period}/{symbol}.json"
        indexDataFileName = f"{period}/{symbol}.csv"

        indexJsonFilePath = f"{settings.INDEX_DATA_SAVE_DIR_NAME}/{indexJsonFileName}"
        indexDataFilePath = f"{settings.INDEX_DATA_SAVE_DIR_NAME}/{indexDataFileName}"

        if os.path.exists(indexJsonFilePath) and os.path.exists(indexDataFilePath):

            indexData = pd.read_csv(indexDataFilePath)

            indexData = indexData.to_dict(orient='records')

            indexInfo = misc.loadJsonData(indexJsonFilePath,"dict")
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return indexInfo,indexData


# 过滤行业数据, 只保留在startYMD和endYMD之间的数据
def filterIndexData(indexDataList,startYMD,endYMD):
    result = []
    try:
        dateData = {}
        for indexData in indexDataList:
            date = indexData.get("date","")
            if date not in dateData:
                dateData[date] = date
                if date >= startYMD and date <= endYMD:
                    result.append(indexData)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def getHistoryIndexData(symbol,period,startYMD, endYMD):
    result = []
    try:
        if startYMD < endYMD:
            #获取行业历史数据(申银万国)
            result = comAK.swGetIndexHistory(symbol, period, startYMD)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


# 计算行业RSI指标,根据industry_symbol获取行业数据
RSI_PERIOD = settings.STOCK_RSI_CALCULATION_PERIODS.get("rsi_period",14)
def calcIndexRSI(indexDataList,period=RSI_PERIOD):
    result = []
    try:
        #获取行业历史数据(申银万国)
        indexData = pd.DataFrame(indexDataList,columns=["date","symbol","open","high","low","close","volume","amount"])
        # indexData['date'] = pd.to_datetime(indexData['date']) # 转换为日期时间格式, 用于排序, 暂时不需要, 原来格式也可以保证顺序
        indexData = indexData.sort_values('date')
        # indexData.set_index('date', inplace=True) #已经用sort_values排序, 不需要再设置索引
        indexData["rsi14"] = ta.rsi(indexData["close"],period)
        lookbackWeeks = settings.STOCK_RSI_CALCULATION_PERIODS.get("lookback_weeks",104)
        indexData = indexData.tail(lookbackWeeks)
        result = indexData.to_dict(orient='records')
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#计算行业波动率 σ (sigma),根据rsi14指标计算
def calcIndexSigma(indexDataList):
    result = 0.0
    try:
        indexData = pd.DataFrame(indexDataList,columns=["date","symbol","open","high","low","close","volume","amount","rsi14"])
        result = float(indexData["rsi14"].std())
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")   
    return result


def calcIndustrySigma(symbolList):
    result = []
    try:
        indexPeriod = "week"
        for symbol in symbolList:
            indexInfo,indexDataList = readIndexData(symbol, indexPeriod)
            if indexDataList:
                indexDataList = calcIndexRSI(indexDataList)
                lastRsi = indexDataList[-1]["rsi14"]
                sigma = calcIndexSigma(indexDataList)
                item = {"symbol":symbol,"sigma":sigma,"rsi":lastRsi}
                result.append(item)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")   
    return result


#生成行业波动率分类
def generateIndustryVolatilityStratification():
    result = []
    try:
        # 读取行业映射数据
        industryMappingData = readStockIndustryMapping()
        industry_data = industryMappingData["industry_data"]
        industrySymbolList = list(industry_data.keys())

        # 计算行业波动率 σ (sigma)
        sigmaDataList = calcIndustrySigma(industrySymbolList)
        sigmaList = [item["sigma"] for item in sigmaDataList]

        # 计算波动率分位数
        q1Percentage = settings.STOCK_RSI_CALCULATION_PERIODS.get("volatility_quantiles",{}).get("q1",25)
        q3Percentage = settings.STOCK_RSI_CALCULATION_PERIODS.get("volatility_quantiles",{}).get("q3",75)
        q1 = float(np.percentile(sigmaList, q1Percentage))
        q3 = float(np.percentile(sigmaList, q3Percentage))

        lookbackWeeks = settings.STOCK_RSI_CALCULATION_PERIODS.get("lookback_weeks",104)

        #计算各个行业的波动率分类
        for item in sigmaDataList:
            symbol = item["symbol"]
            if symbol in industry_data:
                industryName = industry_data[symbol].get("industry_name","")
            else:
                industryName = ""
            
            sigma = item["sigma"]
            if sigma <= q1:
                layer = "低波动"
                pct_low, pct_high = 8, 92
            elif sigma <= q3:
                layer = "中波动"
                pct_low, pct_high = 10, 90
            else:
                layer = "高波动"
                pct_low, pct_high = 5, 95
            
            oversold = float(np.percentile(sigmaList, 15))
            overbought = float(np.percentile(sigmaList, 85))
            extreme_oversold = float(np.percentile(sigmaList, pct_low))
            extreme_overbought = float(np.percentile(sigmaList, pct_high))

            item["industry_name"] = industryName
            item["volatility"] = round(sigma,2)
            item["layer"] = layer
            item["rsi"] = round(item["rsi"],2)  
            item["oversold"] =  round(oversold,2)
            item["overbought"] = round(overbought,2)
            item["extreme_oversold"] = round(extreme_oversold,2)
            item["extreme_overbought"] = round(extreme_overbought,2)
            item["data_points"] = lookbackWeeks 
            item["updateTime"] = misc.getHumanTimeStamp()[0:19]

            del item["sigma"]

            result.append(item)

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")   
    return result


# 保存行业RSI阈值数据,包括波动率, 超卖, 超买, 极端超卖, 极端超买, 数据点数, 更新时间
def saveSWRSIThresholdData(dataList):
    result = False
    try:
        rsiThresoldFileName = settings.STOCK_SW_RSI_THRESHOLD_FILE
        rsiThresoldFilePath = f"{settings.INDEX_DATA_SAVE_DIR_NAME}/{rsiThresoldFileName}"
        createDir(settings.INDEX_DATA_SAVE_DIR_NAME)

        # 保存股票数据文件
        data = pd.DataFrame(dataList,columns=["symbol","industry_name","layer","volatility","rsi","oversold","overbought",\
            "extreme_oversold","extreme_overbought","data_points","updateTime"])

        data = data.sort_values(by=['symbol'], ascending=True)

        data.rename(columns={'symbol':'行业代码', 'industry_name':'行业名称','oversold':'普通超卖',
        'overbought':'普通超买','extreme_oversold':'极端超卖','extreme_overbought':'极端超买',
        'rsi':'current_rsi','updateTime':'更新时间'}, inplace=True)

        data.to_csv(rsiThresoldFilePath,index=False)
        result = True
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


# 读取行业RSI阈值数据,包括波动率, 超卖, 超买, 极端超卖, 极端超买, 数据点数, 更新时间
def readSWRSIThresholdData():
    result = []
    try:
        rsiThresoldFileName = settings.STOCK_SW_RSI_THRESHOLD_FILE
        rsiThresoldFilePath = f"{settings.INDEX_DATA_SAVE_DIR_NAME}/{rsiThresoldFileName}"
        if os.path.exists(rsiThresoldFilePath):
            data = pd.read_csv(rsiThresoldFilePath)
            result = data.to_dict(orient='records')
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#rsi 计算 end


def test():
    # 示例
    # data = getStockBasicInfo()
    # rtn = saveStockBasicInfo(data)
    # data = readStockBasicInfo()

    # indexPeriod = "week"
    # symbol = "801012"
    # indexInfo,indexDataList = readIndexData(symbol, indexPeriod)
    # indexDataList = calcIndexRSI(indexDataList)
    # sigma = calcIndexSigma(indexDataList)
    # industryMappingData = readStockIndustryMapping()
    # industry_data = industryMappingData["industry_data"]
    # industrySymbolList = list(industry_data.keys())
    # sigmaList = calcIndustrySigma(industrySymbolList)
    dataList = generateIndustryVolatilityStratification()
    saveSWRSIThresholdData(dataList)
    dataList = readSWRSIThresholdData()

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
