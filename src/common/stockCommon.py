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
# 3. 股票指标的计算(包括,量能指标, 价格指标等)
# 4. 股票筛选器(包括,量能筛选器, 价格筛选器等)


_VERSION="20260129"


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
import talib

from common import globalDefinition as comGD
from common import miscCommon as misc

# 股票akshare模块
from common import akshareCommon as comAK

# 股票背离检测模块
from common import divergence as comDiv

#申万行业数据规则
#在以前的版本中,rotation_strategy_system\config\comprehensive_industry_rules.py
from common import swIndustryRules as comIndustryRules

from config import basicSettings as settings


if "_LOG" not in dir() or not _LOG:
    logDir = os.path.join(settings._HOME_DIR, "log")
    _LOG = misc.setLogNew("STOCK",comGD._DEF_LOG_STOCK_TEST_NAME, logDir)


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


#读取回测配置文件(csv 格式)
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


#将回测配置文件转换为json 格式
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


#读取回测配置文件(json 格式)
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

#
def getDividendData():
    result = {}
    try:
        result = comAK.getDividendData()
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result

#读取股票分红数据文件(json 格式)
def readDividendData():
    result = {}
    try:
        fileName = settings.STOCK_DIVIDEND_DATA_FILE
        filePath = f"{settings.STOCK_DATA_CACHE_DIR_NAME}/{fileName}"
        result = misc.loadJsonData(filePath,"dict")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result 


#读取股票分红数据文件信息(json 格式)
def getDividendFileInfo():
    result = {}
    try:
        fileName = settings.STOCK_DIVIDEND_DATA_FILE
        filePath = f"{settings.STOCK_DATA_CACHE_DIR_NAME}/{fileName}"
        #获取文件信息
        fileInfo = os.stat(filePath)
        result["fileSize"] = fileInfo.st_size
        result["fileModTime"] = fileInfo.st_mtime

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result 


#保存股票分红数据文件(json 格式)
def saveDividendData(data):
    result = False
    try:
        fileName = settings.STOCK_DIVIDEND_DATA_FILE
        filePath = f"{settings.STOCK_DATA_CACHE_DIR_NAME}/{fileName}"

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


#获取股票历史数据(东方财富)
def getHistoryStockData(symbol,startYMD,endYMD,period="",adjust=""):
    result = []
    try:
        endYMDHMS = endYMD + "000000"
        weekDay = misc.weekDay(endYMDHMS)
        #如果是周五或周六,需要调整数据
        if weekDay.wday == 5:
            endYMD = misc.getPassday(1,endYMD)
        elif weekDay.wday == 6:
            endYMD = misc.getPassday(2,endYMD)

        #其次如果是周数据, 需要至少是5个交易日, 否则, 调整到上一个周五
        if period == "weekly": 
            endYMD = misc.getPreviousFriday(endYMD)
            #计算两个日期之间的交易日天数
            passedDay = misc.getPassday(7,endYMD)
            if passedDay < startYMD:
                startYMD = passedDay

        if startYMD < endYMD:
            #获取股票历史数据(东方财富)
            result = comAK.gmGetHistroryData(symbol, startYMD, endYMD,period,adjust)
            if result == None:
                #出错了, 尝试新浪数据
                result = comAK.sinoGetHistroryData(symbol, startYMD, endYMD,period,adjust)
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
periodList = ["daily"] # 只需要处理日数据, 周数据暂时无法使用(每天都有)
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
                dirName = os.path.split(stockJsonFilePath)[0]
                createDir(dirName)

                # 检查文件是否存在
                changeFlag = False
                if os.path.exists(stockJsonFilePath) and os.path.exists(stockDataFilePath):
                    # 读取文件内容
                    savedStockInfo, savedStockDataList = readStockData(symbol, period, adjust)
                    # savedStartDate = savedStockInfo.get("start_date","")
                    savedEndDate = savedStockInfo.get("end_date","")
                    newStartDate = savedEndDate
                    newStockDataList = getHistoryStockData(symbol, newStartDate, endYMD,period,adjust)
                    if newStockDataList:
                        changeFlag = True
                        stockDataList = savedStockDataList + newStockDataList
                        stockDataList = filterStockData(stockDataList,startYMD,endYMD)
                else:
                    #读取网络数据
                    stockDataList = getHistoryStockData(symbol, startYMD, endYMD,period,adjust)
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


# 将日数据转换为周数据, 每周五为结束日
def convertDaily2Weekly(stockData):
    result = stockData.copy()
    try:
        if not isinstance(stockData.index, pd.DatetimeIndex):
            stockData["date"] = pd.to_datetime(stockData["date"])
            stockData.set_index("date", inplace=True)
            stockData = stockData.resample('W-FRI').agg({'open': 'last', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', 'amount': 'sum'})           
            result = stockData.reset_index()
            result = result.dropna()

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


# 读取股票数据, 包括, daily, weekly, monthly 数据, 包括,当前,前复权, 后复权等
def readStockData(symbol, period="daily", adjust="",startYMD=""):
    stockInfo = {}
    stockData = []
    try:
        if adjust:
            stockJsonFileName = f"{"daily"}/{symbol}_{adjust}.json"
            stockDataFileName = f"{"daily"}/{symbol}_{adjust}.csv"
        else:
            stockJsonFileName = f"{"daily"}/{symbol}.json"
            stockDataFileName = f"{"daily"}/{symbol}.csv"
        stockJsonFilePath = f"{settings.STOCK_DATA_SAVE_DIR_NAME}/{stockJsonFileName}"
        stockDataFilePath = f"{settings.STOCK_DATA_SAVE_DIR_NAME}/{stockDataFileName}"

        stockData = pd.read_csv(stockDataFilePath)
        if startYMD:
            start_date = startYMD[0:4] + "-" + startYMD[4:6] + "-" + startYMD[6:8]
            stockData = stockData[stockData["date"] >= start_date]

        if period == "weekly":
            stockData = convertDaily2Weekly(stockData) #将日数据转换为周数据, 每周五为结束日, 转换后date 是 timestamp, 需转换为字符串格式

        #删除重复数据
        stockData.drop_duplicates(inplace=True)

        #将数据转换为字典格式
        stockData = stockData.to_dict(orient='records')
        for item in stockData:
            if not isinstance(item["date"], str): #weekly 数据的问题
                item["date"] = item["date"].strftime(f"%Y-%m-%d")
           
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


#获取行业历史数据(申银万国)
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


# 计算行业波动率 σ (sigma),根据industry_symbol获取行业数据
def calcIndustrySigma(symbolList):
    result = {}
    try:
        indexPeriod = "week"
        for symbol in symbolList:
            indexInfo,indexDataList = readIndexData(symbol, indexPeriod)
            if indexDataList:
                indexDataList = calcIndexRSI(indexDataList)
                lastRsi = indexDataList[-1]["rsi14"]
                sigma = calcIndexSigma(indexDataList)
                rsiList = []
                for data in indexDataList:
                    rsiList.append(data["rsi14"])
                item = {"symbol":symbol,"sigma":sigma,"rsi":lastRsi,"rsiList":rsiList}
                result[symbol] = item
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
        sigmaDataSet = calcIndustrySigma(industrySymbolList)
        sigmaList = []
        for symbol, item in sigmaDataSet.items():
            sigmaList.append(item["sigma"])

        # 计算波动率分位数
        q1Percentage = settings.STOCK_RSI_CALCULATION_PERIODS.get("volatility_quantiles",{}).get("q1",25)
        q3Percentage = settings.STOCK_RSI_CALCULATION_PERIODS.get("volatility_quantiles",{}).get("q3",75)
        q1 = float(np.percentile(sigmaList, q1Percentage))
        q3 = float(np.percentile(sigmaList, q3Percentage))

        lookbackWeeks = settings.STOCK_RSI_CALCULATION_PERIODS.get("lookback_weeks",104)

        #计算各个行业的波动率分类
        for symbol, item in sigmaDataSet.items():
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
            
            # 计算RSI指标分位数
            rsiList = item.get("rsiList",[])
            oversold = float(np.percentile(rsiList, 15))
            overbought = float(np.percentile(rsiList, 85))
            extreme_oversold = float(np.percentile(rsiList, pct_low))
            extreme_overbought = float(np.percentile(rsiList, pct_high))

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

            #删除无用的数据
            del item["sigma"]
            del item["rsiList"]

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
        df = pd.DataFrame(dataList,columns=["symbol","industry_name","layer","volatility","rsi","oversold","overbought",\
            "extreme_oversold","extreme_overbought","data_points","updateTime"])

        df = df.sort_values(by=['symbol'], ascending=True)

        # 兼容 frank xie数据
        df.rename(columns={'symbol':'行业代码', 'industry_name':'行业名称','oversold':'普通超卖',
        'overbought':'普通超买','extreme_oversold':'极端超卖','extreme_overbought':'极端超买',
        'rsi':'current_rsi','updateTime':'更新时间'}, inplace=True)

        df.to_csv(rsiThresoldFilePath,index=False)

        result = True
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


# 读取行业RSI阈值数据,包括波动率, 超卖, 超买, 极端超卖, 极端超买, 数据点数, 更新时间
def readSWRSIThresholdData():
    result = {}
    try:
        rsiThresoldFileName = settings.STOCK_SW_RSI_THRESHOLD_FILE
        rsiThresoldFilePath = f"{settings.INDEX_DATA_SAVE_DIR_NAME}/{rsiThresoldFileName}"
        if os.path.exists(rsiThresoldFilePath):
            df = pd.read_csv(rsiThresoldFilePath)
            df.rename(columns={'行业代码':'symbol', '行业名称':'industry_name','普通超卖':'oversold',
            '普通超买':'overbought','极端超卖':'extreme_oversold','极端超买':'extreme_overbought',
            'rsi':'current_rsi','更新时间':'updateTime'}, inplace=True)
            rsiDataList = df.to_dict(orient='records')
            for item in rsiDataList:
                industry_code = str(item["symbol"]) #转换为字符串, 避免后续计算错误
                item["industry_code"] = industry_code
                result[industry_code] = item
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#rsi 计算 end

#计算布林线 begin
"""
计算布林线

参数:
symbol: 股票代码
window: 移动平均窗口大小，默认20
std_dev: 标准差倍数，默认2

返回:
包含中轨、上轨、下轨的DataFrame
"""
def calcBollingerBands(symbol, window=20, std_dev=2, period="weekly",adjust="hfq"):
    result = {}
    try:
        stockInfo,stockDataList = readStockData(symbol,period=period,adjust=adjust)
        df = pd.DataFrame(stockDataList)
        df = df.sort_values(by=['date'], ascending=True) # 按日期排序
        df['MA'] = df['close'].rolling(window=window).mean()
        df['STD'] = df['close'].rolling(window=window).std()
        df['upper_band'] = df['MA'] + (df['STD'] * std_dev)
        df['lower_band'] = df['MA'] - (df['STD'] * std_dev)
        df = df.where(pd.notna(df), 0.0)
        result = df.to_dict(orient='records')
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result
#计算布林线 end

#计算背离指标 begin

#计算背离指标 end

#计算技术指标 begin
#计算技术指标,包括布林线, RSI, EMA, 成交量均线, K线等, 并返回最近lastNum条数据 (125条件+14条 window)
def calcTechnicalIndicators(symbol, period="weekly", adjust="hfq",parameters={},lastNum=139):
    result = {}
    try:
        if parameters == {}:
            parameters = {"bolling_window":20, "bolling_std_dev":2, "rsi_window":14, "ema_window":20,
            "volume_window":4,"macd_fastperiod":12, "macd_slowperiod":26, "macd_signalperiod":9}
        stockInfo,stockDataList = readStockData(symbol,period=period,adjust=adjust)
        df = pd.DataFrame(stockDataList)
        df = df.sort_values(by=['date'], ascending=True) # 按日期排序
        
        # 计算移动平均线和标准差,布林线
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma10'] = df['close'].rolling(window=10).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma'] = df['close'].rolling(window=parameters["bolling_window"]).mean()
        df['std'] = df['close'].rolling(window=parameters["bolling_window"]).std()
        df['upper_band'] = df['ma'] + (df['std'] * parameters["bolling_std_dev"])
        df['lower_band'] = df['ma'] - (df['std'] * parameters["bolling_std_dev"])
        
        # 计算RSI
        df['rsi'] = talib.RSI(df['close'], timeperiod=parameters["rsi_window"])

        # 计算MACD数据
        macd, macd_signal, macd_hist = talib.MACD(df['close'], fastperiod=parameters["macd_fastperiod"], slowperiod=parameters["macd_slowperiod"], signalperiod=parameters["macd_signalperiod"])
        df["macd"] = macd # macd（即 DIF）：快慢EMA的差值
        df["macd_signal"] = macd_signal # macdsignal（即 DEA）：DIF的EMA（信号线）
        df["macd_hist"] = macd_hist # DIF与DEA的差值

        #计算ema
        df['ema20'] = talib.EMA(df['close'], timeperiod=20) 
        df['ema50'] = talib.EMA(df['close'], timeperiod=50)
        df['ema60'] = talib.EMA(df['close'], timeperiod=60)
        df['EMA'] = talib.EMA(df['close'], timeperiod=parameters["ema_window"])

        #成交量均线
        df['volume_ma'] = df['volume'].rolling(window=parameters["volume_window"]).mean()
        df['volume_ratio'] = df['volume'] / df['volume_ma']

        #计算K线
        df['k'] = (df['close'] - df['low']) / (df['high'] - df['low'])

        #保留最近lastNum条数据
        df = df.tail(lastNum)

        df = df.where(pd.notna(df), 0.0)
        result = df.to_dict(orient='records')

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#把技术指标数据转换为Frank Xie格式
def covertTechnicalIndicators2FrankFormat(dataList):
    result = pd.DataFrame()
    try:
        df = pd.DataFrame(dataList)
        df.rename(columns={'macd_hist':'macd_histogram',"lower_band":"bb_lower",
        "upper_band":"bb_upper","ma":"bb_middle","ema":"ema_20","volume_ma":"volume_ma_4"}, inplace=True)
        result = df[["date","open","high","low","close","volume","rsi",
        "macd","macd_signal","macd_histogram","ema_20",
        "bb_lower","bb_upper","bb_middle","volume_ma_4","K"]]
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#计算背离指标 begin
def calcRSIDivergence(techIndicators):
    result = {"top_divergence":False, "bottom_divergence":False}
    try:
        df = pd.DataFrame(techIndicators)
        df = df.sort_values(by=['date'], ascending=True) # 按日期排序
        priceData = df["close"]
        rsiData = ta.rsi(priceData, length=14)
        result = comDiv.detect_rsi_divergence(priceData, rsiData)
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#计算MACD背离指标
def calcMacdDivergence(techIndicators):
    result = {"top_divergence":False, "bottom_divergence":False}
    try:
        df = pd.DataFrame(techIndicators)
        df = df.sort_values(by=['date'], ascending=True) # 按日期排序
        priceData = df["close"]
        macdData = df["macd_hist"]
        result = comDiv.detect_macd_divergence(priceData, macdData)
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result
#计算背离指标 end

#计算技术指标 end

#四维信号系统 begin
#四维信号系统描述
'''
策略的决策核心是一个四维信号系统，它以层层递进的方式进行过滤和确认，以确保信号的高质量。在信号确认后，系统采用基于估值水平的动态仓位管理策略。

#### **维度一：价值准入过滤器 (硬性前提)**
这是信号系统的**唯一硬性前提**，所有交易机会必须首先通过此关。

- **主规则**: 基于DCF（现金流折现）估值计算**价值比率** (`当前价格 / DCF估值`)。
  - **买入条件**: `价值比率 < 80%` (处于低估区间)
  - **卖出条件**: `价值比率 > 70%` (处于高估或合理区间)
- **兼容性回退**: 当目标缺少DCF估值数据时，系统自动无缝切换到**"20周EMA趋势过滤器"**，确保策略的普适性。

#### **维度二、三、四：技术择时信号 (三者取二)**
在满足【维度一】的前提下，以下三个技术维度中**至少满足两个**，才能最终触发交易信号。

- **维度二：超买超卖 (动态RSI系统)**
  - **技术参数**: RSI周期14周，124个申万二级行业特定阈值
  - **机制**: 摒弃固定的30/70阈值，采用一套**数据驱动的动态阈值系统**。该系统通过分析行业历史波动率，自动将行业分为"高/中/低"三层，并根据其历史RSI分布的**分位数**来计算超买超卖阈值。
  - **信号**:
    - **普通信号**: RSI触达为该行业计算出的普通阈值，且出现价格背离（比较最近3周）。
    - **极端信号**: RSI触达更严格的极端阈值，**此时无需背离**，直接触发。

- **维度三：动能确认 (MACD)**
  - **技术参数**: 快线12周，慢线26周，信号线9周
  - **机制**: 判断市场动能的转换。
  - **信号**:
    - **买入**: MACD绿色柱体（HIST<0）连续2根缩短，或柱体翻红（HIST由负转正），或DIF金叉DEA。
    - **卖出**: MACD红色柱体（HIST>0）连续2根缩短，或柱体翻绿（HIST由正转负），或DIF死叉DEA。

- **维度四：极端价格+量能 (布林带 + 成交量)**
  - **机制**: 捕捉由资金驱动的极端情绪爆发点。
  - **技术参数**:
    - 布林带：20周期，2倍标准差
    - 成交量均线：4周均量
    - 量能倍数：买入0.8倍，卖出1.3倍
  - **信号**:
    - **买入**: 收盘价 ≤ 布林下轨 且 本周成交量 ≥ 4周均量 × 0.8
    - **卖出**: 收盘价 ≥ 布林上轨 且 本周成交量 ≥ 4周均量 × 1.3
'''

def detectEmaTrend(ema: pd.Series, regression_periods: int = 8, flat_threshold: float = 0.003) -> str:
    """
    使用线性回归法检测EMA趋势方向
    
    Args:
        ema: EMA序列
        regression_periods: 用于线性回归的周期数，默认8周
        flat_threshold: 判断走平的相对斜率阈值，默认0.003(0.3%)
        
    Returns:
        str: 趋势方向，"向上"、"向下"或"走平"
        
    Raises:
        IndicatorCalculationError: 计算失败
        InsufficientDataError: 数据不足
        InvalidParameterError: 参数无效
    """
    result = comGD._DEF_STOCK_EMA_TREND_HOLD
    try:
        if isinstance(ema, pd.Series):
            ema_clean = ema.dropna()
            # 去除空值
            if len(ema_clean) >= regression_periods:       
                # 获取最近N周的EMA数据
                recent_ema = ema_clean.iloc[-regression_periods:].values
       
                # 创建X轴数据
                x = np.arange(len(recent_ema))
                
                # 计算线性回归
                slope, _, _, _, _ = np.polyfit(x, recent_ema, 1, full=True)
                
                # 计算相对斜率：斜率除以均值，得到归一化的斜率
                relative_slope = slope[0] / np.mean(recent_ema)
        
                # 判断走平：相对斜率的绝对值小于阈值
                if abs(relative_slope) < flat_threshold:
                    trend = comGD._DEF_STOCK_EMA_TREND_HOLD
                elif relative_slope > 0:
                    trend = comGD._DEF_STOCK_EMA_TREND_UP
                else:
                    trend = comGD._DEF_STOCK_EMA_TREND_DOWN 

                result = trend
        
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        
    return result
        


#价值筛选器
'''
2. 价值比过滤器判断 (硬性前提)
  ├── 获取DCF估值
  ├── 计算价值比 = 当前价格 / DCF估值
  ├── 卖出判断：价值比 > 80%
  └── 买入判断：价值比 < 70%
'''
def valueInvestingScreener(symbol,currentPrice,dcfValue,technicalData,backtestSettings):
    result = {"scores":{"trend_filter_low":False,"trend_filter_high":False}}
    try:
        result["symbol"] = symbol
        result["currentPrice"] = currentPrice
        result["dcfValue"] = dcfValue
        result["valueRatio"] = 0.0
        scores = result["scores"]

        if dcfValue > 0.0:
            #获取价值比过滤器参数
            try:
                valueRatioBuyThresold = float(backtestSettings.get("value_ratio_buy_threshold").get("value"))
            except:
                valueRatioBuyThresold = 0.80
            try:
                valueRatioSellThresold = float(backtestSettings.get("value_ratio_sell_threshold").get("value"))
            except:
                valueRatioSellThresold = 0.70
            result["method"] = "valueRatio"
            valueRatio = currentPrice / dcfValue 
            valueRatio = round(valueRatio,2)
            #价值比过滤器判断
            result["valueRatio"] = valueRatio
            if valueRatio > valueRatioSellThresold:
                result["action"] = comGD._DEF_STOCK_VALUE_SCREEN_SELL
                scores["trend_filter_high"] = True
            elif valueRatio < valueRatioBuyThresold:
                result["action"] = comGD._DEF_STOCK_VALUE_SCREEN_BUY
                scores["trend_filter_low"] = True
            else:
                result["action"] = comGD._DEF_STOCK_VALUE_SCREEN_HOLD   
            pass
        else:
            result["method"] = "ema" #默认使用20周EMA趋势过滤器
            trend = detectEmaTrend(technicalData["ema20"])
            result["trend"] = trend
            if trend == comGD._DEF_STOCK_EMA_TREND_UP:
                result["action"] = comGD._DEF_STOCK_VALUE_SCREEN_BUY
                scores["trend_filter_high"] = True
            elif trend == comGD._DEF_STOCK_EMA_TREND_DOWN:
                result["action"] = comGD._DEF_STOCK_VALUE_SCREEN_SELL
                scores["trend_filter_low"] = True
            else:
                result["action"] = comGD._DEF_STOCK_VALUE_SCREEN_HOLD   
            pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#维度二：超买超卖
def calcOverBuySellFactor(symbol,backtestData):
    result = {"scores":{"overbought_oversold_high":False,"overbought_oversold_low":False}}
    try:
        result["symbol"] = symbol
        scores = result["scores"]

        #获取股票行业映射
        industryMapping = backtestData.get("industryMapping",{})
        industryInfo = industryMapping.get(symbol,"")

        #获取股票行业代码和名称
        industry_code = industryInfo.get("industry_code")
        industry_name = industryInfo.get("industry_name")

        #获取行业RSI数据
        industryRSIData = backtestData.get("industryRSIData",{})
        currIndustryRSI = industryRSIData.get(industry_code,{})
        #获取当前行业的RSI值
        currRSI = currIndustryRSI.get("current_rsi",0.0)

        #获取背离数据
        divergenceData = backtestData.get("divergenceData",{})
        currDivergence = divergenceData.get(symbol,{})

        #获取行业规则
        industryRule = comIndustryRules.get_comprehensive_industry_rules(industry_name)
        if not industryRule:
            _LOG.warning(f"industryRule is None, industry_name: {industry_name}")
        #是否考虑背离情况
        needDivergenceBuySell = industryRule['divergence_required']
        
        #1. 判断是否是极端超买/超卖
        extermeOversold = currIndustryRSI.get("extreme_oversold",0.0)
        extermeOverbought = currIndustryRSI.get("extreme_overbought",0.0)
        if extermeOverbought and currRSI >= extermeOverbought:
            #极端超买
            scores["overbought_oversold_high"] = True #为啥是oversold_high? 应该 是overbought = True
        elif extermeOversold and currRSI <= extermeOversold:
            #极端超卖
            scores["overbought_oversold_low"] = True #为啥是oversold_low? 应该 oversold = True
        else:
            #其他情况, 要考虑是否 背离情况
            oversold = currIndustryRSI.get("oversold",0.0)
            overbought = currIndustryRSI.get("overbought",0.0)
            topDivergence = currDivergence.get("top_divergence",False)
            bottomDivergence = currDivergence.get("bottom_divergence",False)
            # 2. 普通RSI阈值：需要考虑背离条件
            # 阶段高点：14周RSI > 行业特定超买阈值 且 (出现顶背离 或 不要求背离)
            # if needDivergenceBuySell:
            #     divergenceResult = topDivergence
            # else:
            #     divergenceResult = True
            # if currRSI > overbought and divergenceResult:
            if currRSI > overbought and (not needDivergenceBuySell or topDivergence):
                scores["overbought_oversold_high"] = True
            #阶段低点：14周RSI <= 行业特定超卖阈值 且 (出现底背离 或 不要求背离)
            elif currRSI <= oversold and (not needDivergenceBuySell or bottomDivergence):
                scores["overbought_oversold_low"] = True
            pass
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#动能确认
def calcMomentumValidation(symbol,backtestData):
    result = {"scores":{"momentum_high":False,"momentum_low":False}}
    try:
        result["symbol"] = symbol
        scores = result["scores"]

        stockIndicators = backtestData.get("stockIndicators",{})
        currStockIndicators = stockIndicators.get(symbol,{})
        if currStockIndicators:
            currDIF = currStockIndicators[-1].get("macd",0.0) # 当前DIF值,macd（即 DIF）：快慢EMA的差值
            currDEA = currStockIndicators[-1].get("macd_signal",0.0) # 当前DEA值,macdsignal（即 DEA）：DIF的EMA（信号线）
            currMACDHist = currStockIndicators[-1].get("macd_hist",0.0) # 当前DIF与DEA的差值

            if len(currStockIndicators) > 3: 
                macdHistPrev1 = currStockIndicators[-2].get("macd_hist",0.0) # 当前DIF与DEA的差值
                macdHistPrev2 = currStockIndicators[-3].get("macd_hist",0.0) # 当前DIF与DEA的差值

                # 红色柱体连续2根缩短（用于卖出信号）
                redHistShrinking = False
                if currMACDHist > 0 and macdHistPrev1 > 0 and macdHistPrev2 > 0:
                    if currMACDHist < macdHistPrev1 < macdHistPrev2:
                        redHistShrinking = True
                
                #绿色柱体连续2根缩短（用于买入信号）
                greenHistShrinking = False
                if currMACDHist < 0 and macdHistPrev1 < 0 and macdHistPrev2 < 0:
                    if abs(currMACDHist) < abs(macdHistPrev1) < abs(macdHistPrev2):
                        greenHistShrinking = True   
                
                # MACD柱体颜色状态
                isMACDGreen = currMACDHist < 0 # 当前为绿色柱体
                isMACDRed = currMACDHist > 0 # 当前为红色柱体

                # 金叉死叉
                difCrossUp = False
                difCrossDown = False
                difPrev = currStockIndicators[-2].get("macd",0.0) # 前一个DIF值
                deaPrev = currStockIndicators[-2].get("macd_signal",0.0) # 前一个DEA值
                if currDIF > currDEA and difPrev <= deaPrev: # 金叉
                    difCrossUp = True
                if currDIF < currDEA and difPrev >= deaPrev: # 死叉
                    difCrossDown = True

                # 检查前期柱体缩短 + 当前转色的严谨条件
                # 买入：前2根绿柱缩短 + 当前转红
                green2redTransition = False
                if (macdHistPrev1 < 0 and macdHistPrev2 < 0 and  # 前2根是绿柱
                    abs(macdHistPrev1) < abs(macdHistPrev2) and  # 前期绿柱在缩短
                    currMACDHist > 0):  # 当前转为红柱
                    green2redTransition = True

                # 卖出：前2根红柱缩短 + 当前转绿
                red2greenTransition = False
                if (macdHistPrev1 > 0 and macdHistPrev2 > 0 and  # 前2根是红柱
                    macdHistPrev1 < macdHistPrev2 and  # 前期红柱在缩短
                    currMACDHist < 0):  # 当前转为绿柱
                    red2greenTransition = True

                # 阶段高点（卖出）：MACD红色柱体连续2根缩短 或 前期红柱缩短+当前转绿 或 DIF死叉DEA
                sellCondition = [redHistShrinking, red2greenTransition, difCrossDown]
                if any(sellCondition):
                    scores['momentum_high'] = True
                
                # 阶段低点（买入）：MACD绿色柱体连续2根缩短 或 前期绿柱缩短+当前转红 或 DIF金叉DEA
                buyCondition = [greenHistShrinking, green2redTransition, difCrossUp]
                if any(buyCondition):
                    scores['momentum_low'] = True
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#极端价格 + 量能
def calcExtremePriceVolume(symbol,backtestData):
    result = {"scores":{"extreme_price_volume_high":False,"extreme_price_volume_low":False}}
    try:
        result["symbol"] = symbol
        scores = result["scores"]

        backtestSettings = backtestData.get("backtestSettings",{})

        stockIndicators = backtestData.get("stockIndicators",{})
        currStockIndicators = stockIndicators.get(symbol,{})
        if currStockIndicators:
            currPrice = currStockIndicators[-1].get("close",0.0) # 当前收盘价
            currVolume = currStockIndicators[-1].get("volume",0.0) # 当前量能

            currBBUpper = currStockIndicators[-1].get("upper_band",0.0) # 当前布林带上限 Bollinger Bands
            currBBLower = currStockIndicators[-1].get("lower_band",0.0) # 当前布林带下限 Bollinger Bands

            currVolumeMA = currStockIndicators[-1].get("volume_ma",0.0) # 当前量能移动平均线

            # 阶段高点：收盘价 ≥ 布林上轨 且 本周量 ≥ 4周均量 × 1.3
            try:
                valumeSellRatio = settings.STOCK_STRATEGY_PARAMS.get("volume_multiplier_high")
            except:
                valumeSellRatio = 1.3

            if currPrice >= currBBUpper and currVolume >= currVolumeMA * valumeSellRatio:
                scores["extreme_price_volume_high"] = True

            # 阶段低点：收盘价 ≤ 布林下轨 且 本周量 ≥ 4周均量 × 0.8
            try:
                valumeBuyRatio = settings.STOCK_STRATEGY_PARAMS.get("volume_multiplier_low")
            except:
                valumeBuyRatio = 0.8

            if currPrice <= currBBLower and currVolume >= currVolumeMA * valumeBuyRatio:
                scores["extreme_price_volume_low"] = True

            pass

        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#2/3因子筛选器,从维度二、三、四中至少满足两个
def twoOutOfThreeFactors(symbol,backtestData):
    result = {}
    try:
        result["symbol"] = symbol
        
        buyParameters = []
        sellParameters = []

        #计算超买超卖因子
        buySellFactor = calcOverBuySellFactor(symbol,backtestData)

        buyParameters.append(buySellFactor["scores"]["overbought_oversold_high"])
        sellParameters.append(buySellFactor["scores"]["overbought_oversold_low"])

        #计算动能确认因子
        momentumFactor = calcMomentumValidation(symbol,backtestData)

        buyParameters.append(buySellFactor["scores"]["momentum_high"])
        sellParameters.append(buySellFactor["scores"]["momentum_low"])

        #极端价格 + 量能
        extremePriceVolumeFactor = calcExtremePriceVolume(symbol,backtestData)

        buyParameters.append(buySellFactor["scores"]["extreme_price_volume_high"])
        sellParameters.append(buySellFactor["scores"]["extreme_price_volume_low"])

        #合并因子
        scores = {**buySellFactor["scores"],**momentumFactor["scores"],**extremePriceVolumeFactor["scores"]}
        
        # 从维度二、三、四中至少满足两个
        if buyParameters.count(True) >= 2:
            result["action"] = comGD._DEF_STOCK_VALUE_SCREEN_BUY
        elif sellParameters.count(True) >= 2:
            result["action"] = comGD._DEF_STOCK_VALUE_SCREEN_SELL
        else:
            result["action"] = comGD._DEF_STOCK_VALUE_SCREEN_HOLD

        result["scores"] = scores

        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result

#四维信号系统 end

def test():
    # 示例
    # data = getStockBasicInfo()
    # rtn = saveStockBasicInfo(data)
    # data = readStockBasicInfo()
    # YMD = "20260125"
    # fridayYMD = misc.getPreviousFriday(YMD)
    # YMD = "20260123"
    # fridayYMD = misc.getPreviousFriday(YMD)
    # YMD = "20260121"
    # fridayYMD = misc.getPreviousFriday(YMD)

    # symbol = "600989"
    # data = calcBollingerBands(symbol)
    # data = calcTechnicalIndicators(symbol)
    # result = covertTechnicalIndicators2FrankFormat(data)
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
