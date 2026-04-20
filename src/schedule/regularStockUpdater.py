#! /usr/bin/env python3
#encoding: utf-8

#Filename: regularStockUpdater.py
#Author: Steven Lian's team/xie_frank@163.com
#E-mail:  steven.lian@gmail.com  
#Date: 2026-01-12
#Description: regular fetch stock data from web and upload to database

_VERSION="20260419"

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
    _LOG = misc.setLogNew("UPLOAD", comGD._DEF_LOG_STOCK_REGULAR_UPDATE_NAME, logDir)

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

def ifTradeDay():
    result = True
    try:
        #首先判断是否是周末
        currWeekDay = misc.weekDay()
        if currWeekDay.wday >= 5: #0-4 周一到周五, 5-6 周六周日
            result = False
        else:
            #其次判断是否是假期
            #http://timor.tech/api/holiday/info/
            currYMD = misc.getTime()[0:8]
            isPublicHoliday = comFC.isPublicHoliday(currYMD)
            if isPublicHoliday:
                result = False
            else:
                #其次判断深交所数据
                result = comStock.ifSzseTradeDay(currYMD)

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#比较股票历史数据日期和交易日期, 找出不在股票历史数据中的交易日期
#就是查找缺失的数据
def compareStockDateWithTradeDate(stockHistoryDataList,tradeDateList):
    compareDateList = []
    try:
        stockTradeDateList = []
        for stockData in stockHistoryDataList:
            stockDate = stockData.get("date")
            stockTradeDateList.append(stockDate)
        for tradeDate in tradeDateList:
            if tradeDate not in stockTradeDateList:
                compareDateList.append(tradeDate)
        compareDateList.sort()
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return compareDateList


#把缺失的日期按照年, 分成多个日期列表
def split2PhaseDateList(dateList,symbol=""):
    fillDateList = []
    try:
        aList = []
        preYear = ""
        for item in dateList:
            YY = item[0:4]
            if YY != preYear:
                preYear = YY
                if aList:
                    phaseStartDate = aList[0]
                    phaseEndDate = aList[-1]
                    aSet = {"startDate":phaseStartDate, "endDate":phaseEndDate}
                    fillDateList.append(aSet)
                aList = []
            aList.append(item)
        if aList:
            addFlag = True
            aList.sort()
            phaseStartDate = aList[0]
            phaseEndDate = aList[-1]
            combinationYMD = phaseStartDate.replace("-","") + "-" + phaseEndDate.replace("-","")
            # 可能是股票停牌, 这个以后再考虑是否需要处理
            # if combinationYMD in ["20250804-20250815","20220426-20220512","20211129-20211210"]:
            #     #20250804-20250815,20220426-20220512 日期范围, 不更新,没有数据
            #     addFlag = False
            # elif phaseStartDate >= phaseEndDate:
            #     addFlag = False
            # else:
            #     pass
           
            if addFlag:
                aSet = {"startDate":phaseStartDate, "endDate":phaseEndDate}
                fillDateList.append(aSet)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return fillDateList


#获取交易日期列表中, 在指定日期范围内的日期
def getTradeDateList(startDate, endDate, tradeDateList):
    result = []
    try:
        for tradeDate in tradeDateList:
            if startDate <= tradeDate and tradeDate <= endDate:
                result.append(tradeDate)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#获取正确的股票日期
def getRightStockDate():
    result = ""
    try:
        currYMDHMS = misc.getTime()
        # currYMD = currYMDHMS[0:8]
        currHMS = currYMDHMS[8:]
        currWeekDay = misc.weekDay()
        passdayNum = 0
        if ifTradeDay():
            #周六,周日 + 节假日
            if currWeekDay.wday == 0: #周一, 取前一个周五
                passdayNum = 3
            elif currWeekDay.wday == 5: #周六, 取前一个周五
                passdayNum = 1
            elif currWeekDay.wday == 6: #周日, 取前一个周五
                passdayNum = 2
            else:
                #其他,不太精准, 就先这样了, 例如春节
                pass               
        else:
            if currWeekDay.wday == 0: 
                if currHMS < "150000":
                    #周一而且时间在15:00之前, 取前一个周五
                    passdayNum = 3
                    pass
            else:
                #周二到周五, 取前一日
                passdayNum = 1
        result = misc.getPassday(passdayNum)
        if result:
            result = result[0:4] + "-" + result[4:6] + "-" + result[6:]
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#增加数据检查日志
def addDataCheckLog(logSet):
    result = 0
    try:
        currHumanTime = misc.getHumanTimeStamp()
        currHumanTime = currHumanTime[0:19]
        currDate = currHumanTime[0:10]
        cmd = "datachecklogadd"
        logSet["report_date"] = currDate
        logSet["start_date"] = currHumanTime
        rtnData = ylwzStockServer.query(cmd,logSet)
        if rtnData:
            data = rtnData.get("data",{})
            recID = data.get("recID",0)
            if int(recID) > 0:
                result = int(recID)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#修改数据检查日志
def modifyDataCheckLog(recID,logSet):
    result = 0
    try:
        currHumanTime = misc.getHumanTimeStamp()
        currHumanTime = currHumanTime[0:19]
        currDate = currHumanTime[0:10]
        cmd = "datachecklogmodify"
        logSet["id"] = recID
        logSet["end_date"] = currHumanTime
        rtnData = ylwzStockServer.query(cmd,logSet)
        if rtnData:
            data = rtnData.get("data",{})
            rtn = data.get("rtn",0)
            if int(rtn) == 0:
                result = 1
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result

#common function end

#检查股票基本信息是否存在, 如果不存在,则自动增加和更新
def updateStockBasicInfo():
    result = 0
    try:
        currWeekDay = misc.weekDay()
        if currWeekDay.wday != 6: #0-4 周一到周五, 5-6 周六周日
            return result

        #只在周日检查股票基本信息是否存在
        misInfoSymbolList = []
        stockInfoDataList = ylwzStockServer.queryStockInfo()
        for stockInfo in stockInfoDataList:
            symbol = stockInfo.get("stock_code","")
            stock_name = stockInfo.get("stock_name","")
            industry_code = stockInfo.get("industry_code","")
            industry_name = stockInfo.get("industry_name","")
            if industry_name and not industry_code:
                #有行业名称, 但是行业代码不存在, 从数据库查询
                industryInfoSet = ylwzStockServer.readIndustryInfo(industryName=industry_name)
                for industry_code, industryInfo in industryInfoSet.items():
                    if industry_name == industryInfo.get("industry_name",""):
                        #找到行业代码,修改股票基本信息
                        saveSet = {}
                        saveSet["industry_code"] = industry_code
                        rtn = ylwzStockServer.modifyStockInfo(symbol,saveSet)
                        if rtn == 0:
                            result = 1
                            _LOG.info(f"I: 股票基本信息更新, 股票代码:{symbol}, 行业代码:{industry_code}, 行业名称:{industry_name}")
                        break

            #检查股票名称和行业代码是否存在
            if not stock_name:
                misInfoSymbolList.append(symbol)
        #如果有缺失的股票基本信息,则自动增加和更新
        if misInfoSymbolList:
            stockBasicInfoSet = comStock.getStockBasicInfo()
            for symbol in misInfoSymbolList:
                stockBasicInfo = stockBasicInfoSet.get(symbol,{})
                if stockBasicInfo:
                    saveSet = {}
                    saveSet["stock_code"] = stockBasicInfo.get("stock_code","")
                    saveSet["stock_name"] = stockBasicInfo.get("stock_name","")
                    saveSet["industry_code"] = stockBasicInfo.get("industry_code","")
                    saveSet["industry_name"] = stockBasicInfo.get("industry_name","")
                    result = ylwzStockServer.modifyStockInfo(symbol,saveSet)
                    if _DEBUG:
                        _LOG.info(f"I: 股票基本信息缺失, 股票代码:{symbol}, 股票名称:{stock_name}, 行业代码:{industry_code}, 行业名称:{industry_name}")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#上传股票数据
def uploadSingleStockData(symbol,dataList,period="day",adjust=""):
    result = 0
    try:
        _LOG.info(f"I: 上传股票数据开始... 股票代码:{symbol}")

        for currStockData in dataList:      
            uploadSuccess = False

            saveSet = {}

            for k,v in currStockData.items():    
                if pd.isna(v):
                    v = 0.0
                    currStockData[k] = v
                if k == "change":
                    k = "price_change"
                saveSet[k] = v

            saveSet["symbol"] = symbol
            saveSet["period"] = period
            saveSet["adjust"] = adjust

            cmd = "stockhistoryadd"
            rtnData = ylwzStockServer.query(cmd,saveSet)
            if rtnData:
                data = rtnData.get("data",[])
                recID = data.get("recID",0)
                if int(recID) > 0:
                    uploadSuccess = True

            if uploadSuccess:
                result += 1
                _LOG.info(f"I: 上传股票数据完成... 股票代码:{symbol}")
            else:
                _LOG.warning(f"W: 上传股票数据失败... 股票代码:{symbol}")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#检查股票是否存在, 如果不存在,则自动增加
def checkSingleStockExist(symbol,stock_name):
    result = 0
    try:
        _LOG.info(f"I: 检查股票是否存在开始... 股票代码:{symbol}, 股票名称:{stock_name}")

        cmd = "stockinfoqry"
        querySet = {"symbol":symbol}
        rtnData = ylwzStockServer.query(cmd,querySet)
        if rtnData:
            data = rtnData.get("data",{})
            dataList = data.get("data",[])
            if dataList:
                result = 1
                _LOG.info(f"I: 检查股票是否存在完成... 股票代码:{symbol}, 股票名称:{stock_name}")
            else:
                _LOG.info(f"I: 检查股票是否不存在... 股票代码:{symbol}, 股票名称:{stock_name}")
                #如果不存在,则自动增加
                cmd = "stockinfoadd"
                if stock_name:
                    addSet = {"symbol":symbol,"stock_name":stock_name}
                else:
                    addSet = {"symbol":symbol}
                rtnData = ylwzStockServer.query(cmd,addSet)
                if rtnData:
                    #检查是否增加成功
                    data = rtnData.get("data",{})
                    recID = data.get("recID",0)
                    if int(recID) > 0:
                        result = 1
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


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


#检查股票数据是否是正确的
def checkStockDataValid(stockDataList):
    result = []
    try:
        for stockData in stockDataList:
            price = float(stockData["high"])
            lastTime = stockData["datetime"]
            if price > 0.0 and lastTime >= "15:30:00":
                result.append(stockData)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#更新股票收盘数据
def updateStockCloseData(rightDate):
    result = 0
    try:
        _LOG.info(f"I: 更新股票收盘数据开始...,请等待,大约2分钟 ")
        rightYMD = misc.humanDate2YMD(rightDate)
        #增加数据检查日志
        logSet = {}
        logSet["check_processor"] = "update_stock_close_data"
        logSet["check_type"] = "close_price"
        logRecID = addDataCheckLog(logSet)

        #首先尝试从tushare里面读取数据
        stockDataList = comStock.comTS.getStockDailyData(rightYMD)
        if not stockDataList:
            #如果从tushare里面读取数据失败, 则从从sina里面读取数据
            tryTimes = 3
            # sleepTime = random.randint(10*60,20*60)
            while tryTimes > 0:
                sleepTime = random.randint(10*60,20*60)
                #读取当日股票信息
                stockDataList = comStock.comAK.sinoGetStockList()
                if stockDataList:
                    if len(stockDataList) > comGD._DEF_STOCK_MIN_DAILY_STOCK_NUM:
                        break
                else:
                    tryTimes -= 1
                    _LOG.warning(f"E: 获取股票列表失败, 等待{sleepTime}秒后重试...")
                    misc.time.sleep(sleepTime)
        
        stockDataList = checkStockDataValid(stockDataList)
        if stockDataList:       
            _LOG.info(f"I: 今天{rightYMD}的股票数据数量:{len(stockDataList)}")
            for stockData in stockDataList:
                symbol = stockData["symbol"]
                stock_name = stockData.get("stock_name","")
                #首先查询系统是否存在该股票, 如果不存在,自动增加
                if stock_name:
                    checkSingleStockExist(symbol,stock_name)
                #其次把股票收盘数据上传到系统
                rtn = updateSingleStockCloseData(symbol,stockData,rightDate)
                result += 1

            if logRecID:
                logSet = {}
                logSet["proc_num"] = result
                modifyDataCheckLog(logRecID,logSet)

            _LOG.info(f"I: 更新股票收盘数据完成, 股票数量:{result}")
        else:
            _LOG.info(f"I: 股票数据不完整, 请检查...")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#上传行业数据
def updateIndustryData():
    result = False
    try:
        _LOG.info(f"I: 更新行业数据开始... ")
        industryNum = 0
        #从股票信息里面读取行业数据
        industryBasicData = ylwzStockServer.readIndustryBasicFromStockInfo()

        #读取ylwz系统里面的行业数据
        industryData = ylwzStockServer.readIndustryInfo()

        #由于stock info 数据更新的比较频繁, 索引这里只需要查询industryData里面是否有缺失
        updateFlag = False
        missedIndustryList = []
        for industryCode, industryInfo in industryBasicData.items():
            if industryCode not in industryData:
                missedIndustryList.append(industryCode)
                updateFlag = True
                _LOG.info(f"I: 行业数据缺失, 行业代码:{industryCode}, 行业名称:{industryInfo.get('industry_name','')}")
               
        if updateFlag:
            industryList = comStock.comAK.swGetIndustryList()
            for industryInfo in industryList:
                industryCode = industryInfo.get("industry_symbol", "")
                if industryCode not in missedIndustryList:
                    continue
                industryFullCode = industryInfo.get("industry_code", "")
                industry_name = industryInfo.get("industry_name", "")
                parenet_industry = industryInfo.get("parenet_industry", "")
                num_of_constituents = industryInfo.get("num_of_constituents", 0)
                static_PE_ratio = industryInfo.get("static_PE_ratio", 0.0)
                TTM_PE_ratio = industryInfo.get("TTM_PE_ratio", 0.0)
                PB_ratio = industryInfo.get("PB_ratio", 0.0)
                static_divident_yield = industryInfo.get("static_divident_yield", 0.0)

                saveSet = {}
                saveSet["industry_code"] = industry_symbol
                saveSet["industry_name"] = industry_name
                saveSet["parenet_industry"] = parenet_industry
                saveSet["num_of_constituents"] = num_of_constituents
                saveSet["static_PE_ratio"] = static_PE_ratio
                saveSet["TTM_PE_ratio"] = TTM_PE_ratio
                saveSet["PB_ratio"] = PB_ratio
                saveSet["static_divident_yield"] = static_divident_yield

                cmd = "industryinfoqry"
                querySet = {}
                querySet["industry_code"] = industry_symbol
                rtnData = ylwzStockServer.query(cmd,querySet)
                if rtnData:
                    data = rtnData.get("data", {})
                    dataList = data.get("data", [])
                    if not dataList:
                        cmd = "industryinfoadd"
                        rtnData = ylwzStockServer.query(cmd,saveSet)
                        if rtnData:
                            industryNum += 1
                            _LOG.info(f"I: 行业数据新增, 行业代码:{industry_code}, 行业名称:{industry_name}")
                    else:
                        currDataSet = dataList[0]
                        industry_code = saveSet.get("industry_code", "")
                        curr_parenet_industry = currDataSet.get("parenet_industry", "")
                        if not curr_parenet_industry:
                            cmd = "industryinfomodify"
                            saveSet["id"] = currDataSet.get("id", "")
                            rtnData = ylwzStockServer.query(cmd,saveSet)
                            if rtnData:
                                industryNum += 1
                                _LOG.info(f"I: 行业数据更新, 行业代码:{industry_code}, 行业名称:{industry_name}")

        _LOG.info(f"I: 更新行业数据结束, 行业数量:{industryNum}")

        result = updateFlag
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#检查股票分红数据
def updateDividendData():
    result = {}
    try:
        _LOG.info(f"I: 检查股票分红数据开始... ")

        dividendFileInfo = comStock.getDividendFileInfo()
        if dividendFileInfo:
            fileSize = dividendFileInfo.get("fileSize", 0)
            fileModTime = dividendFileInfo.get("fileModTime", 0)
            currTime = misc.time.time()
            if (currTime - fileModTime ) > (comGD._DEF_STOCK_DIVIDEND_DATA_DAYS * 24 * 60 * 60):
                dividendData = comStock.getDividendData()
            else:
                dividendData = comStock.readDividendData()
            if dividendData:
                result = dividendData
        else:
            dividendData = comStock.getDividendData()
            if dividendData:
                comStock.saveDividendData(dividendData)
                result = dividendData

        #upload to server
        for symbol, dividendInfoList in dividendData.items():
            cmd = "stockdividendqry"
            querySet = {}
            querySet["stock_code"] = symbol
            rtnData = ylwzStockServer.query(cmd,querySet)
            if rtnData:
                data = rtnData.get("data", {})
                dataList = data.get("data", [])
                if not dataList: #stock dividend data not exist,
                    cmd = "stockdividendadd"
                    for dividendInfo in dividendInfoList:
                        dividendInfo["stock_code"] = symbol
                        rtnData = ylwzStockServer.query(cmd,dividendInfo)

        _LOG.info(f"I: 检查股票分红数据结束, 股票数量:{len(dividendData)}")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#数据来源于sqllite
def updateCashFlowStatementsData():
    result = False
    try:
        #读取sqllite数据库中的cash flow statements数据
        currRecID = 0
        while True:
            cashFlowStatementsDataList = comSqlite.query_cash_flow_statements(start_id = currRecID,limitNum=0)
            if cashFlowStatementsDataList:
                for cashFlowStatementsData in cashFlowStatementsDataList:
                    id = cashFlowStatementsData.get("id", 0)
                    try:
                        id = int(id)
                    except:
                        id = 0
                        pass
                    if id > currRecID:
                        currRecID = id
                    stock_code = cashFlowStatementsData.get("stock_code", "")
                    report_date = cashFlowStatementsData.get("report_date", "")
                    if not stock_code:
                        continue
                    cmd = "cashflowqry"
                    querySet = {}
                    querySet["stock_code"] = stock_code
                    querySet["report_date"] = report_date
                    rtnData = ylwzStockServer.query(cmd,querySet)
                    if rtnData:
                        data = rtnData.get("data", {})
                        dataList = data.get("data", [])
                        if not dataList: #cash flow statements data not exist,
                            cmd = "cashflowadd"
                            rtnData = ylwzStockServer.query(cmd,cashFlowStatementsData)
            result = True
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#数据来源于sqllite
def updateIncomeStatementsData():
    result = False
    try:
        #读取sqllite数据库中的income statements数据
        currRecID = 0
        while True:
            incomeStatementsDataList = comSqlite.query_income_statements(start_id = currRecID,limitNum=0)
            if incomeStatementsDataList:
                for incomeStatementsData in incomeStatementsDataList:
                    id = incomeStatementsData.get("id", 0)
                    try:
                        id = int(id)
                    except:
                        id = 0
                        pass
                    if id > currRecID:
                        currRecID = id
                    stock_code = incomeStatementsData.get("stock_code", "")
                    report_date = incomeStatementsData.get("report_date", "")
                    if not stock_code:
                        continue
                    cmd = "incomestatementsqry"
                    querySet = {}
                    querySet["stock_code"] = stock_code
                    querySet["report_date"] = report_date
                    rtnData = ylwzStockServer.query(cmd,querySet)
                    if rtnData:
                        data = rtnData.get("data", {})
                        dataList = data.get("data", [])
                        if not dataList: #income statements data not exist,
                            cmd = "incomestatementsadd"
                            rtnData = ylwzStockServer.query(cmd,incomeStatementsData)
            result = True
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#数据来源于sqllite
def updateBalanceSheetData():
    result = False
    try:
        #读取sqllite数据库中的balance sheet数据
        currRecID = 0
        while True:
            balanceSheetDataList = comSqlite.query_balance_sheet(start_id = currRecID,limitNum=0)
            if balanceSheetDataList:
                for balanceData in balanceSheetDataList:
                    id = balanceData.get("id", 0)
                    try:
                        id = int(id)
                    except:
                        id = 0
                        pass
                    if id > currRecID:
                        currRecID = id
                    stock_code = balanceData.get("stock_code", "")
                    report_date = balanceData.get("report_date", "")
                    if not stock_code:
                        continue
                    cmd = "balancesheetqry"
                    querySet = {}
                    querySet["stock_code"] = stock_code
                    querySet["report_date"] = report_date
                    rtnData = ylwzStockServer.query(cmd,querySet)
                    if rtnData:
                        data = rtnData.get("data", {})
                        dataList = data.get("data", [])
                        if not dataList: #stock balance sheet data not exist,
                            cmd = "balancesheetadd"
                            rtnData = ylwzStockServer.query(cmd,balanceData)
            result = True
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#数据来源于sqllite
def uploadIndicatorMediansData():
    result = False
    try:
        #读取sqllite数据库中的indicator medians数据
        currRecID = 0
        while True:
            indicatorMediansDataList = comSqlite.query_indicator_medians(start_id = currRecID,limitNum=0)
            if indicatorMediansDataList:
                for indicatorMediansData in indicatorMediansDataList:
                    id = indicatorMediansData.get("id", 0)
                    try:
                        id = int(id)
                    except:
                        id = 0
                        pass
                    if id > currRecID:
                        currRecID = id
                    indicator_name = indicatorMediansData.get("indicator_name", "")
                    report_date = indicatorMediansData.get("report_date", "")
                    if not indicator_name:
                        continue
                    cmd = "indicatorqry"
                    querySet = {}
                    querySet["indicator_name"] = indicator_name
                    querySet["report_date"] = report_date
                    rtnData = ylwzStockServer.query(cmd,querySet)
                    if rtnData:
                        data = rtnData.get("data", {})
                        dataList = data.get("data", [])
                        if not dataList: #stock balance sheet data not exist,
                            cmd = "indicatoradd"
                            rtnData = ylwzStockServer.query(cmd,indicatorMediansData)
            result = True
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#上传股票交易日数据
def updateTradeDayInfo(tradeDay):
    result = 0
    try:
        uploadSuccess = False
        saveSet={"trade_day":tradeDay }
        cmd = "tradedayqry"
        querySet = {"tradeDay":tradeDay}
        rtnData = ylwzStockServer.query(cmd,querySet)
        if rtnData:
            data = rtnData.get("data", {})
            dataList = data.get("data", [])
            #如果交易日数据不存在,则上传
            if not dataList:
                cmd = "tradedayadd"
                rtnData = ylwzStockServer.query(cmd,saveSet)
                if rtnData:
                    data = rtnData.get("data",[])
                    recID = data.get("recID",0)
                    if int(recID) > 0:
                        uploadSuccess = True

                if uploadSuccess:
                    _LOG.info(f"I: 上传股票交易日数据完成... 交易日:{tradeDay},recID:{recID}")
                    result = 1
                else:
                    _LOG.warning(f"E: 上传股票交易日数据失败... 交易日:{tradeDay}")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#更新一个申万指数的历史行情
# period = "day", "week", "month" #日k线、周k线、月k线
def updateOneIndustryHistoryData(symbol,period="day"):
    result = 0
    try:
        #首先获取当前的历史行情更新情况
        dataType = "industry"
        maxminData = ylwzStockServer.getMaxMinData(dataType,period=period,industry_code=symbol)
        lastDate = maxminData.get("max_data","")
        if lastDate:
            lastYMD = misc.humanDate2YMD(lastDate)
        else:
            passDays = comGD._DEF_STOCK_KEEP_HISTORY_DATA_DAYS
            lastYMD = misc.getPassday(passDays)
    
        if _DEBUG:
            _LOG.info(f"  - 申万指数代码:{symbol}, 技术指标:{dataType}, 最后一次更新日期:{lastDate}")

        startYMD = lastYMD
        endYMD = misc.getTime()[0:8]
        indexHistoryDataList = comStock.getHistoryIndexData(symbol,period=period,startYMD=startYMD,endYMD=endYMD)
        if indexHistoryDataList:
            #上传行业历史数据(申银万国)
            for data in indexHistoryDataList:
                recID = ylwzStockServer.addIndustryHistoryData(symbol,data,period=period)
                if recID > 0:
                    _LOG.info(f"I: 上传行业历史数据完成... 申万指数代码:{symbol},周期:{period},日期:{data.get('date','')},recID:{recID}")
                    result += 1
                else:
                    _LOG.warning(f"E: 上传行业历史数据失败... 申万指数代码:{symbol},周期:{period},日期:{data.get('date','')}")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#上传股票行业当日数据
def updateStockIndustryHistoryData(YMD):
    result = 0
    try:
        currWeekDay = misc.weekDay()
        if currWeekDay.wday != 5: #0-4 周一到周五, 5-6 周六周日
            return result

        _LOG.info(f"I: 只在周六更新股票行业历史数据(申万数据)")
        #首先获取申万指数代码
        industrySymbolDataSet = ylwzStockServer.readIndustryInfo()
        for industrySymbol, item in industrySymbolDataSet.items():
            industry_name = item.get("industry_name", "")
            for period in ["day","week","month"]:
                _LOG.info(f"I: 申万指数代码:{industrySymbol},行业名称:{industry_name},周期:{period}")
                rtn = updateOneIndustryHistoryData(industrySymbol,period=period)
                result += rtn

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#这里是更新在交易日的数据
def tradeDayDataUpdater(rightDate):
    result = 0
    try:
        if not ifTradeDay(): #非交易日, 不更新股票数据,第一次总是出错
            _LOG.info(f"I: 非交易日, 不更新股票数据,第一次")
            return result

        #判断是否是交易日
        if not ifTradeDay():
            _LOG.info(f"I: 非交易日, 不更新股票数据,第二次")
            return result

        _LOG.info(f"I: 交易日, 准备更新股票数据")
        rtn = updateTradeDayInfo(rightDate)

        #收盘时间,才更新数据的业务 
        _LOG.info(f"I: 收盘时间, 需要更新股票数据的业务 ...{rightDate}... ")

        #获取当日收盘数据
        rtn = updateStockCloseData(rightDate)
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


def filterTechnicalIndicators(indicators,oldIndicators):
    """过滤技术指标, 只保留需要上传的指标"""
    result = []
    try:
        #过滤出需要上传的技术指标
        oldIndicatorsSet = {}
        for item in oldIndicators:
            date = item.get("date", "")
            stock_code = item.get("stock_code", "")
            if not date or not stock_code:
                continue
            oldIndicatorsSet[f"{stock_code}_{date}"] = item
        #开始过滤指标
        for indicator in indicators:
            date = indicator.get("date", "")
            stock_code = indicator.get("stock_code", "")
            if not date or not stock_code:
                continue
            if f"{stock_code}_{date}" not in oldIndicatorsSet:
                result.append(indicator)
            else:
                #补充缺失指标数据,临时代码 need to remove or modify
                #如果指标存在, 则判断是否需要更新, 只有macd_line_long,macd_signal,macd_histogram 有变化, 才需要更新
                if False:
                # if True:
                    oldItem = stockCodeDateSet[f"{stock_code}_{date}"]
                    if oldItem.get("macd_line_long",0) != indicator.get("macd_line_long",0) or oldItem.get("ma_60",0) != indicator.get("ma_60",0):
                        result.append(indicator)
                pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#更新某个股票的技术指标
def updateOneTechnicalIndicators(symbol,period,adjust,rightDate):
    result = 0
    try:
        _LOG.info(f" - 更新股票: {symbol} 的技术指标...周期:{period},调整:{adjust},日期:{rightDate}")
        #查询股票的当前技术指标
        currIndicators = ylwzStockServer.readTechnicalIndicators(symbol,period=period,adjust=adjust)
        if currIndicators:
            #如果技术指标存在, 则增量更新
            pass
        else:
            pass
        if True:
            #目前数据不多, 暂时全部在本地计算
            #第一次上传技术指标
            #获取股票历史数据
            startDate = settings.STOCK_TECHNICAL_INDICATORS_START_DATE
            endDate = rightDate
            stockHistoryDataList = ylwzStockServer.queryStockData(symbol=symbol,startDate=startDate,endDate=rightDate,period=period,adjust=adjust)
            if not stockHistoryDataList or len(stockHistoryDataList) == 0:
                _LOG.warning(f"W: 获取股票历史数据失败... 股票:{symbol},周期:{period},调整:{adjust}")
                return result
            #计算技术指标
            ti = comTI.StockTI()
            indicators = ti.calculateTechnicalIndicators(stockHistoryDataList)
            if not indicators or len(indicators) == 0:
                _LOG.warning(f"W: 计算股票技术指标失败... 股票:{symbol},周期:{period},调整:{adjust}")
                return result
            #上传技术指标
            newIndicators = filterTechnicalIndicators(indicators,currIndicators)
            #上传技术指标
            for indicator in newIndicators:
                recID = ylwzStockServer.addTechnicalIndicator(symbol=symbol,dataSet=indicator,period=period,adjust=adjust)
                if recID:
                    result += 1
                    _LOG.info(f"I: 上传股票技术指标成功... 股票:{symbol},周期:{period},调整:{adjust},recID:{recID}")

        _LOG.info(f"I: 更新股票技术指标... 股票:{symbol},周期:{period},调整:{adjust},日期:{rightDate},新增指标数:{result}")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#计算技术指标
def updateTechnicalIndicators(currYMD):
    result = 0
    try:
        #获取所有用户股票列表
        allUserStockList = ylwzStockServer.getUniqueUserStockList()
        for symbol in allUserStockList:
            if symbol == comGD._DEF_STOCK_PORTFOLIO_CASH_NAME:
                #不更新现金(特殊类型)
                continue
            for period in ["day","week"]: #不更新月数据,只更新日数据和周数据
                if period == "week":
                    currWeekDay = misc.weekDay()
                    if currWeekDay.wday < 5: #0-4 周一到周五,不更新week 数据
                        _LOG.info(f"I: 非周五, 不更新股票技术指标周数据... 股票:{symbol},周期:{period},调整:{adjust}")
                        continue
                for adjust in ["","hfq","qfq"]:
                    rtn = updateOneTechnicalIndicators(symbol,period,adjust,currYMD)
                    result += rtn
                    pass
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#更新股票日数据,补齐历史数据
def updateDayStockFullData(symbol,updateStartDate,updateEndDate,tradeDateList):
    result = False
    try:
        _LOG.info(f"I: 更新股票日数据  - 股票代码:{symbol}")

        # if updateStartDate < updateEndDate:
            #获取交易日期列表中, 在指定日期范围内的日期
            # newTradeDataList = getTradeDateList(updateStartDate, updateEndDate,tradeDateList)
            
        #获取股票历史数据,日数据
        period = "day"
        adjustList = ["","qfq","hfq"]
        for adjust in adjustList:
            _LOG.info(f"I: 查询股票历史数据  - 股票代码:{symbol}, 调整方式:{adjust}, 开始日期:{updateStartDate}, 结束日期:{updateEndDate}")
            #day 数据更新判断方式
            stockHistoryDataList = ylwzStockServer.queryStockData(symbol=symbol, startDate=updateStartDate,endDate=updateEndDate,period=period,adjust=adjust,mode="short")
            if not stockHistoryDataList:
                _LOG.warning(f"W: 获取股票历史数据失败... 股票:{symbol},周期:{period},调整:{adjust}")
            # startDate = stockHistoryDataList[0].get("date")
            # if startDate > updateStartDate: #如果股票历史数据开始日期大于更新开始日期, 则需要更新
                # _LOG.info(f"I: - 股票代码:{symbol},现有数据开始日期:{startDate},晚于更新开始日期:{updateStartDate},需要更新股票开始日期")
                # startDate = updateStartDate
            # endDate = stockHistoryDataList[-1].get("date")
            #tradeDataList 就应该是updateStartDate 到 updateEndDate 之间的交易日期列表
            newTradeDataList = getTradeDateList(updateStartDate, updateEndDate,tradeDateList)
            compareDateList = compareStockDateWithTradeDate(stockHistoryDataList,newTradeDataList)

            phaseDateList = split2PhaseDateList(compareDateList,symbol)
            if phaseDateList:
                #有数据缺失, 则需要更新股票历史数据
                for phaseData in phaseDateList:
                    phaseStartDate = phaseData.get("startDate")
                    phaseStartYMD = phaseStartDate.replace("-","")
                    phaseEndDate = phaseData.get("endDate")
                    phaseEndYMD = phaseEndDate.replace("-","")
                    # if phaseStartYMD >= phaseEndYMD:
                    #     continue
               
                    newStockDataList = comStock.getHistoryStockData(symbol, phaseStartYMD, phaseEndYMD,period=period,adjust=adjust)

                    if newStockDataList:
                        #upload new stock data to server
                        for stockData in newStockDataList:
                            recID = ylwzStockServer.addStockData(symbol,stockData,period=period,adjust=adjust)
                            if recID != 0:
                                _LOG.info(f"I: 增加股票历史数据  - 股票代码:{symbol}, 日期类别:{period},调整方式:{adjust}, recID:{recID}")

                        result = True
                    else:
                        _LOG.warning(f"W: 更新股票日数据告警  - 股票代码:{symbol}, 日期类别:{period},调整方式:{adjust}, 开始日期:{phaseStartDate}, 结束日期:{phaseEndDate}, 无数据返回")

        _LOG.info(f"I: 更新股票日数据  - 股票代码:{symbol}, 日数据更新完成")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#更新周数据, 只在周六/周日执行
def updateWeekStockFullData(symbol):
    result = False
    try:
        currWeekDay = misc.weekDay()
        if currWeekDay.wday >= 5: #0-4 周一到周五, 5-6 周六周日
            _LOG.info(f"I: 更新股票周数据  - 股票代码:{symbol}")
            currYMDHMS = misc.getTime()
            currYMD = currYMDHMS[0:8]
            lastFriday = misc.getPreviousFriday(currYMD)
            # lastSatDay = misc.getPassday(-1,lastFriday)

            period = "week"
            adjustList = ["","qfq","hfq"]
            for adjust in adjustList:
                #首先获取当前股票周数据
                currWeekData = ylwzStockServer.queryStockData(symbol, startDate="", endDate="",period=period,adjust=adjust,mode="short")
                if currWeekData:
                    #获取当前股票周数据的最大日期
                    weekFirstDate = currWeekData[-1].get("date")
                    weekFirstYMD = weekFirstDate.replace("-","")
                else:
                    weekFirstYMD = misc.getPassday(comGD._DEF_STOCK_KEEP_HISTORY_DATA_DAYS)
                weekLastYMD = lastFriday.replace("-","")
                if weekFirstYMD >= weekLastYMD:
                    continue
                #获取日数据
                weekFirstDate = misc.YMD2HumanDate(weekFirstYMD)
                weekLastDate = misc.YMD2HumanDate(weekLastYMD)
                currDayData = ylwzStockServer.queryStockData(symbol=symbol, startDate=weekFirstDate,endDate=weekLastDate,period="day",adjust=adjust)
                df = pd.DataFrame(currDayData)
                df = comStock.convertDaily2Weekly(df)
                newWeekData = df.to_dict(orient="records")
                for item in newWeekData:
                    item["date"] = item["date"].strftime('%Y-%m-%d')
                if newWeekData:
                    #upload new stock data to server
                    for stockData in newWeekData:
                        recID = ylwzStockServer.addStockData(symbol,stockData,period=period,adjust=adjust)
                        if recID != 0:
                            _LOG.info(f"I: 增加股票历史数据  - 股票代码:{symbol}, 日期类别:{period},调整方式:{adjust}, recID:{recID}")
                            result = True
            _LOG.info(f"I: 更新股票周数据  - 股票代码:{symbol}, 周数据更新完成")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#检查并更新股票历史数据
# def checkReadStockFullData(symbol,stockConfig, tradeDateList):
def checkReadStockFullData(symbol, tradeDateList):
    result = False
    try:
        #这个是默认的股票历史数据更新结束日期, 超过这个日期, 都是每天更新
        # historyDefaultEndDate = settings.STOCK_HISTORY_DAY_UPDATE_END_DATE
        #获取交易日期列表开始和结束日期
        tradeStartDate = tradeDateList[0]
        tradeEndDate = tradeDateList[-1]
        currYMDHMS = misc.getTime()
        currYMD = currYMDHMS[0:8]
        currDate = currYMD[0:4] + "-" + currYMD[4:6] + "-" + currYMD[6:8]
        if tradeEndDate >= currDate:
            tradeEndDate = currDate

        # #获取股票配置开始和结束日期
        # historyUpdateFlag = stockConfig.get("history_update")
        # startDate = stockConfig.get("history_start_date","")
        # endDate = stockConfig.get("history_end_date","")
        # if historyUpdateFlag == comGD._CONST_YES:
        #     # updateStartDate = endDate
        #     updateStartDate = startDate
        #     updateEndDate = tradeEndDate
        # else:
        #     updateStartDate = tradeStartDate
        #     updateEndDate = tradeEndDate
        updateStartDate = tradeStartDate
        updateEndDate = tradeEndDate

        updateDataFlag = updateDayStockFullData(symbol,updateStartDate,updateEndDate,tradeDateList)

        # if updateDataFlag:            
        #     #更新股票配置
        #     queryData = {}
        #     queryData["id"] = stockConfig.get("id","")
        #     queryData["history_update"] = comGD._CONST_YES
        #     queryData["history_start_date"] = updateStartDate
        #     queryData["history_end_date"] = updateEndDate
        #     cmd = "userstocklistmodify"
        #     rtnData = ylwzStockServer.query(cmd,stockConfig)
        #     if rtnData and "data" in rtnData:
        #         data = rtnData["data"]
        #         if data.get("errCode") == "B0":
        #             result = True

        #周六或者周日,更新周数据
        updateDataFlag = updateWeekStockFullData(symbol)

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#根据用户股票列表, 更股票历史数据
def updateStockHistoryData(currYMD):
    result = 0
    try:
        _LOG.info(f"I: 读取股票配置数据开始... ")

        allUserStockList = ylwzStockServer.getUniqueUserStockList()

        _LOG.info(f"I: 读取股票配置数据完成, 股票数量:{len(allUserStockList)}")
        
        if allUserStockList:
            #计算股票信息开始和结束日期
            startYMD = misc.getPassday(comGD._DEF_STOCK_KEEP_HISTORY_DATA_DAYS)

            startDate = startYMD[0:4] + "-" + startYMD[4:6] + "-" + startYMD[6:8]
            endDate = currYMD[0:4] + "-" + currYMD[4:6] + "-" + currYMD[6:8]

            _LOG.info(f"I: 开始检查股票信息是否存在... ")
            #获取股票交易日数据
            stockTradeDateList = ylwzStockServer.readStockTradeDateList(startYMD, currYMD)
            tradeDateList = ylwzStockServer.convertTradeDate2Set(stockTradeDateList)
        
            for symbol in allUserStockList:
                if symbol == comGD._DEF_STOCK_PORTFOLIO_CASH_NAME:
                    continue
                # #获取股票配置数据
                # stockConfigList = ylwzStockServer.readUserStockList(symbol=symbol)
                # if not stockConfigList:
                #     continue
                # stockConfig = stockConfigList[0]
                # rtn = checkReadStockFullData(symbol,stockConfig,tradeDateList)
                rtn = checkReadStockFullData(symbol,tradeDateList)
                if rtn:
                    _LOG.info(f"  - 股票代码:{symbol}, 检查结果:存在")
                    result += 1
                else:
                    _LOG.info(f"  - 股票代码:{symbol}, 检查结果:不存在")

            _LOG.info(f"I: 开始检查股票信息是否存在... 结束 ")
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#计算某个股票的技术指标信号
def calcOneTechnicalSignal(symbol,period,adjust):
    result = {}
    try:
        _LOG.info(f" - 计算股票: {symbol} 的技术信号(雷达指标),周期:{period},调整:{adjust}")
        currHistoryIndicators = ylwzStockServer.readHistoryTechnicalIndicators(symbol=symbol,period=period,adjust=adjust)
        if currHistoryIndicators:
            ts = comTS.StockTS()
            result = ts.calcTechnicalSignals(symbol,currHistoryIndicators)
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#上传股票技术指标信号
def uploadOneStockTechnicalSignal(symbol,techSignal,period,adjust):
    result = 0
    try:
        _LOG.info(f" - 上传股票: {symbol} 的技术信号(雷达指标),周期:{period},调整:{adjust}")
        dataType = "signal"
        for signalType, signal in techSignal.items():
            currIndicator = comTS.getIndicatorName(signalType)
            if not currIndicator:
                continue
            maxminData = ylwzStockServer.getMaxMinData(dataType,period=period,adjust=adjust,indicator=currIndicator,symbol=symbol)
            # min_date = maxminData.get("min_data","")
            lastDate = maxminData.get("max_data","")
            if _DEBUG:
                _LOG.info(f"  - 股票代码:{symbol}, 技术指标:{signalType}, 最后一次更新指标:{currIndicator}, 最后一次更新日期:{lastDate}")
           
            finalSignals = signal.get("finalSignals",{})
            for YMD, item in finalSignals.items():
                date = misc.YMD2HumanDate(YMD)
                if date <= lastDate:
                    continue
                saveSet = {}
                saveSet["date"] = date
                saveSet["period"] = period
                saveSet["adjust"] = adjust
                saveSet["action"] = item.get("suggestion","")
                saveSet["subtype"] = item.get("subtype","")
                saveSet["indicator"] = item.get("indicator","")
                saveSet["description"] = item.get("description","")
                saveSet["market_trend"] = item.get("marketTrend","")
                saveSet["calc_result"] = item.get("detail",{})
                recID = ylwzStockServer.addTechnicalSignal(symbol,saveSet)
                if recID:
                    result += 1
                    _LOG.info(f"  - 上传股票: {symbol} 的技术信号(雷达指标),{YMD} 成功,recID:{recID}")
                pass
        _LOG.info(f" - 上传股票: {symbol} 的技术信号(雷达指标),周期:{period},调整:{adjust},成功数量:{result}")        
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
                # for adjust in ["","hfq","qfq"]:
                for adjust in ["qfq"]: #技术信号用qfq调整(前复权)
                    techSignal = calcOneTechnicalSignal(symbol,period,adjust)
                    #更新股票配置
                    rtn = uploadOneStockTechnicalSignal(symbol,techSignal,period,adjust)
                    pass
            
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#全部时间都可以检查和更新的数据
def normalDataUpdate(currYMD):
    result = 0
    try:
        #更新股票基本信息
        rtn = updateStockBasicInfo()

        # 根据用户股票列表, 更股票历史数据
        rtn = updateStockHistoryData(currYMD)

        # 计算技术指标
        rtn = updateTechnicalIndicators(currYMD)

        #获取行业数据(每周更新一次)
        rtn = updateStockIndustryHistoryData(currYMD)

        #检查行业数据是否存在
        # rtn = updateIndustryData()

        #更新股票分红数据文件
        # dividendData = updateDividendData()

        #更新股票 balance sheet data
        # balanceSheetData = updateBalanceSheetData()

        #更新 income_statements
        # income_statements = updateIncomeStatementsData()

        #更新 cash_flow_statements
        # cash_flow_statements = updateCashFlowStatementsData()
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#上传股票相关数据
def regularDataUpdater():
    result = {}
    try:
        rightDate  = getRightStockDate()

        #如果时间不是收盘时间,则不更新
        if rightDate:
            #更新交易日数据
            _LOG.info(f"I: 更新交易日数据 ...{rightDate}... ")
            rtn = tradeDayDataUpdater(rightDate)
            _LOG.info(f"I: 更新交易日数据 ...{rightDate}...结束 ")

        _LOG.info(f"I: 其他数据的计算和更新 ... ")
        currYMD = misc.getTime()[0:8]
        rtn = normalDataUpdate(currYMD)   
        _LOG.info(f"I: 其他数据的计算和更新 ...结束 ")

        _LOG.info(f"I: 计算技术指标信号 ... ")
        rtn = calcTechnicalSignals()
        _LOG.info(f"I: 计算技术指标信号 ...结束 ")

        pass
        
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


