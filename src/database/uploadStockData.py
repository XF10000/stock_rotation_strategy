#! /usr/bin/env python3
#encoding: utf-8

#Filename: uploadStockData.py
#Author: Steven Lian's team/xie_frank@163.com
#E-mail:  steven.lian@gmail.com  
#Date: 2026-01-12
#Description: regular fetch portfolio data from server

_VERSION="20260201"

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
from common import sqliteCommon as comSqlite

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


#common function begin
sessionID = "ylwz-wwdukrb3peywxiwphlwnpfzutkq0spwr8urmqvoa"
host = "www.iottest.online"

ylwzStockServer = comYlwz.StockServer(host=host, sessionID=sessionID)
#common function end


#上传个人股票数据
def uploadUserStockList(symbol):
    result = 0
    try:
        _LOG.info(f"I: 上传个人股票数据开始... 股票代码:{symbol}")

        saveSet = {}
        saveSet["symbol"] = symbol

        cmd = "userstocklistadd"
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
            _LOG.error(f"E: 上传股票数据失败... 股票代码:{symbol}")
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
                _LOG.error(f"E: 上传股票数据失败... 股票代码:{symbol}")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result

#读取股票配置文件
def uploadStockPortfolioData():
    result = []
    try:
        _LOG.info(f"I: 读取股票配置文件开始... ")

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
                if symbol == comGD._DEF_STOCK_PORTFOLIO_CASH_NAME:
                    continue
                #upload stock_code info
                uploadUserStockList(symbol)
                #upload stock info
                stockInfo,stockData = comStock.readStockData(symbol)
                if stockInfo and stockData:
                    rtn = uploadSingleStockData(symbol,stockData)
                    pass
                    result.append(symbol)

            _LOG.info(f"I: 开始检查股票信息是否存在... 结束 ")
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#检查股票行业映射文件, 如果不存在或者过期, 则重新生成
def uploadStockIndustryMappingFile():
    result = False
    try:
        data = comStock.readStockIndustryMapping()
        metaData = data.get("metadata", {})
        stockBasicInfo = data.get("mapping", {})
        industryData = data.get("industry_data", {})

        _LOG.info(f"I: 上传股票映射文件开始... ")

        #上传股票基本信息
        stockNum = 0
        for symbol, stockInfo in stockBasicInfo.items():
            stockNum += 1
            saveSet = {}
            saveSet["stock_code"] = symbol
            saveSet["stock_name"] = stockInfo.get("stock_name", "")
            saveSet["industry_code"] = stockInfo.get("industry_code", "")
            saveSet["industry_name"] = stockInfo.get("industry_name", "")
            saveSet["industry_name_sw"] = stockInfo.get("industry_name_sw", "")
            saveSet["industry_name_em"] = stockInfo.get("industry_name_em", "")
            cmd = "stockinfoqry"
            querySet = {}
            querySet["stock_code"] = symbol
            rtnData = ylwzStockServer.query(cmd,querySet)
            if rtnData:
                data = rtnData.get("data", {})
                dataList = data.get("data", [])
                if not dataList:
                    cmd = "stockinfoadd"
                    rtnData = ylwzStockServer.query(cmd,saveSet)
                else:
                    currDataSet = dataList[0]
                    industry_code = saveSet.get("industry_code", "")
                    currIndustryCode = currDataSet.get("industry_code", "")
                    if not currIndustryCode:
                        cmd = "stockinfomodify"
                        saveSet = {}
                        saveSet["id"] = currDataSet.get("id", "")
                        saveSet["industry_code"] = industry_code
                        rtnData = ylwzStockServer.query(cmd,saveSet)

        _LOG.info(f"I: 上传股票文件完成, 股票数量:{stockNum}")

        #
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#上传行业数据
def uploadIndustryData():
    result = False
    try:
        _LOG.info(f"I: 上传行业数据开始... ")
        data = comStock.readStockIndustryMapping()
        metaData = data.get("metadata", {})
        version = data.get("version", "")
        industryData = data.get("industry_data", {})
        
        #判断时间是否最新
        lastYMD = ""
        if "industryYMDHMS" in metaData:
            lastYMD = metaData["industryYMDHMS"][0:8]
        
        updateFlag = False
        currYMDHMS = misc.getTime()
        currYMD = currYMDHMS[0:8]
        startYMD = misc.getPassday(comGD._DEF_STOCK_KEEP_HISTORY_DATA_DAYS)

        updateYMD = misc.getPassday(comGD._DEF_STOCK_INDUSTRY_MAPING_DAYS)
        if lastYMD == "":
            updateFlag = True
        else:
            if lastYMD < updateYMD:
                updateFlag = True

        industryNum = len(industryData)
        if updateFlag:
            industryNum = 0
            industryList = comStock.comAK.swGetIndustryList()
            for industryInfo in industryList:
                industryNum += 1
                industry_symbol = industryInfo.get("industry_symbol", "")
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
                    else:
                        currDataSet = dataList[0]
                        industry_code = saveSet.get("industry_code", "")
                        curr_parenet_industry = currDataSet.get("parenet_industry", "")
                        if not curr_parenet_industry:
                            cmd = "industryinfomodify"
                            saveSet["id"] = currDataSet.get("id", "")
                            rtnData = ylwzStockServer.query(cmd,saveSet)

        _LOG.info(f"I: 检查行业数据结束, 行业数量:{industryNum}")

        result = updateFlag
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#检查股票分红数据
def uploadDividendData():
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
def uploadCashFlowStatementsData():
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
def uploadIncomeStatementsData():
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
def uploadBalanceSheetData():
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


#上传股票相关数据
def dataUploader():
    result = {}
    try:
        #首先升级股票基本信息
        # rtn = uploadStockIndustryMappingFile()

        #其次检查行业数据是否存在, 行业数据是为计算rsi而准备的
        # rtn = uploadIndustryData()

        #读取股票配置文件
        stockPortfolio = uploadStockPortfolioData()

        #上传股票分红数据文件
        # dividendData = uploadDividendData()

        #上传股票 balance sheet data
        # balanceSheetData = uploadBalanceSheetData()

        #indicator_medians
        # indicator_medians = uploadIndicatorMediansData()

        #income_statements
        # income_statements = uploadIncomeStatementsData()

        #cash_flow_statements
        # cash_flow_statements = uploadCashFlowStatementsData()
        
        pass
        
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd", ["help", "debug"])
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

    dataUploader()


if __name__ == "__main__":
    main()


