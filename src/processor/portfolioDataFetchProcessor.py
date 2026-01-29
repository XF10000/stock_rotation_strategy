#! /usr/bin/env python3
#encoding: utf-8

#Filename: portfolioDataFetchProcessor.py
#Author: Steven Lian's team/xie_frank@163.com
#E-mail:  steven.lian@gmail.com  
#Date: 2026-01-12
#Description: regular fetch portfolio data from server

_VERSION="20260125"

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
            #计算股票信息开始和结束日期
            currYMDHMS = misc.getTime()
            currYMD = currYMDHMS[0:8]
            startYMD = misc.getPassday(comGD._DEF_STOCK_KEEP_HISTORY_DATA_DAYS)

            _LOG.info(f"I: 开始检查股票信息是否存在... ")
        
            for stockConfig in stockConfigList:
                symbol = stockConfig["symbol"]
                if symbol == comGD._DEF_STOCK_PORTFOLIO_CASH_NAME:
                    continue
                rtn = comStock.checkReadStockFullData(symbol, startYMD, currYMD)
                if rtn:
                    _LOG.info(f"  - 股票代码:{symbol}, 股票名称:{stockConfig['stock_name']}, 检查结果:存在")
                    result.append(stockConfig)
                else:
                    _LOG.info(f"  - 股票代码:{symbol}, 股票名称:{stockConfig['stock_name']}, 检查结果:不存在")

            _LOG.info(f"I: 开始检查股票信息是否存在... 结束 ")
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#检查股票行业映射文件, 如果不存在或者过期, 则重新生成
def checkStockIndustryMappingFile():
    result = False
    try:
        _LOG.info(f"I: 检查股票映射文件开始... ")
        data = comStock.readStockIndustryMapping()
        metaData = data.get("metadata", {})
        stockBasicInfo = data.get("mapping", {})
        
        #判断时间是否最新
        lastYMD = ""
        if "YMDHMS" in metaData:
            lastYMD = metaData["YMDHMS"][0:8]
        elif "generated_at" in metaData:
            generated_at = metaData["generated_at"]
            lastYMD = generated_at[0:4] + generated_at[5:7] + generated_at[8:10]
        
        updateFlag = False
        currYMDHMS = misc.getTime()
        # currYMD = currYMDHMS[0:8]
        updateYMD = misc.getPassday(comGD._DEF_STOCK_INDUSTRY_MAPING_DAYS)
        if lastYMD == "":
            updateFlag = True
        else:
            if lastYMD < updateYMD:
                updateFlag = True

        if updateFlag:
            stockBasicInfo = comStock.getStockBasicInfo() 
            stockNum = len(stockBasicInfo)
            metaData["YMDHMS"] = currYMDHMS
            metaData["version"] = "1.1"
            metaData["total_stocks"] = stockNum
            metaData["generated_at"] = currYMDHMS[0:4] + "-" + currYMDHMS[4:6] + "-" + currYMDHMS[6:8] + "T" + currYMDHMS[8:10] + ":" + currYMDHMS[10:12] + ":" + currYMDHMS[12:14]
            data["mapping"] = stockBasicInfo
            #计算行业分类
            industryData = {}
            for symbol, stockInfo in stockBasicInfo.items():
                industry_code = stockInfo.get("industry_code", "")
                if industry_code and industry_code not in industryData:
                    industryData[industry_code] = {}
                    industryData[industry_code]["industry_name"] = stockInfo.get("industry_name", "")
                    industryData[industry_code]["industry_type"] = stockInfo.get("industry_type", "")
                    industryData[industry_code]["symbol_list"] = []
                industryData[industry_code]["symbol_list"].append(symbol)
            data["industry_data"] = industryData
            comStock.saveStockIndustryMapping(data)

        stockNum = len(stockBasicInfo)
        _LOG.info(f"I: 检查股票映射文件完成, 股票数量:{stockNum}")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#检查行业数据
def checkIndustryData():
    result = False
    try:
        _LOG.info(f"I: 检查行业数据开始... ")
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
            if version == "1.0":
                industryList = comStock.comAK.swGetIndustryList()
                industryData = {}
                for industrInfo in industryList:
                    industry_symbol = industrInfo.get("industry_symbol", "")
                    industryData[industry_symbol] = {}
                    industryData[industry_symbol]["industry_name"] = industrInfo.get("industry_name", "")
                    industryData[industry_symbol]["industry_type"] = industrInfo.get("industry_type", "")
                    industryData[industry_symbol]["symbol_list"] = []
                industryNum = len(industryData)

            #获取和更新行业数据
            for industry_symbol, symbole_list in industryData.items():
                rtn = comStock.checkReadIndexFullData(industry_symbol, startYMD, currYMD)
                pass

            metaData["industryYMDHMS"] = currYMDHMS
            comStock.saveStockIndustryMapping(data)

            #生成行业波动率分类
            _LOG.info(f"I: 生成行业波动率分类")
            industryRsiThresoldDataList = comStock.generateIndustryVolatilityStratification()
            if industryRsiThresoldDataList:
                comStock.saveSWRSIThresholdData(industryRsiThresoldDataList)
                _LOG.info(f"I: 保存行业波动率分类数据")

        _LOG.info(f"I: 检查行业数据结束, 行业数量:{industryNum}")

        result = updateFlag
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result

#检查股票分红数据
def checkDividendData():
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
        _LOG.info(f"I: 检查股票分红数据结束, 股票数量:{len(dividendData)}")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#读取回测配置文件
def readBacktestSettings(inputBacktestSettingsFileName=""):
    result = {}
    try:
        _LOG.info(f"I: 读取回测配置文件开始... ")

        backtestSettingsList = comStock.readBacktestSetting(inputBacktestSettingsFileName)
        backtestSettings = comStock.convertBacktestSetting2Json(backtestSettingsList)
        comStock.saveBacktestSettingJson(backtestSettings)
        backtestSettings = comStock.readBacktestSettingJson()
        
        _LOG.info(f"I: 读取回测配置文件完成, 配置如下: {backtestSettings}")
        
        if backtestSettings:
            result = backtestSettings
            pass

            _LOG.info(f"I: 读取回测配置文件结束... ")
        pass
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


def dataFetchProcessor(inputPortfolioFileName=""):
    result = {}
    try:
        #首先检查股票映射文件是否存在
        rtn = checkStockIndustryMappingFile()

        #其次检查行业数据是否存在, 行业数据是为计算rsi而准备的
        rtn = checkIndustryData()

        #读取股票配置文件
        stockPortfolio = readStockPortfolioConfig(inputPortfolioFileName)
        result["stockPortfolio"] = stockPortfolio

        #读取股票分红数据文件
        dividendData = checkDividendData()
        result["dividendData"] = dividendData

        #最后读取回测配置文件
        backtestSettings = readBacktestSettings()
        result["backtestSettings"] = backtestSettings
        
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
    inputPortfolioFileName = ""

    for name, value in opts:
        if name in ("-h", "--help"):
            # 打印帮助信息
            print("-d debug")
            sys.exit()

        elif name in ("-d", "--debug"):
            debugFlag = True

        elif name in ("-i", "--input"):
            inputPortfolioFileName = value

    if debugFlag:
        import pdb
        pdb.set_trace()

    _LOG.info(f"I: PID:{_processorPID}, debug:{debugFlag}, portfolio:{inputPortfolioFileName}")

    dataFetchProcessor(inputPortfolioFileName)


if __name__ == "__main__":
    main()


