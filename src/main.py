#! /usr/bin/env python3
#encoding: utf-8

#Filename: main.py
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com/xie_frank@163.com
#Date: 2026-01-12
#Description: 主程序

_VERSION="20260127"

_DEBUG=True

import os
import sys

import config
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

# from common import divergence as comDiv
from processor import portfolioDataFetchProcessor as comFetch

#setting files
from config import basicSettings as settings


_processorPID = os.getpid()

_DEBUG = True
_HOME_DIR =  settings._HOME_DIR

if "_LOG" not in dir() or not _LOG:
    logDir = os.path.join(_HOME_DIR, "log")
    _LOG = misc.setLogNew("MAIN", comGD._DEF_LOG_STOCK_MAIN_LOG_NAME, logDir,stdout=True)

systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
_LOG.info(f"PID:{_processorPID}, python version:{systemVersion}, main code version:{_VERSION}")


#设置股票数据获取器的日志
comFetch._LOG = _LOG
#设置股票common的日志
comStock._LOG = _LOG

#common function begin

#common function end

def fetchStockData(inputPortfolioFileName):
    result = {}
    try:
        #首先读取股票配置文件
        _LOG.info(f"1: 检查配置文件,并获取相关股票参数... ")
        currConfig = comFetch.dataFetchProcessor(inputPortfolioFileName)
        if currConfig:
            stockNum = len(currConfig.get("stockPortfolio",[]))
            result = currConfig
            _LOG.info(f"I: 数据准备完毕, 股票数量:{stockNum}")
        else:
            _LOG.error(f"E: 数据准备失败, 股票数量:0")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#准备回测数据
def prepareBacktestData(configData):
    result = {}
    try:
        #准备回测数据
        _LOG.info(f"2: 准备回测数据开始,... ")

        #读取股票行业映射文件
        stockIndustryMapping = comStock.readStockIndustryMapping()
        mapping = stockIndustryMapping.get("mapping", {})

        #获取股票配置文件中的股票列表
        stockPortfolio = configData.get("stockPortfolio",[])

        #保存股票技术指标, 并计算技术指标, 
        stockIndicators = {}
        count = 0 #股票计数器
        for stockInfo in stockPortfolio:
            symbol = stockInfo.get("symbol", "")
            count += 1
            #获取股票信息
            stockInfo = mapping.get(symbol, {})
            stockName = stockInfo.get("stock_name", "")
            industryName = stockInfo.get("industry_name", "")
            techIndicators = comStock.calcTechnicalIndicators(symbol)
            stockIndicators[symbol] = techIndicators
            _LOG.info(f"  - {count}. 股票代码:{symbol}, 股票名称:{stockName}, 所属行业:{industryName}, 技术指标数量:{len(techIndicators)}")
            pass
        
        #读取行业数据
        industryRSIData = comStock.readSWRSIThresholdData()

        #读取最近的 daily 数据,并保存最近的10天数据
        stockDailyData = {}
        passedYMD = misc.getPassday(10)
        for stockInfo in stockPortfolio:
            symbol = stockInfo.get("symbol", "")
            period = "daily"
            stockInfo,stockDataList = comStock.readStockData(symbol,period,startYMD=passedYMD)
            if stockDataList:
                stockDailyData[symbol] = stockDataList

        #读取最近的周数据,并保存最近的30周数据
        stockWeeklyData = {}
        passedYMD = misc.getPassday(7*30)
        for stockInfo in stockPortfolio:
            symbol = stockInfo.get("symbol", "")
            period = "weekly"
            stockInfo,stockDataList = comStock.readStockData(symbol,period,startYMD=passedYMD)
            if stockDataList:
                stockWeeklyData[symbol] = stockDataList

        #计算背离数据
        divergenceData = {}
        for stockInfo in stockPortfolio:
            symbol = stockInfo.get("symbol", "")
            techIndicators = stockIndicators.get(symbol,[])
            # symbolWeeklyData = stockWeeklyData.get(symbol,[])
            symbolRSIDivergenceData = comStock.calcRSIDivergence(techIndicators)
            symbolMacdDivergenceData = comStock.calcMacdDivergence(techIndicators,)
            divergenceData[symbol] = {}
            divergenceData[symbol]["rsi"] = symbolRSIDivergenceData
            divergenceData[symbol]["macd"] = symbolMacdDivergenceData

        result["stockIndicators"] = stockIndicators
        result["industryMapping"] = mapping
        result["industryRSIData"] = industryRSIData
        result["stockDailyData"] = stockDailyData
        result["stockWeeklyData"] = stockWeeklyData
        result["divergenceData"] = divergenceData

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#计算回测数据的四维度数据
def calcFourFactorData(backtestData):
    result = {}
    try:
        #计算回测数据的四维度数据
        _LOG.info(f"3: 计算回测数据的四维度数据开始,... ")

        stockPortfolio = backtestData.get("stockPortfolio",[])
        stockDailyData = backtestData.get("stockDailyData",{})
        stockWeeklyData = backtestData.get("stockWeeklyData",{})
        divergenceData = backtestData.get("divergenceData",{})
        stockIndicators = backtestData.get("stockIndicators",{})
        dividendData = backtestData.get("dividendData",{})
        backtestSettings = backtestData.get("backtestSettings",{})

        #首先计算 维度一：价值准入过滤器 结果
        valueResult = {}
        for stockInfo in stockPortfolio:
            symbol = stockInfo.get("symbol", "")
            stockDataList = stockDailyData.get(symbol,[])
            stockIndicatorList = stockIndicators.get(symbol,[])
            dcfValue = stockInfo.get("DCF_value_per_share",0.0)
            if stockDataList:
                currStockPrice = stockDataList[-1].get("close",0.0)
                valueResult = comStock.valueInvestingScreener(symbol,currStockPrice,dcfValue,stockIndicatorList,backtestSettings)
                valueAction = valueResult.get("action", comGD._DEF_STOCK_VALUE_SCREEN_HOLD)

                #action == hold 时, 考虑其他过滤器,三选二. 暂时都计算其他过滤器, 后续再根据需要筛选
                otherResult = comStock.twoOutOfThreeFactors(symbol,backtestData)
                otherAction = otherResult.get("action", comGD._DEF_STOCK_VALUE_SCREEN_HOLD)

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#主处理函数
def mainProcessor(inputPortfolioFileName=""):
    result = {}
    try:
        #开始处理
        _LOG.info(f"I: 中线轮动策略系统开始处理... ")

        configData = fetchStockData(inputPortfolioFileName)
        if configData:
            backtestData = prepareBacktestData(configData)

            if backtestData:
                #合并股票配置文件中的股票列表和回测数据中的股票列表
                backtestData["stockPortfolio"] = configData.get("stockPortfolio",[])
                backtestData["dividendData"] = configData.get("dividendData", {})
                backtestData["backtestSettings"] = configData.get("backtestSettings", {})

                #计算回测数据的四维度数据
                fourFactorData = calcFourFactorData(backtestData)
                backtestData["fourFactorData"] = fourFactorData
            pass
        
        _LOG.info(f"I: 中线轮动策略系统...结束 ")

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
    if inputPortfolioFileName:
        _LOG.info(f"I: PID:{_processorPID}, debug:{debugFlag}, portfolioFileName:{inputPortfolioFileName}") 
    else:
        _LOG.info(f"I: PID:{_processorPID}, debug:{debugFlag}, portfolioFileName:默认") 

    mainProcessor(inputPortfolioFileName)


if __name__ == "__main__":
    main()


