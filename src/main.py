#! /usr/bin/env python3
#encoding: utf-8

#Filename: main.py
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com/xie_frank@163.com
#Date: 2026-01-12
#Description: 主程序

_VERSION="20260118"

_DEBUG=True

import os
import sys
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

from procesor import portfolioDataFetchProcessor as comFetch

#setting files
from config import basicSettings as settings


_processorPID = os.getpid()

_DEBUG = True
_HOME_DIR =  settings._HOME_DIR

if "_LOG" not in dir() or not _LOG:
    logDir = os.path.join(_HOME_DIR, "log")
    _LOG = misc.setLogNew("MAIN", comGD._DEF_LOG_STOCK_MAIN_LOG_NAME, logDir)

systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
_LOG.info(f"PID:{_processorPID}, python version:{systemVersion}, main code version:{_VERSION}")


#common function begin

#common function end

def fetchStockData(inputPortfolioFileName):
    result = False
    try:
        # 检查输入的股票配置文件是否存在
        if not os.path.exists(inputPortfolioFileName):
            _LOG.error(f"E: 输入的股票配置文件不存在, fileName:{inputPortfolioFileName}")
            return result

        #首先读取股票配置文件
        _LOG.info(f"1: 检查配置文件,并获取相关股票参数... ")
        fetchList = comFetch.dataFetchProcessor(inputPortfolioFileName)
        if fetchList:
            result = True
            _LOG.info(f"I: 数据准备完毕, 股票数量:{len(fetchList)}")
        else:
            _LOG.error(f"E: 数据准备失败, 股票数量:0")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def mainProcessor(inputPortfolioFileName=""):
    result = {}
    try:
        #开始处理
        _LOG.info(f"I: 中线轮动策略系统开始处理... ")

        if fetchStockData(inputPortfolioFileName):
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

    _LOG.info(f"I: PID:{_processorPID}, debug:{debugFlag}, portfolio:{inputPortfolioFileName}")

    mainProcessor(inputPortfolioFileName)


if __name__ == "__main__":
    main()


