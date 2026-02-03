#!/usr/bin/env python3
#encoding: utf-8

#Filename: ylwzStockCommon.py  
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com/xie_frank@163.com  
#Date: 2026-01-16
#Description:   ylwz 股票相关的接口
#所有股票内容, symbol = 纯数字代码, 其他英文内容均采用小写,并用"_"连接


_VERSION="20260203"


import os
import sys

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

import pathlib
import requests

import pandas as pd

from common import globalDefinition as comGD
from common import miscCommon as misc

# 股票akshare模块
# from common import akshareCommon as comAK

#申万行业数据规则
#在以前的版本中,rotation_strategy_system\config\comprehensive_industry_rules.py
# from common import swIndustryRules as comIndustryRules

from config import basicSettings as settings


if "_LOG" not in dir() or not _LOG:
    logDir = os.path.join(settings._HOME_DIR, "log")
    _LOG = misc.setLogNew("YLWZ",comGD._DEF_LOG_STOCK_TEST_NAME, logDir)


#common begin
_processorPID = os.getpid()

YLWZ_STOCK_API_URL_DATA = {
    #common 
    "generalnext":
    {
        "method":"post",
        "description":"获取后续数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/generalnext",
        "params":{}
    },
    #industry info
    "industryinfoadd":
    {
        "method":"post",
        "description":"添加行业信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/industryinfoadd",
        "params":{}
    },
    "industryinfodel":
    {
        "method":"post",
        "description":"删除行业信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/industryinfodel",   
        "params":{}
    },
    "industryinfomodify":
    {
        "method":"post",
        "description":"修改行业信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/industryinfomodify",   
        "params":{}
    },
    "industryinfoqry":
    {
        "method":"post",
        "description":"查询行业信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/industryinfoqry",   
        "params":{}
    },
    #stock info
    "stockinfoadd":
    {
        "method":"post",
        "description":"添加股票信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockinfoadd",  
        "params":{}
    },
    "stockinfodel":
    {
        "method":"post",
        "description":"删除股票信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockinfodel",  
        "params":{}
    },
    "stockinfomodify":
    {
        "method":"post",
        "description":"修改股票信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockinfomodify",  
        "params":{}
    },
    "stockinfoqry":
    {
        "method":"post",
        "description":"查询股票信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockinfoqry",  
        "params":{}
    },
    #stock history data
    "stockhistoryadd":
    {
        "method":"post",
        "description":"添加股票历史数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockhistoryadd",  
        "params":{}
    },
    "stockhistorydel":
    {
        "method":"post",
        "description":"删除股票历史数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockhistorydel",  
        "params":{}
    },
    "stockhistorymodify":
    {
        "method":"post",
        "description":"修改股票历史数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockhistorymodify",  
        "params":{}
    },
    "stockhistoryqry":
    {
        "method":"post",
        "description":"查询股票历史数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockhistoryqry",  
        "params":{}
    },
    #industry history data
    "industryhistoryadd":
    {
        "method":"post",
        "description":"添加行业历史数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/industryhistoryadd",  
        "params":{}
    },
    "industryhistorydel":
    {
        "method":"post",
        "description":"删除行业历史数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/industryhistorydel",  
        "params":{}
    },
    "industryhistorymodify":
    {
        "method":"post",
        "description":"修改行业历史数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/industryhistorymodify",  
        "params":{}
    },
    "industryhistoryqry":
    {
        "method":"post",
        "description":"查询行业历史数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/industryhistoryqry",  
        "params":{}
    },
    #stock dividend data
    "stockdividendadd":
    {
        "method":"post",
        "description":"添加股票分红信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockdividendadd",  
        "params":{}
    },
    "stockdividenddel":
    {
        "method":"post",
        "description":"删除股票分红信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockdividenddel",  
        "params":{}
    },
    "stockdividendmodify":
    {
        "method":"post",
        "description":"修改股票分红信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockdividendmodify",  
        "params":{}
    },
    "stockdividendqry":
    {
        "method":"post",
        "description":"查询股票分红信息",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/stockdividendqry",  
        "params":{}
    },
    #balance sheets
    "balancesheetadd":
    {
        "method":"post",
        "description":"添加股票财务数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/balancesheetadd",  
        "params":{}
    },
    "balancesheetdel":
    {
        "method":"post",
        "description":"删除股票财务数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/balancesheetdel",  
        "params":{}
    },
    "balancesheetmodify":
    {
        "method":"post",
        "description":"修改股票财务数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/balancesheetmodify",  
        "params":{}
    },
    "balancesheetqry":
    {
        "method":"post",
        "description":"查询股票财务数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/balancesheetqry",  
        "params":{}
    },
    #income statements
    "incomestatementsadd":
    {
        "method":"post",
        "description":"添加股票收入数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/incomestatementsadd",  
        "params":{}
    },
    "incomestatementsdel":
    {
        "method":"post",
        "description":"删除股票收入数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/incomestatementsdel",  
        "params":{}
    },
    "incomestatementsmodify":
    {
        "method":"post",
        "description":"修改股票收入数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/incomestatementsmodify",  
        "params":{}
    },
    "incomestatementsqry":
    {
        "method":"post",
        "description":"查询股票收入数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/incomestatementsqry",  
        "params":{}
    },
    #indicator medians
    "indicatoradd":
    {
        "method":"post",
        "description":"添加股票指标中位数数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/indicatoradd",  
        "params":{}
    },
    "indicatordel":
    {
        "method":"post",
        "description":"删除股票指标中位数数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/indicatordel",  
        "params":{}
    },
    "indicatormodify":
    {
        "method":"post",
        "description":"修改股票指标中位数数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/indicatormodify",  
        "params":{}
    },
    "indicatorqry":
    {
        "method":"post",
        "description":"查询股票指标中位数数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/indicatorqry",  
        "params":{}
    },
    #cash flow
    "cashflowadd":
    {
        "method":"post",
        "description":"添加股票现金流数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/cashflowadd",  
        "params":{}
    },
    "cashflowdel":
    {
        "method":"post",
        "description":"删除股票现金流数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/cashflowdel",  
        "params":{}
    },
    "cashflowmodify":
    {
        "method":"post",
        "description":"修改股票现金流数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/cashflowmodify",  
        "params":{}
    },
    "cashflowqry":
    {
        "method":"post",
        "description":"查询股票现金流数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/cashflowqry",   
        "params":{}
    },
    #user stock list
    "userstocklistadd":
    {
        "method":"post",
        "description":"添加用户股票列表数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/userstocklistadd",  
        "params":{}
    },
    "userstocklistdel":
    {
        "method":"post",
        "description":"删除用户股票列表数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/userstocklistdel",  
        "params":{}
    },
    "userstocklistmodify":
    {
        "method":"post",
        "description":"修改用户股票列表数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/userstocklistmodify",  
        "params":{}
    },
    "userstocklistqry":
    {
        "method":"post",
        "description":"查询用户股票列表数据",
        "host":"",
        "port":80,
        "headers":{"content-type": "application/json"},
        "urlPath":"stockapi/userstocklistqry",   
        "params":{}
    },
}

QUERY_CMD_LIST = [
    "stockinfoqry",
    "industryinfoqry",
    "stockhistoryqry",
    "stockdividendqry",
    "industryhistoryqry",
    "balancesheetqry",
    "incomestatementsqry",
    "cashflowqry",
    "indicatorqry",
    "userstocklistqry",
]
#common end

class StockServer:
    _HOST = '127.0.0.1' 
    _PORT = 80
    _ROOT_PATH = ""
    _errMsg = ""
    _project = None
    _case = None
    _indexKey = ""
    _indexBeginNum = 0
    _indexEndNum = 0
    _indexTotal = 0

    
    def __init__(self,host="",port=80,sessionID="",rootPath="",pdFormat=False):
        self._ready = False
        self._HOST = host
        self._PORT = port
        self._sessionID = sessionID
        self._ROOT_PATH = rootPath
        self._pdFormat = pdFormat

    def getRequest(self,url,paramsData,headers={"content-type": "application/json"},authFlag=True):
        result = {}
        
        rtnData = {}

        try:
            if authFlag:
                paramsData["sessionID"] = self._sessionID
                pass
            r = requests.get(url, params = paramsData, headers = headers)
            if r.status_code == 200:
                try:
                    rtnData["data"] = misc.jsonLoads(r.content)
                except:
                    rtnData["data"] = r.content
            else:
                self._responseCode = r.status_code
                pass

            rtnData["status"] = r.status_code
            result = rtnData

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e),traceback.format_exc()}"
            self._errMsg = errMsg
    
        return result   
  
    def postRequest(self,url,requestData,paramsData={},headers={"content-type": "application/json"},authFlag=True):
        result = {}

        rtnData = {}
        try:
            if authFlag:
                requestData["sessionID"] = self._sessionID
                pass
            payload = misc.jsonDumps(requestData)
    
            if paramsData:
                r = requests.post(url, data = payload, params=paramsData,headers = headers)
            else:
                r = requests.post(url, data = payload, headers = headers)

            if r.status_code == 200:
                try:
                    rtnData["data"] = misc.jsonLoads(r.content)
                except:
                    rtnData["data"] = r.content
            else:
                self._responseCode = r.status_code
                pass

            rtnData["status"] = r.status_code
            result = rtnData

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e),traceback.format_exc()}"
            self._errMsg = errMsg
    
        return result   

    def query(self,cmd,requestData={},paramsData={},authFlag=True):
        result = {}
        try:
            cmd = cmd.lower()
            requestTypeData = YLWZ_STOCK_API_URL_DATA.get(cmd)
            if requestTypeData:
                method = requestTypeData["method"]
                host = requestTypeData["host"]
                port = requestTypeData["port"]
                urlPath = requestTypeData["urlPath"]
                localParams = requestTypeData["params"]

                if not paramsData and localParams:
                    paramsData = localParams

                if self._HOST: #如果class初始化指定了IP就用指定的IP 
                    host = self._HOST

                if self._PORT:
                    port = self._PORT
                
                if self._ROOT_PATH:
                    url = "http://" + host + ":" + str(port) + "/" + self._ROOT_PATH + "/" + urlPath
                else:
                    url = "http://" + host + ":" + str(port) + "/" + urlPath

                if method == "post":
                    result = self.postRequest(url,requestData,paramsData)
                else:
                    result = self.getRequest(url,paramsData)                 
                
                #处理qry命令
                if cmd in QUERY_CMD_LIST:
                    self.handleQueryResult(cmd,result)

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e),traceback.format_exc()}"
            self._errMsg = errMsg
    
        return result   
    

    def handleQueryResult(self,cmd,dataSet):
        result = False
        try:
            if dataSet and "data" in dataSet:
                data = dataSet["data"]
                if data:
                    self._indexKey = data.get("indexKey","")
                    self._indexTotal = int(data.get("total",0))
                    self._indexBeginNum = int(data.get("beginNum",0))
                    self._indexEndNum = int(data.get("endNum",0))
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e),traceback.format_exc()}"
            self._errMsg = errMsg
    
        return result   


    #获取下一批数据
    def getNext(self,num = 100):
        result = []
        try:
            nextBeginNum = self._indexEndNum + 1
            nextEndNum = nextBeginNum + num - 1
            if nextEndNum > self._indexTotal:
                nextEndNum = self._indexTotal
            querySet = {"indexKey":self._indexKey,"beginNum":nextBeginNum,"endNum":nextEndNum}
            cmd = "generalnext"
            result = self.query(cmd,querySet)
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e),traceback.format_exc()}"
            self._errMsg = errMsg
        

        return result


def readSessionIDFromEnv():
    try:
        sessionID = os.getenv("YLWZ_SESSION_ID")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return sessionID


def test():
    # 示例
    host = "www.iottest.online"
    sessionID = readSessionIDFromEnv()

    ylwzStockServer = StockServer(host=host, sessionID=sessionID)

    cmd = "stockinfoqry"
    # querySet = {"symbol":"920000"}
    querySet = {}
    rtnData = ylwzStockServer.query(cmd,querySet)
    print(rtnData)
    rtnData = ylwzStockServer.getNext(100)

    pass


if __name__ == "__main__":
    if len(sys.argv) > 1:
        pass
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
    
    test()
