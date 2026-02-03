#!/usr/bin/env python3
# -*- coding: utf-8 -*-


#Filename: sqlliteCommon.py  
#Date: 2020-04-01
#Description:   sqllite 处理代码

#sqllite数据库信息也存储在这里, 主要是只有部分程序需要处理sqllite数据库, 读写已经分离, 目前主要是采用sql语句处理, 已经防止注入攻击. 


_VERSION="20260202"

#add src directory
import os
from re import S
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')
    
#import decimal 
#import requests
import traceback
# import copy

#global defintion/common var etc.
from common import globalDefinition as comGD

# from common import funcCommon as comFC
#code/decode functions
#from common import codingDecoding as comCD

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#setting files
from config import sqliteSettings as sqliteSettings

from config import basicSettings as settings

HOME_DIR = settings._HOME_DIR

if "_LOG" not in dir() or not _LOG:
    logfilepath = os.path.join(HOME_DIR, "sqllitelog")
#    _LOG = misc.setLogNew(comGD._DEF_XJY_MYSQL_TITLE, logfilepath)
    
_DEBUG = settings._DEBUG

auto_increment_default_value = 10000

# SYS_DEFAULT_AUTO_LOGINID = settings.SYS_DEFAULT_AUTO_LOGINID

if "sqliteDB" not in dir() or not sqliteDB:
    sqliteDB = sqliteSettings.sqliteDB

database_name = sqliteSettings.SQLITE_READ_FILENAME

#common begin

def dataFormatConvert(dataList):
    result = dataList
    for data in dataList:
        for k, v in data.items():
            if isinstance(v, int):
                data[k] = str(v)
            if isinstance(v, float):
                data[k] = str(v)
            if v == None:
                data[k] = ""
            if k in ["position", "regPosition","fileIDList",
                     "deviceList","enableDeviceList"]:
                if v:
                    v = v.replace("'", "\"")
                    data[k] = misc.jsonLoads(v)
    result = dataList
    return result


def chkTableExist(tableName):
    result = False
    sqlStr = "SELECT table_name FROM information_schema.TABLES WHERE table_schema = %s and table_name = %s;"
    rtn = sqliteDB.executeRead(sqlStr, (database_name, tableName))
    if rtn > 0:
        result = True
    return result


def dropTableGeneral(tableName):
    result = False
    try:
        sqlStr = "DROP TABLE %s;" % tableName
        rtn = sqliteDB.executeWrite(sqlStr)
        rtn = chkTableExist(tableName)
        if rtn == False:
            result = True
    except:
        pass
    return result    


def insertTableGeneral(tableName, dataSet, selfDefinedPrimaryKey = comGD._CONST_NO):
    result = 0
    try:
        if sys.version_info.major <= 2:
            insertStr = ("INSERT INTO %s (" % tableName).encode("utf-8")
        else:
            insertStr = ("INSERT INTO %s (" % tableName)
            
        fieldNameList = [insertStr]
        placeHolderList = []
        valuesList = []

        for k,  v in dataSet.items():
            fieldNameList.append(k)
            fieldNameList.append(",")
            stringFlag = False
            if isinstance(v,  bytes):
                pass
#            if isinstance(v, int):
#                v = str(v)
#                    v = v.encode("utf-8")
            if sys.version_info.major <= 2:
                if isinstance(v, unicode):
                    v = v.encode("utf-8")
            valuesList.append(v)
            placeHolderList.append("%s")
            placeHolderList.append(",")

        fieldNameList = fieldNameList[0:-1]
        fieldNameList.append(")  VALUES (" ) 
        placeHolderList = placeHolderList[0:-1]
        fieldNameList.extend(placeHolderList)
        fieldNameList.append(")")
        sqlStr = "".join(fieldNameList)
        rtn = sqliteDB.executeWrite(sqlStr, tuple(valuesList))
#        if _DEBUG:
#            if rtn <=0:
#                _LOG.warning("M: %d %s" % (rtn,  sqlStr)) 
        
        if rtn > 0:
            if selfDefinedPrimaryKey == comGD._CONST_NO:
                #result = sqliteDB.lastrowid
                result = sqliteDB.insertID()
            else:
                result = rtn

    except Exception as e:
        errMsg = '%s %s'%("insertTableGeneral", str(e))
#        if _DEBUG:
#            _LOG.error( '%s' %(errMsg))

    return result


def updateTableGeneral(tableName, keySqlstr, keyValues, dataSet):
    result = 0
    try:
        tempStr = "UPDATE %s SET " % tableName
        fieldNameList = [tempStr]
        valuesList = []
        for k,  v in dataSet.items():
            fieldNameList.append("%s = " % (k))
            fieldNameList.append("%s")
            fieldNameList.append(",")
            valuesList.append(v)

        fieldNameList = fieldNameList[0:-1]
        
        fieldNameList.append("  WHERE %s;" % (keySqlstr)) 
        valuesList.extend(keyValues)
            
        sqlStr = "".join(fieldNameList)
        rtn = sqliteDB.executeWrite(sqlStr, tuple(valuesList))
#        if _DEBUG:
#            if rtn <=0:
#                _LOG.warning("M: %d %s" % (rtn,  sqlStr))                         

    except Exception as e:
        errMsg = '%s %s'%("updateTableGeneral", str(e))
#        if _DEBUG:
#            _LOG.error( '%s' %(errMsg))

    return result


#获取当前数据库表名称和记录数
def getCurrTableNames():
    result = []

    valuesList = []
    sqlStr =  f"SELECT name FROM sqlite_master WHERE type='table';" 

    try:
        rtn = sqliteDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > -2: # 大于-1 表示有记录, 等于-1 表示没有记录,-2 表示错误
            dataList = sqliteDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            for data in dataList:
                sqlStr = f"SELECT COUNT(*) as rowNum FROM {data['name']};"
                valuesList = []
                rtn = sqliteDB.executeRead(sqlStr, tuple(valuesList))
                if rtn > -2:
                    valuesList = sqliteDB.fetchOne()
                    if valuesList:
                        rowNum = valuesList[0].get("rowNum",0)
                        aSet = {}
                        aSet["tableName"] = data["name"]
                        aSet["tableRows"] = rowNum
                        result.append(aSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"

    return result


#获取当前数据库表名称,记录数以及表的结构
def getCurrTableInfo():
    result = []

    valuesList = []
    sqlStr =  f"SELECT name FROM sqlite_master WHERE type='table';" 

    try:
        rtn = sqliteDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > -2: # 大于-1 表示有记录, 等于-1 表示没有记录,-2 表示错误
            dataList = sqliteDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            for data in dataList:
                tableName = data["name"]
                #获取表的记录数
                sqlStr = f"SELECT COUNT(*) as rowNum FROM {tableName};"
                valuesList = []
                sqliteDB.executeRead(sqlStr, tuple(valuesList))
                rowNumList = sqliteDB.fetchOne()
                # 获取表结构
                rtn = sqliteDB.executeRead(f"PRAGMA table_info({tableName})")
                columns = sqliteDB.fetchAll()
                if rowNumList and columns:
                    rowNum = rowNumList[0].get("rowNum",0)
                    aSet = {}
                    aSet["tableName"] = data["name"]
                    aSet["tableRows"] = rowNum
                    aSet["tableColumns"] = columns
                    result.append(aSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"

    return result


def genOrList(IDList, keyName = ""):
    aList = []
    count = 0
    aList.append("( ")
    for ID in IDList:
        if count == 0:
            aList.append(f" {keyName} = %s ")
        else:
            aList.append(f" OR {keyName} = %s ")
        count += 1
    aList.append(") ")
    result = "".join(aList)
    return result

#common end


#balance_sheet 查询记录
def query_balance_sheet(id = "0", start_id = "0", stock_code = "", stock_name = "",
                            delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):

    tableName = "balance_sheets"
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)
        except:
            id = 0

        try:
            start_id = int(start_id)
        except:
            start_id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if start_id > 0:
                if valuesList:
                    sqlStr =  sqlStr + " AND id > %s" 
                else:
                    sqlStr =  sqlStr + " WHERE id > %s" 
                valuesList.append(start_id)

            if stock_code:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_code = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE stock_code = %s" 
                valuesList.append(stock_code)

            if stock_name:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_name = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE stock_name = %s" 
                valuesList.append(stock_name)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = sqliteDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > -2: # 大于-1 表示有记录, 等于-1 表示没有记录,-2 表示错误
            dataList = sqliteDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result
    

#indicator_medians 查询记录
def query_indicator_medians(id = "0", start_id = "0", indicator_name = "", report_date = "",
                            delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):

    tableName = "indicator_medians"
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)
        except:
            id = 0

        try:
            start_id = int(start_id)
        except:
            start_id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if start_id > 0:
                if valuesList:
                    sqlStr =  sqlStr + " AND id > %s" 
                else:
                    sqlStr =  sqlStr + " WHERE id > %s" 
                valuesList.append(start_id)

            if indicator_name:
                if valuesList:
                    sqlStr =  sqlStr + " AND indicator_name = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE indicator_name = %s" 
                valuesList.append(indicator_name)

            if report_date:
                if valuesList:
                    sqlStr =  sqlStr + " AND report_date = %s"  
                else:
                    sqlStr =  sqlStr + " WHERE report_date = %s" 
                valuesList.append(report_date)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = sqliteDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > -2: # 大于-1 表示有记录, 等于-1 表示没有记录,-2 表示错误
            dataList = sqliteDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#income_statements 查询记录
def query_income_statements(id = "0", start_id = "0", stock_code = "", stock_name = "",report_date = "",
                            delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):

    tableName = "income_statements"
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)
        except:
            id = 0

        try:
            start_id = int(start_id)
        except:
            start_id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if start_id > 0:
                if valuesList:
                    sqlStr =  sqlStr + " AND id > %s" 
                else:
                    sqlStr =  sqlStr + " WHERE id > %s" 
                valuesList.append(start_id)

            if stock_code:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_code = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE stock_code = %s" 
                valuesList.append(stock_code)

            if stock_name:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_name = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE stock_name = %s" 
                valuesList.append(stock_name)   

            if report_date:
                if valuesList:
                    sqlStr =  sqlStr + " AND report_date = %s"  
                else:
                    sqlStr =  sqlStr + " WHERE report_date = %s" 
                valuesList.append(report_date)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = sqliteDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > -2: # 大于-1 表示有记录, 等于-1 表示没有记录,-2 表示错误
            dataList = sqliteDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#cash_flow_statements 查询记录
def query_cash_flow_statements(id = "0", start_id = "0", stock_code = "", stock_name = "",report_date = "",
                            delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):

    tableName = "cash_flow_statements"
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)
        except:
            id = 0

        try:
            start_id = int(start_id)
        except:
            start_id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if start_id > 0:
                if valuesList:
                    sqlStr =  sqlStr + " AND id > %s" 
                else:
                    sqlStr =  sqlStr + " WHERE id > %s" 
                valuesList.append(start_id)

            if stock_code:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_code = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE stock_code = %s" 
                valuesList.append(stock_code)

            if stock_name:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_name = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE stock_name = %s" 
                valuesList.append(stock_name)   

            if report_date:
                if valuesList:
                    sqlStr =  sqlStr + " AND report_date = %s"  
                else:
                    sqlStr =  sqlStr + " WHERE report_date = %s" 
                valuesList.append(report_date)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = sqliteDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > -2: # 大于-1 表示有记录, 等于-1 表示没有记录,-2 表示错误
            dataList = sqliteDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#application begin



#application end


def checkMySqlDataBase():
    YMDHMS = misc.getTime()
    currYear = YMDHMS[0:4]
    YM = YMDHMS[0:6]
    YMD = YMDHMS[0:8]
    

    # tableName = tablename_convertor_hwinfo_report_record()
    # if chkTableExist(tableName) == False:
    #     rtn = create_hwinfo_report_record(tableName)


def dropMySqlDataBase():
    YMDHMS = misc.getTime()
    currYear = YMDHMS[0:4]
    YM = YMDHMS[0:6]
    YMD = YMDHMS[0:8]
    
    # tableName = tablename_convertor_hwinfo_report_record()
    # if chkTableExist(tableName):
    #     rtn = drop_hwinfo_report_record(tableName)


checkMySqlDataBase()
#check mysql database end

if __name__ == "__main__":
    if len(sys.argv) > 1:
        pass
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
            msg = sys.argv[1]
            checkMySqlDataBase()

