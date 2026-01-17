#! /usr/bin/env python3
#encoding: utf-8

#Filename: mysqlCodeGenerator.py 
#Author: Steven Lian
#E-mail:  steven.lian@gmail.com  
#Date: 2020-8-5
#Description:  生成标准的mysqlCommon 和RESTful 接口所用的增删改查

_VERSION="20251221"

_DEBUG=True
#auto_increment_default_value = 10000

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import random

#common functions(log,time,string, json etc)
from common import miscCommon as misc

indentLen = 4
HEADER_AUTHOR = "Steven Lian's team"
HEADER_EMAIL = "steven.lian@gmail.com"

#common data begin
MYSQL_REG_KEYS_LIST = ["regID","regYMDHMS"]
MYSQL_MODIFY_KEYS_LIST = ["modifyID","modifyYMDHMS"]
DELETE_FLAG_KEYS_LIST = ["delFlag"]
#common data end

def decodeDataType(typeString):
    result = ""
    
    if result == "":
        intTypeList = ["TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTERGER", "BIGINT"]
        for key in intTypeList:
            pos = typeString.upper().find(key)
            if pos >= 0:
                result = "INT"
                break

    if result == "":
        floatTypeList = ["FLOAT", "DOUBLE", "DECIMAL"]
        for key in floatTypeList:
            pos = typeString.upper().find(key)
            if pos >= 0:
                result = "FLOAT"
                break
            
    if result == "":
        charTypeList = ["CHAR", "VARCHAR", "TINYBLOB", "TINYTEXT", "BOLB", "TEXT","MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT"]
        for key in charTypeList:
            pos = typeString.upper().find(key)
            if pos >= 0:
                result = "CHAR"
                break

    if result == "":
        dateTypeList = ["DATE", "TIME", "DATETIME", "YEAR", "TIMESTAMP"]
        for key in dateTypeList:
            pos = typeString.upper().find(key)
            if pos >= 0:
                result = "DATE"
                break
                
    return result
    

def genTableNameConvertorCode(tableName,keysList):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    
    TS2 = TS + TS

    aList = []
    keysStringList = []
    for key in keysList:
        keysStringList.append(str(key))
    keysString = ",".join(keysStringList)

    tempString = f'def tablename_convertor_{tableName}({keysString}):'
    aList.append(tempString)
    tempString = "_".join(keysStringList)
    tempString = TS + f'tableName = "{tableName}"' + tempString
    aList.append(tempString)
    tempString = TS + 'tableName = tableName.lower()'
    aList.append(tempString)
    tempString = TS + 'return tableName'
    aList.append(tempString)

    tempString = ""
    aList.append(tempString)
    
    result = "\n".join(aList)

    return result


def genTableNameDecodeCode(tableName):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    
    TS2 = TS + TS

    aList = []

    tempString = f'def decode_tablename_{tableName}(tableName):'
    aList.append(tempString)
    tempString = TS + 'result = {}'
    aList.append(tempString)
    tempString = TS + 'aList = tableName.split("_")'
    aList.append(tempString)

    tempString = TS
    aList.append(tempString)

    tempString = TS + 'return result'
    aList.append(tempString)

    tempString = ""
    aList.append(tempString)
    
    result = "\n".join(aList)
    return result


def genCreateCode(tableName, dataStructure, primaryKeys = []):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    
    TS2 = TS + TS
    
    aList = []

    tempString = f'#创建{tableName}表'
    aList.append(tempString)
    
    tempString = f'def create_{tableName}(tableName):'
    aList.append(tempString)
    tempString = TS + 'aList = ["CREATE TABLE IF NOT EXISTS %s("'
    aList.append(tempString)
    #fields
    autoIncrementExist = False
    for data in dataStructure:
        isPrimaryKey = data.get("isPrimaryKey")
        rest = data.get("rest")
        if isPrimaryKey:
            if rest.find("AUTO_INCREMENT") >=0:
                autoIncrementExist = True        
        tempString = TS + '"' + data.get("fieldName", " ") 
        tempString += " " + data.get("dataTypeString", " ") +  " " + data.get("rest", "") +',",' 
        aList.append(tempString)
    
    #primary key
    if primaryKeys != []:
        primaryString = ",".join(primaryKeys)
        tempString = TS + '"' + f"PRIMARY KEY ({primaryString})" +'",' 
        aList.append(tempString)
    
    nLen = len(result)
    tempString = aList[nLen-1]
    pos = tempString.upper().find("PRIMARY KEY")
    if pos < 0:
        pos = tempString.find(",")
        if pos >=0:
            tempString = tempString[0:pos] + '"'
            aList[nLen-1] = tempString
        
    tempString = TS + '")  ENGINE=INNODB DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;"'
    aList.append(tempString)
    tempString = TS + ']'
    aList.append(tempString)
    tempString = TS + 'tempStr = "".join(aList)'
    aList.append(tempString)
    tempString = TS + 'sqlStr = tempStr % (tableName)'
    aList.append(tempString)
    tempString = TS + 'rtn = mysqlDB.executeWrite(sqlStr)'
    aList.append(tempString)
    tempString = TS + 'result = chkTableExist(tableName)'
    aList.append(tempString)
    tempString = TS + 'if result:'
    aList.append(tempString)
    tempString = TS2 + 'pass'
    aList.append(tempString)
    tempString = TS2 + '#sqlStr = "CREATE INDEX {1} ON {0}({1}) ".format(tableName, "indexKey")'
    aList.append(tempString)
    tempString = TS2 + '#rtn = mysqlDB.executeWrite(sqlStr)'
    aList.append(tempString)
    if autoIncrementExist:
        tempString = TS2 + '#sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)'
        aList.append(tempString)
        tempString = TS2 + '#rtn = mysqlDB.executeWrite(sqlStr)'
        aList.append(tempString)

    aList.append("")
    
    tempString = TS + 'return result'
    aList.append(tempString)
    aList.append("")

    result = "\n".join(aList)
    
    return result
    

def genDropCode(tableName,dataStructure):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    
    if len(dataStructure) > 0:


        aList = []

        tempString = f'#删除{tableName}表'
        aList.append(tempString)

        tempString = f'def drop_{tableName}(tableName):'
        aList.append(tempString)
        tempString = TS + 'result = dropTableGeneral(tableName)'
        aList.append(tempString)
        tempString = TS + 'return result'
        aList.append(tempString)
        aList.append("")

    result = "\n".join(aList)
    
    return result


def genQueryCode(tableName, dataStructure):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    TS2 = TS + TS
    TS3 = TS2 + TS
    TS4 = TS3 + TS

    nLen = len(dataStructure)
    if nLen > 0:
        #find isPrimaryKey
        primaryKey = ""
        dataType = ""
        for data in dataStructure:
            if data.get("isPrimaryKey"):
                primaryKey = data.get("fieldName")
                dataType = data.get("dataType")                    
                break

        aList = []

        tempString = f'#{tableName} 查询记录'
        aList.append(tempString)

        if dataType in ["INT"]:
            tempString = f'def query_{tableName}(tableName,{primaryKey} = "0", delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):' 
        else:
            tempString = f'def query_{tableName}(tableName,{primaryKey} = "", delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):' 

        aList.append(tempString)
        
        tempString = TS + 'result = []'
        aList.append(tempString)

        tempString = TS + 'columns = "*"'
        aList.append(tempString)

        tempString = TS + 'valuesList = []' 
        aList.append(tempString)

        tempString = TS + 'sqlStr = f"SELECT {columns} FROM {tableName}"' 
        aList.append(tempString)

        aList.append("")
        tempString = TS + 'try:'
        aList.append(tempString)
        aList.append("")

        if dataType in ["INT"]:
            tempString = TS2 + 'try:'
            aList.append(tempString)

            tempString = TS3 + f'{primaryKey} = int({primaryKey})'
            aList.append(tempString)
            aList.append("")

            tempString = TS2 + 'except:'
            aList.append(tempString)

            tempString = TS3 + f'{primaryKey} = 0'
            aList.append(tempString)
            aList.append("")

            tempString = TS2 + f'if {primaryKey} > 0:'
            aList.append(tempString)

            tempString = TS3 + f'sqlStr =  sqlStr + " WHERE {primaryKey} = %s" '
            aList.append(tempString)
            tempString = TS3 + f'valuesList = [{primaryKey}]  '
            aList.append(tempString)
        else:
            tempString = TS2 + '#recID = int(recID)'
            aList.append(tempString)
            aList.append("")

            tempString = TS2 + '#if recID > 0:'
            aList.append(tempString)

            tempString = TS3 + '#sqlStr =  sqlStr + " WHERE recID = %s" '
            aList.append(tempString)
            tempString = TS3 + '#valuesList = [recID]  '
            aList.append(tempString)

        aList.append("")

        tempString = TS2 + '#if limitNum > 0:'
        aList.append(tempString)

        tempString = TS3 + '#sqlStr += " LIMIT {0}".format(limitNum)'
        aList.append(tempString)

        aList.append("")
            
        tempString = TS2 + 'rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))'
        aList.append(tempString)

        tempString = TS2 + 'if rtn > 0:'
        aList.append(tempString)

        tempString = TS3 + 'dataList = mysqlDB.fetchAll()'
        aList.append(tempString)

        tempString = TS3 + 'dataList = dataFormatConvert(dataList)'
        aList.append(tempString)

        tempString = TS3 + 'result = list(dataList)'
        aList.append(tempString)
        aList.append("")

        tempString = TS + 'except Exception as e:'
        aList.append(tempString)
        tempString = TS2 + 'traceMsg = traceback.format_exc().strip("")'
        aList.append(tempString)
        tempString = TS2 + 'errMsg = f"{e},{traceMsg}"'
        aList.append(tempString)
        tempString = TS+ TS + '# if _DEBUG:'
        aList.append(tempString)
        tempString = TS+ TS + TS + '# _LOG.error(f"{errMsg}")'
        aList.append(tempString)
        aList.append("")
        tempString = TS + 'return result'
        aList.append(tempString)
        aList.append("")

        result = "\n".join(aList)
    
    return result
    

def genDeleteCode(tableName, dataStructure):
    result = ""
    TS = ""
    TS2 = TS + TS
    for i in range(indentLen):
        TS+=" "

    nLen = len(dataStructure)

    if nLen > 0:
        #find isPrimaryKey
        primaryKey = ""
        for data in dataStructure:
            if data.get("isPrimaryKey"):
                primaryKey = data.get("fieldName")
                break

        aList = []

        tempString = f'#{tableName} 删除记录'
        aList.append(tempString)

        tempString = f'def delete_{tableName}(tableName,{primaryKey}):'
        aList.append(tempString)
        tempString = TS + 'result = 0'
        aList.append(tempString)
        tempString = TS + 'sqlStr = f"DELETE FROM {tableName}"'
        aList.append(tempString)
        tempString = TS + 'try:'
        aList.append(tempString)
        aList.append("")
        tempString = TS + TS + f'sqlStr += " WHERE {primaryKey} = %s"'
        aList.append(tempString)
        tempString = TS + TS + f'valuesList = [{primaryKey}] '
        aList.append(tempString)
        tempString = TS +  TS + 'result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))'
        aList.append(tempString)
        aList.append("")
        tempString = TS + 'except Exception as e:'
        aList.append(tempString)
        tempString = TS + TS + 'traceMsg = traceback.format_exc().strip("")'
        aList.append(tempString)
        tempString = TS+ TS + 'errMsg = f"{e},{traceMsg}"'
        aList.append(tempString)
        tempString = TS+ TS + '# if _DEBUG:'
        aList.append(tempString)
        tempString = TS+ TS + TS + '# _LOG.error(f"{errMsg}")'
        aList.append(tempString)
        aList.append("")
        tempString = TS + 'return result'
        aList.append(tempString)
        aList.append("")

        result = "\n".join(aList)
    
    return result


def genInsertCode(tableName, dataStructure):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "

    TS2 = TS + TS

    AUTO_INCREMENT_KEYS = []

    nLen = len(dataStructure)
    if nLen > 0:
        aList = []

        tempString = f'#{tableName} 增加记录'
        aList.append(tempString)

        keyData = dataStructure[0]
        fieldName = keyData.get("fieldName", "")
        rest = keyData.get("rest", "")
        dataTypeString = keyData.get("dataTypeString", "")
        pos = dataTypeString.upper().find("AUTO_INCREMENT")
        restPos = rest.upper().find("AUTO_INCREMENT")
        if pos >= 0 or restPos >=0:
            autoIncrementFlag = True
        else:
            autoIncrementFlag = False
        
        if autoIncrementFlag:
            tempString = f'def insert_{tableName}(tableName,dataSet):'
            AUTO_INCREMENT_KEYS.append(fieldName)
        else:
            tempString = f'def insert_{tableName}(tableName,{fieldName},dataSet):'

        aList.append(tempString)
        
        tempString = TS + 'result = 0'
        aList.append(tempString)
        tempString = TS + 'try:'
        aList.append(tempString)
        aList.append("")
        
        tempString = TS + TS + 'saveSet = {}'
        aList.append(tempString)
        aList.append("")
        
        for data in dataStructure:
            dataType = data.get("dataType")
            fieldName = data.get("fieldName")
            if fieldName in MYSQL_MODIFY_KEYS_LIST: #插入的数据不修改modify相关字段
                continue

            if fieldName in AUTO_INCREMENT_KEYS: #自动增加的数据不需要插入
                continue

            if dataType in ["INT"]:
                tempString = TS + TS + 'try:'
                aList.append(tempString)
                tempString = TS + TS + TS + f'{fieldName} = int(dataSet.get("{fieldName}")) '
                aList.append(tempString)
                tempString = TS + TS + 'except:'
                aList.append(tempString)
                tempString = TS + TS + TS + f'{fieldName} = 0 '
                aList.append(tempString)
                tempString = TS + TS + f'saveSet["{fieldName}"] = {fieldName}'
                aList.append(tempString)
            elif dataType in ["FLOAT"]:
                tempString = TS + TS + 'try:'
                aList.append(tempString)
                tempString = TS + TS + TS + '{0} = float(dataSet.get("{0}")) '.format(fieldName)
                aList.append(tempString)
                tempString = TS + TS + 'except:'
                aList.append(tempString)
                tempString = TS + TS + TS + '{0} = 0 '.format(fieldName)
                aList.append(tempString)
                tempString = TS + TS + 'saveSet["{0}"] = {0}'.format(fieldName)
                aList.append(tempString)
            elif fieldName in DELETE_FLAG_KEYS_LIST:
                tempString = TS2 + 'saveSet["{0}"] = dataSet.get("{0}", "0") '.format(fieldName)
                aList.append(tempString)
            else:
                tempString = TS2 + 'saveSet["{0}"] = dataSet.get("{0}", "") '.format(fieldName)
                aList.append(tempString)

            aList.append("")

        #aList.append("")
        tempString = TS +  TS + 'result = insertTableGeneral(tableName, saveSet)'
        aList.append(tempString)
        aList.append("")
        tempString = TS + 'except Exception as e:'
        aList.append(tempString)
        tempString = TS2 + 'traceMsg = traceback.format_exc().strip("")'
        aList.append(tempString)
        tempString = TS2 + 'errMsg = f"{e},{traceMsg}"'
        aList.append(tempString)
        tempString = TS+ TS + '# if _DEBUG:'
        aList.append(tempString)
        tempString = TS+ TS + TS + '# _LOG.error(f"{errMsg}")'
        aList.append(tempString)
        aList.append("")
        tempString = TS + 'return result'
        aList.append(tempString)
        aList.append("")

        result = "\n".join(aList)
    
    return result


def genUpdateCode(tableName, dataStructure):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "

    AUTO_INCREMENT_KEYS = []

    nLen = len(dataStructure)

    if nLen > 0:
        #find primaryKey
        primaryKey = ""
        for data in dataStructure:
            if data.get("isPrimaryKey"):
                primaryKey = data.get("fieldName")
                break

        for data in dataStructure:
            #查找 AUTO_INCREMENTk keys
            rest = data.get("rest", "")
            dataTypeString = data.get("dataTypeString", "")
            pos = dataTypeString.upper().find("AUTO_INCREMENT")
            restPos = rest.upper().find("AUTO_INCREMENT")
            if pos >= 0 or restPos >=0:
                autoIncrementFlag = True
            else:
                autoIncrementFlag = False
                        
            if autoIncrementFlag:
                fieldName = data.get("fieldName")
                if fieldName:
                    AUTO_INCREMENT_KEYS.append(fieldName)

        aList = []
        
        tempString = '#{0} 修改记录'.format(tableName)
        aList.append(tempString)

        tempString = f'def update_{tableName}(tableName,{primaryKey},dataSet):'
        aList.append(tempString)
        
        tempString = TS + 'result = -2'
        aList.append(tempString)
        tempString = TS + 'try:'
        aList.append(tempString)
        
        tempString = TS + TS + 'saveSet = {}'
        aList.append(tempString)
        aList.append("")
        
        for data in dataStructure:
            dataType = data.get("dataType")
            fieldName = data.get("fieldName")
            if fieldName in MYSQL_REG_KEYS_LIST: #修改数据不修改reg相关字段
                continue

            if fieldName in AUTO_INCREMENT_KEYS: #自动增加的数据不需要修改
                continue
                
            isPrimaryKey = data.get("isPrimaryKey") 
            if isPrimaryKey: #prirmary key 数据不需要修改
                continue

            tempString = TS + TS + '{0} = dataSet.get("{0}") '.format(fieldName)
            aList.append(tempString)
            tempString = TS + TS + 'if {0}:'.format(fieldName)
            aList.append(tempString)

            if dataType in ["INT"]:
                tempString = TS + TS + TS + 'try:'
                aList.append(tempString)
                tempString = TS + TS + TS + TS + '{0} = int(dataSet.get("{0}")) '.format(fieldName)
                aList.append(tempString)
                tempString = TS + TS + TS + TS + 'saveSet["{0}"] = {0}'.format(fieldName)
                aList.append(tempString)
                tempString = TS + TS + TS + 'except:'
                aList.append(tempString)
                tempString = TS + TS + TS + TS + 'pass'.format(fieldName)
                aList.append(tempString)
            elif dataType in ["FLOAT"]:
                aList.append("")
                tempString = TS + TS + TS + 'try:'
                aList.append(tempString)
                tempString = TS + TS + TS + TS + '{0} = float(dataSet.get("{0}")) '.format(fieldName)
                aList.append(tempString)
                tempString = TS + TS + TS + TS + 'saveSet["{0}"] = {0}'.format(fieldName)
                aList.append(tempString)
                tempString = TS + TS + TS + 'except:'
                aList.append(tempString)
                tempString = TS + TS + TS + TS + 'pass'.format(fieldName)
                aList.append(tempString)
            else: #char type
                tempString = TS + TS + TS + 'saveSet["{0}"] = {0}'.format(fieldName)
                aList.append(tempString)

            aList.append("")

        tempString = TS +  TS + 'keySqlstr = "{0} = %s"'.format(primaryKey)
        aList.append(tempString)
        tempString = TS +  TS + 'keyValues = [{0}]'.format(primaryKey)
        aList.append(tempString)
        aList.append("")
            
        tempString = TS +  TS + 'result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)'
        aList.append(tempString)
        aList.append("")
        tempString = TS + 'except Exception as e:'
        aList.append(tempString)
        tempString = TS + TS + 'traceMsg = traceback.format_exc().strip("")'
        aList.append(tempString)
        tempString = TS + TS + 'errMsg = f"{e},{traceMsg}"'
        aList.append(tempString)
        tempString = TS+ TS + '# if _DEBUG:'
        aList.append(tempString)
        tempString = TS+ TS + TS + '# _LOG.error(f"{errMsg}")'
        aList.append(tempString)
        aList.append("")
        tempString = TS + 'return result'
        aList.append(tempString)
        aList.append("")

        result = "\n".join(aList)
    
    return result


#Server REST增加代码
def genCmdAddCode(tableName, dataStructure):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    TS2 = TS+TS
    TS3 = TS2+TS
    TS4 = TS3+TS
    TS5 = TS4+TS
    TS6 = TS5+TS
    
    nLen = len(dataStructure)
    if nLen > 0:
        aList = []

        tempString = '#Server REST增加代码'
        aList.append(tempString)

        tempString = 'def cmdAdd(CMD,dataSet,sessionIDSet):'
        aList.append(tempString)
        
        tempString = TS + 'result = {}'
        aList.append(tempString)
        tempString = TS + 'errCode = "B0"'
        aList.append(tempString)
        #tempString = TS + 'rtnCMD = CMD[0:2]'
        tempString = TS + 'rtnCMD = CMD'
        aList.append(tempString)
        tempString = TS + 'rtnField = ""'
        aList.append(tempString)
        tempString = TS + 'rtnData = {}'
        aList.append(tempString)
        #tempString = TS + 'lowerCMD = CMD.lower()'
        #aList.append(tempString)
        aList.append("")

        tempString = TS + 'dataValidFlag = True #数据是否有效的标志'
        aList.append(tempString)
        tempString = TS + 'rtnErrMsgList = [] #数据错误原因'
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'try:'
        aList.append(tempString)
        
        tempString = TS2 + 'lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)'
        aList.append(tempString)

        tempString = TS2 + 'msgKey = \"applicationMsgKey\"' 
        aList.append(tempString)

        tempString = TS2 + 'openID = sessionIDSet.get("openID", "")'
        aList.append(tempString)

        tempString = TS2 + 'roleName = sessionIDSet.get("roleName", "")'
        aList.append(tempString)

        tempString = TS2 + 'tempUserID = sessionIDSet.get("loginID", "")'
        aList.append(tempString)

        aList.append("")

        tempString = TS2 + 'if tempUserID != "":'
        aList.append(tempString)

        tempString = TS3 + 'loginID = tempUserID'
        aList.append(tempString)
                
        tempString = TS3 + '#权限检查'
        aList.append(tempString)

        aList.append("")

        tempString = TS3 + 'if errCode == "B0": #'
        aList.append(tempString)

        tempString = TS4 + '#data validation check'
        aList.append(tempString)

        tempString = TS4 + 'dataValidFlag = True'
        aList.append(tempString)
        
        tempString = TS4 + 'if dataValidFlag:'
        aList.append(tempString)

        tempString = TS5 + 'saveSet = {}'
        aList.append(tempString)
        
        for data in dataStructure:
            dataType = data.get("dataType")
            fieldName = data.get("fieldName")
            dataTypeString = data.get("dataTypeString")
            if dataTypeString.find("AUTO_INCREMENT") >= 0: #自动增加的数据不需要插入
                continue 
            if fieldName in DELETE_FLAG_KEYS_LIST:
                tempString = TS5 + 'saveSet["{0}"] = dataSet.get("{0}", "0") '.format(fieldName)
            else:
                tempString = TS5 + 'saveSet["{0}"] = dataSet.get("{0}", "") '.format(fieldName)
            aList.append(tempString)

        tempString = TS5 + 'saveSet["regID"] = loginID'
        aList.append(tempString)

        tempString = TS5 + 'saveSet["regYMDHMS"] = misc.getTime()'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + 'tableName = comMysql.tablename_convertor_{0}()'.format(tableName)
        aList.append(tempString)
        tempString = TS5 + 'recID = comMysql.insert_{0}(tableName,saveSet)'.format(tableName)
        aList.append(tempString)
        tempString = TS5 + 'rtnData["recID"] = str(recID)'
        aList.append(tempString)
        
        aList.append("")
        tempString = TS5 + 'if recID <= 0:'
        aList.append(tempString)
        tempString = TS6 + '#记录添加失败'
        aList.append(tempString)
        tempString = TS6 + 'errCode = "CG"'
        aList.append(tempString)
        
        tempString = TS6 + '_LOG.warning(f"rtn:{recID},saveSet:{saveSet}")'
        aList.append(tempString)

        tempString = TS5 + 'else:'
        aList.append(tempString)

        tempString = TS6 + 'if _DEBUG:'
        aList.append(tempString)

        tempString = TS + TS6 + 'pass'
        aList.append(tempString)
        tempString = TS + TS6 + '_LOG.info(f"D: recID:{recID}")'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + 'result = rtnData'
        aList.append(tempString)

        aList.append("")

        tempString = TS4 + 'else:'
        aList.append(tempString)
        tempString = TS5 + '#data invalid'
        aList.append(tempString)
        tempString = TS5 + 'errCode = "BA"'
        aList.append(tempString)

        aList.append("")

        tempString = TS2 + 'else:'
        aList.append(tempString)
        tempString = TS3 + 'errCode = "B8"'
        aList.append(tempString)

        aList.append("")
        
        #tempString = TS2 + 'rtnCMD = CMD[0:2]'
        tempString = TS2 + 'rtnCMD = CMD'
        aList.append(tempString)
        tempString = TS2 + 'rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)'
        aList.append(tempString)
        tempString = TS2 + 'result["CMD"] = rtnCMD'
        aList.append(tempString)
        tempString = TS2 + 'result["msgKey"] = msgKey'
        aList.append(tempString)
        tempString = TS2 + 'result["MSG"] = rtnSet["MSG"]'
        aList.append(tempString)
        tempString = TS2 + 'result["errCode"] = errCode'
        aList.append(tempString)
        tempString = TS2 + 'result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)'
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'except Exception as e:'
        aList.append(tempString)
        tempString = TS2 + 'errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"'
        aList.append(tempString)
        tempString = TS2 + '_LOG.error(f"{errMsg}, {traceback.format_exc()}")'
        aList.append(tempString)

        aList.append("")
        
        tempString = TS2 + 'rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")'
        aList.append(tempString)

        tempString = TS2 + 'result = rtnSet'
        aList.append(tempString)

        aList.append("")
        tempString = TS + 'return result'
        aList.append(tempString)
        aList.append("")

        result = "\n".join(aList)
    
    return result


#Server REST删除代码
def genCmdDelCode(tableName, dataStructure):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    TS2 = TS+TS
    TS3 = TS2+TS
    TS4 = TS3+TS
    TS5 = TS4+TS
    TS6 = TS5+TS
    
    nLen = len(dataStructure)
    if nLen > 0:
        #find primaryKey
        primaryKey = ""
        for data in dataStructure:
            if data.get("isPrimaryKey"):
                primaryKey = data.get("fieldName")
                break

        aList = []

        tempString = '#Server REST删除代码'
        aList.append(tempString)

        tempString = 'def cmdDel(CMD,dataSet,sessionIDSet):'
        aList.append(tempString)
        
        tempString = TS + 'result = {}'
        aList.append(tempString)
        tempString = TS + 'errCode = "B0"'
        aList.append(tempString)
        #tempString = TS + 'rtnCMD = CMD[0:2]'
        tempString = TS + 'rtnCMD = CMD'
        aList.append(tempString)
        tempString = TS + 'rtnField = ""'
        aList.append(tempString)
        tempString = TS + 'rtnData = {}'
        aList.append(tempString)
        #tempString = TS + 'lowerCMD = CMD.lower()'
        #aList.append(tempString)
        aList.append("")
        
        tempString = TS + 'dataValidFlag = True #数据是否有效的标志'
        aList.append(tempString)
        tempString = TS + 'rtnErrMsgList = [] #数据错误原因'
        aList.append(tempString)
        
        aList.append("")

        tempString = TS + 'try:'
        aList.append(tempString)

        aList.append("")
        
        tempString = TS2 + 'lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)'
        aList.append(tempString)

        tempString = TS2 + 'msgKey = \"applicationMsgKey\"' 
        aList.append(tempString)
        
        tempString = TS2 + 'openID = sessionIDSet.get("openID", "")'
        aList.append(tempString)

        tempString = TS2 + 'roleName = sessionIDSet.get("roleName", "")'
        aList.append(tempString)

        tempString = TS2 + 'tempUserID = sessionIDSet.get("loginID", "")'
        aList.append(tempString)

        aList.append("")

        tempString = TS2 + 'if tempUserID != "":'
        aList.append(tempString)
        
        tempString = TS3 + 'loginID = tempUserID'
        aList.append(tempString)
        
        tempString = TS3 + '#权限检查'
        aList.append(tempString)

        aList.append("")

        tempString = TS3 + 'if errCode == "B0": #'
        aList.append(tempString)

        tempString = TS4 + '{0} = dataSet.get("{0}", "")'.format(primaryKey)
        aList.append(tempString)

        tempString = TS4 + 'tableName = comMysql.tablename_convertor_{0}()'.format(tableName)
        aList.append(tempString)

        tempString = TS4 + 'currDataList = comMysql.query_{0}(tableName,{1})'.format(tableName,primaryKey)
        aList.append(tempString)
        
        tempString = TS4 + 'if len(currDataList) == 1:'
        aList.append(tempString)
        
        tempString = TS5 + 'saveSet = {}'
        aList.append(tempString)

        tempString = TS5 + 'saveSet["modifyID"] = loginID'
        aList.append(tempString)

        tempString = TS5 + 'saveSet["modifyYMDHMS"] = misc.getTime()'
        aList.append(tempString)

        tempString = TS5 + '#saveSet["delFlag"] = "1"'
        aList.append(tempString)

        aList.append("")

        #tempString = TS5 + 'tableName = comMysql.tablename_convertor_{0}()'.format(tableName)
        #aList.append(tempString)

        #tempString = TS5 + '#rtn = comMysql.delete_{0}(tableName,{1}, saveSet)'.format(tableName,primaryKey)
        #aList.append(tempString)

        tempString = TS5 + 'rtn = comMysql.delete_{0}(tableName,{1})'.format(tableName,primaryKey)
        aList.append(tempString)
        
        tempString = TS5 + 'rtnData["rtn"] = str(rtn)'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + 'if _DEBUG:'
        aList.append(tempString)

        tempString = TS6 + '_LOG.info(f"D: rtn:{rtn}")'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + 'result = rtnData'
        aList.append(tempString)
        aList.append("")
        
        tempString = TS4 + 'else:'
        aList.append(tempString)

        tempString = TS5 + 'errCode = "CB"'
        aList.append(tempString)

        aList.append("")

        #tempString = TS3 + 'else:'
        #aList.append(tempString)
        #tempString = TS4 + '#data invalid'
        #aList.append(tempString)
        #tempString = TS4 + 'errCode = "BC"'
        #aList.append(tempString)

        aList.append("")

        tempString = TS2 + 'else:'
        aList.append(tempString)
        tempString = TS3 + 'errCode = "B8"'
        aList.append(tempString)

        aList.append("")
        
        #tempString = TS2 + 'rtnCMD = CMD[0:2]'
        tempString = TS2 + 'rtnCMD = CMD'
        aList.append(tempString)
        tempString = TS2 + 'rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)'
        aList.append(tempString)
        tempString = TS2 + 'result["CMD"] = rtnCMD'
        aList.append(tempString)
        tempString = TS2 + 'result["msgKey"] = msgKey'
        aList.append(tempString)
        tempString = TS2 + 'result["MSG"] = rtnSet["MSG"]'
        aList.append(tempString)
        tempString = TS2 + 'result["errCode"] = errCode'
        aList.append(tempString)
        tempString = TS2 + 'result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)'
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'except Exception as e:'
        aList.append(tempString)
        tempString = TS2 + 'errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"'
        aList.append(tempString)
        tempString = TS2 + '_LOG.error(f"{errMsg}, {traceback.format_exc()}")'
        aList.append(tempString)

        aList.append("")
        
        tempString = TS2 + 'rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")'
        aList.append(tempString)

        tempString = TS2 + 'result = rtnSet'
        aList.append(tempString)

        aList.append("")
        tempString = TS + 'return result'
        aList.append(tempString)
        aList.append("")

        result = "\n".join(aList)
    
    return result


#Server REST修改代码
def genCmdUpdateCode(tableName, dataStructure):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    TS2 = TS+TS
    TS3 = TS2+TS
    TS4 = TS3+TS
    TS5 = TS4+TS
    TS6 = TS5+TS
    TS7 = TS6+TS
    TS8 = TS7+TS
    
    nLen = len(dataStructure)
    if nLen > 0:
        aList = []

        tempString = '#Server REST修改代码'
        aList.append(tempString)

        tempString = 'def cmdModify(CMD,dataSet,sessionIDSet):'
        aList.append(tempString)
        
        tempString = TS + 'result = {}'
        aList.append(tempString)
        tempString = TS + 'errCode = "B0"'
        aList.append(tempString)
        #tempString = TS + 'rtnCMD = CMD[0:2]'
        tempString = TS + 'rtnCMD = CMD'
        aList.append(tempString)
        tempString = TS + 'rtnField = ""'
        aList.append(tempString)
        tempString = TS + 'rtnData = {}'
        aList.append(tempString)
        #tempString = TS + 'lowerCMD = CMD.lower()'
        #aList.append(tempString)
        aList.append("")

        tempString = TS + 'dataValidFlag = True #数据是否有效的标志'
        aList.append(tempString)
        tempString = TS + 'rtnErrMsgList = [] #数据错误原因'
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'try:'
        aList.append(tempString)

        aList.append("")
        
        tempString = TS2 + 'lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)'
        aList.append(tempString)

        tempString = TS2 + 'msgKey = \"applicationMsgKey\"' 
        aList.append(tempString)
        
        tempString = TS2 + 'openID = sessionIDSet.get("openID", "")'
        aList.append(tempString)

        tempString = TS2 + 'roleName = sessionIDSet.get("roleName", "")'
        aList.append(tempString)

        tempString = TS2 + 'tempUserID = sessionIDSet.get("loginID", "")'
        aList.append(tempString)

        aList.append("")

        tempString = TS2 + 'if tempUserID != "":'
        aList.append(tempString)

        tempString = TS3 + 'loginID = tempUserID'
        aList.append(tempString)
        
        aList.append("")

        tempString = TS3 + '#权限检查/功能检测'
        aList.append(tempString)

        aList.append("")
        
        tempString = TS3 + 'if errCode == "B0": #'
        aList.append(tempString)

        tempString = TS4 + '#data validation check'
        aList.append(tempString)

        tempString = TS4 + 'dataValidFlag = True'
        aList.append(tempString)

        aList.append("")
        
        primaryKey = ""
        for data in dataStructure:
            dataType = data.get("dataType")
            fieldName = data.get("fieldName")
            if data.get("isPrimaryKey"):
                primaryKey = fieldName
            dataTypeString = data.get("dataTypeString")
            if dataTypeString.find("AUTO_INCREMENT") >= 0: #自动增加的数据不需要修改
                continue 
            tempString = TS4 + '{0} = dataSet.get("{0}") '.format(fieldName)
            aList.append(tempString)
        
        tempString = TS4 + '#data valid 检查'
        aList.append(tempString)

        aList.append("")

        tempString = TS4 + 'if dataValidFlag:'
        aList.append(tempString)

        tempString = TS5 + '#当前记录获取'
        aList.append(tempString)

        tempString = TS5 + f'recID = dataSet.get("{primaryKey}", "")'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + 'tableName = comMysql.tablename_convertor_{0}()'.format(tableName)
        aList.append(tempString)

        tempString = TS5 + 'currDataList = comMysql.query_{0}(tableName,recID)'. format(tableName)
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + 'if len(currDataList) == 1:'
        aList.append(tempString)

        tempString = TS6 + 'currDataSet = currDataList[0]'
        aList.append(tempString)

        aList.append("")

        tempString = TS6 + '#权限或其他检查'
        aList.append(tempString)

        tempString = TS6 + 'if errCode == "B0": #'
        aList.append(tempString)

        aList.append("")

        tempString = TS7 + 'saveSet = {}'
        aList.append(tempString)

        aList.append("")
        
        for data in dataStructure:
            dataType = data.get("dataType")
            fieldName = data.get("fieldName")
            dataTypeString = data.get("dataTypeString")
            if dataTypeString.find("AUTO_INCREMENT") >= 0: #自动增加的数据不需要修改
                continue 
            tempString = TS7 + 'if {0} != currDataSet.get("{0}") and {0}:'.format(fieldName)
            aList.append(tempString)
            tempString = TS8 + 'saveSet["{0}"] = {0}'.format(fieldName)
            aList.append(tempString)
            aList.append("")

        aList.append("")

        #delFlag
        tempString = TS7 + 'if saveSet:'
        aList.append(tempString)
        tempString = TS7 + TS + '#saveSet["delFlag"] = "0"'
        aList.append(tempString)
        
        tempString = TS8 + 'saveSet["modifyID"] = loginID'
        aList.append(tempString)

        tempString = TS8 + 'saveSet["modifyYMDHMS"] = misc.getTime()'
        aList.append(tempString)

        aList.append("")

        tempString = TS8 + '#保存数据'
        aList.append(tempString)

        tempString = TS8 + 'tableName = comMysql.tablename_convertor_{0}()'.format(tableName)
        aList.append(tempString)

        tempString = TS8 + 'rtn = comMysql.update_{0}(tableName,{1},saveSet)'.format(tableName, primaryKey)
        aList.append(tempString)
        tempString = TS8 + 'rtnData["rtn"] = str(rtn)'
        aList.append(tempString)

        aList.append("")

        tempString = TS8 + 'if rtn < 0:'
        aList.append(tempString)
 
        tempString = TS+TS8 + '_LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")'
        aList.append(tempString)
 
        tempString = TS8 + 'else:'
        aList.append(tempString)
    
        tempString = TS + TS8 + 'if _DEBUG:'
        aList.append(tempString)

        tempString = TS2 + TS8 + 'pass'
        aList.append(tempString)

        tempString = TS2 + TS8 + '_LOG.info(f"D: rtn:{rtn}")'
        aList.append(tempString)
        
        aList.append("")

        tempString = TS8 + 'result = rtnData'
        aList.append(tempString)

        aList.append("")

        tempString = TS6 + 'else:'
        aList.append(tempString)
        tempString = TS7 + '#BT'
        aList.append(tempString)        
        tempString = TS7 + 'errCode = "BT"'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + 'else:'
        aList.append(tempString)
        tempString = TS6 + '#CB'
        aList.append(tempString)        
        tempString = TS6 + 'errCode = "CB"'
        aList.append(tempString)

        aList.append("")

        tempString = TS4 + 'else:'
        aList.append(tempString)
        tempString = TS5 + '#data invalid'
        aList.append(tempString)
        tempString = TS5 + 'errCode = "BA"'
        aList.append(tempString)

        aList.append("")

        #tempString = TS3 + 'else:'
        #aList.append(tempString)
        #tempString = TS4 + '#BT'
        #aList.append(tempString)
        #tempString = TS4 + 'errCode = "BT"'
        #aList.append(tempString)

        aList.append("")

        tempString = TS2 + 'else:'
        aList.append(tempString)
        tempString = TS3 + 'errCode = "B8"'
        aList.append(tempString)

        aList.append("")
        
        #tempString = TS2 + 'rtnCMD = CMD[0:2]'
        tempString = TS2 + 'rtnCMD = CMD'
        aList.append(tempString)
        tempString = TS2 + 'rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)'
        aList.append(tempString)
        tempString = TS2 + 'result["CMD"] = rtnCMD'
        aList.append(tempString)
        tempString = TS2 + 'result["msgKey"] = msgKey'
        aList.append(tempString)
        tempString = TS2 + 'result["MSG"] = rtnSet["MSG"]'
        aList.append(tempString)
        tempString = TS2 + 'result["errCode"] = errCode'
        aList.append(tempString)
        tempString = TS2 + 'result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)'
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'except Exception as e:'
        aList.append(tempString)
        tempString = TS2 + 'errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"'
        aList.append(tempString)
        tempString = TS2 + '_LOG.error(f"{errMsg}, {traceback.format_exc()}")'
        aList.append(tempString)

        aList.append("")
        
        tempString = TS2 + 'rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")'
        aList.append(tempString)

        tempString = TS2 + 'result = rtnSet'
        aList.append(tempString)

        aList.append("")
        tempString = TS + 'return result'
        aList.append(tempString)
        aList.append("")

        result = "\n".join(aList)
    
    return result


#Server REST查询代码
def genCmdQueryCode(tableName, dataStructure):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    TS2 = TS+TS
    TS3 = TS2+TS
    TS4 = TS3+TS
    TS5 = TS4+TS
    TS6 = TS5+TS
    TS7 = TS6+TS
    TS8 = TS7+TS
    TS9 = TS8+TS
    
    nLen = len(dataStructure)
    if nLen > 0:
        #find primaryKey
        primaryKey = ""
        for data in dataStructure:
            if data.get("isPrimaryKey"):
                primaryKey = data.get("fieldName")
                break

        aList = []

        tempString = '#Server REST查询代码'
        aList.append(tempString)

        tempString = 'def cmdQry(CMD,dataSet,sessionIDSet):'
        aList.append(tempString)
        
        tempString = TS + 'result = {}'
        aList.append(tempString)
        tempString = TS + 'errCode = "B0"'
        aList.append(tempString)
        #tempString = TS + 'rtnCMD = CMD[0:2]'
        tempString = TS + 'rtnCMD = CMD'
        aList.append(tempString)
        tempString = TS + 'rtnField = ""'
        aList.append(tempString)
        tempString = TS + 'rtnData = {}'
        aList.append(tempString)
        #tempString = TS + 'lowerCMD = CMD.lower()'
        #aList.append(tempString)
        aList.append("")
        
        tempString = TS + 'dataValidFlag = True #数据是否有效的标志'
        aList.append(tempString)
        tempString = TS + 'rtnErrMsgList = [] #数据错误原因'
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'try:'
        aList.append(tempString)

        aList.append("")
        
        tempString = TS2 + 'lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)'
        aList.append(tempString)
        
        tempString = TS2 + 'msgKey = \"applicationMsgKey\"' 
        aList.append(tempString)
        
        tempString = TS2 + 'openID = sessionIDSet.get("openID", "")'
        aList.append(tempString)

        tempString = TS2 + 'roleName = sessionIDSet.get("roleName", "")'
        aList.append(tempString)

        tempString = TS2 + 'tempUserID = sessionIDSet.get("loginID", "")'
        aList.append(tempString)

        aList.append("")

        tempString = TS2 + 'if tempUserID != "":'
        aList.append(tempString)

        tempString = TS3 + 'loginID = tempUserID'
        aList.append(tempString)
        
        aList.append("")

        tempString = TS3 + '#权限检查'
        aList.append(tempString)

        aList.append("")
        
        tempString = TS3 + 'if errCode == "B0": #'
        aList.append(tempString)

        tempString = TS4 + '#获取查询输入参数'
        aList.append(tempString)

        tempString = TS4 + '{0} = dataSet.get("{0}", "")'.format(primaryKey)
        aList.append(tempString)

        aList.append("")

        tempString = TS4 + '#houseID = dataSet.get("houseID", "")'
        aList.append(tempString)

        aList.append("")

        tempString = TS4 + 'forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记'
        aList.append(tempString)
        aList.append("")

        tempString = TS4 + 'searchOption = dataSet.get("searchOption")'
        aList.append(tempString)
        aList.append("")

        tempString = TS4 + 'mode = dataSet.get("mode", "full")'
        aList.append(tempString)
        aList.append("")

        tempString = TS4 + '#limitNum = dataSet.get("limitNum",0)'
        aList.append(tempString)
        aList.append("")

        tempString = TS4 + '#权限检查/功能检测'
        aList.append(tempString)

        aList.append("")

        tempString = TS4 + 'rightCheckFlag = True'
        aList.append(tempString)

        aList.append("")

        tempString = TS4 + 'if rightCheckFlag:'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + '#生成indexKey'
        aList.append(tempString)

        tempString = TS5 + 'indexKeyDataSet = {} #查询生成index的因素'
        aList.append(tempString)

        tempString = TS5 + 'if {0}:'.format(primaryKey)
        aList.append(tempString)
        tempString = TS6 + 'indexKeyDataSet["{0}"] = {0}'.format(primaryKey)
        aList.append(tempString)

        tempString = TS5 + 'if searchOption:'
        aList.append(tempString)
        tempString = TS6 + 'indexKeyDataSet["searchOption"] = searchOption'
        aList.append(tempString)

        tempString = TS5 + 'if mode:'
        aList.append(tempString)
        tempString = TS6 + 'indexKeyDataSet["mode"] = mode'
        aList.append(tempString)

        aList.append("")
        tempString = TS5 + '#if limitNum:'
        aList.append(tempString)
        tempString = TS6 + '#indexKeyDataSet["limitNum"] = mode'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + 'sessionID = sessionIDSet.get("sessionID", "")'
        aList.append(tempString)
        tempString = TS5 + 'indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) '
        aList.append(tempString)
        tempString = TS5 + 'beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) '
        aList.append(tempString)
        tempString = TS5 + 'endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) '
        aList.append(tempString)

        aList.append("")
        
        tempString = TS5 + '#判断数据是否在缓冲区:'
        aList.append(tempString)
        tempString = TS5 + 'if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:'
        aList.append(tempString)

        aList.append("")

        tempString = TS6 + 'if searchOption:'
        aList.append(tempString)
        tempString = TS7 + 'currDataList = []'
        aList.append(tempString)

        tempString = TS7 + 'tableName = comMysql.tablename_convertor_{0}()'.format(tableName)
        aList.append(tempString)

        tempString = TS7 + 'allDataList = comMysql.query_{0}(tableName,mode = mode)'. format(tableName)
        aList.append(tempString)
        tempString = TS7 + 'allowList = ["description", "label"] #筛选字段'
        aList.append(tempString)
        tempString = TS7 + 'serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)'
        aList.append(tempString)
        tempString = TS7 + 'if serachResultSet["rtn"] == "B0":'
        aList.append(tempString)
        tempString = TS8 + 'currDataList = serachResultSet.get("data", [])'
        aList.append(tempString)
        tempString = TS6 + 'else:'
        aList.append(tempString)

        tempString = TS7 + 'if {0}:'.format(primaryKey)
        aList.append(tempString)

        tempString = TS8 + 'tableName = comMysql.tablename_convertor_{0}()'.format(tableName)
        aList.append(tempString)

        if primaryKey != "":
            tempString = TS8 + 'currDataList = comMysql.query_{0}(tableName,{1},mode = mode)'. format(tableName,primaryKey)
        else:
            tempString = TS8 + 'currDataList = comMysql.query_{0}(tableName,mode = mode)'. format(tableName)
        aList.append(tempString)

        tempString = TS7 + 'else:'
        aList.append(tempString)

        tempString = TS8 + 'tableName = comMysql.tablename_convertor_{0}()'.format(tableName)
        aList.append(tempString)

        tempString = TS8 + 'currDataList = comMysql.query_{0}(tableName)'. format(tableName)
        aList.append(tempString)

        aList.append("")

        tempString = TS6 + 'dataList = []'
        aList.append(tempString)

        aList.append("")

        tempString = TS6 + 'for currDataSet in currDataList:'
        aList.append(tempString)

        tempString = TS7 + 'aSet = {}'
        aList.append(tempString)

        aList.append("")
        
        #需要把文件转移到public domain 
        tempString = TS7 + '#需要把文件转移到public domain'
        aList.append(tempString)
        tempString = TS7 + '#appendixFileID00 =  currDataSet.get("appendixFileID00", "")'
        aList.append(tempString)
        tempString = TS7 + '#appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)'
        aList.append(tempString)

        aList.append("")

        #不同模式的数据输出 
        tempString = TS7 + '#if mode == "full":'
        aList.append(tempString)
        tempString = TS8 + '#aSet["houseID"] = currDataSet.get("houseID", "")'
        aList.append(tempString)

        aList.append("")
        
        for data in dataStructure:
            dataType = data.get("dataType")
            fieldName = data.get("fieldName")
            tempString = TS7 + 'aSet["{0}"] = currDataSet.get("{0}","")'.format(fieldName)
            aList.append(tempString)

        aList.append("")

        tempString = TS7 + 'dataList.append(aSet)'
        aList.append(tempString)

        aList.append("")

        tempString = TS6 + '#临时缓存机制,改进型, 2023/10/16'
        aList.append(tempString)

        tempString = TS6 + 'indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + 'rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + '#rtnData["limitNum"] = limitNum'
        aList.append(tempString)

        aList.append("")

        tempString = TS5 + 'result = rtnData'
        aList.append(tempString)

        aList.append("")

        tempString = TS4 + 'else:'
        aList.append(tempString)
        tempString = TS5 + 'errCode = "BT"'
        aList.append(tempString)

        aList.append("")

        #tempString = TS3 + 'else:'
        #aList.append(tempString)
        #tempString = TS4 + 'errCode = "BT"'
        #aList.append(tempString)

        #aList.append("")

        tempString = TS2 + 'else:'
        aList.append(tempString)
        tempString = TS3 + 'errCode = "B8"'
        aList.append(tempString)

        aList.append("")
        
        #tempString = TS2 + 'rtnCMD = CMD[0:2]'
        tempString = TS2 + 'rtnCMD = CMD'
        aList.append(tempString)
        tempString = TS2 + 'rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)'
        aList.append(tempString)
        tempString = TS2 + 'result["CMD"] = rtnCMD'
        aList.append(tempString)
        tempString = TS2 + 'result["msgKey"] = msgKey'
        aList.append(tempString)
        tempString = TS2 + 'result["MSG"] = rtnSet["MSG"]'
        aList.append(tempString)
        tempString = TS2 + 'result["errCode"] = errCode'
        aList.append(tempString)
        tempString = TS2 + 'result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)'
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'except Exception as e:'
        aList.append(tempString)
        tempString = TS2 + 'errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"'
        aList.append(tempString)
        tempString = TS2 + '_LOG.error(f"{errMsg}, {traceback.format_exc()}")'
        aList.append(tempString)

        aList.append("")
        
        tempString = TS2 + 'rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")'
        aList.append(tempString)

        tempString = TS2 + 'result = rtnSet'
        aList.append(tempString)

        aList.append("")
        tempString = TS + 'return result'
        aList.append(tempString)
        aList.append("")

        result = "\n".join(aList)
    
    return result


#testMysql code 
def genTestMysqlCode(tableName, dataStructure):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    TS2 = TS+TS
    
    aList = []
    
    dataLen = len(dataStructure)
    if dataLen > 0:

        tempString = '#testMysql code '
        aList.append(tempString)

        tempString = 'def test_{0}():'.format(tableName)
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'data = {}'
        aList.append(tempString)

        aList.append("")
        
        primaryKey = dataStructure[0]["fieldName"]
        for data in dataStructure:
            isPrimaryKey = data.get("isPrimaryKey")
            if isPrimaryKey:
                primaryKey = data["fieldName"]
            dataType = data.get("dataType")
            dataTypeString = data.get("dataTypeString")
            fieldName = data.get("fieldName")
            rest = data.get("rest", "")
            pos = rest.upper().find("AUTO_INCREMENT")
            if pos >= 0:
                continue
                
            if dataType == "INT":
                val = random.randint(-100, 100)
            elif dataType == "FLOAT":
                val = float(random.randint(-1000, 10000)/7)
            elif dataType == "CHAR":
                posBegin = dataTypeString.find("(")
                posEnd = dataTypeString.find(")")
                if posBegin >= 0 and posEnd > posBegin:
                    val = dataTypeString[posBegin+1:posEnd]
                else:
                    val = "1"
                nT1 = int(val)
                nLen = random.randint(1, nT1)
                tempList = []
                for i in range(nLen):
                    nT1 = random.randint(97, 122)
                    tempList.append(chr(nT1))
                val = "".join(tempList)
            else:
                val = ""

            tempString = TS + 'data["{0}"] = "{1}"'.format(fieldName, val)
            aList.append(tempString)
            
        aList.append("")

        tempString = TS + 'tableName = comMysql.tablename_convertor_{0}()'.format(tableName)
        aList.append(tempString)

        tempString = TS + 'recID = comMysql.insert_{0}(tableName,data)'.format(tableName)
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'dataList = comMysql.query_{0}(tableName,{1})'.format(tableName, primaryKey)
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'data = {}'
        aList.append(tempString)

        tempString = TS + 'data["{0}"] = "test"'.format(fieldName)
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'rtn = comMysql.update_{0}(tableName,recID,data)'.format(tableName)
        aList.append(tempString)

        aList.append("")

        tempString = TS + 'rtn = comMysql.delete_{0}(tableName,recID)'.format(tableName)
        aList.append(tempString)

        aList.append("")
        
    result = "\n".join(aList)

    return result


# http interface test msg
def genTestMsgCode(tableName, dataStructure):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    TS2 = TS+TS
    aList = []
    
    dataLen = len(dataStructure)
    if dataLen > 0:

        msgList = []

        tempString = '#http interface test msg '
        aList.append(tempString)

        tempString = '"CMD":"{0}"'.format(tableName)
        msgList.append(tempString)

        tempString = '"sessionID":"adminonly"'
        msgList.append(tempString)
        
        for data in dataStructure:
            dataType = data.get("dataType")
            dataTypeString = data.get("dataTypeString")
            fieldName = data.get("fieldName")
            rest = data.get("rest", "")
            pos = rest.upper().find("AUTO_INCREMENT")
            if pos >= 0:
                continue
                
            if dataType == "INT":
                val = random.randint(-100, 100)
            elif dataType == "FLOAT":
                val = float(random.randint(-1000, 10000)/7)
            elif dataType == "CHAR":
                posBegin = dataTypeString.find("(")
                posEnd = dataTypeString.find(")")
                if posBegin >= 0 and posEnd > posBegin:
                    val = dataTypeString[posBegin+1:posEnd]
                else:
                    val = "1"
                nT1 = int(val)
                nLen = random.randint(1, nT1)
                tempList = []
                for i in range(nLen):
                    nT1 = random.randint(97, 122)
                    tempList.append(chr(nT1))
                val = "".join(tempList)
            else:
                val = ""

            tempString = '"{0}":"{1}"'.format(fieldName, val)
            msgList.append(tempString)
            
        aList.append("")

        tempString = "msg = "+"'{"+",".join(msgList)+"}'"

        aList.append(tempString)

        aList.append("")

    result = "\n".join(aList)

    return result


def anaTableData(tableName,  dataList):
    result = []
    #格式整理
    dataStructure = []
    for data in dataList:
        aSet = {}
        try:
            #find primary key
            pos = data.upper().find ("PRIMARY")
            if pos > 0:
                aSet["isPrimaryKey"] = True
            else:
                aSet["isPrimaryKey"] = False

            #分割处理
            aList = data.split(" ")
            aSet["fieldName"]  = aList[0]
            dataTypeString = aList[1].strip()            
            dataTypeString = dataTypeString.strip(",")
            
            aSet["dataTypeString"] = dataTypeString
            aSet["dataType"] = decodeDataType(aList[1])
            
            newList = aList[2:]
            restString = " ".join(newList)
            
            commentString = ""
            pos  = restString.find("#")
            if pos >= 0:
                commentString = restString[pos:]
                restString = restString[0:pos]
            
            pos = restString.find(",")
            if pos >= 0:
                restString = restString[0:pos]
            
            #查找comment部分,存到 text里面
            pos = restString.find("COMMENT")
            if pos >= 0:
                text = restString[pos+8:]
                pos = text.find("'")
                if pos >= 0:
                    text = text[pos+1:-1]
                    aSet["text"] = text
            aSet["rest"] = restString
            aSet["comment"] = commentString
            
            dataStructure.append(aSet)

        except:
            pass
    result  = dataStructure
    return result
    
    
def  generateFuncs(tableName, dataStructure,  primaryKeys = []):
    result = []

    tempString = "\n#mysqlCommon code begin \n"
    result.append(tempString) 
    
    tempString = "#{0} begin ".format(tableName)
    result.append(tempString) 

    #mysqlCommon 表名生成代码
    keysList = []
    tempString = genTableNameConvertorCode(tableName,keysList)
    result.append(tempString) 

    #mysqlCommon 表名解码代码
    tempString = genTableNameDecodeCode(tableName)
    result.append(tempString) 

    #mysqlCommon 建表代码
    tempString = genCreateCode(tableName, dataStructure,  primaryKeys)
    result.append(tempString) 
    
    #mysqlCommon 删表代码
    tempString = genDropCode(tableName,dataStructure)
    result.append(tempString) 

    #mysqlCommon 删除记录代码
    tempString = genDeleteCode(tableName, dataStructure)
    result.append(tempString) 

    #mysqlCommon 增加记录代码
    tempString = genInsertCode(tableName, dataStructure)
    result.append(tempString) 

    #mysqlCommon 修改记录代码
    tempString = genUpdateCode(tableName, dataStructure)
    result.append(tempString) 

    #mysqlCommon 基本记录代码
    tempString = genQueryCode(tableName, dataStructure)
    result.append(tempString) 
    
    tempString = "#{0} end ".format(tableName)
    result.append(tempString) 

    tempString = "\n#mysqlCommon code end \n"
    result.append(tempString) 

    tempString = "\n#http interface code begin \n"
    result.append(tempString) 
    
    #Server REST增加代码
    tempString = genCmdAddCode(tableName, dataStructure)
    result.append(tempString) 

    #Server REST删除代码
    tempString = genCmdDelCode(tableName, dataStructure)
    result.append(tempString) 

    #Server REST修改代码
    tempString = genCmdUpdateCode(tableName, dataStructure)
    result.append(tempString) 

    #Server REST查询代码
    tempString = genCmdQueryCode(tableName, dataStructure)
    result.append(tempString) 

    tempString = "\n#http interface code end \n"
    result.append(tempString) 

    tempString = "\n#testMysql code begin \n"
    result.append(tempString) 

    #mysqlCommon 测试代码
    tempString = genTestMysqlCode(tableName, dataStructure)
    result.append(tempString) 

    tempString = "\n#testMysql code end \n"
    result.append(tempString) 

    tempString = "\n#http interface test msg begin \n"
    result.append(tempString) 

    #http interface test msg 测试代码
    tempString = genTestMsgCode(tableName, dataStructure)
    result.append(tempString) 

    tempString = "\n#http interface test msg end \n"
    result.append(tempString) 
    
    return result

#split sql INSERT INTO `nideshop_user` VALUES (25, '微信用户jdncllp188tj', 'o1PSB4spiAtvSxKmu8Z7GYyiQ8ik', 1, NULL, '2020-05-11 03:32:40', '2020-05-16 05:57:35', '8.8.8.8', NULL, '好人郝事', NULL, '8.8.8.8', 'https://wx.qlogo.cn/mmopen/vi_32/Q0j4TwGTfTJ3njxtiaXvibH0E1UnslmQndvdclj7UOKv8FJ0wUSmMR8ibZ5QpQmIfkoSHqI1ia11XibJNNcz7Sib847g/132', 'o1PSB4spiAtvSxKmu8Z7GYyiQ8ik', NULL, NULL, 0.00, 0.00, 0.00, NULL);


def splitInsertSQL(data):
    sqlStr= ""
    valuesList = []
    beginPos = data.find("(")
    endPos = data.find(")")
    sqlInsert = data[0:beginPos].replace("`", " ")
    valuesStr = data[beginPos+1:endPos]
    aList = valuesStr.split(",")
    for a in aList:
        if a.upper() == "NULL" or a == " NULL":
            a = None
        else:
            pos = a.find("'")
            if pos < 0:
                pos = a.find(".")
                if pos < 0:
                    a = int (a)
                else:
                    a = float(a)
            else:
                a = a.replace("'", "").strip()
        valuesList.append(a)
    nLen = len(valuesList)
    aList = []
    for i in range(nLen):
        aList.append("%s")
    sqlStr = sqlInsert + "({0})".format(",".join(aList))
            
    return sqlStr, valuesList

# insert test code
def genRecordAddCode(dataList):
    result = ""
    TS = ""
    for i in range(indentLen):
        TS+=" "
    TS2 = TS+TS
    aList = []

    tempString = '#已有数据插入函数'
    aList.append(tempString)
    
    tempString = 'def recordsAdd():'
    aList.append(tempString)

    dataLen = len(dataList)
    if dataLen > 0:

        for data in dataList:
            sqlStr, vluesList = splitInsertSQL(data)

            tempString = TS + "sqlStr = \"{0}\"".format(sqlStr)
            aList.append(tempString)

            tempString = TS + "vluesList = {0}".format(vluesList)
            aList.append(tempString)
            
            tempString = TS + "rtn = mysqlDB.executeWrite(sqlStr, tuple(valuesList)))"
            aList.append(tempString)
            
        aList.append("")
    else:
        tempString = TS + "pass"
        aList.append(tempString)

    result = "\n".join(aList)

    return result


def generateRecordsInsert(dataList):
    result = []

    tempString = "\n#insert code begin \n"
    result.append(tempString) 
    
    #code generator 
    tempString = genRecordAddCode(dataList)
    result.append(tempString)     

    tempString = "\n#insert dode end \n"
    result.append(tempString) 

    return result
    
    
def writeCode(fileName, funcsCodeList,insertsCodeList):
    with open (fileName, "w") as hFile:
        tempString = '#! /usr/bin/env python3'
        hFile.write(tempString)
        tempString = '\n#encoding: utf-8\n'
        hFile.write(tempString)
        tempString = '\n#Filename: %s' % fileName
        hFile.write(tempString)
        tempString = '\n#Author: {0}'.format(HEADER_AUTHOR)
        hFile.write(tempString)
        tempString = '\n#E-mail: {0}'.format(HEADER_EMAIL)
        hFile.write(tempString)
        tempString = '\n#Date: %s' %  misc.getHumanTimeStamp()[0:11]
        hFile.write(tempString)
        tempString = '\n#Description: %s 服务接口' %  fileName
        hFile.write(tempString)

        for funcsCode in funcsCodeList:
            hFile.write("\n\n")
            hFile.write(funcsCode)
        hFile.write("\n\n")

        for code in insertsCodeList:
            hFile.write(code)
        hFile.write("\n\n")
            
        
def coderGenerator(tableName,  fieldsList,  primaryKeys = [], insertsList = []):
    dataStructure = anaTableData(tableName,  fieldsList)
    funcsCodeList = generateFuncs(tableName, dataStructure, primaryKeys)
    insertsCodeList = generateRecordsInsert(insertsList)
    fileName = "auto_gen_code_" + tableName + ".py"
    writeCode(fileName,  funcsCodeList,insertsCodeList)
    
    
def handleSQL(fileName):
    with open (fileName, "r",  encoding = "utf-8") as hFile:
        lines = hFile.readlines()
    
    tableCommand = []
    commentSign = "-"
    tableBegin = False
    aList = []
    
    #拼装成单一行SQL命令
    for line in lines:
        newLine = line.strip("\n")
        nLen = len(newLine)
        if nLen > 0:
            if newLine[0] == commentSign:
                #注释忽略
                continue
                
        if tableBegin == False:
            pos = newLine.upper().find("CREATE TABLE")
            if pos >= 0:
                tableBegin = True
                aList = []
                aList.append(newLine)
        else:
            if nLen == 0:
                tableBegin = False
                cmdString = "".join(aList)
                tableCommand.append(cmdString)
            else:
                aList.append(newLine)
    
    #处理sql语句
    tableStructure = []
    for cmdString in tableCommand:
        aSet = {}
        #find tableName
        pos = cmdString.find("(")
        if pos > 0:
            tableName = ""
            temp = cmdString[0:pos]
            restString = cmdString[pos+1:]
            beginPos = temp.find("`")
            if beginPos > 0:
                endPos = temp[beginPos+1:].find("`")
                if endPos > 0:
                    tableName = temp[beginPos+1:beginPos+endPos+1]
            #find fields
            fieldsList = []
            primaryKeys = []
            pos = restString.upper().find("PRIMARY KEY")
            if pos > 0:
                temp = restString[0:pos]
                aList = temp.split(",")
                for a in aList:
                    line = a.strip()
                    line = line.replace("`", '').strip()
                    if len(line) > 0:
                        fieldsList.append(line)
                #find primary key
                restString = restString[pos:]
                pos = restString.find("(")
                if pos > 0:
                    endPos = restString.find(")")
                    if endPos > 0:
                        temp = restString[pos+1:endPos]
                        aList = temp.split(",")
                        for a in aList:
                            line = a.strip()
                            line = line.replace("`", '').strip()
                            primaryKeys.append(line)
                
            if tableName != "":
                aSet["tableName"] = tableName
                aSet["fieldsList"] = fieldsList
                aSet["primaryKeys"] = primaryKeys
                tableStructure.append(aSet)

    #查找insert 语句并生成相关代码
    insertStatmentDataSet = {}
    for data in tableStructure:
        tableName = data.get("tableName", "")
        #查找 'Records of tablename'
        keyString = "Records of {0}".format(tableName)
        recordsOfFindFlag = False
        insertFindFlag = False
        insertStatmentDataSet[tableName] = []

        for line in lines:
            pos = line.find(keyString)
            if pos >= 0:
                recordsOfFindFlag = True
            if recordsOfFindFlag:
                pos = line.upper().find("INSERT INTO")
                if pos >= 0:
                    #insert sql statment found
                    insertStatmentDataSet[tableName].append(line.strip("\n"))
                    insertFindFlag = True
                if insertFindFlag:
                    newLine = line.strip("\n")
                    if len(newLine) == 0:
                        break
                    if newLine[0] == "-":
                        break

    #生成代码
    for data in tableStructure:
        tableName = data.get("tableName", "")
        fieldsList = data.get("fieldsList", [])
        primaryKeys = data.get("primaryKeys", [])
        insertsList =  insertStatmentDataSet.get(tableName)
        coderGenerator(tableName, fieldsList, primaryKeys,insertsList)
   
        
def standardCode():
    tableName = "BLOCK_INFO"
    fieldsList = [
    "blockID CHAR(32) PRIMARY KEY,", 
    "blockName VARCHAR(128) NOT NULL,", 
    "blockPhoto VARCHAR(128) NOT NULL,", 
    "blockDescription VARCHAR(500) NOT NULL,", 
    "contactName CHAR(40) NOT NULL,", 
    "tel CHAR(32) NOT NULL,", 
    "province CHAR(32) NOT NULL,", 
    "city CHAR(32) NOT NULL,", 
    "area CHAR(32) NOT NULL,", 
    "address VARCHAR(200) NOT NULL,", 
    "position VARCHAR(100) NOT NULL,", 
    "regID CHAR(32) NOT NULL,", 
    "regYMDHMS CHAR(16) NOT NULL,",
    "modifyID CHAR(32) NOT NULL,", 
    "modifyYMDHMS CHAR(16) NOT NULL,",
     "delFlag CHAR(1) NOT NULL,"
    #block data end

    ]
    coderGenerator(tableName, fieldsList)


#生成表格文件
def wordTableGenerator(tableName, fieldsList):
    dataStructure = anaTableData(tableName,  fieldsList)
    fileNamePrefix = "word_table_"
    fileName = fileNamePrefix + tableName + ".csv"
    splitChar = "\t"
    try:
        with open(fileName, "w") as hFile:            
            titleLine = "参数" + splitChar + "是否必须" + splitChar + "数据类型" + splitChar + "说明" 
            hFile.write(titleLine)
            hFile.write("\n")
            #必须项目
            strT = "SN" + splitChar + "可选" + splitChar + "数字" + splitChar + "序号,和上一次不同即可" 
            hFile.write(strT)
            hFile.write("\n")
            strT = "sessionID" + splitChar + "是" + splitChar + "字符串" + splitChar + "登录服务后服务器返回的sessionID" 
            hFile.write(strT)
            hFile.write("\n")
            #实际内容
            for data in dataStructure:
                filedName = data.get("fieldName", "")
                dataType = data.get("dataType", "")
                dataTypeString = data.get("dataTypeString", "")
                text = data.get("text", "")
                necessary = "可选"
                strT = str(filedName) + splitChar + str(necessary) + splitChar + str(dataTypeString) + splitChar + str(text) 
                hFile.write(strT)
                hFile.write("\n")
                pass
            #可选其他项目
            strT = "position" + splitChar + "可选" + splitChar + "字符串" + splitChar + "当前位置.是经纬度.{“lng”:”xx1”,”lat”:”xx2”},没有可以是{“lng”:”0”,”lat”:”0”}" 
            hFile.write(strT)
            hFile.write("\n")
            strT = "lang" + splitChar + "可选" + splitChar + "CN/EN" + splitChar + "错误信息语言类型, CN=中文(默认), EN=English" 
            hFile.write(strT)
            hFile.write("\n")
            strT = "YMDHMS" + splitChar + "可选" + splitChar + "字符串" + splitChar + "时间日期，是14个字节字符串例： 20170823102359 或者是unix 时间h" 
            hFile.write(strT)
            hFile.write("\n")
    except:
        pass
    
    


def readFromFile(fileName = "table.txt"):
    tableName = "TABLE"
    fieldsList = []
    #默认表名是文件名
    aList = fileName.split(".")
    if len(aList) > 0:
        tableName = aList[0].strip()

    with open (fileName,"r", encoding = "utf-8") as hFile:
        lines = hFile.readlines()
    
    for line in lines:
        line = line.strip("\n")
        #表名可以用tableName = "table"指定
        line.find("=")
        aList = line.split("=")
        if len(aList) >=2:
            tableName = aList[1].strip()
            continue
        fieldsList.append(line)
    coderGenerator(tableName, fieldsList)
    wordTableGenerator(tableName, fieldsList)


def mysqlStatementHandle():
    fileName = r"D:\StevenLian360\0301. Private Project\gongyijia\project\013. 客户需求\user.sql"
    handleSQL(fileName)
    

def main():
    #mysqlStatementHandle()
    #standardCode()
    readFromFile()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        pass
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
        fileName = sys.argv[1]
        readFromFile(fileName)
    else:
        main()
    



