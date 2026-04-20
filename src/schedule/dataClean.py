#! /usr/bin/env python3
#encoding: utf-8

#Filename: dataClean.py 
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com  
#Date: 2019-10-21
#Description:   服务器数据清理和统计部分

_VERSION="20260419"

_DEBUG=True

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import json
import traceback

#from collections import Counter

import requests
import copy
import pathlib

#global defintion/common var etc.
from common import globalDefinition as comGD

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#common functions(database operation)
from common import redisCommon as comDB

from common import mysqlCommon as comMysql


#common functions(funct operation)
from common import funcCommon as comFC

#from common import codingDecoding as comCD
from common import aliyunOSS as OSS


#setting files
# from config import settings as settings
from config import basicSettings as settings


_processorPID = os.getpid()

HOME_DIR = settings._HOME_DIR

if "_LOG" not in dir() or not _LOG:
    logDir = os.path.join(HOME_DIR, "log")
    _LOG = misc.setLogNew("DATACLEAN", "datacleanlog", logDir)
systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
_LOG.info(f"PID:{_processorPID}, python version:{systemVersion}, main code version:{_VERSION}")  
  
SYS_DEFAULT_AUTO_LOGINID = settings.SYS_DEFAULT_AUTO_LOGINID

ACCOUNT_SERVICE_URL = settings.ACCOUNT_SERVICE_URL

statDataSet = {}

# command part

def getUserInfo(loginID,sessionID):
    result = {}
    url = ACCOUNT_SERVICE_URL
    headers = {'content-type': 'application/json'}

    requestData = {}
    requestData["CMD"] = "A3A0"
    requestData["loginID"] = loginID
    requestData["sessionID"] = sessionID
    try:
        payload = misc.jsonDumps(requestData)
        r = requests.post(url, data = payload, headers = headers)

        if r.status_code == 200:
            rtnData = misc.jsonLoads(r.text)
            errCode = rtnData.get("errCode")
            if errCode == "B0":            
                result["loginID"] = rtnData.get("loginID","")
                result["nickName"] = rtnData.get("nickName","")
                result["realName"] = rtnData.get("realName","")
                result["email"] = rtnData.get("email","")
                result["sex"] = rtnData.get("sex","")
                #设置默认的roleName
                roleName = rtnData.get("roleName","")
                if roleName not in settings.ROLE_CMD_LIST:
                    roleName = settings.accountServiceDefaultRoleName
                result["roleName"] = roleName
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#获取本地用户信息mysql
def getUserInfoMysql(loginID):
    result = {}
    try:
        result = comMysql.getUserInfoMysql(loginID)

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def userSearch(loginIDPrefix,sessionID):
    result = {}
    url = ACCOUNT_SERVICE_URL
    headers = {'content-type': 'application/json'}

    requestData = {}
    requestData["CMD"] = "AGA0"
    requestData["loginID"] = loginIDPrefix
    requestData["sessionID"] = sessionID
    try:
        payload = misc.jsonDumps(requestData)
        r = requests.post(url, data = payload, headers = headers)

        if r.status_code == 200:
            rtnData = misc.jsonLoads(r.text)
            errCode = rtnData.get("errCode")
            if errCode == "B0":            
                result["loginID"] = rtnData.get("loginID","")
                result["nickName"] = rtnData.get("nickName","")
                result["realName"] = rtnData.get("realName","")
                result["email"] = rtnData.get("email","")
                result["sex"] = rtnData.get("sex","")
                #设置默认的roleName
                roleName = rtnData.get("roleName","")
                if roleName not in settings.ROLE_CMD_LIST:
                    roleName = settings.accountServiceDefaultRoleName
                result["roleName"] = roleName
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result

#del files 
_SYS_SERVER_NAME = settings._SYS_SERVER_NAME
FILE_SYSTEM_MODE = settings.FILE_SYSTEM_MODE

def delPermanentFile(fileID, privateFlag = False):
    result = False
    serverName = _SYS_SERVER_NAME
    fileInfoData = {}
    fileInfoData["CMD"] = "F2A0" #删除长期文件服务
    fileInfoData["fileID"] = fileID
    fileInfoData["fileSystem"] = FILE_SYSTEM_MODE
    fileInfoData["privateFlag"] = privateFlag
    fileInfoData["YMDHMS"] = misc.getTime()
    fileInfoData["token"] = comFC.genDigest(settings.GEN_DIGIST_KEY, fileInfoData["CMD"], fileInfoData["YMDHMS"])
    rtnSet = comFC.fileServerRequest(serverName, fileInfoData)
    if rtnSet.get("CMD")[2:4] == "B0":
        result = True

    if _DEBUG:
        _LOG.info(f"DEBUG: delPermanentFile, FILE_SYSTEM_MODE:[{FILE_SYSTEM_MODE}] fileID:[{fileID}] result:[{result}]")
    
    return result


def checkMySqlDataBase():
    #user basic
    _LOG.info(f"checkMySqlDataBase begin ")
    try:

        rtn = comMysql.checkMySqlDataBase()
    
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    _LOG.info(f"checkMySqlDataBase end ")


def redisSave():
    # redis save
    _LOG.info(f"redisSave begin ")
    try:
        #comDB.save() #only local need
        pass

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    
    _LOG.info(f"redisSave end ")


#写入用户数据库 mysql
def writeUser2UserBasic(dataSet,operatorLoginID="system"):
    result = -1
    try:
        loginID = dataSet.get("loginID")
        openID = dataSet.get("regOpenID", "")
        passwd = dataSet.get("passwd", "")
        roleName = dataSet.get("roleName", "")
        nickName = dataSet.get("nickName", "")
        realName = dataSet.get("realName", "")
        gender = dataSet.get("gender", "")
        avatarID = dataSet.get("avatarID", "")
        masterID = dataSet.get("masterID", "")
        mobilePhoneNo = dataSet.get("mobilePhoneNo", "")
        province = dataSet.get("province", "")
        city = dataSet.get("city", "")
        area = dataSet.get("area", "")
        address = dataSet.get("address", "")
        email = dataSet.get("email", "")
        PID = dataSet.get("PID", "")
        photoIDFront = dataSet.get("photoIDFront", "")
        photoIDBack = dataSet.get("photoIDBack", "")
        photoID = dataSet.get("photoID", "")
        delFlag = dataSet.get("delFlag", "")
        activeFlag = dataSet.get("activeFlag", comGD._CONST_YES)
        regID = dataSet.get("regID", "")
        regYMDHMS = dataSet.get("regYMDHMS", "")
        regPosition = dataSet.get("regPosition", "")
        updateYMDHMS = dataSet.get("updateYMDHMS", "")
        lastOpenID = dataSet.get("lastOpenID", "")
        lastLoginYMDHMS = dataSet.get("lastLoginYMDHMS", "")
        modifyID = dataSet.get("modifyID", "")
        modifyYMDHMS = dataSet.get("modifyYMDHMS", "")
        passwdYMDHMS = dataSet.get("passwdYMDHMS", "")

        #extend items begin
        extSessionID = dataSet.get("extSessionID") 
        extCapital = dataSet.get("extCapital") 
        extStartYMDHMS = dataSet.get("extStartYMDHMS") 
        extLeaveYMDHMS = dataSet.get("extLeaveYMDHMS") 
        extJobPosition = dataSet.get("extJobPosition") 
        extDepartment = dataSet.get("extDepartment") 
        extOrgName = dataSet.get("extOrgName") 
        extOrgID = dataSet.get("extOrgID") 
        extInService = dataSet.get("extInService") 
        extJobLabel = dataSet.get("extJobLabel") 
        extJobDetail = dataSet.get("extJobDetail") 
        extBrief = dataSet.get("extBrief") 
        extManualTagList = dataSet.get("extManualTagList") 
        extManagementAreaList = dataSet.get("extManagementAreaList") 
        extMemo = dataSet.get("extMemo") 
        #extend items end

        mysqlDataList = comMysql.queryUserBasic(loginID)
        saveSet = {}
        if len(mysqlDataList) == 1:
            #exist, update
            currDataSet = mysqlDataList[0]
            
            if openID != currDataSet.get("openID"):
                saveSet["openID"] = openID
            if passwd != currDataSet.get("passwd"):
                saveSet["passwd"] = passwd
            if roleName != currDataSet.get("roleName"):
                saveSet["roleName"] = roleName
            if nickName != currDataSet.get("nickName"):
                saveSet["nickName"] = nickName
            if realName != currDataSet.get("realName"):
                saveSet["realName"] = realName
            if gender != currDataSet.get("gender"):
                saveSet["gender"] = gender
            if avatarID != currDataSet.get("avatarID"):
                saveSet["avatarID"] = avatarID
            if masterID != currDataSet.get("masterID"):
                saveSet["masterID"] = masterID
            if mobilePhoneNo != currDataSet.get("mobilePhoneNo"):
                saveSet["mobilePhoneNo"] = mobilePhoneNo
            if province != currDataSet.get("province"):
                saveSet["province"] = province
            if city != currDataSet.get("city"):
                saveSet["city"] = city
            if area != currDataSet.get("area"):
                saveSet["area"] = area
            if address != currDataSet.get("address"):
                saveSet["address"] = address
            if email != currDataSet.get("email"):
                saveSet["email"] = email
            if PID != currDataSet.get("PID"):
                saveSet["PID"] = PID
            if photoIDFront != currDataSet.get("photoIDFront"):
                saveSet["photoIDFront"] = photoIDFront
            if photoIDBack != currDataSet.get("photoIDBack"):
                saveSet["photoIDBack"] = photoIDBack
            if photoID != currDataSet.get("photoID"):
                saveSet["photoID"] = photoID
            if delFlag != currDataSet.get("delFlag"):
                saveSet["delFlag"] = delFlag
            if activeFlag != currDataSet.get("activeFlag"):
                saveSet["activeFlag"] = activeFlag
            if regYMDHMS != currDataSet.get("regYMDHMS"):
                saveSet["regYMDHMS"] = regYMDHMS
            if regID != currDataSet.get("regID"):
                saveSet["regID"] = regID
            if regPosition != currDataSet.get("regPosition"):
                saveSet["regPosition"] = regPosition
            if updateYMDHMS != currDataSet.get("updateYMDHMS"):
                saveSet["updateYMDHMS"] = updateYMDHMS
            if lastOpenID != currDataSet.get("lastOpenID"):
                saveSet["lastOpenID"] = lastOpenID
            if lastLoginYMDHMS != currDataSet.get("lastLoginYMDHMS"):
                saveSet["lastLoginYMDHMS"] = lastLoginYMDHMS
            if modifyID != currDataSet.get("modifyID"):
                saveSet["modifyID"] = modifyID
            if modifyYMDHMS != currDataSet.get("modifyYMDHMS"):
                saveSet["modifyYMDHMS"] = modifyYMDHMS
            if passwdYMDHMS != currDataSet.get("passwdYMDHMS"):
                saveSet["passwdYMDHMS"] = passwdYMDHMS

            #extend items begin
            if extSessionID != currDataSet.get("extSessionID") and extSessionID:
                saveSet["extSessionID"] = extSessionID

            if extCapital != currDataSet.get("extCapital") and extCapital:
                saveSet["extCapital"] = extCapital

            if extStartYMDHMS != currDataSet.get("extStartYMDHMS") and extStartYMDHMS:
                saveSet["extStartYMDHMS"] = extStartYMDHMS

            if extLeaveYMDHMS != currDataSet.get("extLeaveYMDHMS") and extLeaveYMDHMS:
                saveSet["extLeaveYMDHMS"] = extLeaveYMDHMS

            if extJobPosition != currDataSet.get("extJobPosition") and extJobPosition:
                saveSet["extJobPosition"] = extJobPosition

            if extDepartment != currDataSet.get("extDepartment") and extDepartment:
                saveSet["extDepartment"] = extDepartment

            if extOrgName != currDataSet.get("extOrgName") and extOrgName:
                saveSet["extOrgName"] = extOrgName

            if extOrgID != currDataSet.get("extOrgID") and extOrgID:
                saveSet["extOrgID"] = extOrgID

            if extInService != currDataSet.get("extInService") and extInService:
                saveSet["extInService"] = extInService

            if extJobLabel != currDataSet.get("extJobLabel") and extJobLabel:
                saveSet["extJobLabel"] = extJobLabel

            if extJobDetail != currDataSet.get("extJobDetail") and extJobDetail:
                saveSet["extJobDetail"] = extJobDetail

            if extBrief != currDataSet.get("extBrief") and extBrief:
                saveSet["extBrief"] = extBrief

            if extManualTagList != currDataSet.get("extManualTagList") and extManualTagList:
                saveSet["extManualTagList"] = extManualTagList

            if extManagementAreaList != currDataSet.get("extManagementAreaList") and extManagementAreaList:
                saveSet["extManagementAreaList"] = extManagementAreaList

            if extMemo != currDataSet.get("extMemo") and extMemo:
                saveSet["extMemo"] = extMemo

            #extend items end
        
            if saveSet:
                saveSet["modifyID"] = operatorLoginID
                saveSet["modifyYMDHMS"] = misc.getTime()

                result = comMysql.updateUserBasic(loginID, saveSet)
                if result < 0:
                    _LOG.warning(f"update loginID:{loginID},{saveSet}")
                
        else:
            saveSet["openID"] = openID
            saveSet["passwd"] = passwd
            saveSet["roleName"] = roleName
            saveSet["nickName"] = nickName
            saveSet["realName"] = realName
            saveSet["gender"] = gender
            saveSet["avatarID"] = avatarID
            saveSet["masterID"] = masterID
            saveSet["mobilePhoneNo"] = mobilePhoneNo
            saveSet["province"] = province
            saveSet["city"] = city
            saveSet["area"] = area
            saveSet["address"] = address
            saveSet["email"] = email
            saveSet["PID"] = PID
            saveSet["photoIDFront"] = photoIDFront
            saveSet["photoIDBack"] = photoIDBack
            saveSet["photoID"] = photoID
            saveSet["delFlag"] = delFlag
            saveSet["activeFlag"] = activeFlag
            saveSet["regYMDHMS"] = regYMDHMS
            saveSet["regID"] = regID
            saveSet["regPosition"] = regPosition
            saveSet["updateYMDHMS"] = updateYMDHMS
            saveSet["lastOpenID"] = lastOpenID
            saveSet["lastLoginYMDHMS"] = lastLoginYMDHMS
            saveSet["modifyID"] = modifyID
            saveSet["modifyYMDHMS"] = modifyYMDHMS
            saveSet["passwdYMDHMS"] = passwdYMDHMS
    
            #extend items begin
            saveSet["extSessionID"] = extSessionID
            saveSet["extCapital"] = extCapital               
            saveSet["extStartYMDHMS"] = extStartYMDHMS
            saveSet["extLeaveYMDHMS"] = extLeaveYMDHMS
            saveSet["extJobPosition"] = extJobPosition
            saveSet["extDepartment"] = extDepartment
            saveSet["extOrgName"] = extOrgName
            saveSet["extOrgID"] = extOrgID
            saveSet["extInService"] = extInService
            saveSet["extJobLabel"] = extJobLabel
            saveSet["extJobDetail"] = extJobDetail
            saveSet["extBrief"] = extBrief
            saveSet["extManualTagList"] = extManualTagList
            saveSet["extManagementAreaList"] = extManagementAreaList
            saveSet["extMemo"] = extMemo

            #extend items end
    
            saveSet["regID"] = operatorLoginID
            saveSet["regYMDHMS"] = misc.getTime()

            result = comMysql.insertUserBasic(loginID, saveSet)
            if result < 0:
                _LOG.warning(f"insert loginID:{loginID},{saveSet}")

    except Exception as e:
        errMsg = f"PID: {_processorPID},dataSet:{dataSet},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


'''
这个是基于redis直接存取的用户同步方式
只适用于accountService使用同一个redis分区的情况
'''
def cmpUserBasicInfoRedis():
    nextPos = 0
    _LOG.info(f"cmpUserBasicInfo begin")

    #sync from redis -> mysql
    while True:
        nextPos, keysList = comDB.scanUserAllInfo(nextPos)
        for key in keysList:
            if isinstance(key,bytes):
                key = key.decode()
            aList = key.split(".")
            if len(aList) != 3:
                continue
            userID = aList[2]
            userInfoSet = comDB.getUserAllInfo(userID)
            rtn = writeUser2UserBasic(userInfoSet)
            _LOG.info(f"D: writeUser2UserBasic,rtn: {rtn},userID:{userID}")
            
        if nextPos <= 0:
            break

    #sync from mysql -> redis
    mysqlUserIDInfoList = comMysql.queryUserBasic(mode="short")
    for userInfo in mysqlUserIDInfoList:
        loginID = userInfo.get("loginID")
        if loginID:
            if not comDB.chkUserExist(loginID):
                comMysql.deleteUserBasic(loginID)
                _LOG.info(f"D: delete user in mysql,loginID: {loginID},userInfo:{userInfo}")

    _LOG.info(f"cmpUserBasicInfo end ")


'''
这个是基于 accountService 存取的用户的同步方式
'''
def cmpUserBasicInfo():
    nextPos = 0
    _LOG.info(f"cmpUserBasicInfo begin")
    try:

        #sync from account service -> mysql
        while True:
            nextPos, keysList = comDB.scanUserAllInfo(nextPos)
            for key in keysList:
                if isinstance(key,bytes):
                    key = key.decode()
                aList = key.split(".")
                if len(aList) != 3:
                    continue
                userID = aList[2]
                userInfoSet = comDB.getUserAllInfo(userID)
                rtn = writeUser2UserBasic(userInfoSet)
                _LOG.info(f"D: writeUser2UserBasic,rtn: {rtn},userID:{userID}")
                
            if nextPos <= 0:
                break

        #sync from mysql -> redis
        mysqlUserIDInfoList = comMysql.queryUserBasic(mode="short")
        for userInfo in mysqlUserIDInfoList:
            loginID = userInfo.get("loginID")
            if loginID:
                if not comDB.chkUserExist(loginID):
                    comMysql.deleteUserBasic(loginID)
                    _LOG.info(f"D: delete user in mysql,loginID: {loginID},userInfo:{userInfo}")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    _LOG.info(f"cmpUserBasicInfo end ")


def cmpUserWechatOpenID():
    nextPos = 0
    _LOG.info(f"cmpUserWechatOpenID begin")
    try:
        while True:
            nextPos, keysList = comDB.scanUserOpenIDList(nextPos)
            for key in keysList:
                aList = key.split(".")
                if len(aList) != 3:
                    continue
                userID = aList[2]
                infoList = comDB.getUserOpenIDList(userID)
                mysqlInfoList = comMysql.queryUserWechatCode(userID)
                #add
                for openID in infoList:
                    findFlag = False
                    for b in mysqlInfoList:
                        bOpenID = b.get("openID")
                        if openID == bOpenID:
                            findFlag = True
                            break
                    if findFlag == False:
                        saveSet = {}
                        saveSet["loginID"] = userID
                        saveSet["openID"] = openID
                        saveSet["YMDHMS"] = misc.getTime()
                        comMysql.insertUserWechatCode(userID, saveSet)
                        _LOG.warning(f"cmpUserWechatOpenID loginID:{userID} no openID:{openID}, insert")
                #remove
                for b in mysqlInfoList:
                    openID = b.get("openID")
                    for a in infoList:
                        if openID in a:
                            findFlag = True
                    if findFlag == False:
                        comMysql.deleteUserWechatCode(userID, openID)
                        _LOG.warning(f"cmpUserWechatOpenID openID:{openID} does not belong to loginID:{userID}, deleted")
                
            if nextPos <= 0:
                break
                    
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    _LOG.info(f"cmpUserWechatOpenID end ")


def cmpRedisMysql():
    _LOG.info(f"cmpRedisMysql begin")    
    try:
        #check user basic
        _LOG.info("check user basic")
        # cmpUserBasicInfo()
        cmpUserBasicInfoRedis()
               
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    _LOG.info(f"cmpRedisMysql end ")
    

#清除过期文件
def cleanDataFiles():
    _LOG.info(f"cleanDataFiles begin")

    try:    

        #clean file based on redis , 2 days 
        delDays = 2
        
        YMD = misc.getPassday(delDays)
        YMDHMS = YMD + "000000"
        count = 0

        dataSet = comDB.getAllFilesInfo()
        for k, v in dataSet.items():
            description = v.get("description")
            fileName = v.get("fileName")
            fileUrl = v.get("fileUrl")
            serverName = v.get("serverName")
            if serverName == None:
                serverName = settings._SYS_SERVER_NAME
            fileSystem = v.get("fileSystem")
            if fileSystem == None:
                fileSystem = settings.FILE_SYSTEM_MODE
            if _DEBUG:
                _LOG.info(f"serverName:{serverName}, fileSystem:{fileSystem},")
            uploadYMDHMS = v.get("uploadYMDHMS", misc.getTime())
            
            #删除2天之前的图片
            if uploadYMDHMS < YMDHMS:
                if serverName == settings._SYS_SERVER_NAME:
                    requestSet = {}
                    requestSet["CMD"] = "F1A0" #删除临时文件
                    requestSet["description"] = description
                    requestSet["fileName"] = fileName
                    requestSet["fileUrl"] = fileUrl
                    requestSet["fileID"] = fileUrl
                    requestSet["serverName"] = serverName
                    requestSet["fileSystem"] = fileSystem
                    
                    requestSet["YMDHMS"] = misc.getTime()
                    requestSet["token"] = comFC.genDigest(settings.GEN_DIGIST_KEY, requestSet["CMD"], requestSet["YMDHMS"])        
                    
                    rtnSet = comFC.fileServerRequest(serverName, requestSet)
                    
                    count +=1

                    if rtnSet:
                        if rtnSet.get("CMD")[2:4] == "B0":
                            comDB.delFileInfo(k)       
                        else:
                            _LOG.warning(f"cleanDataFiles: {requestSet}")
                else:
                    comDB.delFileInfo(k)       
                    _LOG.warning(f"cleanDataFiles: {k}")

        _LOG.info(f"cleanDataFiles F1A0 {count} files")
        
        #遍历文件目录
        delDays = 3

        YMD = misc.getPassday(delDays)
        YMDHMS = YMD + "000000"
        count = 0
        
        #删除三天之前的文件
        tempFileDir =  settings.LOCAL_FILE_SERVER_BASE
        filePathList = misc.getFileList(tempFileDir)
        for filePath in filePathList:
            createTime = misc.getFileCreatTime(filePath)
            if createTime < YMDHMS:
                os.remove(filePath)
                count += 1

        _LOG.info(f"cleanDataFiles remove {count} files")
        
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    _LOG.info(f"cleanDataFiles end")    


#清除redis过期键值
def cleanRedisKeys():
    _LOG.info(f"cleanRedisKeys begin")        

    cleanKeysPrefix = ["BUFFER.DATA*","USER.sessionIDList*","STAT.IP*"]
    limitNum = 100000
    for keyPrefix in cleanKeysPrefix:
        try:
            rtn = comDB.scanLimitKeys(keyPrefix,limitNum)
            _LOG.info(f"keyPrefix:{keyPrefix}")        
            
        except:
            pass
    _LOG.info(f"cleanRedisKeys end")        


#检查并删除过期数据
def deleteOutdateRegularTables():
    _LOG.info(f"deleteOutdateRegularTables begin")

    try:

        currYMDHMS = misc.getTime()
        currYear = currYMDHMS[0:4]
        currYM = currYMDHMS[0:6]

        deleteDays = comGD._DEF_STOCK_KEEP_HISTORY_DATA_DAYS + 365 #保留5年+1年
        deleteYMD = misc.getPassday(deleteDays)
        deleteYear = deleteYMD[0:4]
        deleteYM = deleteYMD[0:6]
        deleteYMDHMS = deleteYMD + "000000"
        deleteDate = misc.YMD2HumanDate(deleteYMD)
        _LOG.info(f"deleteOutdateRegularTables deleteDate:{deleteDate}")
        
        #首先获取tabaleName
        # currTableNameList = comMysql.getCurrTableNames()    

        #首先处理历史数据,删除过期数据       
        periods = ["day","week","month"]
        adjusts = ["","hfq","qfq"]
        for period in periods:
            for adjust in adjusts:
                _LOG.info(f"clean stock_history_data periods:{period},adjust:{adjust},deleteDate:{deleteDate}")
                tableName = comMysql.tablename_convertor_stock_history_data(period,adjust)
                dataList = comMysql.query_stock_history_data(tableName,end_date=deleteDate)
                for data in dataList:
                    recID = data.get("id")
                    dataDate = data.get("date")
                    rtn = comMysql.delete_stock_history_data(tableName,recID)
                    _LOG.info(f"   - rtn:{rtn},recID:{recID},date:{dataDate}")

        #删除过期的industry 数据
        periods = ["day","week","month"]
        # adjusts = ["","hfq","qfq"]
        for period in periods:
            # for adjust in adjusts:
                _LOG.info(f"clean industry_history_data periods:{period},deleteDate:{deleteDate}")
                tableName = comMysql.tablename_convertor_industry_history_data(period)
                dataList = comMysql.query_industry_history_data(tableName,end_date=deleteDate)
                for data in dataList:
                    recID = data.get("id")
                    dataDate = data.get("date")
                    if recID:
                        rtn = comMysql.delete_industry_history_data(tableName,recID)
                        _LOG.info(f"   - rtn:{rtn},recID:{recID},date:{dataDate}")
                
        #删除过期的technical_indicators 数据
        periods = ["day","week","month"]
        adjusts = ["","hfq","qfq"]
        for period in periods:
            for adjust in adjusts:
                _LOG.info(f"clean technical_indicators periods:{period},adjust:{adjust},deleteDate:{deleteDate}")
                tableName = comMysql.tablename_convertor_technical_indicators(period,adjust)
                dataList = comMysql.query_technical_indicators(tableName,end_date=deleteDate)
                for data in dataList:
                    recID = data.get("id")
                    dataDate = data.get("date")
                    if recID:
                        rtn = comMysql.delete_technical_indicators(tableName,recID)
                        _LOG.info(f"   - rtn:{rtn},recID:{recID},date:{dataDate}")

        #删除过期的technical_signal 数据
        _LOG.info(f"clean technical_signal deleteDate:{deleteDate}")
        tableName = comMysql.tablename_convertor_technical_signal()
        dataList = comMysql.query_technical_signal(tableName,end_date=deleteDate)
        for data in dataList:
            recID = data.get("id")
            dataDate = data.get("date")
            if recID:
                rtn = comMysql.delete_technical_signal(tableName,recID)
                _LOG.info(f"   - rtn:{rtn},recID:{recID},date:{dataDate}")               

        #删除过期的data_check_log 数据
        _LOG.info(f"clean data_check_log deleteDate:{deleteDate}")
        tableName = comMysql.tablename_convertor_data_check_log()
        dataList = comMysql.query_data_check_log(tableName,end_date=deleteDate)
        for data in dataList:
            recID = data.get("id")
            dataDate = data.get("date")
            if recID:
                rtn = comMysql.delete_data_check_log(tableName,recID)
                _LOG.info(f"   - rtn:{rtn},recID:{recID},date:{dataDate}")            

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    _LOG.info(f"deleteOutdateRegularTables end")


# 清理hwinfo
def cleanHWInfo():
    result = 0
    
    _LOG.info(f"cleanHWInfo begin")        

    delDays = 30 #保留30天数据

    YMDHMS = misc.getPassday(delDays) + "000000"

    try:
        tableName = comMysql.tablename_convertor_hwinfo_report_record()
        dataList = comMysql.query_hwinfo_report_record(tableName,endYMDHMS=YMDHMS)
        for data in dataList:
            recID = data.get("recID")
            YMDHMS = data.get("YMDHMS")
            rtn = comMysql.delete_hwinfo_report_record(tableName,recID)
            _LOG.info(f"   - rtn:{rtn},recID:{recID},YMDHMS:{YMDHMS}")
            pass
        result = len(dataList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")    

    _LOG.info(f"cleanHWInfo end")        

    return result


def main():
    checkMySqlDataBase()   
  
    # redisSave()

    cmpRedisMysql()
    cleanRedisKeys()
    cleanDataFiles()
    cleanHWInfo()
    deleteOutdateRegularTables()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
    main()    



