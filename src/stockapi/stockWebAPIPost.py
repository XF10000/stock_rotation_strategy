#!/usr/bin/env python3
#encoding: utf-8

#Filename: ntnWebAPIPost.py 
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com/xie_frank@163.com
#Date: 2023-06-29
#Description:  stock web api


_VERSION="20260203"


import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

import platform
        
# import json
# import copy
import traceback
# import random
# import uuid

import pathlib
import requests

import subprocess
import shutil

# from xpinyin import Pinyin

#global defintion/common var etc.
from common import globalDefinition as comGD

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#common functions(funct operation)
from common import funcCommon as comFC
from common import redisCommon as comDB

from common import mysqlCommon as comMysql

from common import aliyunOSS as OSS
# from common import tencentCOS as COS

#setting files
from config import basicSettings as settings

#
# from config import swDistributorSettings as swDistributorSettings

_processorPID = os.getpid()

HOME_DIR = settings._HOME_DIR    

auto_increment_default_value = 10000

if __name__ != "__main__":
    _LOG = "" #上级已经有_LOG设置的情况
    
else:
    if "_LOG" not in dir() or not _LOG:
        logDir = os.path.join(HOME_DIR, "log")
        _LOG = misc.setLogNew(comGD._DEF_LOG_STOCK_WEBAPI_TITLE, comGD._DEF_LOG_STOCK_WEB_API_NAME) #modify here

    systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
    _LOG.info(f"PID:{_processorPID}, python version:{systemVersion}, main code version:{_VERSION}")

_DEBUG = settings._DEBUG

useQueryBufferFlag = False

_SYS_SERVER_NAME = settings._SYS_SERVER_NAME

FILE_SYSTEM_MODE = settings.FILE_SYSTEM_MODE
FASTDFS_SERVER_PATH = settings.FASTDFS_SERVER_PATH
LOCAL_FILE_SERVER_PATH = settings.LOCAL_FILE_SERVER_PATH
LOCAL_FILE_SERVER_BASE = settings.LOCAL_FILE_SERVER_BASE
LOCAL_FILE_TEMP_WEB_DIR = 'web/'
LOCAL_TEMP_FILE_PATH_DIR = LOCAL_FILE_SERVER_BASE + "output"


gSN = 0

# _DEBUG_RIGHT_CHECK = True #是否做权限检查, 正式上线是False

ACCOUNT_SERVICE_URL = settings.ACCOUNT_SERVICE_URL

# SWUPGRADE_SERVICE_URL = settings.SWUPGRADE_SERVICE_URL

# DEVICE_MENU_ICON_SERVER = settings.DEVICE_MENU_ICON_SERVER
FILE_SERVER_URL = settings.FILE_SERVER_URL


gSourceServerAddr = ""

#function part

# command part begin

#考虑到部分查询数据较多, 因此查询结果先缓存到redis,然后根据用户要求取出
#考虑到查询性能, 这个存放到redis的过程是一个同步代码
#利用sessionID和CMD构成key,保证一个进程一类命令合用一个缓存, 避免浪费
#改进的方式是同一类查询结果公用一个缓存, 有时间限制,自动删除
#这个是旧的函数,后续添加几个新函数
def putQueryResult(CMD, sessionID, dataList, indexKeyDataSet={}, overwriteFlag = True):
    # if indexKeyDataSet:
    #     aList = []
    #     keys = list(indexKeyDataSet.keys())
    #     keys.sort()
    #     for k in keys:
    #         v = indexKeyDataSet[k]
    #         aList.append(str(k))
    #         aList.append(str(v))
    #     strT = "".join(aList)
    #     indexKey = comFC.genDigest(CMD, strT)
    # else:
    #     indexKey = comFC.genDigest(CMD, sessionID)
    # indexKey = CMD + "_" + indexKey #方便未来区分

    indexKey = genBufferIndexKey(CMD,sessionID,indexKeyDataSet)

    #同步代码部分
    if overwriteFlag:
        rtn =  comDB.putAllDataBuffer(indexKey, dataList)
    else:
        if not comDB.chkBufferExist(indexKey):
            rtn =  comDB.putAllDataBuffer(indexKey, dataList)

    result = indexKey
    return result


def getQueryResult(indexKey, beginNum = 0,  endNum = -1):
    dataExist = comDB.chkBufferExist(indexKey)
    dataList = comDB.getDataBuffer(indexKey,  beginNum,  endNum)
    return dataExist, dataList


#这个是生成buffer的index key
def genBufferIndexKey(CMD,sessionID,indexKeyDataSet):
    if indexKeyDataSet:
        aList = []
        keys = list(indexKeyDataSet.keys())
        keys.sort()
        for k in keys:
            v = indexKeyDataSet[k]
            aList.append(str(k))
            aList.append(str(v))
        strT = "".join(aList)
        indexKey = comFC.genDigest(CMD, strT)
    else:
        indexKey = comFC.genDigest(CMD, sessionID)

    indexKey = CMD + "_" + indexKey #方便未来区分

    return indexKey


#判断缓冲是否存在
def chkBufferExist(indexKey):
    return comDB.chkBufferExist(indexKey)


#获取缓冲区数据长度
def getBufferDataLen(indexKey):
    return comDB.getBufferDataLen(indexKey)


#把数据存储在指定的缓冲区
def putQuery2Buffer(indexKey,dataList,overwriteFlag = True):
    #同步代码部分
    if overwriteFlag:
        rtn =  comDB.putAllDataBuffer(indexKey, dataList)
    else:
        if not comDB.chkBufferExist(indexKey):
            rtn =  comDB.putAllDataBuffer(indexKey, dataList)

    result = indexKey
    return result


#获取缓冲数据
def getQueryBuffer(indexKey, beginNum = 0,  endNum = -1):
    bufferTotal = comDB.getBufferDataLen(indexKey)
    dataList = comDB.getDataBuffer(indexKey,  beginNum,  endNum)
    return bufferTotal, dataList


#获取缓冲数据
def getQueryBufferComplte(indexKey, beginNum = 0,  endNum = -1):
    result = {}
    bufferTotal = comDB.getBufferDataLen(indexKey)
    dataList = comDB.getDataBuffer(indexKey,  beginNum,  endNum)

    result["indexKey"] = indexKey

    result["total"] = bufferTotal
    result["beginNum"]  = str(beginNum)

    if endNum >= bufferTotal:
        endNum = bufferTotal-1 #java/c rule, not python rule
    if beginNum > endNum:
        beginNum = 0
    result["endNum"]  = str(endNum)

    if bufferTotal > 0:
        result["data"]  = dataList
    else:
        result["data"]  = []

    return result

#buffer相关, end


#upload file to aliyun oss/tencent cos begin
def urlSaveFileUpload(fileID):
    result = fileID

    if fileID[0:4] == "http":
        #download file
        tempFileName = comFC.genTempFileName()
        tempFileSize = comFC.downloadFile(fileID, tempFileName)
        if tempFileSize > 0:
            tempFileInfo = comFC.sendFile(tempFileName)
            result = tempFileInfo.get("fileUrl", "") 

    return result


#copy file to private network, and change the url, new version
def save2newLocation(fileID,  objectName=None, requestType = "", prefix = "", 
                     privateFlag = False, compressFlag = comGD._CONST_NO):
    result = fileID
    try:

        fileInfoData = {}

        fileInfoData["CMD"] = "F0A0"
        fileInfoData["serverName"] = _SYS_SERVER_NAME

        fileInfoData["fileID"] = fileID
        if objectName:
            fileInfoData["objectName"] = objectName
        if requestType:
            fileInfoData["requestType"] = requestType
        if prefix:
            fileInfoData["prefix"] = prefix
        fileInfoData["privateFlag"] = privateFlag
        fileInfoData["compressFlag"] = compressFlag

        fileInfoData["YMDHMS"] = misc.getTime()

        fileInfoData["token"] = comFC.genDigest(settings.GEN_DIGIST_KEY, fileInfoData["CMD"], fileInfoData["YMDHMS"])

        rtnData = comFC.fileServerRequest(_SYS_SERVER_NAME, fileInfoData)
        if rtnData:
            if rtnData.get("errCode") == "B0":
                fileUrl = rtnData.get("fileUrl")
                if fileUrl:
                    result = fileUrl
                pass

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result
  
#copy file to private network, and generate thumbnail photo and change the url, 必须在 save2newLocation之前调用
def generateThumbnail(fileID,  objectName=None,prefix = "", privateFlag = False):
    requestType = comGD._DEF_FILE_REQUEST_TYPE_THUMBNAIL
    return save2newLocation(fileID, objectName, requestType,  prefix, privateFlag)


#上传文件并生成相关保存的文件ID 和 缩略图
def save2newLocationWithThumbail(fileID,  objectName=None, requestType = "", prefix = "",  
                                 privateFlag = False, compressFlag = comGD._CONST_NO):
    thumbnailID = generateThumbnail(fileID, objectName, prefix)
    # if compressFlag == comGD._CONST_YES:
    #     fileID = save2newLocation(fileID=fileID, objectName=objectName, requestType=requestType,  
    #                               prefix=prefix,  privateFlag = privateFlag , compressFlag = compressFlag)
    # else:
    #     fileID = save2newLocation(fileID=fileID, objectName=objectName, requestType=requestType,  
    #                               prefix=prefix,  privateFlag = privateFlag , compressFlag = compressFlag )
    fileID = save2newLocation(fileID=fileID, objectName=objectName, requestType=requestType,  
                                prefix=prefix,  privateFlag = privateFlag , compressFlag = compressFlag )
        
    return (fileID,  thumbnailID)
    

#del files 
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


#把文件ID转成临时url
def getTempLocation(fileID, privateFlag = True,localAccess = False,localAddress = False,targetFileName="",sourceServerAddr=""):
    global gSourceServerAddr
    result = fileID
    try:
        serverName = _SYS_SERVER_NAME

        fileInfoData = {}
        fileInfoData["CMD"] = "F7A0" #把文件转存到本地临时目录
        fileInfoData["fileID"] = fileID
        fileInfoData["fileSystem"] = FILE_SYSTEM_MODE
        fileInfoData["privateFlag"] = privateFlag
        fileInfoData["localAccess"] = localAccess
        fileInfoData["localAddress"] = localAddress
        fileInfoData["targetFileName"] = targetFileName
        if gSourceServerAddr:
            fileInfoData["sourceServerAddr"] = gSourceServerAddr
        else:
            fileInfoData["sourceServerAddr"] = sourceServerAddr

        fileInfoData["YMDHMS"] = misc.getTime()
        fileInfoData["token"] = comFC.genDigest(settings.GEN_DIGIST_KEY, fileInfoData["CMD"], fileInfoData["YMDHMS"])
        rtnSet = comFC.fileServerRequest(serverName, fileInfoData)
        if rtnSet:
            if rtnSet.get("CMD")[2:4] == "B0":
                result = rtnSet.get("fileUrl")
                if result == None:
                    result = ""

        if _DEBUG:
            _LOG.info(f"DEBUG: getTempLocation, FILE_SYSTEM_MODE:[{FILE_SYSTEM_MODE}] fileID:[{fileID}] result:[{result}]")
    
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#获取文件信息
def getFileInfo(fileID):
    result = fileID
    serverName = _SYS_SERVER_NAME
    
    fileInfoData = {}
    fileInfoData["CMD"] = "F6A0" #获取文件信息
    fileInfoData["fileID"] = fileID

    fileInfoData["YMDHMS"] = misc.getTime()
    fileInfoData["token"] = comFC.genDigest(settings.GEN_DIGIST_KEY, fileInfoData["CMD"], fileInfoData["YMDHMS"])
    rtnSet = comFC.fileServerRequest(serverName, fileInfoData)
    if rtnSet.get("CMD")[2:4] == "B0":
        result = rtnSet
        try:
            del result["CMD"]
            del result["errCode"]
            del result["MSG"]
        except:
            pass

    if _DEBUG:
        _LOG.info(f"DEBUG: getTempLocation, FILE_SYSTEM_MODE:[{FILE_SYSTEM_MODE}] fileID:[{fileID}] result:[{result}]")
    
    return result


#upload file to aliyun oss/tencent cos end

#getFileDataInJson
def getFileDataInJson(fileID):
    result = {}

    localFileName = getTempLocation(fileID, localAccess = True,localAddress=True)

    data = misc.loadJsonData(localFileName,"dict")
    if data:
        result = data

    return result


#保存数据到文件存储
def saveData2FileStorage(fileID,data):
    result = None
    if data:
        #保存到临时文件目录
        tempFileName = os.path.join(LOCAL_TEMP_FILE_PATH_DIR,fileID)
        tempFileName = pathlib.Path(tempFileName).as_posix()

        try:
            rtn = 0
            with open (tempFileName,"wb") as hFile:
                if not isinstance(data,bytes):
                    data = data.encode()
                rtn = hFile.write(data)

            #发送文件到文件系统(必须等文件写完毕,再发送)
            if rtn:
                tempFileInfo = comFC.sendFile(tempFileName)
                fileUrl = tempFileInfo.get("fileUrl", "") 
                rtnData = getFileInfo(fileUrl)

                if rtnData:
                    #保存文件到文件系统(长久存储)
                    fileID = save2newLocation(fileUrl, fileID,  privateFlag = True)
                    if fileID:
                        result = fileID

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
            _LOG.error(f"{errMsg}, {traceback.format_exc()}")    

    return result


#保存文件到文件存储
def saveFile2FileStorage(fileName,fileID):
    result = None

    try:
        tempFileInfo = comFC.sendFile(fileName)
        fileUrl = tempFileInfo.get("fileUrl", "") 
        rtnData = getFileInfo(fileUrl)

        if rtnData:
            #保存文件到文件系统(长久存储)
            fileID = save2newLocation(fileUrl, fileID,  privateFlag = True)
            if fileID:
                result = fileID

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")    

    return result


#account service user info
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
    except:
        pass
    return result


#判断用户是否认证
def chkIsAuthenticatedUser(orgID):
    result = comGD._CONST_NO
    #extOrgID == 0 or null, 是非认证用户
    try:
        orgID = int(orgID)
        if orgID > 0:
            result = comGD._CONST_YES
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")

    return result


#判断用户是否在服务中
def chkIsInService(inService,activeFlag):
    result = comGD._CONST_NO
    #activeFlag = Y and inService != "N"
    try:
        if activeFlag == comGD._CONST_YES and inService != comGD._CONST_NO:
            result = comGD._CONST_YES
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")

    return result


#获取本地用户信息mysql
def getUserInfoMysql(loginID):
    result = {}
    try:
        result = comMysql.getUserInfoMysql(loginID) #这里面需要转换的dict/list已经转换
        if result:
            avatarID = result.get("avatarID","")
            if avatarID:
                avatarID = getTempLocation(avatarID, privateFlag = True)
                result["avatarID"] = avatarID
            else:
                result["avatarID"] = ""
            #extOrgID == 0 or null, 是非认证用户
            result["authenticatedUser"] = chkIsAuthenticatedUser(result["extOrgID"])
            result["extInService"] = chkIsInService(result["extInService"],result["activeFlag"])
    except:
        pass
    return result


def modifyUserRoleName(loginID,roleName,sessionID):
    result = {}
    url = ACCOUNT_SERVICE_URL
    headers = {'content-type': 'application/json'}

    requestData = {}
    requestData["CMD"] = "AEA0"
    requestData["loginID"] = loginID
    requestData["roleName"] = roleName
    requestData["sessionID"] = sessionID
    try:
        payload = misc.jsonDumps(requestData)
        r = requests.post(url, data = payload, headers = headers)

        if r.status_code == 200:
            rtnData = misc.jsonLoads(r.text)
            errCode = rtnData.get("errCode")
            if errCode == "B0":
                #修改本地mysql 数据
                saveSet = {}
                saveSet["roleName"] = roleName
                rtn = comMysql.updateUserBasic(loginID,saveSet)
                if rtn >= 0:
                    result["roleName"] = roleName
    except:
        pass
    return result 


#通过accountService 检测用户是否存在
def accChkUserExist(loginID):
    result = False

    url = ACCOUNT_SERVICE_URL
    headers = {'content-type': 'application/json'}

    requestData = {}
    requestData["CMD"] = "AIA0"
    requestData["userID"] = loginID
            
    try:
        payload = misc.jsonDumps(requestData)
        r = requests.post(url, data = payload, headers = headers)

        if r.status_code == 200:
            rtnData = misc.jsonLoads(r.text)
            userExistFlag = rtnData.get("userExistFlag")
            if userExistFlag == comGD._CONST_YES:
                result = True
 
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


# ntn common begin
def getEnabledDeviceList():
    result = []
    try:
        #get enabledDeviceList
        enabledDeviceList = comDB.getRunParameter("enabledDeviceList")
        if not enabledDeviceList:
            allDeviceList = comDB.getRunParameter("deviceList")
            allDeviceList = misc.jsonLoads(allDeviceList)
            for deviceData in allDeviceList:
                enable = deviceData.get("deviceData")
                if enable == comGD._CONST_YES:
                    instrumentName = deviceData.get("instrumentName")
                    enabledDeviceList.append(instrumentName)
        else:
            enabledDeviceList = misc.jsonLoads(enabledDeviceList)

        result = enabledDeviceList

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result
# ntn common end

# command part end


#user related begin

#用户是否存在
def funcChkUserExist(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "account"

        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

        loginID = dataSet.get("loginID")

        if not loginID:
            loginID = tempUserID

        if errCode == "B0":

            mode = dataSet.get("mode","short")
            mode = mode.lower()

            # if dataValidFlag:

            currDataList = comMysql.queryUserBasic(loginID,mode = mode)
            if currDataList:
                rtnData["exist"] = comGD._CONST_YES
            else:
                rtnData["exist"] = comGD._CONST_NO

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#用户注册和登录部分
def funcUserRegistration(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        openID = sessionIDSet.get("openID", "")
        tempUserID = sessionIDSet.get("loginID", "")
        sessionID = dataSet.get("sessionID")

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        requestData = dataSet
        requestData["CMD"] = "A0A0"
                
        try:
            payload = misc.jsonDumps(requestData)
            r = requests.post(url, data = payload, headers = headers)

            if r.status_code == 200:
                rtnData = misc.jsonLoads(r.text)
                roleName = rtnData.get("roleName")
                if not roleName or roleName == "visitor":
                    rtnData["roleName"] = settings.accountServiceDefaultRoleName #修改默认的用户角色, modify default rolename 

                msgData = rtnData.get("MSG",{})
                errCode = msgData.get("errCode","B0")

                #保存数据到mysql
                saveSet = {}
                saveSet["data"] = dataSet
                saveSet["rtnData"] = rtnData
                saveSet["CMD"] = CMD

                saveSet["loginID"] = tempUserID #操作人员的loginID
                
                rtn = comDB.putMsg2Queue(comGD._DEF_STOCK_MYSQL_TITLE,saveSet)

                if _DEBUG:
                    _LOG.info(f"D: DEBUG,rtn:{rtn},saveSet:{saveSet}")

            else:
                errCode = "CI"

        except:
            pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result
    

#用户登录部分
def funcUserLogin(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        openID = sessionIDSet.get("openID", "")
        tempUserID = sessionIDSet.get("loginID", "")
        sessionID = dataSet.get("sessionID")

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}
                
        try:
            loginID = dataSet.get("loginID")
            if loginID:
                #检查用户是否存在, 如果不存在,就报错
                mode = dataSet.get("mode","short")
                mode = mode.lower()

                # if dataValidFlag:

                # currDataList = comMysql.queryUserBasic(loginID,mode = mode)
                # if len(currDataList) != 1:
                #     #用户不存在,或者不唯一
                #     errCode = "B1"
                if not accChkUserExist(loginID):
                    errCode = "B1"
            else:
                errCode = "B7"

            if errCode == "B0":

                requestData = dataSet
                requestData["CMD"] = "A0A0"

                payload = misc.jsonDumps(requestData)
                r = requests.post(url, data = payload, headers = headers)

                if r.status_code == 200:
                    rtnData = misc.jsonLoads(r.text)
                    roleName = rtnData.get("roleName")
                    if not roleName or roleName == "visitor":
                        rtnData["roleName"] = settings.accountServiceDefaultRoleName #修改默认的用户角色, modify default rolename 

                    msgData = rtnData.get("MSG",{})
                    errCode = msgData.get("errCode","B0")

                    #保存数据到mysql
                    saveSet = {}
                    saveSet["data"] = dataSet
                    saveSet["rtnData"] = rtnData
                    saveSet["CMD"] = CMD

                    saveSet["loginID"] = tempUserID #操作人员的loginID
                    
                    rtn = comDB.putMsg2Queue(comGD._DEF_STOCK_MYSQL_TITLE,saveSet)

                    if _DEBUG:
                        _LOG.info(f"D: DEBUG,rtn:{rtn},saveSet:{saveSet}")

                else:
                    errCode = "CI"
            # else:
            #     errCode = "BT"

        except:
            pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result
    

#用户增加
def funcUserAdd(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        openID = sessionIDSet.get("openID", "")
        tempUserID = sessionIDSet.get("loginID", "")
        sessionID = dataSet.get("sessionID")

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        if "loginID" in dataSet: 
            # if "password" in dataSet:
            #     dataSet["passwd"] = dataSet["password"]
            requestData = dataSet
            requestData["CMD"] = "AHA0"
            requestData["userID"] = dataSet["loginID"] #需要增加的人员的ID
            # #临时处理
            # passwd = dataSet.get("passwd","")
            # userLoginID = dataSet.get("loginID","")
            # dataSet["passwd"] = comFC.genLoginIDPasswd(userLoginID, passwd)

            #roleName 转换
            userRoleName = dataSet.get("roleName")
            if userRoleName in settings.ROLE_ACCOUNT_ROLE:
                requestData["roleName"] = settings.ROLE_ACCOUNT_ROLE[userRoleName]
            else:
                errCode = "B8"

            #extend items begin
            #list/dict 数据处理
            if "extManagementAreaList" in dataSet:
                extManagementAreaList = dataSet.get("extManagementAreaList")
                if isinstance(extManagementAreaList,list):
                    extManagementAreaList = misc.jsonDumps(extManagementAreaList)
                # elif isinstance(extManagementAreaList,dict):
                #     extManagementAreaList = misc.jsonDumps(extManagementAreaList)
                else:
                    extManagementAreaList = "[]"
                dataSet["extManagementAreaList"] = extManagementAreaList
                
            #extend items end

            if errCode == "B0":
                try:
                    payload = misc.jsonDumps(requestData)
                    r = requests.post(url, data = payload, headers = headers)

                    if r.status_code == 200:
                        rtnData = misc.jsonLoads(r.text)
                        msgData = rtnData.get("MSG",{})
                        errCode = msgData.get("errCode","B0")

                        rtnData["sessionID"] = sessionID

                        #保存数据到mysql
                        saveSet = {}
                        saveSet["data"] = dataSet
                        saveSet["rtnData"] = rtnData
                        saveSet["CMD"] = CMD

                        saveSet["loginID"] = tempUserID #操作人员的loginID

                        rtn = comDB.putMsg2Queue(comGD._DEF_STOCK_MYSQL_TITLE,saveSet)

                        if _DEBUG:
                            _LOG.info(f"D: DEBUG,rtn:{rtn},saveSet:{saveSet}")

                    else:
                        errCode = "CI"

                except:
                    pass
        else:
            errCode = "BA"
            rtnField = "missing userID"

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result
    

#用户删除
def funcUserDelete(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        openID = sessionIDSet.get("openID", "")
        tempUserID = sessionIDSet.get("loginID", "")
        sessionID = dataSet.get("sessionID")
        
        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        requestData = dataSet
        requestData["CMD"] = "A1A0"
                
        try:
            #roleName 转换
            userRoleName = dataSet.get("roleName")
            if userRoleName in settings.ROLE_ACCOUNT_ROLE:
                requestData["roleName"] = settings.ROLE_ACCOUNT_ROLE[userRoleName]
            else:
                errCode = "B8"

            payload = misc.jsonDumps(requestData)
            r = requests.post(url, data = payload, headers = headers)

            if r.status_code == 200:
                rtnData = misc.jsonLoads(r.text)
                msgData = rtnData.get("MSG",{})
                errCode = msgData.get("errCode","B0")

                #保存数据到mysql
                saveSet = {}
                saveSet["data"] = dataSet
                saveSet["rtnData"] = rtnData
                saveSet["CMD"] = CMD

                saveSet["loginID"] = tempUserID #操作人员的loginID

                rtn = comDB.putMsg2Queue(comGD._DEF_STOCK_MYSQL_TITLE,saveSet)

                if _DEBUG:
                    _LOG.info(f"D: DEBUG,rtn:{rtn},saveSet:{saveSet}")

            else:
                errCode = "CI"

        except:
            pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result
    

#用户修改
def funcUserModify(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        openID = sessionIDSet.get("openID", "")
        tempUserID = sessionIDSet.get("loginID", "")
        sessionID = dataSet.get("sessionID")
        roleName = sessionIDSet.get("roleName")

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        if "loginID" in dataSet: 
            loginID = dataSet["loginID"]
            requestData = dataSet
            if tempUserID == loginID:
                #本人修改本人, 不能修改roleName 和 activeFlag
                if "roleName" in requestData:
                    del requestData["roleName"]
                if "activeFlag" in requestData:
                    del requestData["activeFlag"]
                requestData["CMD"] = "A2A0" #只能修改自己
            else:
                if comFC.chkIsManager(roleName):
                # if comFC.chkIsOperator(roleName):
                    #管理员
                    requestData["CMD"] = "AHA0"
                else:
                    errCode = "BT"
            requestData["userID"] = loginID #需要修改的人员的ID

            # #roleName 转换
            # userRoleName = dataSet.get("roleName")
            # if userRoleName:
            #     if userRoleName in settings.ROLE_ACCOUNT_ROLE:
            #         requestData["roleName"] = settings.ROLE_ACCOUNT_ROLE[userRoleName]
            #     else:
            #         errCode = "B8"

            # if not comFC.chkIsOperator(roleName):

            #文件ID处理, 保存文件到永久存储
            avatarID = dataSet.get("avatarID")
            if avatarID: 
                avatarID = save2newLocation(avatarID)
                if _DEBUG:
                    _LOG.info(f"D: DEBUG,avatarID:{avatarID}")
                requestData["avatarID"] = avatarID

            #extManagementAreaList:
            extManagementAreaList = dataSet.get("extManagementAreaList")
            if extManagementAreaList:
                try:
                    extManagementAreaList = misc.jsonDumps(extManagementAreaList)
                except:
                    extManagementAreaList = "[]"
                requestData["extManagementAreaList"] = extManagementAreaList

            if errCode == "B0":
                try:
                    # del requestData["roleName"]
                    payload = misc.jsonDumps(requestData)
                    r = requests.post(url, data = payload, headers = headers)

                    if r.status_code == 200:
                        rtnData = misc.jsonLoads(r.text)
                        msgData = rtnData.get("MSG",{})
                        errCode = msgData.get("errCode","B0")

                        #保存数据到mysql
                        saveSet = {}
                        saveSet["data"] = dataSet
                        saveSet["rtnData"] = rtnData
                        saveSet["CMD"] = CMD

                        saveSet["loginID"] = tempUserID #操作人员的loginID

                        rtn = comDB.putMsg2Queue(comGD._DEF_STOCK_MYSQL_TITLE,saveSet)

                        if _DEBUG:
                            _LOG.info(f"D: DEBUG,rtn:{rtn},saveSet:{saveSet}")

                    else:
                        errCode = "CI"
                except:
                    pass
        else:
            errCode = "BW"

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result
    

#用户注销/登出
def funcUserLogout(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        openID = sessionIDSet.get("openID", "")
        tempUserID = sessionIDSet.get("loginID", "")
        sessionID = dataSet.get("sessionID")

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        requestData = dataSet
        requestData["CMD"] = "A5A0"
                
        try:
            #roleName 转换
            userRoleName = dataSet.get("roleName")
            if userRoleName in settings.ROLE_ACCOUNT_ROLE:
                requestData["roleName"] = settings.ROLE_ACCOUNT_ROLE[userRoleName]
            else:
                errCode = "B8"

            payload = misc.jsonDumps(requestData)
            r = requests.post(url, data = payload, headers = headers)

            if r.status_code == 200:
                rtnData = misc.jsonLoads(r.text)
                msgData = rtnData.get("MSG",{})
                errCode = msgData.get("errCode","B0")
            else:
                errCode = "CI"

        except:
            pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  

    return result
    

#用户查询(按照前缀),redis version
def funcUserSearch(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}
        
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")
        sessionID = dataSet.get("sessionID")

        if comFC.chkIsOperator(roleName):

            loginIDPrefix = dataSet.get("loginIDPrefix")

            requestData = dataSet
            requestData["CMD"] = "AGA0"
                    
            try:
                payload = misc.jsonDumps(requestData)
                r = requests.post(url, data = payload, headers = headers)

                if r.status_code == 200:
                    rtnData = misc.jsonLoads(r.text)
                    msgData = rtnData.get("MSG",{})
                    errCode = msgData.get("errCode","B0")
                    dataList = rtnData.get("data",[])
                    detailsList = []
                    for userID in dataList:
                        userInfo = getUserInfo(userID,{})
                        if "roleName" not in userInfo:
                            userInfo["roleName"] = settings.accountServiceDefaultRoleName
                        detailsList.append(userInfo)

                    # rtnData["details"] = detailsList
                    dataList = detailsList

                    #临时缓存机制,改进型
                    indexKeyDataSet = {} #查询生成index的因素
                    if loginIDPrefix:
                        indexKeyDataSet["loginIDPrefix"] = loginIDPrefix

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = putQueryResult(CMD, sessionID, dataList,indexKeyDataSet) #存放数据到临时缓冲区去

                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM))
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM))
                    rtnData["indexKey"]  = indexKey
                    total = len(dataList)
                    rtnData["total"]  = str(total)
                    rtnData["beginNum"]  = str(beginNum)
                    if endNum >= total:
                        endNum = total-1 #java/c rule, not python rule
                    if beginNum > endNum:
                        beginNum = 0
                    rtnData["endNum"]  = str(endNum)
                    if total > 0:
                        rtnData["data"]  = dataList[beginNum:endNum+1]
                    else:
                        rtnData["data"]  = []

                else:
                    errCode = "CI"

            except:
                pass
        else:
            errCode = "B8"

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#用户查询,mysql version, 
# championship only version
# 仅支持区域管理员(operator以上查询)
def funcUserSearchMysql(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "account"
        
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")
        sessionID = dataSet.get("sessionID")

        if tempUserID != "":
            loginID = tempUserID

            roleNameList = dataSet.get("roleNameList",[])
            userLoginID = dataSet.get("loginID")
            
            # 权限检查/功能检测
            if not comFC.chkIsOperator(roleName):
                #可以查询自己 
                if not userLoginID or userLoginID == loginID:
                    userLoginID = loginID
                else:       
                    errCode = "BG"
            else:
                #区域管理员无法查询管理员
                if roleName == "operator":
                    if "manager" in roleNameList:
                        del roleNameList["manager"]
                    if "administrator" in roleNameList:
                        del roleNameList["administrator"]
                    if not roleNameList:
                        roleNameList = ["operator","customer","orgContact","expert"]

            if errCode == "B0": #

                if "loginIDPrefix" in dataSet:
                    mobile = dataSet.get("loginIDPrefix")
                else:
                    mobile = dataSet.get("mobile") #可以是手机号
                if mobile:
                    try:
                        mobile = mobile.strip()
                    except:
                        pass

                name = dataSet.get("name") #可以是名字
                if name:
                    try:
                        name = name.strip()
                    except:
                        pass

                searchOption = dataSet.get("searchOption")

                keyword = dataSet.get("keyword")

                manualTag = dataSet.get("manualTag")
                if manualTag:
                    try:
                        manualTag = manualTag.strip()
                    except:
                        pass
                
                order = dataSet.get("order","create")

                mode = dataSet.get("mode","full")
                mode = mode.lower()

                limitNum = dataSet.get("limitNum")
                if limitNum:
                    try:
                        limitNum = int(limitNum)
                    except:
                        limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM
                else:
                    limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM

                if dataValidFlag:

                    if userLoginID:
                        currDataList = comMysql.queryUserBasic(userLoginID,mode = mode)
                    #add search option
                    elif searchOption:
                        currDataList = comMysql.queryUserBasic(name=name,mobile=mobile,manualTag=manualTag,keyword=keyword,
                                                               roleNameList = roleNameList,order=order,mode = mode)
                        allowList = ["mobilePhoneNo","roleName","nickName","realName","province","city","area","address",
                                    "extJobPosition", "extOverallEvaluation","extJobLabel","extMemo"]
                        serachResultSet = comFC.handleSearchOption(searchOption,allowList, currDataList)
                        if serachResultSet["rtn"] == "B0":
                            currDataList = serachResultSet.get("data", [])
                    else:
                        currDataList = comMysql.queryUserBasic(name=name,mobile=mobile,manualTag=manualTag,keyword=keyword,
                                                               roleNameList = roleNameList,order=order,mode = mode)

                    dataList = []
                    for currDataSet in currDataList:

                        aSet = {}

                        aSet["loginID"] = currDataSet.get("loginID","")
                        # aSet["openID"] = currDataSet.get("openID","")
                        aSet["roleName"] = currDataSet.get("roleName","")
                        aSet["nickName"] = currDataSet.get("nickName","")
                        aSet["realName"] = currDataSet.get("realName","")
                        aSet["gender"] = currDataSet.get("gender","")

                        avatarID = currDataSet.get("avatarID","")
                        if avatarID:
                            avatarID = getTempLocation(avatarID, privateFlag = True)
                        aSet["avatarID"] = avatarID

                        aSet["mobilePhoneNo"] = currDataSet.get("mobilePhoneNo","")
                        aSet["masterID"] = currDataSet.get("masterID","")
                        aSet["province"] = currDataSet.get("province","")
                        aSet["city"] = currDataSet.get("city","")
                        aSet["area"] = currDataSet.get("area","")
                        aSet["address"] = currDataSet.get("address","")
                        aSet["email"] = currDataSet.get("email","")
                        aSet["PID"] = currDataSet.get("PID","")

                        # photoIDFront = currDataSet.get("photoIDFront","")
                        # if photoIDFront:
                        #     photoIDFront = getTempLocation(photoIDFront, privateFlag = True)
                        # aSet["photoIDFront"] = photoIDFront

                        # photoIDBack = currDataSet.get("photoIDBack","")
                        # if photoIDBack:
                        #     photoIDBack = getTempLocation(photoIDBack, privateFlag = True)
                        # aSet["photoIDBack"] = photoIDBack

                        # photoID = currDataSet.get("photoID","")
                        # if photoID:
                        #     photoID = getTempLocation(photoID, privateFlag = True)
                        # aSet["photoID"] = photoID

                        # aSet["delFlag"] = currDataSet.get("delFlag","")
                        aSet["activeFlag"] = currDataSet.get("activeFlag","")

                        aSet["regPosition"] = currDataSet.get("regPosition","")
                        aSet["regID"] = currDataSet.get("regID","")
                        aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                        aSet["updateYMDHMS"] = currDataSet.get("updateYMDHMS","")
                        # aSet["lastOpenID"] = currDataSet.get("lastOpenID","")
                        aSet["lastLoginYMDHMS"] = currDataSet.get("lastLoginYMDHMS","")
                        aSet["modifyID"] = currDataSet.get("modifyID","")
                        aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                        # aSet["passwdYMDHMS"] = currDataSet.get("passwdYMDHMS","")

                        # extend items begin, per project
                        aSet["extStartYMDHMS"] = currDataSet.get("extStartYMDHMS","")
                        aSet["extLeaveYMDHMS"] = currDataSet.get("extLeaveYMDHMS","")
                        aSet["extJobPosition"] = currDataSet.get("extJobPosition","")
                        aSet["extDepartment"] = currDataSet.get("extDepartment","")
                        aSet["extOrgName"] = currDataSet.get("extOrgName","")
                        aSet["extOrgID"] = currDataSet.get("extOrgID","")
                        
                        aSet["authenticatedUser"] = chkIsAuthenticatedUser(aSet["extOrgID"])

                        aSet["extInService"] = currDataSet.get("extInService","")
                        aSet["extInService"] = chkIsInService(aSet["extInService"],aSet["activeFlag"])

                        aSet["extJobLabel"] = currDataSet.get("extJobLabel","")
                        aSet["extJobDetail"] = currDataSet.get("extJobDetail","")
                        aSet["extBrief"] = currDataSet.get("extBrief","")

                        #list/dict处理
                        extManualTagList = currDataSet.get("extManualTagList")
                        try:
                            extManualTagList = misc.jsonLoads(extManualTagList)
                        except:
                            extManualTagList = []
                        aSet["extManualTagList"] = extManualTagList

                        #list/dict处理
                        extManagementAreaList = currDataSet.get("extManagementAreaList")
                        try:
                            extManagementAreaList = misc.jsonLoads(extManagementAreaList)
                        except:
                            extManagementAreaList = []
                        aSet["extManagementAreaList"] = extManagementAreaList

                        aSet["extMemo"] = currDataSet.get("extMemo","")
                        # extend items end, per project

                        dataList.append(aSet)

                    #临时缓存机制,改进型
                    indexKeyDataSet = {} #查询生成index的因素
                    if loginID:
                        indexKeyDataSet["loginID"] = userLoginID
                    if searchOption:
                        indexKeyDataSet["searchOption"] = misc.jsonDumps(searchOption)
                    if mode:
                        indexKeyDataSet["mode"] = mode
                    if order:
                        indexKeyDataSet["order"] = order

                    indexKeyDataSet["limitNum"] = str(limitNum)

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = putQueryResult(CMD, sessionID, dataList,indexKeyDataSet) #存放数据到临时缓冲区去

                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM))
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM))
                    rtnData["indexKey"]  = indexKey
                    total = len(dataList)
                    rtnData["total"]  = str(total)
                    rtnData["beginNum"]  = str(beginNum)
                    if endNum >= total:
                        endNum = total-1 #java/c rule, not python rule
                    if beginNum > endNum:
                        beginNum = 0
                    rtnData["endNum"]  = str(endNum)
                    if total > 0:
                        rtnData["data"]  = dataList[beginNum:endNum+1]
                    else:
                        rtnData["data"]  = []
                    
                    rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "EA"

            # else:
            #     errCode = "BG"

        else:
            errCode = "B8"

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#用户信息获取, redis version
def funcGetUserInfo(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        loginID = dataSet.get("loginID")
        sessionID = dataSet.get("sessionID")

        if not loginID:
            loginID = tempUserID
        if tempUserID != loginID:
            if not comFC.chkIsManager(roleName):
                errCode = "BG"

        if errCode == "B0":
            try:
                rtnData = getUserInfo(loginID,sessionID)
                if not rtnData:
                    errCode = "CI"
            except:
                pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#用户信息获取, mysql version
def funcGetUserInfoMysql(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "account"

        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

        loginID = dataSet.get("loginID")

        if not loginID:
            loginID = tempUserID

        if tempUserID != loginID:
            if not comFC.chkIsManager(roleName):
                errCode = "BG"

        if errCode == "B0":

            mode = dataSet.get("mode","full")
            mode = mode.lower()

            if dataValidFlag:
                currDataList = comMysql.queryUserBasic(loginID,mode = mode)

                dataList = []

                for currDataSet in currDataList:
                    aSet = {}

                    aSet["loginID"] = currDataSet.get("loginID","")
                    # aSet["openID"] = currDataSet.get("openID","")
                    # aSet["roleName"] = currDataSet.get("roleName","")
                    aSet["roleName"] = roleName # 采用redis的roleName
                    aSet["nickName"] = currDataSet.get("nickName","")
                    aSet["realName"] = currDataSet.get("realName","")
                    aSet["gender"] = currDataSet.get("gender","")

                    avatarID = currDataSet.get("avatarID","")
                    if avatarID:
                        avatarID = getTempLocation(avatarID, privateFlag = True)
                    aSet["avatarID"] = avatarID

                    aSet["mobilePhoneNo"] = currDataSet.get("mobilePhoneNo","")
                    aSet["masterID"] = currDataSet.get("masterID","")
                    aSet["province"] = currDataSet.get("province","")
                    aSet["city"] = currDataSet.get("city","")
                    aSet["area"] = currDataSet.get("area","")
                    aSet["address"] = currDataSet.get("address","")
                    aSet["email"] = currDataSet.get("email","")
                    aSet["PID"] = currDataSet.get("PID","")

                    # photoIDFront = currDataSet.get("photoIDFront","")
                    # if photoIDFront:
                    #     photoIDFront = getTempLocation(photoIDFront, privateFlag = True)
                    # aSet["photoIDFront"] = photoIDFront

                    # photoIDBack = currDataSet.get("photoIDBack","")
                    # if photoIDBack:
                    #     photoIDBack = getTempLocation(photoIDBack, privateFlag = True)
                    # aSet["photoIDBack"] = photoIDBack

                    # photoID = currDataSet.get("photoID","")
                    # if photoID:
                    #     photoID = getTempLocation(photoID, privateFlag = True)
                    # aSet["photoID"] = photoID

                    # aSet["delFlag"] = currDataSet.get("delFlag","")

                    aSet["activeFlag"] = currDataSet.get("activeFlag","")

                    aSet["regPosition"] = currDataSet.get("regPosition","")
                    aSet["regID"] = currDataSet.get("regID","")
                    aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                    aSet["updateYMDHMS"] = currDataSet.get("updateYMDHMS","")
                    # aSet["lastOpenID"] = currDataSet.get("lastOpenID","")
                    aSet["lastLoginYMDHMS"] = currDataSet.get("lastLoginYMDHMS","")
                    aSet["modifyID"] = currDataSet.get("modifyID","")
                    aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                    # aSet["passwdYMDHMS"] = currDataSet.get("passwdYMDHMS","")

                    # extend items begin, per project
                    aSet["extStartYMDHMS"] = currDataSet.get("extStartYMDHMS","")
                    aSet["extLeaveYMDHMS"] = currDataSet.get("extLeaveYMDHMS","")
                    aSet["extJobPosition"] = currDataSet.get("extJobPosition","")
                    aSet["extDepartment"] = currDataSet.get("extDepartment","")
                    aSet["extOrgName"] = currDataSet.get("extOrgName","")
                    aSet["extOrgID"] = currDataSet.get("extOrgID","")
                    aSet["authenticatedUser"] = chkIsAuthenticatedUser(aSet["extOrgID"])

                    aSet["extInService"] = currDataSet.get("extInService","")
                    aSet["extInService"] = chkIsInService(aSet["extInService"],aSet["activeFlag"])

                    aSet["extJobLabel"] = currDataSet.get("extJobLabel","")
                    aSet["extJobDetail"] = currDataSet.get("extJobDetail","")
                    aSet["extBrief"] = currDataSet.get("extBrief","")

                    #list/dict处理
                    extManualTagList = currDataSet.get("extManualTagList")
                    try:
                        extManualTagList = misc.jsonLoads(extManualTagList)
                    except:
                        extManualTagList = []
                    aSet["extManualTagList"] = extManualTagList

                    #list/dict处理
                    extManagementAreaList = currDataSet.get("extManagementAreaList")
                    try:
                        extManagementAreaList = misc.jsonLoads(extManagementAreaList)
                    except:
                        extManagementAreaList = []
                    aSet["extManagementAreaList"] = extManagementAreaList
                    aSet["extMemo"] = currDataSet.get("extMemo","")
                    # extend items end, per project

                    dataList.append(aSet)
                    if dataList:
                        rtnData = dataList[0]

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result

    
#短信验证请求
def funcSMSRequest(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        requestData = dataSet
        requestData["CMD"] = "A6A0"
                
        try:
            payload = misc.jsonDumps(requestData)
            r = requests.post(url, data = payload, headers = headers)

            if r.status_code == 200:
                rtnData = misc.jsonLoads(r.text)
                msgData = rtnData.get("MSG",{})
                errCode = msgData.get("errCode","B0")
            else:
                errCode = "CI"

        except:
            pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result
    

#验证反馈
def funcSMSVerify(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    lowerCMD = CMD.lower()

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        requestData = dataSet
        requestData["CMD"] = "A7A0"
                
        try:
            payload = misc.jsonDumps(requestData)
            r = requests.post(url, data = payload, headers = headers)

            if r.status_code == 200:
                rtnData = misc.jsonLoads(r.text)
                msgData = rtnData.get("MSG",{})
                errCode = msgData.get("errCode","B0")
            else:
                errCode = "CI"

        except:
            pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#用户,重置passwd
# 前端的passwd计算方法
# passwd(用户输入的)+loginID 然后再md5
def funcResetPasswd(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    lowerCMD = CMD.lower()

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        requestData = dataSet
        requestData["CMD"] = "A9A0"
                
        try:
            payload = misc.jsonDumps(requestData)
            r = requests.post(url, data = payload, headers = headers)

            if r.status_code == 200:
                rtnData = misc.jsonLoads(r.text)
                msgData = rtnData.get("MSG",{})
                errCode = msgData.get("errCode","B0")
            else:
                errCode = "CI"

        except:
            pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#用户,用户信息查询
def funcUserInfoQuery(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    lowerCMD = CMD.lower()

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        searchLoginID = dataSet.get("loginID")
        loginIDPrefix = dataSet.get("loginIDPrefix")
        mode = dataSet.get("mode", "normal")
        limitNum = dataSet.get("limitNum")

        requestData = dataSet
        # requestData["CMD"] = "ADA0" #mysql
        requestData["CMD"] = "AEA0"  #redis
                
        try:
            payload = misc.jsonDumps(requestData)
            r = requests.post(url, data = payload, headers = headers)

            if r.status_code == 200:
                rtnData = misc.jsonLoads(r.text)
                msgData = rtnData.get("MSG",{})
                errCode = msgData.get("errCode","B0")
                # addtional data processing 
                dataList = rtnData.get("data",[])
                for userInfo in dataList:
                    if "roleName" not in userInfo:
                        userInfo["roleName"] = settings.accountServiceDefaultRoleName #修改默认的用户角色, modify default rolename 
                    roleName = userInfo.get("roleName")
                    roleCNName = settings.ROLE_EN_CN_NAME_DATA.get(roleName)
                    userInfo["roleName"] = roleCNName

                #临时缓存机制,改进型
                indexKeyDataSet = {} #查询生成index的因素
                if searchLoginID:
                    indexKeyDataSet["searchLoginID"] = searchLoginID
                if loginIDPrefix:
                    indexKeyDataSet["loginIDPrefix"] = loginIDPrefix
                if mode:
                    indexKeyDataSet["mode"] = mode
                if limitNum:
                    indexKeyDataSet["limitNum"] = str(limitNum)

                sessionID = sessionIDSet.get("sessionID", "")
                indexKey = putQueryResult(CMD, sessionID, dataList,indexKeyDataSet) #存放数据到临时缓冲区去

                beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM))
                endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM))
                rtnData["indexKey"]  = indexKey
                total = len(dataList)
                rtnData["total"]  = str(total)
                rtnData["beginNum"]  = str(beginNum)
                if endNum >= total:
                    endNum = total-1 #java/c rule, not python rule
                if beginNum > endNum:
                    beginNum = 0
                rtnData["endNum"]  = str(endNum)
                if total > 0:
                    rtnData["data"]  = dataList[beginNum:endNum+1]
                else:
                    rtnData["data"]  = []

            else:
                errCode = "CI"

        except:
            pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#用户是否存在
def funcPasswdValidCheck(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        requestData = dataSet
        requestData["CMD"] = "AKA0"

        try:
            payload = misc.jsonDumps(requestData)
            r = requests.post(url, data = payload, headers = headers)

            if r.status_code == 200:
                rtnData = misc.jsonLoads(r.text)
                msgData = rtnData.get("MSG",{})
                errCode = msgData.get("errCode")
                if errCode == "B0":
                    rtnData["validFlag"] = comGD._CONST_YES
                else:
                    rtnData["validFlag"] = comGD._CONST_NO
        except:
            pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#存储数据 --  usersavedata
def funcUserSaveData(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        tempLoginID = sessionIDSet.get("loginID")
        if tempLoginID:

            requestData = dataSet
            requestData["CMD"] = "G1A0"
                    
            try:
                payload = misc.jsonDumps(requestData)
                r = requests.post(url, data = payload, headers = headers)

                if r.status_code == 200:
                    rtnData = misc.jsonLoads(r.text)
                    msgData = rtnData.get("MSG",{})
                    errCode = msgData.get("errCode","B0")
                else:
                    errCode = "CI"

            except:
                pass
        else:
            errCode = "BA"

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#获取存储数据 --  G2A0
def funcUserGetData(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "account"
        url = ACCOUNT_SERVICE_URL
        headers = {'content-type': 'application/json'}

        tempLoginID = sessionIDSet.get("loginID")
        if tempLoginID:

            requestData = dataSet
            requestData["CMD"] = "G2A0"
                    
            try:
                payload = misc.jsonDumps(requestData)
                r = requests.post(url, data = payload, headers = headers)

                if r.status_code == 200:
                    rtnData = misc.jsonLoads(r.text)
                    msgData = rtnData.get("MSG",{})
                    errCode = msgData.get("errCode","B0")
                else:
                    errCode = "CI"

            except:
                pass
        else:
            errCode = "BA"

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#获取下一批数据 --  G3A0
def funGeneralNext(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    lowerCMD = CMD.lower()

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "default"

        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")
        
        indexKey = dataSet.get("indexKey", "")
        beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM))
        endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM))

        if indexKey and tempUserID:
            dataExist,  dataList = getQueryResult(indexKey, beginNum, endNum)
            if dataExist:
                dataLen = len(dataList)
                endNum = (beginNum + dataLen)
                rtnData["indexKey"] = indexKey
                rtnData["beginNum"] = str(beginNum)
                rtnData["endNum"] = str(endNum)
                rtnData["data"] = dataList
                rtnData["dataLen"] = str(dataLen)
                    
                result = rtnData
            else:
                errCode = "CC"

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result

#user related end


#系统版本查询
def funcServerVersionQry(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "swupgrade"
        url = SWUPGRADE_SERVICE_URL + "/swupload"
        headers = {'content-type': 'application/json'}

        tempLoginID = sessionIDSet.get("loginID")
        if tempLoginID:

            dataValidFlag = True
           
            if dataValidFlag:
                
                try:
                    saveFilePath = os.path.join(settings._DATA_DIR, "sysVersionReport.json")
                    rtnData = misc.loadJsonData(saveFilePath)

                except:
                    pass
            else:
                errCode = "BA"
        else:
            errCode = "BA"

        result["data"] = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#sw upgrade related begin


#上传升级软件
def funcSWUpload(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "swupgrade"
        url = SWUPGRADE_SERVICE_URL + "/swupload"
        headers = {'content-type': 'application/json'}

        tempLoginID = sessionIDSet.get("loginID")
        if tempLoginID:

            dataValidFlag = False

            requestData = dataSet
            requestData["CMD"] = CMD

            fileID = dataSet.get("fileID")
            filePath = dataSet.get("fileName")
            oldFileName = dataSet.get("oldFileName")

            if _DEBUG:
                _LOG.info(f"fileID:{fileID},filePath:{filePath},oldFileName:{oldFileName}")

            if fileID and filePath and oldFileName:
                if os.path.exists(filePath):
                    #分离目录和文件名
                    srcSubDirName = os.path.dirname(filePath)
                    srcFileName = os.path.basename(filePath)
                    #组成目标目录名和文件名
                    destDirName = srcSubDirName
                    destFilePath = os.path.join(destDirName,oldFileName)
                    # comFC.createDir(destDirName)
                    #文件复制
                    shutil.copy2(filePath, destFilePath)
                    if _DEBUG:
                        _LOG.info(f"I: copy file from {filePath} -> {destFilePath}")

                    requestData["downloadFilePath"] = destFilePath
                    dataValidFlag = True
            
            if dataValidFlag:
                
                try:
                    payload = misc.jsonDumps(requestData)
                    r = requests.post(url, data = payload, headers = headers)

                    if r.status_code == 200:
                        rtnData = misc.jsonLoads(r.text)
                        msgData = rtnData.get("MSG",{})
                        errCode = msgData.get("errCode","B0")
                        #复制文件到分系统
                        srcPath = settings._HOME_DIR + "/src"
                        cmdLine = f"sh f{srcPath}/master-slave.sh"
                        #执行代码
                        tempData = subprocess.run(cmdLine, shell=True, capture_output=True, text=True)
                        if _DEBUG:
                            _LOG.info(f"I: execute cmd:{cmdLine}, tempData:{tempData} ")
                        #初始化系统
                        if errCode == "B0":
                            _LOG.info(f"I: SW upload success, begin to init system")
                            # funcInitSystem(CMD, dataSet, sessionIDSet)
                            _LOG.info(f"I: SW upload success, end to init system")
                    else:
                        errCode = "CI"

                except:
                    pass
            else:
                errCode = "BA"
        else:
            errCode = "BA"

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result

#sw upgrade related end

#application functions begin
#hotel begin

#硬件信息报告
# "hwinforeport"    
def funcHWInfoReport(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    lowerCMD = CMD.lower()

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "hotel"

        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            hwInfo = dataSet.get("hwInfo")
            processorInfo = dataSet.get("processorInfo")
            dataSource = dataSet.get("dataSource")
            addtionalInfo = dataSet.get("addtionalInfo",{})

            dataValidFlag = True
            # if not comFC.chkIsOperator(roleName):
            #     errCode = "BG"
                    
             #权限检查
            if errCode == "B0": #
                if dataValidFlag:
                    saveSet = {}
                    #避免用户服务器起名重复
                    hostName = hwInfo.get("hostName")
                    IP = hwInfo.get("IP")
                    # key = hostName + "_" + IP
                    key = hostName 

                    saveSet["hostName"] = key
                    saveSet["description"] = hwInfo.get("hostName", "") 
                    saveSet["IP"] = hwInfo.get("IP", "") 

                    saveSet["IPs"] = misc.jsonDumps(hwInfo.get("IPs", ""))
                    
                    saveSet["os"] = hwInfo.get("os", "") 
                    saveSet["osVersion"] = hwInfo.get("version", "") 
                    saveSet["mac"] = hwInfo.get("mac", "") 
                    saveSet["cpuCount"] = hwInfo.get("CPUCount",0) 
                    saveSet["cpuLoad"] = hwInfo.get("CPULoad",0) 
                    saveSet["RAMTotal"] = hwInfo.get("RAMTotal","") 
                    saveSet["RAMUsed"] = hwInfo.get("RAMUsed", "") 
                    saveSet["RAMFree"] = hwInfo.get("RAMFree", "") 
                    saveSet["RAMPercent"] = hwInfo.get("percent",0) 
                    
                    saveSet["disk"] = misc.jsonDumps(hwInfo.get("disk", ""))
                    
                    saveSet["diskTotal"] = hwInfo.get("diskTotal", "") 
                    saveSet["diskUsed"] = hwInfo.get("diskUsed", "") 
                    saveSet["diskPercent"] = hwInfo.get("diskPercent", 0) 

                    saveSet["processorInfo"] = misc.jsonDumps(processorInfo)
                    saveSet["addtionalInfo"] = misc.jsonDumps(addtionalInfo)
                    
                    saveSet["YMDHMS"] = hwInfo.get("YMDHMS", "") 
                    saveSet["label1"] = hwInfo.get("runProcs")
                    saveSet["label2"] = hwInfo.get("runProc")
                    saveSet["label3"] = ""
                    saveSet["memo"] = hwInfo.get("memo") 
                    saveSet["regID"] = loginID 
                    saveSet["regYMDHMS"] = misc.getTime() 
                    saveSet["dispFlag"] = "Y"
                    saveSet["delFlag"] = "0" 

                    tableName = comMysql.tablename_convertor_hwinfo_report_record(dataSource)
                    recID = comMysql.insert_hwinfo_report_record(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},dataSource:{dataSource},saveSet:{saveSet}")
                    else:
                        #增加到redis,保存最后数据
                        #避免用户服务器起名重复,用上面的key
                        payload = misc.jsonDumps(saveSet)
                        rtn = comDB.setHWInfo(key,payload,dataSource)
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: dataSource:{dataSource},recID:{recID},rtn:{rtn}")

                    result = rtnData
                pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#获取硬件信息报告
# "gethwinfo"    
def funcGetHWInfo(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    # rtnCMD = CMD[0:2]+errCode
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}
    msgData = {}

    lowerCMD = CMD.lower()

    try:
        
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)

        msgKey = "hotel"

        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            lastStatusFlag = dataSet.get("lastStatusFlag",comGD._CONST_NO)

            hostName = dataSet.get("hostName")

            dataSource = dataSet.get("dataSource","")

            limitNum = dataSet.get("limitNum")
            if not limitNum:
                limitNum = 20

            dataValidFlag = True
            # if not comFC.chkIsOperator(roleName):
            #     errCode = "BG"
                    
             #权限检查
            if errCode == "B0": #
                if dataValidFlag:
                    newHwDataInfo = {}
                    if lastStatusFlag == comGD._CONST_YES:
                        if hostName:
                            hwInfo = comDB.getHWInfo(hostName,dataSource)
                            newHwDataInfo = {hostName:[hwInfo]}
                        else:
                            hwInfoData = comDB.getAllHWInfo(dataSource)
                            for hostName,data in hwInfoData.items():
                                newHwDataInfo[hostName] = [data]
                        
                        for hostName,dataList in newHwDataInfo.items():
                            newDataList = []
                            for data in dataList:
                                data = misc.jsonLoads(data)
                                newData = {}
                                for k,v in data.items():
                                    if k in ["IPs","disk","processorInfo","addtionalInfo"]:
                                        try:
                                            v = misc.jsonLoads(v)
                                            newData[k] = v
                                            if k in ["processorInfo"]:
                                                if isinstance(v, list):
                                                    newData["processorNum"] = len(v)
                                        except:
                                            pass
                                    else:
                                        newData[k] = v

                                    #厂家的特殊处理
                                    memo = newData.get("memo")
                                    if memo == "netitest":
                                        processorNum = newData.get("label1")
                                        try:
                                            processorNum = int(processorNum)
                                        except:
                                            processorNum = 0
                                        newData["processorNum"] = processorNum
                                    newHwDataInfo[hostName].append(newData)

                                newDataList.append(newData)
                            newHwDataInfo[hostName] = newDataList

                    else:
                        tableName = comMysql.tablename_convertor_hwinfo_report_record(dataSource)
                        dataList = comMysql.query_hwinfo_report_record(tableName,hostName = hostName, limitNum = limitNum)
                    
                        for hwInfoData in dataList:
                            hostName = hwInfoData.get("hostName")
                            if hostName not in newHwDataInfo:
                                newHwDataInfo[hostName] = []
                            
                            newData = {}
                            for k,v in hwInfoData.items():
                                if k in ["IPs","disk","processorInfo","addtionalInfo"]:
                                    try:
                                        v = misc.jsonLoads(v)
                                        newData[k] = v
                                        if k in ["processorInfo"]:
                                            if isinstance(v, list):
                                                newData["processorNum"] = len(v)
                                    except:
                                        pass
                                else:
                                    newData[k] = v

                            #厂家的特殊处理
                            memo = newData.get("memo")
                            if memo == "netitest":
                                processorNum = newData.get("label1")
                                try:
                                    processorNum = int(processorNum)
                                except:
                                    processorNum = 0
                                newData["processorNum"] = processorNum
                            newHwDataInfo[hostName].append(newData)
                    
                    #计算平均数, 算数平均
                    total = 0
                    serverNum = len(newHwDataInfo)
                    cpuLoadTotal = 0
                    RAMPercentTotal = 0
                    diskPercentTotal = 0
                    for hostName,dataList in newHwDataInfo.items():
                        for data in dataList:
                            total += 1
                            cpuLoad = data.get("cpuLoad")
                            RAMPercent = data.get("RAMPercent")
                            diskPercent = data.get("diskPercent")
                            try:
                                cpuLoadTotal += float(cpuLoad)
                                RAMPercentTotal += float(RAMPercent)
                                diskPercentTotal += float(diskPercent)
                            except:
                                pass
                            #修改YMDHMS 
                            YMDHMS = data.get("YMDHMS","")
                            if YMDHMS:
                                HMS = YMDHMS[8:10] + ":" + YMDHMS[10:12] +":" + YMDHMS[12:14]
                                data["YMDHMS"] = HMS

                    #计算算术平均
                    avgData = {}
                    avgData["serverNum"] = ""
                    avgData["cpuLoadTotal"] = ""
                    avgData["RAMPercentTotal"] = ""
                    avgData["diskPercentTotal"] = ""
                    if total > 0:
                        cpuLoadTotal = cpuLoadTotal/total
                        RAMPercentTotal = RAMPercentTotal/total
                        diskPercentTotal = diskPercentTotal/total
                        avgData["serverNum"] = str(serverNum)
                        avgData["cpuLoadTotal"] = f"{cpuLoadTotal:.2f}"
                        avgData["RAMPercentTotal"] = f"{RAMPercentTotal:.2f}"
                        avgData["diskPercentTotal"] = f"{diskPercentTotal:.2f}"

                    rtnData["data"] = newHwDataInfo
                    rtnData["avgData"] = avgData

                    result = rtnData
                pass

        result = rtnData

        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        msgData = rtnSet["MSG"]
        result["CMD"] = CMD
        result["msgKey"] = msgKey
        result["MSG"] = msgData
        result["errCode"] = errCode

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnSet  
       
    return result


#获取运维信息
def funcGetOmcInfo(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "hotel"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                instrumentName = dataSet.get("instrumentName") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    instrumentInfo = comFC.readInstrumentInfo(instrumentName)
                    deviceType = instrumentInfo.get("deviceType","")
                    IP = instrumentInfo.get("IP","")
                    port = instrumentInfo.get("port",80)
                    INSTRUMENT_DEVICE_BASIC_TYPE_INFO = settings.INSTRUMENT_DEVICE_BASIC_TYPE_INFO
                    instrumentBasicTypeInfo = INSTRUMENT_DEVICE_BASIC_TYPE_INFO.get(deviceType,{})
                    
                    omcUrlPath = instrumentBasicTypeInfo.get("omcUrlPath")
                    omcUserName = instrumentBasicTypeInfo.get("omcUserName")
                    omcPassword = instrumentBasicTypeInfo.get("omcPassword")

                    omcUrlFullPath = ""
                    if "omcUrl" in instrumentInfo:
                        omcUrlFullPath = instrumentInfo.get("omcUrl","")
                    else:
                        if omcUrlPath:
                            omcUrlFullPath = "http://" + IP + omcUrlPath

                    if "omcUserName" in instrumentInfo:
                        omcUserName = instrumentInfo.get("omcUserName","")
                    if "omcPassword" in instrumentInfo:
                        omcUserName = instrumentInfo.get("omcPassword","")

                    rtnData["omcUrl"] = omcUrlFullPath
                    rtnData["omcUserName"] = omcUserName
                    rtnData["omcPassword"] = omcPassword
                    rtnData["instrumentName"] = instrumentName

                    result["data"] = rtnData

                else:
                    #data invalid
                    errCode = "BA"
        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#stock related begin

#行业信息增加代码
def funcIndustryInfoAdd(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                #data validation check
                industry_code = dataSet.get("industry_code", "")
                if industry_code:
                    dataValidFlag = True
                else:
                    dataValidFlag = False                
                if dataValidFlag:
                    saveSet = {}
                    saveSet["industry_code"] = industry_code 
                    saveSet["industry_code_sw"] = dataSet.get("industry_code_sw", "") 
                    saveSet["industry_name"] = dataSet.get("industry_name", "") 
                    saveSet["industry_name_sw"] = dataSet.get("industry_name_sw", "") 
                    saveSet["industry_name_em"] = dataSet.get("industry_name_em", "") 
                    saveSet["parenet_industry"] = dataSet.get("parenet_industry", "") 
                    saveSet["parenet_industry_sw"] = dataSet.get("parenet_industry_sw", "") 
                    saveSet["parenet_industry_em"] = dataSet.get("parenet_industry_em", "") 
                    saveSet["industry_level_sw"] = dataSet.get("industry_level_sw", "") 
                    saveSet["industry_level_em"] = dataSet.get("industry_level_em", "") 
                    saveSet["num_of_constituents"] = dataSet.get("num_of_constituents", "") 
                    saveSet["static_PE_ratio"] = dataSet.get("static_PE_ratio", "") 
                    saveSet["TTM_PE_ratio"] = dataSet.get("TTM_PE_ratio", "") 
                    saveSet["PB_ratio"] = dataSet.get("PB_ratio", "") 
                    saveSet["static_divident_yield"] = dataSet.get("static_divident_yield", "") 
                    saveSet["label1"] = dataSet.get("label1", "") 
                    saveSet["label2"] = dataSet.get("label2", "") 
                    saveSet["label3"] = dataSet.get("label3", "") 
                    saveSet["memo"] = dataSet.get("memo", "") 
                    saveSet["dispFlag"] = dataSet.get("dispFlag", "") 
                    saveSet["delFlag"] = dataSet.get("delFlag", "0") 
                    saveSet["regID"] = loginID
                    saveSet["regYMDHMS"] = misc.getTime()

                    tableName = comMysql.tablename_convertor_industry_info()
                    recID = comMysql.insert_industry_info(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},saveSet:{saveSet}")
                    else:
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: recID:{recID}")

                    result = rtnData

                else:
                    #data invalid
                    errCode = "BA"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#行业信息删除代码
def funcIndustryInfoDel(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                id = dataSet.get("id", "")
                tableName = comMysql.tablename_convertor_industry_info()
                currDataList = comMysql.query_industry_info(tableName,id)
                if len(currDataList) == 1:
                    saveSet = {}
                    saveSet["modifyID"] = loginID
                    saveSet["modifyYMDHMS"] = misc.getTime()
                    #saveSet["delFlag"] = "1"

                    rtn = comMysql.delete_industry_info(tableName,id)
                    rtnData["rtn"] = str(rtn)

                    if _DEBUG:
                        _LOG.info(f"D: rtn:{rtn}")

                    result = rtnData

                else:
                    errCode = "CB"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#行业信息修改代码
def funcIndustryInfoModify(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                id = dataSet.get("id") 
                industry_code = dataSet.get("industry_code") 
                industry_code_sw = dataSet.get("industry_code_sw") 
                industry_name = dataSet.get("industry_name") 
                industry_name_sw = dataSet.get("industry_name_sw") 
                industry_name_em = dataSet.get("industry_name_em") 
                parenet_industry = dataSet.get("parenet_industry") 
                parenet_industry_sw = dataSet.get("parenet_industry_sw") 
                parenet_industry_em = dataSet.get("parenet_industry_em") 
                industry_level_sw = dataSet.get("industry_level_sw") 
                industry_level_em = dataSet.get("industry_level_em") 
                num_of_constituents = dataSet.get("num_of_constituents") 
                static_PE_ratio = dataSet.get("static_PE_ratio") 
                TTM_PE_ratio = dataSet.get("TTM_PE_ratio") 
                PB_ratio = dataSet.get("PB_ratio") 
                static_divident_yield = dataSet.get("static_divident_yield") 
                label1 = dataSet.get("label1") 
                label2 = dataSet.get("label2") 
                label3 = dataSet.get("label3") 
                memo = dataSet.get("memo") 
                regID = dataSet.get("regID") 
                regYMDHMS = dataSet.get("regYMDHMS") 
                modifyID = dataSet.get("modifyID") 
                modifyYMDHMS = dataSet.get("modifyYMDHMS") 
                dispFlag = dataSet.get("dispFlag") 
                delFlag = dataSet.get("delFlag") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    recID = dataSet.get("id", "")

                    tableName = comMysql.tablename_convertor_industry_info()
                    currDataList = comMysql.query_industry_info(tableName,recID)

                    if len(currDataList) == 1:
                        currDataSet = currDataList[0]

                        #权限或其他检查
                        if errCode == "B0": #

                            saveSet = {}

                            if industry_code != currDataSet.get("industry_code") and industry_code:
                                saveSet["industry_code"] = industry_code

                            if industry_code_sw != currDataSet.get("industry_code_sw") and industry_code_sw:
                                saveSet["industry_code_sw"] = industry_code_sw

                            if industry_name != currDataSet.get("industry_name") and industry_name:
                                saveSet["industry_name"] = industry_name

                            if industry_name_sw != currDataSet.get("industry_name_sw") and industry_name_sw:
                                saveSet["industry_name_sw"] = industry_name_sw

                            if industry_name_em != currDataSet.get("industry_name_em") and industry_name_em:
                                saveSet["industry_name_em"] = industry_name_em

                            if parenet_industry != currDataSet.get("parenet_industry") and parenet_industry:
                                saveSet["parenet_industry"] = parenet_industry

                            if parenet_industry_sw != currDataSet.get("parenet_industry_sw") and parenet_industry_sw:
                                saveSet["parenet_industry_sw"] = parenet_industry_sw

                            if parenet_industry_em != currDataSet.get("parenet_industry_em") and parenet_industry_em:
                                saveSet["parenet_industry_em"] = parenet_industry_em

                            if industry_level_sw != currDataSet.get("industry_level_sw") and industry_level_sw:
                                saveSet["industry_level_sw"] = industry_level_sw

                            if industry_level_em != currDataSet.get("industry_level_em") and industry_level_em:
                                saveSet["industry_level_em"] = industry_level_em

                            if num_of_constituents != currDataSet.get("num_of_constituents") and num_of_constituents:
                                saveSet["num_of_constituents"] = num_of_constituents

                            if static_PE_ratio != currDataSet.get("static_PE_ratio") and static_PE_ratio:
                                saveSet["static_PE_ratio"] = static_PE_ratio

                            if TTM_PE_ratio != currDataSet.get("TTM_PE_ratio") and TTM_PE_ratio:
                                saveSet["TTM_PE_ratio"] = TTM_PE_ratio

                            if PB_ratio != currDataSet.get("PB_ratio") and PB_ratio:
                                saveSet["PB_ratio"] = PB_ratio

                            if static_divident_yield != currDataSet.get("static_divident_yield") and static_divident_yield:
                                saveSet["static_divident_yield"] = static_divident_yield

                            if label1 != currDataSet.get("label1") and label1:
                                saveSet["label1"] = label1

                            if label2 != currDataSet.get("label2") and label2:
                                saveSet["label2"] = label2

                            if label3 != currDataSet.get("label3") and label3:
                                saveSet["label3"] = label3

                            if memo != currDataSet.get("memo") and memo:
                                saveSet["memo"] = memo

                            if regID != currDataSet.get("regID") and regID:
                                saveSet["regID"] = regID

                            if regYMDHMS != currDataSet.get("regYMDHMS") and regYMDHMS:
                                saveSet["regYMDHMS"] = regYMDHMS

                            if modifyID != currDataSet.get("modifyID") and modifyID:
                                saveSet["modifyID"] = modifyID

                            if modifyYMDHMS != currDataSet.get("modifyYMDHMS") and modifyYMDHMS:
                                saveSet["modifyYMDHMS"] = modifyYMDHMS

                            if dispFlag != currDataSet.get("dispFlag") and dispFlag:
                                saveSet["dispFlag"] = dispFlag

                            if delFlag != currDataSet.get("delFlag") and delFlag:
                                saveSet["delFlag"] = delFlag


                            if saveSet:
                                #saveSet["delFlag"] = "0"
                                saveSet["modifyID"] = loginID
                                saveSet["modifyYMDHMS"] = misc.getTime()

                                #保存数据
                                tableName = comMysql.tablename_convertor_industry_info()
                                rtn = comMysql.update_industry_info(tableName,id,saveSet)
                                rtnData["rtn"] = str(rtn)

                                if rtn < 0:
                                    _LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")
                                else:
                                    if _DEBUG:
                                        pass
                                        _LOG.info(f"D: rtn:{rtn}")

                                result = rtnData

                        else:
                            #BT
                            errCode = "BT"

                    else:
                        #CB
                        errCode = "CB"

                else:
                    #data invalid
                    errCode = "BA"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#行业信息查询代码
def funcIndustryInfoQry(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查

            if errCode == "B0": #
                #获取查询输入参数
                id = dataSet.get("id", "")

                industry_code = dataSet.get("industry_code", "")

                industry_name = dataSet.get("industry_name", "")

                forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记

                searchOption = dataSet.get("searchOption")

                mode = dataSet.get("mode", "full")

                #limitNum = dataSet.get("limitNum",0)

                #权限检查/功能检测

                rightCheckFlag = True

                if rightCheckFlag:

                    #生成indexKey
                    indexKeyDataSet = {} #查询生成index的因素
                    if id:
                        indexKeyDataSet["id"] = id
                    if industry_code:
                        indexKeyDataSet["industry_code"] = industry_code
                    if industry_name:
                        indexKeyDataSet["industry_name"] = industry_name
                    if searchOption:
                        indexKeyDataSet["searchOption"] = searchOption
                    if mode:
                        indexKeyDataSet["mode"] = mode

                    #if limitNum:
                        #indexKeyDataSet["limitNum"] = mode

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) 
                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) 
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) 

                    #判断数据是否在缓冲区:
                    if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:

                        if searchOption:
                            currDataList = []
                            tableName = comMysql.tablename_convertor_industry_info()
                            allDataList = comMysql.query_industry_info(tableName,mode = mode)
                            allowList = ["description", "label"] #筛选字段
                            serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)
                            if serachResultSet["rtn"] == "B0":
                                currDataList = serachResultSet.get("data", [])
                        else:
                            if id:
                                tableName = comMysql.tablename_convertor_industry_info()
                                currDataList = comMysql.query_industry_info(tableName,id,mode = mode)
                            else:
                                tableName = comMysql.tablename_convertor_industry_info()
                                currDataList = comMysql.query_industry_info(tableName,industry_code=industry_code,industry_name=industry_name,mode = mode)

                        dataList = []

                        for currDataSet in currDataList:
                            aSet = {}

                            #需要把文件转移到public domain
                            #appendixFileID00 =  currDataSet.get("appendixFileID00", "")
                            #appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)

                            #if mode == "full":
                                #aSet["houseID"] = currDataSet.get("houseID", "")

                            aSet["id"] = currDataSet.get("id","")
                            aSet["industry_code"] = currDataSet.get("industry_code","")
                            aSet["industry_code_sw"] = currDataSet.get("industry_code_sw","")
                            aSet["industry_name"] = currDataSet.get("industry_name","")
                            aSet["industry_name_sw"] = currDataSet.get("industry_name_sw","")
                            aSet["industry_name_em"] = currDataSet.get("industry_name_em","")
                            aSet["parenet_industry"] = currDataSet.get("parenet_industry","")
                            aSet["parenet_industry_sw"] = currDataSet.get("parenet_industry_sw","")
                            aSet["parenet_industry_em"] = currDataSet.get("parenet_industry_em","")
                            aSet["industry_level_sw"] = currDataSet.get("industry_level_sw","")
                            aSet["industry_level_em"] = currDataSet.get("industry_level_em","")
                            aSet["num_of_constituents"] = currDataSet.get("num_of_constituents","")
                            aSet["static_PE_ratio"] = currDataSet.get("static_PE_ratio","")
                            aSet["TTM_PE_ratio"] = currDataSet.get("TTM_PE_ratio","")
                            aSet["PB_ratio"] = currDataSet.get("PB_ratio","")
                            aSet["static_divident_yield"] = currDataSet.get("static_divident_yield","")
                            aSet["label1"] = currDataSet.get("label1","")
                            aSet["label2"] = currDataSet.get("label2","")
                            aSet["label3"] = currDataSet.get("label3","")
                            aSet["memo"] = currDataSet.get("memo","")
                            aSet["regID"] = currDataSet.get("regID","")
                            aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                            aSet["modifyID"] = currDataSet.get("modifyID","")
                            aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                            aSet["dispFlag"] = currDataSet.get("dispFlag","")
                            aSet["delFlag"] = currDataSet.get("delFlag","")

                            dataList.append(aSet)

                        #临时缓存机制,改进型, 2023/10/16
                        indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去

                    rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)

                    #rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "BT"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票信息增加代码
def funcStockInfoAdd(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                #data validation check
                symbol = dataSet.get("symbol", "")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code","")
                if stock_code:
                    dataValidFlag = True
                else:
                    dataValidFlag = False                

                if dataValidFlag:
                    saveSet = {}
                    saveSet["stock_code"] = stock_code 
                    saveSet["stock_name"] = dataSet.get("stock_name", "") 
                    saveSet["total_shares_outstanding"] = dataSet.get("total_shares_outstanding", "") 
                    saveSet["public_float"] = dataSet.get("public_float", "") 
                    saveSet["market_cap"] = dataSet.get("market_cap", "") 
                    saveSet["free_market_cap"] = dataSet.get("free_market_cap", "") 
                    saveSet["industry_code"] = dataSet.get("industry_code", "") 
                    saveSet["industry_name"] = dataSet.get("industry_name", "") 
                    saveSet["industry_name_sw"] = dataSet.get("industry_name_sw", "") 
                    saveSet["industry_name_em"] = dataSet.get("industry_name_em", "") 
                    saveSet["ipo_date"] = dataSet.get("ipo_date", "") 
                    saveSet["label1"] = dataSet.get("label1", "") 
                    saveSet["label2"] = dataSet.get("label2", "") 
                    saveSet["label3"] = dataSet.get("label3", "") 
                    saveSet["memo"] = dataSet.get("memo", "") 
                    saveSet["dispFlag"] = dataSet.get("dispFlag", "") 
                    saveSet["delFlag"] = dataSet.get("delFlag", "0") 
                    saveSet["regID"] = loginID
                    saveSet["regYMDHMS"] = misc.getTime()

                    tableName = comMysql.tablename_convertor_stock_info()
                    recID = comMysql.insert_stock_info(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},saveSet:{saveSet}")
                    else:
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: recID:{recID}")

                    result = rtnData

                else:
                    #data invalid
                    errCode = "BA"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票信息删除代码
def funcStockInfoDel(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                id = dataSet.get("id", "")
                tableName = comMysql.tablename_convertor_stock_info()
                currDataList = comMysql.query_stock_info(tableName,id)
                if len(currDataList) == 1:
                    saveSet = {}
                    saveSet["modifyID"] = loginID
                    saveSet["modifyYMDHMS"] = misc.getTime()
                    #saveSet["delFlag"] = "1"

                    rtn = comMysql.delete_stock_info(tableName,id)
                    rtnData["rtn"] = str(rtn)

                    if _DEBUG:
                        _LOG.info(f"D: rtn:{rtn}")

                    result = rtnData

                else:
                    errCode = "CB"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票信息修改代码
def funcStockInfoModify(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                id = dataSet.get("id") 
                stock_code = dataSet.get("stock_code") 
                stock_name = dataSet.get("stock_name") 
                total_shares_outstanding = dataSet.get("total_shares_outstanding") 
                public_float = dataSet.get("public_float") 
                market_cap = dataSet.get("market_cap") 
                free_market_cap = dataSet.get("free_market_cap") 
                industry_code = dataSet.get("industry_code") 
                industry_name = dataSet.get("industry_name") 
                industry_name_sw = dataSet.get("industry_name_sw") 
                industry_name_em = dataSet.get("industry_name_em") 
                ipo_date = dataSet.get("ipo_date") 
                label1 = dataSet.get("label1") 
                label2 = dataSet.get("label2") 
                label3 = dataSet.get("label3") 
                memo = dataSet.get("memo") 
                dispFlag = dataSet.get("dispFlag") 
                delFlag = dataSet.get("delFlag") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    recID = dataSet.get("id", "")

                    tableName = comMysql.tablename_convertor_stock_info()
                    currDataList = comMysql.query_stock_info(tableName,recID)

                    if len(currDataList) == 1:
                        currDataSet = currDataList[0]

                        #权限或其他检查
                        if errCode == "B0": #

                            saveSet = {}

                            if stock_code != currDataSet.get("stock_code") and stock_code:
                                saveSet["stock_code"] = stock_code

                            if stock_name != currDataSet.get("stock_name") and stock_name:
                                saveSet["stock_name"] = stock_name

                            if total_shares_outstanding != currDataSet.get("total_shares_outstanding") and total_shares_outstanding:
                                saveSet["total_shares_outstanding"] = total_shares_outstanding

                            if public_float != currDataSet.get("public_float") and public_float:
                                saveSet["public_float"] = public_float

                            if market_cap != currDataSet.get("market_cap") and market_cap:
                                saveSet["market_cap"] = market_cap

                            if free_market_cap != currDataSet.get("free_market_cap") and free_market_cap:
                                saveSet["free_market_cap"] = free_market_cap

                            if industry_code != currDataSet.get("industry_code") and industry_code:
                                saveSet["industry_code"] = industry_code

                            if industry_name != currDataSet.get("industry_name") and industry_name:
                                saveSet["industry_name"] = industry_name

                            if industry_name_sw != currDataSet.get("industry_name_sw") and industry_name_sw:
                                saveSet["industry_name_sw"] = industry_name_sw

                            if industry_name_em != currDataSet.get("industry_name_em") and industry_name_em:
                                saveSet["industry_name_em"] = industry_name_em

                            if ipo_date != currDataSet.get("ipo_date") and ipo_date:
                                saveSet["ipo_date"] = ipo_date

                            if label1 != currDataSet.get("label1") and label1:
                                saveSet["label1"] = label1

                            if label2 != currDataSet.get("label2") and label2:
                                saveSet["label2"] = label2

                            if label3 != currDataSet.get("label3") and label3:
                                saveSet["label3"] = label3

                            if memo != currDataSet.get("memo") and memo:
                                saveSet["memo"] = memo

                            if dispFlag != currDataSet.get("dispFlag") and dispFlag:
                                saveSet["dispFlag"] = dispFlag

                            if delFlag != currDataSet.get("delFlag") and delFlag:
                                saveSet["delFlag"] = delFlag

                            if saveSet:
                                #saveSet["delFlag"] = "0"
                                saveSet["modifyID"] = loginID
                                saveSet["modifyYMDHMS"] = misc.getTime()

                                #保存数据
                                tableName = comMysql.tablename_convertor_stock_info()
                                rtn = comMysql.update_stock_info(tableName,id,saveSet)
                                rtnData["rtn"] = str(rtn)

                                if rtn < 0:
                                    _LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")
                                else:
                                    if _DEBUG:
                                        pass
                                        _LOG.info(f"D: rtn:{rtn}")

                                result = rtnData

                        else:
                            #BT
                            errCode = "BT"

                    else:
                        #CB
                        errCode = "CB"

                else:
                    #data invalid
                    errCode = "BA"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票信息查询代码
def funcStockInfoQry(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查

            if errCode == "B0": #
                #获取查询输入参数
                id = dataSet.get("id", "")

                #houseID = dataSet.get("houseID", "")
                symbol = dataSet.get("symbol","")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code","")

                stock_name = dataSet.get("stock_name","")
                industry_code = dataSet.get("industry_code","")
                industry_name = dataSet.get("industry_name","")

                forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记

                searchOption = dataSet.get("searchOption")

                mode = dataSet.get("mode", "full")

                limitNum = dataSet.get("limitNum",0)

                #权限检查/功能检测

                rightCheckFlag = True

                if rightCheckFlag:

                    #生成indexKey
                    indexKeyDataSet = {} #查询生成index的因素
                    if id:
                        indexKeyDataSet["id"] = id
                    if stock_code:
                        indexKeyDataSet["stock_code"] = stock_code
                    if stock_name:
                        indexKeyDataSet["stock_name"] = stock_name
                    if industry_code:
                        indexKeyDataSet["industry_code"] = industry_code
                    if industry_name:
                        indexKeyDataSet["industry_name"] = industry_name

                    if searchOption:
                        indexKeyDataSet["searchOption"] = searchOption
                    if mode:
                        indexKeyDataSet["mode"] = mode

                    #if limitNum:
                        #indexKeyDataSet["limitNum"] = mode

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) 
                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) 
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) 

                    #判断数据是否在缓冲区:
                    if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:

                        if searchOption:
                            currDataList = []
                            tableName = comMysql.tablename_convertor_stock_info()
                            allDataList = comMysql.query_stock_info(tableName,mode = mode)
                            allowList = ["description", "label"] #筛选字段
                            serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)
                            if serachResultSet["rtn"] == "B0":
                                currDataList = serachResultSet.get("data", [])
                        else:
                            if id:
                                tableName = comMysql.tablename_convertor_stock_info()
                                currDataList = comMysql.query_stock_info(tableName,id,mode = mode)
                            else:
                                tableName = comMysql.tablename_convertor_stock_info()
                                currDataList = comMysql.query_stock_info(tableName,stock_code=stock_code,stock_name=stock_name,
                                    industry_code=industry_code,industry_name=industry_name)

                        dataList = []

                        for currDataSet in currDataList:
                            aSet = {}

                            #需要把文件转移到public domain
                            #appendixFileID00 =  currDataSet.get("appendixFileID00", "")
                            #appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)

                            #if mode == "full":
                                #aSet["houseID"] = currDataSet.get("houseID", "")

                            aSet["id"] = currDataSet.get("id","")
                            aSet["stock_code"] = currDataSet.get("stock_code","")
                            aSet["stock_name"] = currDataSet.get("stock_name","")
                            aSet["total_shares_outstanding"] = currDataSet.get("total_shares_outstanding","")
                            aSet["public_float"] = currDataSet.get("public_float","")
                            aSet["market_cap"] = currDataSet.get("market_cap","")
                            aSet["free_market_cap"] = currDataSet.get("free_market_cap","")
                            aSet["industry_code"] = currDataSet.get("industry_code","")
                            aSet["industry_name"] = currDataSet.get("industry_name","")
                            aSet["industry_name_sw"] = currDataSet.get("industry_name_sw","")
                            aSet["industry_name_em"] = currDataSet.get("industry_name_em","")
                            aSet["ipo_date"] = currDataSet.get("ipo_date","")
                            aSet["label1"] = currDataSet.get("label1","")
                            aSet["label2"] = currDataSet.get("label2","")
                            aSet["label3"] = currDataSet.get("label3","")
                            aSet["memo"] = currDataSet.get("memo","")
                            aSet["regID"] = currDataSet.get("regID","")
                            aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                            aSet["modifyID"] = currDataSet.get("modifyID","")
                            aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                            aSet["dispFlag"] = currDataSet.get("dispFlag","")
                            aSet["delFlag"] = currDataSet.get("delFlag","")

                            dataList.append(aSet)

                        #临时缓存机制,改进型, 2023/10/16
                        indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去

                    rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)

                    #rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "BT"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result



#股票历史数据增加代码
def funcStockHistoryAdd(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                #data validation check
                period = dataSet.get("period", "")
                adjust = dataSet.get("adjust", "")

                symbol = dataSet.get("symbol", "")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code","")
                if stock_code:
                    dataValidFlag = True
                else:
                    dataValidFlag = False                
                if dataValidFlag:
                    saveSet = {}
                    saveSet["stock_code"] = stock_code
                    saveSet["date"] = dataSet.get("date", "") 
                    saveSet["open"] = dataSet.get("open", "") 
                    saveSet["close"] = dataSet.get("close", "") 
                    saveSet["high"] = dataSet.get("high", "") 
                    saveSet["low"] = dataSet.get("low", "") 
                    saveSet["volume"] = dataSet.get("volume", "") 
                    saveSet["amount"] = dataSet.get("amount", "") 
                    saveSet["amplitude"] = dataSet.get("amplitude", "") 
                    saveSet["pct_change"] = dataSet.get("pct_change", "") 
                    saveSet["price_change"] = dataSet.get("price_change", "") 
                    saveSet["turnover_rate"] = dataSet.get("turnover_rate", "") 
                    saveSet["label1"] = dataSet.get("label1", "") 
                    saveSet["label2"] = dataSet.get("label2", "") 
                    saveSet["label3"] = dataSet.get("label3", "") 
                    saveSet["memo"] = dataSet.get("memo", "") 
                    saveSet["dispFlag"] = dataSet.get("dispFlag", "") 
                    saveSet["delFlag"] = dataSet.get("delFlag", "0") 
                    saveSet["regID"] = loginID
                    saveSet["regYMDHMS"] = misc.getTime()

                    tableName = comMysql.tablename_convertor_stock_history_data(period=period,adjust=adjust)
                    recID = comMysql.insert_stock_history_data(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},saveSet:{saveSet}")
                    else:
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: recID:{recID}")

                    result = rtnData

                else:
                    #data invalid
                    errCode = "BA"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票历史数据删除代码
def funcStockHistoryDel(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                period = dataSet.get("period", "")
                adjust = dataSet.get("adjust", "")

                id = dataSet.get("id", "")
                tableName = comMysql.tablename_convertor_stock_history_data(period=period,adjust=adjust)
                currDataList = comMysql.query_stock_history_data(tableName,id)
                if len(currDataList) == 1:
                    saveSet = {}
                    saveSet["modifyID"] = loginID
                    saveSet["modifyYMDHMS"] = misc.getTime()
                    #saveSet["delFlag"] = "1"

                    rtn = comMysql.delete_stock_history_data(tableName,id)
                    rtnData["rtn"] = str(rtn)

                    if _DEBUG:
                        _LOG.info(f"D: rtn:{rtn}")

                    result = rtnData

                else:
                    errCode = "CB"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票历史数据修改代码
def funcStockHistoryModify(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                period = dataSet.get("period", "")
                adjust = dataSet.get("adjust", "")

                id = dataSet.get("id") 
                stock_code = dataSet.get("stock_code") 
                date = dataSet.get("date") 
                open = dataSet.get("open") 
                close = dataSet.get("close") 
                high = dataSet.get("high") 
                low = dataSet.get("low") 
                volume = dataSet.get("volume") 
                amount = dataSet.get("amount") 
                amplitude = dataSet.get("amplitude") 
                pct_change = dataSet.get("pct_change") 
                price_change = dataSet.get("price_change") 
                turnover_rate = dataSet.get("turnover_rate") 
                label1 = dataSet.get("label1") 
                label2 = dataSet.get("label2") 
                label3 = dataSet.get("label3") 
                memo = dataSet.get("memo") 
                dispFlag = dataSet.get("dispFlag") 
                delFlag = dataSet.get("delFlag") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    recID = dataSet.get("id", "")

                    tableName = comMysql.tablename_convertor_stock_history_data(period=period,adjust=adjust)
                    currDataList = comMysql.query_stock_history_data(tableName,recID)

                    if len(currDataList) == 1:
                        currDataSet = currDataList[0]

                        #权限或其他检查
                        if errCode == "B0": #

                            saveSet = {}

                            if stock_code != currDataSet.get("stock_code") and stock_code:
                                saveSet["stock_code"] = stock_code

                            if date != currDataSet.get("date") and date:
                                saveSet["date"] = date

                            if open != currDataSet.get("open") and open:
                                saveSet["open"] = open

                            if close != currDataSet.get("close") and close:
                                saveSet["close"] = close

                            if high != currDataSet.get("high") and high:
                                saveSet["high"] = high

                            if low != currDataSet.get("low") and low:
                                saveSet["low"] = low

                            if volume != currDataSet.get("volume") and volume:
                                saveSet["volume"] = volume

                            if amount != currDataSet.get("amount") and amount:
                                saveSet["amount"] = amount

                            if amplitude != currDataSet.get("amplitude") and amplitude:
                                saveSet["amplitude"] = amplitude

                            if pct_change != currDataSet.get("pct_change") and pct_change:
                                saveSet["pct_change"] = pct_change

                            if price_change != currDataSet.get("price_change") and price_change:
                                saveSet["price_change"] = price_change

                            if turnover_rate != currDataSet.get("turnover_rate") and turnover_rate:
                                saveSet["turnover_rate"] = turnover_rate

                            if label1 != currDataSet.get("label1") and label1:
                                saveSet["label1"] = label1

                            if label2 != currDataSet.get("label2") and label2:
                                saveSet["label2"] = label2

                            if label3 != currDataSet.get("label3") and label3:
                                saveSet["label3"] = label3

                            if memo != currDataSet.get("memo") and memo:
                                saveSet["memo"] = memo

                            if dispFlag != currDataSet.get("dispFlag") and dispFlag:
                                saveSet["dispFlag"] = dispFlag

                            if delFlag != currDataSet.get("delFlag") and delFlag:
                                saveSet["delFlag"] = delFlag


                            if saveSet:
                                #saveSet["delFlag"] = "0"
                                saveSet["modifyID"] = loginID
                                saveSet["modifyYMDHMS"] = misc.getTime()

                                #保存数据
                                tableName = comMysql.tablename_convertor_stock_history_data(period=period,adjust=adjust)
                                rtn = comMysql.update_stock_history_data(tableName,id,saveSet)
                                rtnData["rtn"] = str(rtn)

                                if rtn < 0:
                                    _LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")
                                else:
                                    if _DEBUG:
                                        pass
                                        _LOG.info(f"D: rtn:{rtn}")

                                result = rtnData

                        else:
                            #BT
                            errCode = "BT"

                    else:
                        #CB
                        errCode = "CB"

                else:
                    #data invalid
                    errCode = "BA"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票历史数据查询代码
def funcStockHistoryQry(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查

            if errCode == "B0": #
                #获取查询输入参数
                id = dataSet.get("id", "")

                #houseID = dataSet.get("houseID", "")
                symbol = dataSet.get("symbol", "")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code", "")  

                period = dataSet.get("period", "")

                adjust = dataSet.get("adjust", "")
                
                stock_name = dataSet.get("stock_name", "")                 

                forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记

                searchOption = dataSet.get("searchOption")

                mode = dataSet.get("mode", "full")

                #limitNum = dataSet.get("limitNum",0)

                #权限检查/功能检测

                rightCheckFlag = True

                if rightCheckFlag:

                    #生成indexKey
                    indexKeyDataSet = {} #查询生成index的因素
                    if id:
                        indexKeyDataSet["id"] = id
                    if stock_code:
                        indexKeyDataSet["stock_code"] = stock_code
                    if period:
                        indexKeyDataSet["period"] = period
                    if adjust:
                        indexKeyDataSet["adjust"] = adjust
                    if stock_code:
                        indexKeyDataSet["stock_code"] = stock_code
                    if stock_name:
                        indexKeyDataSet["stock_name"] = stock_name
                    if searchOption:
                        indexKeyDataSet["searchOption"] = searchOption
                    if mode:
                        indexKeyDataSet["mode"] = mode

                    #if limitNum:
                        #indexKeyDataSet["limitNum"] = mode

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) 
                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) 
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) 

                    #判断数据是否在缓冲区:
                    if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:

                        if searchOption:
                            currDataList = []
                            tableName = comMysql.tablename_convertor_stock_history_data(period=period,adjust=adjust)
                            allDataList = comMysql.query_stock_history_data(tableName,mode = mode)
                            allowList = ["description", "label"] #筛选字段
                            serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)
                            if serachResultSet["rtn"] == "B0":
                                currDataList = serachResultSet.get("data", [])
                        else:
                            if id:
                                tableName = comMysql.tablename_convertor_stock_history_data(period=period,adjust=adjust)
                                currDataList = comMysql.query_stock_history_data(tableName,id,mode = mode)
                            else:
                                tableName = comMysql.tablename_convertor_stock_history_data(period=period,adjust=adjust)
                                currDataList = comMysql.query_stock_history_data(tableName,stock_code=stock_code,stock_name=stock_name)

                        dataList = []

                        for currDataSet in currDataList:
                            aSet = {}

                            #需要把文件转移到public domain
                            #appendixFileID00 =  currDataSet.get("appendixFileID00", "")
                            #appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)

                            #if mode == "full":
                                #aSet["houseID"] = currDataSet.get("houseID", "")

                            aSet["id"] = currDataSet.get("id","")
                            aSet["stock_code"] = currDataSet.get("stock_code","")
                            aSet["date"] = currDataSet.get("date","")
                            aSet["open"] = round(float(currDataSet.get("open","")),2)
                            aSet["close"] = round(float(currDataSet.get("close","")),2)
                            aSet["high"] = round(float(currDataSet.get("high","")),2)
                            aSet["low"] = round(float(currDataSet.get("low","")),2)
                            aSet["volume"] = round(float(currDataSet.get("volume","")),2)
                            aSet["amount"] = round(float(currDataSet.get("amount","")),2)
                            aSet["amplitude"] = round(float(currDataSet.get("amplitude","")),2)
                            try:
                                aSet["price_change"] = round(aSet["close"] - aSet["open"],2)
                                aSet["pct_change"] = round((aSet["price_change"] * 100 / aSet["open"]),2)
                            except:
                                pass
                            aSet["turnover_rate"] = round(float(currDataSet.get("turnover_rate","")),2)
                            aSet["label1"] = currDataSet.get("label1","")
                            aSet["label2"] = currDataSet.get("label2","")
                            aSet["label3"] = currDataSet.get("label3","")
                            aSet["memo"] = currDataSet.get("memo","")
                            aSet["regID"] = currDataSet.get("regID","")
                            aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                            aSet["modifyID"] = currDataSet.get("modifyID","")
                            aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                            aSet["dispFlag"] = currDataSet.get("dispFlag","")
                            aSet["delFlag"] = currDataSet.get("delFlag","")

                            dataList.append(aSet)

                        #临时缓存机制,改进型, 2023/10/16
                        indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去

                    rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)

                    #rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "BT"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票分红数据增加代码
def funcStockDividendAdd(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                #data validation check
                symbol = dataSet.get("symbol", "")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code","")
                if stock_code:
                    dataValidFlag = True
                else:
                    dataValidFlag = False
                    
                if dataValidFlag:
                    saveSet = {}
                    saveSet["stock_code"] = stock_code 
                    saveSet["stock_name"] = dataSet.get("stock_name", "") 
                    saveSet["ipo_date"] = dataSet.get("ipo_date", "") 
                    saveSet["cumulative_dividend"] = dataSet.get("cumulative_dividend", "") 
                    saveSet["annual_dividend"] = dataSet.get("annual_dividend", "") 
                    saveSet["dividend_count"] = dataSet.get("dividend_count", "") 
                    saveSet["total_financing"] = dataSet.get("total_financing", "") 
                    saveSet["financing_count"] = dataSet.get("financing_count", "") 
                    saveSet["label1"] = dataSet.get("label1", "") 
                    saveSet["label2"] = dataSet.get("label2", "") 
                    saveSet["label3"] = dataSet.get("label3", "") 
                    saveSet["memo"] = dataSet.get("memo", "") 
                    saveSet["dispFlag"] = dataSet.get("dispFlag", "") 
                    saveSet["delFlag"] = dataSet.get("delFlag", "0") 
                    saveSet["regID"] = loginID
                    saveSet["regYMDHMS"] = misc.getTime()

                    tableName = comMysql.tablename_convertor_stock_dividend_data()
                    recID = comMysql.insert_stock_dividend_data(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},saveSet:{saveSet}")
                    else:
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: recID:{recID}")

                    result = rtnData

                else:
                    #data invalid
                    errCode = "BA"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票分红数据删除代码
def funcStockDividendDel(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                id = dataSet.get("id", "")
                tableName = comMysql.tablename_convertor_stock_dividend_data()
                currDataList = comMysql.query_stock_dividend_data(tableName,id)
                if len(currDataList) == 1:
                    saveSet = {}
                    saveSet["modifyID"] = loginID
                    saveSet["modifyYMDHMS"] = misc.getTime()
                    #saveSet["delFlag"] = "1"

                    rtn = comMysql.delete_stock_dividend_data(tableName,id)
                    rtnData["rtn"] = str(rtn)

                    if _DEBUG:
                        _LOG.info(f"D: rtn:{rtn}")

                    result = rtnData

                else:
                    errCode = "CB"
        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票分红数据修改代码
def funcStockDividendModify(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                id = dataSet.get("id") 
                stock_code = dataSet.get("stock_code") 
                stock_name = dataSet.get("stock_name") 
                ipo_date = dataSet.get("ipo_date") 
                cumulative_dividend = dataSet.get("cumulative_dividend") 
                annual_dividend = dataSet.get("annual_dividend") 
                dividend_count = dataSet.get("dividend_count") 
                total_financing = dataSet.get("total_financing") 
                financing_count = dataSet.get("financing_count") 
                label1 = dataSet.get("label1") 
                label2 = dataSet.get("label2") 
                label3 = dataSet.get("label3") 
                memo = dataSet.get("memo") 
                regID = dataSet.get("regID") 
                regYMDHMS = dataSet.get("regYMDHMS") 
                modifyID = dataSet.get("modifyID") 
                modifyYMDHMS = dataSet.get("modifyYMDHMS") 
                dispFlag = dataSet.get("dispFlag") 
                delFlag = dataSet.get("delFlag") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    recID = dataSet.get("id", "")

                    tableName = comMysql.tablename_convertor_stock_dividend_data()
                    currDataList = comMysql.query_stock_dividend_data(tableName,recID)

                    if len(currDataList) == 1:
                        currDataSet = currDataList[0]

                        #权限或其他检查
                        if errCode == "B0": #

                            saveSet = {}

                            if stock_code != currDataSet.get("stock_code") and stock_code:
                                saveSet["stock_code"] = stock_code

                            if stock_name != currDataSet.get("stock_name") and stock_name:
                                saveSet["stock_name"] = stock_name

                            if ipo_date != currDataSet.get("ipo_date") and ipo_date:
                                saveSet["ipo_date"] = ipo_date

                            if cumulative_dividend != currDataSet.get("cumulative_dividend") and cumulative_dividend:
                                saveSet["cumulative_dividend"] = cumulative_dividend

                            if annual_dividend != currDataSet.get("annual_dividend") and annual_dividend:
                                saveSet["annual_dividend"] = annual_dividend

                            if dividend_count != currDataSet.get("dividend_count") and dividend_count:
                                saveSet["dividend_count"] = dividend_count

                            if total_financing != currDataSet.get("total_financing") and total_financing:
                                saveSet["total_financing"] = total_financing

                            if financing_count != currDataSet.get("financing_count") and financing_count:
                                saveSet["financing_count"] = financing_count

                            if label1 != currDataSet.get("label1") and label1:
                                saveSet["label1"] = label1

                            if label2 != currDataSet.get("label2") and label2:
                                saveSet["label2"] = label2

                            if label3 != currDataSet.get("label3") and label3:
                                saveSet["label3"] = label3

                            if memo != currDataSet.get("memo") and memo:
                                saveSet["memo"] = memo

                            if dispFlag != currDataSet.get("dispFlag") and dispFlag:
                                saveSet["dispFlag"] = dispFlag

                            if delFlag != currDataSet.get("delFlag") and delFlag:
                                saveSet["delFlag"] = delFlag

                            if saveSet:
                                #saveSet["delFlag"] = "0"
                                saveSet["modifyID"] = loginID
                                saveSet["modifyYMDHMS"] = misc.getTime()

                                #保存数据
                                tableName = comMysql.tablename_convertor_stock_dividend_data()
                                rtn = comMysql.update_stock_dividend_data(tableName,id,saveSet)
                                rtnData["rtn"] = str(rtn)

                                if rtn < 0:
                                    _LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")
                                else:
                                    if _DEBUG:
                                        pass
                                        _LOG.info(f"D: rtn:{rtn}")

                                result = rtnData

                        else:
                            #BT
                            errCode = "BT"

                    else:
                        #CB
                        errCode = "CB"

                else:
                    #data invalid
                    errCode = "BA"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#股票分红数据查询代码
def funcStockDividendQry(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查

            if errCode == "B0": #
                #获取查询输入参数
                id = dataSet.get("id", "")

                symbol = dataSet.get("symbol","")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code","")

                stock_name = dataSet.get("stock_name","")

                forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记

                searchOption = dataSet.get("searchOption")

                mode = dataSet.get("mode", "full")

                #limitNum = dataSet.get("limitNum",0)

                #权限检查/功能检测

                rightCheckFlag = True

                if rightCheckFlag:

                    #生成indexKey
                    indexKeyDataSet = {} #查询生成index的因素
                    if id:
                        indexKeyDataSet["id"] = id
                    if stock_code:
                        indexKeyDataSet["stock_code"] = stock_code
                    if stock_name:
                        indexKeyDataSet["stock_name"] = stock_name
                    if searchOption:
                        indexKeyDataSet["searchOption"] = searchOption
                    if mode:
                        indexKeyDataSet["mode"] = mode

                    #if limitNum:
                        #indexKeyDataSet["limitNum"] = mode

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) 
                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) 
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) 

                    #判断数据是否在缓冲区:
                    if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:

                        if searchOption:
                            currDataList = []
                            tableName = comMysql.tablename_convertor_stock_dividend_data()
                            allDataList = comMysql.query_stock_dividend_data(tableName,mode = mode)
                            allowList = ["description", "label"] #筛选字段
                            serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)
                            if serachResultSet["rtn"] == "B0":
                                currDataList = serachResultSet.get("data", [])
                        else:
                            if id:
                                tableName = comMysql.tablename_convertor_stock_dividend_data()
                                currDataList = comMysql.query_stock_dividend_data(tableName,id,mode = mode)
                            else:
                                tableName = comMysql.tablename_convertor_stock_dividend_data()
                                currDataList = comMysql.query_stock_dividend_data(tableName,stock_code=stock_code,stock_name=stock_name,mode = mode)

                        dataList = []

                        for currDataSet in currDataList:
                            aSet = {}

                            #需要把文件转移到public domain
                            #appendixFileID00 =  currDataSet.get("appendixFileID00", "")
                            #appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)

                            #if mode == "full":
                                #aSet["houseID"] = currDataSet.get("houseID", "")

                            aSet["id"] = currDataSet.get("id","")
                            aSet["stock_code"] = currDataSet.get("stock_code","")
                            aSet["stock_name"] = currDataSet.get("stock_name","")
                            aSet["ipo_date"] = currDataSet.get("ipo_date","")
                            aSet["cumulative_dividend"] = currDataSet.get("cumulative_dividend","")
                            aSet["annual_dividend"] = currDataSet.get("annual_dividend","")
                            aSet["dividend_count"] = currDataSet.get("dividend_count","")
                            aSet["total_financing"] = currDataSet.get("total_financing","")
                            aSet["financing_count"] = currDataSet.get("financing_count","")
                            aSet["label1"] = currDataSet.get("label1","")
                            aSet["label2"] = currDataSet.get("label2","")
                            aSet["label3"] = currDataSet.get("label3","")
                            aSet["memo"] = currDataSet.get("memo","")
                            aSet["regID"] = currDataSet.get("regID","")
                            aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                            aSet["modifyID"] = currDataSet.get("modifyID","")
                            aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                            aSet["dispFlag"] = currDataSet.get("dispFlag","")
                            aSet["delFlag"] = currDataSet.get("delFlag","")

                            dataList.append(aSet)

                        #临时缓存机制,改进型, 2023/10/16
                        indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去

                    rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)

                    #rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "BT"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#行业历史数据增加代码
def funcIndustryHistoryAdd(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                #data validation check
                industry_code = dataSet.get("industry_code", "")
                if industry_code:
                    dataValidFlag = True
                else:
                    dataValidFlag = False                
                if dataValidFlag:
                    saveSet = {}
                    saveSet["industry_code"] = industry_code 
                    saveSet["date"] = dataSet.get("date", "") 
                    saveSet["open"] = dataSet.get("open", "") 
                    saveSet["close"] = dataSet.get("close", "") 
                    saveSet["high"] = dataSet.get("high", "") 
                    saveSet["low"] = dataSet.get("low", "") 
                    saveSet["volume"] = dataSet.get("volume", "") 
                    saveSet["amount"] = dataSet.get("amount", "") 
                    saveSet["amplitude"] = dataSet.get("amplitude", "") 
                    saveSet["pct_change"] = dataSet.get("pct_change", "") 
                    saveSet["price_change"] = dataSet.get("price_change", "") 
                    saveSet["turnover_rate"] = dataSet.get("turnover_rate", "") 
                    saveSet["label1"] = dataSet.get("label1", "") 
                    saveSet["label2"] = dataSet.get("label2", "") 
                    saveSet["label3"] = dataSet.get("label3", "") 
                    saveSet["memo"] = dataSet.get("memo", "") 
                    saveSet["dispFlag"] = dataSet.get("dispFlag", "") 
                    saveSet["delFlag"] = dataSet.get("delFlag", "0") 
                    saveSet["regID"] = loginID
                    saveSet["regYMDHMS"] = misc.getTime()

                    tableName = comMysql.tablename_convertor_industry_history_data()
                    recID = comMysql.insert_industry_history_data(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},saveSet:{saveSet}")
                    else:
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: recID:{recID}")

                    result = rtnData

                else:
                    #data invalid
                    errCode = "BA"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#行业历史数据删除代码
def funcIndustryHistoryDel(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                id = dataSet.get("id", "")
                tableName = comMysql.tablename_convertor_industry_history_data()
                currDataList = comMysql.query_industry_history_data(tableName,id)
                if len(currDataList) == 1:
                    saveSet = {}
                    saveSet["modifyID"] = loginID
                    saveSet["modifyYMDHMS"] = misc.getTime()
                    #saveSet["delFlag"] = "1"

                    rtn = comMysql.delete_industry_history_data(tableName,id)
                    rtnData["rtn"] = str(rtn)

                    if _DEBUG:
                        _LOG.info(f"D: rtn:{rtn}")

                    result = rtnData

                else:
                    errCode = "CB"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#行业历史数据修改代码
def funcIndustryHistoryModify(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                id = dataSet.get("id") 
                industry_code = dataSet.get("industry_code") 
                date = dataSet.get("date") 
                open = dataSet.get("open") 
                close = dataSet.get("close") 
                high = dataSet.get("high") 
                low = dataSet.get("low") 
                volume = dataSet.get("volume") 
                amount = dataSet.get("amount") 
                amplitude = dataSet.get("amplitude") 
                pct_change = dataSet.get("pct_change") 
                price_change = dataSet.get("price_change") 
                turnover_rate = dataSet.get("turnover_rate") 
                label1 = dataSet.get("label1") 
                label2 = dataSet.get("label2") 
                label3 = dataSet.get("label3") 
                memo = dataSet.get("memo") 
                dispFlag = dataSet.get("dispFlag") 
                delFlag = dataSet.get("delFlag") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    recID = dataSet.get("id", "")

                    tableName = comMysql.tablename_convertor_industry_history_data()
                    currDataList = comMysql.query_industry_history_data(tableName,recID)

                    if len(currDataList) == 1:
                        currDataSet = currDataList[0]

                        #权限或其他检查
                        if errCode == "B0": #

                            saveSet = {}

                            if industry_code != currDataSet.get("industry_code") and industry_code:
                                saveSet["industry_code"] = industry_code

                            if date != currDataSet.get("date") and date:
                                saveSet["date"] = date

                            if open != currDataSet.get("open") and open:
                                saveSet["open"] = open

                            if close != currDataSet.get("close") and close:
                                saveSet["close"] = close

                            if high != currDataSet.get("high") and high:
                                saveSet["high"] = high

                            if low != currDataSet.get("low") and low:
                                saveSet["low"] = low

                            if volume != currDataSet.get("volume") and volume:
                                saveSet["volume"] = volume

                            if amount != currDataSet.get("amount") and amount:
                                saveSet["amount"] = amount

                            if amplitude != currDataSet.get("amplitude") and amplitude:
                                saveSet["amplitude"] = amplitude

                            if pct_change != currDataSet.get("pct_change") and pct_change:
                                saveSet["pct_change"] = pct_change

                            if price_change != currDataSet.get("price_change") and price_change:
                                saveSet["price_change"] = price_change

                            if turnover_rate != currDataSet.get("turnover_rate") and turnover_rate:
                                saveSet["turnover_rate"] = turnover_rate

                            if label1 != currDataSet.get("label1") and label1:
                                saveSet["label1"] = label1

                            if label2 != currDataSet.get("label2") and label2:
                                saveSet["label2"] = label2

                            if label3 != currDataSet.get("label3") and label3:
                                saveSet["label3"] = label3

                            if memo != currDataSet.get("memo") and memo:
                                saveSet["memo"] = memo

                            if dispFlag != currDataSet.get("dispFlag") and dispFlag:
                                saveSet["dispFlag"] = dispFlag

                            if delFlag != currDataSet.get("delFlag") and delFlag:
                                saveSet["delFlag"] = delFlag


                            if saveSet:
                                #saveSet["delFlag"] = "0"
                                saveSet["modifyID"] = loginID
                                saveSet["modifyYMDHMS"] = misc.getTime()

                                #保存数据
                                tableName = comMysql.tablename_convertor_industry_history_data()
                                rtn = comMysql.update_industry_history_data(tableName,id,saveSet)
                                rtnData["rtn"] = str(rtn)

                                if rtn < 0:
                                    _LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")
                                else:
                                    if _DEBUG:
                                        pass
                                        _LOG.info(f"D: rtn:{rtn}")

                                result = rtnData

                        else:
                            #BT
                            errCode = "BT"

                    else:
                        #CB
                        errCode = "CB"

                else:
                    #data invalid
                    errCode = "BA"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#行业历史数据查询代码
def funcIndustryHistoryQry(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查

            if errCode == "B0": #
                #获取查询输入参数
                id = dataSet.get("id", "")

                #houseID = dataSet.get("houseID", "")
                industry_code = dataSet.get("industry_code", "")

                forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记

                searchOption = dataSet.get("searchOption")

                mode = dataSet.get("mode", "full")

                limitNum = dataSet.get("limitNum",0)

                #权限检查/功能检测

                rightCheckFlag = True

                if rightCheckFlag:

                    #生成indexKey
                    indexKeyDataSet = {} #查询生成index的因素
                    if id:
                        indexKeyDataSet["id"] = id
                    if industry_code:
                        indexKeyDataSet["industry_code"] = industry_code
                    if searchOption:
                        indexKeyDataSet["searchOption"] = searchOption
                    if mode:
                        indexKeyDataSet["mode"] = mode

                    if limitNum:
                        indexKeyDataSet["limitNum"] = mode

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) 
                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) 
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) 

                    #判断数据是否在缓冲区:
                    if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:

                        if searchOption:
                            currDataList = []
                            tableName = comMysql.tablename_convertor_industry_history_data()
                            allDataList = comMysql.query_industry_history_data(tableName,mode = mode)
                            allowList = ["description", "label"] #筛选字段
                            serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)
                            if serachResultSet["rtn"] == "B0":
                                currDataList = serachResultSet.get("data", [])
                        else:
                            if id:
                                tableName = comMysql.tablename_convertor_industry_history_data()
                                currDataList = comMysql.query_industry_history_data(tableName,id,mode = mode)
                            else:
                                tableName = comMysql.tablename_convertor_industry_history_data()
                                currDataList = comMysql.query_industry_history_data(tableName,industry_code=industry_code,mode = mode)

                        dataList = []

                        for currDataSet in currDataList:
                            aSet = {}

                            #需要把文件转移到public domain
                            #appendixFileID00 =  currDataSet.get("appendixFileID00", "")
                            #appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)

                            #if mode == "full":
                                #aSet["houseID"] = currDataSet.get("houseID", "")

                            aSet["id"] = currDataSet.get("id","")
                            aSet["industry_code"] = currDataSet.get("industry_code","")
                            aSet["date"] = currDataSet.get("date","")
                            aSet["open"] = currDataSet.get("open","")
                            aSet["close"] = currDataSet.get("close","")
                            aSet["high"] = currDataSet.get("high","")
                            aSet["low"] = currDataSet.get("low","")
                            aSet["volume"] = currDataSet.get("volume","")
                            aSet["amount"] = currDataSet.get("amount","")
                            aSet["amplitude"] = currDataSet.get("amplitude","")
                            aSet["pct_change"] = currDataSet.get("pct_change","")
                            aSet["price_change"] = currDataSet.get("price_change","")
                            aSet["turnover_rate"] = currDataSet.get("turnover_rate","")
                            aSet["label1"] = currDataSet.get("label1","")
                            aSet["label2"] = currDataSet.get("label2","")
                            aSet["label3"] = currDataSet.get("label3","")
                            aSet["memo"] = currDataSet.get("memo","")
                            aSet["regID"] = currDataSet.get("regID","")
                            aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                            aSet["modifyID"] = currDataSet.get("modifyID","")
                            aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                            aSet["dispFlag"] = currDataSet.get("dispFlag","")
                            aSet["delFlag"] = currDataSet.get("delFlag","")

                            dataList.append(aSet)

                        #临时缓存机制,改进型, 2023/10/16
                        indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去

                    rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)

                    #rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "BT"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#balance sheet增加代码
def funcBalanceSheetAdd(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                #data validation check
                symbol = dataSet.get("symbol","")
                if symbol:
                    stock_code = symbol
                else:                
                    stock_code = dataSet.get("stock_code", "")
                    
                if stock_code:
                    dataValidFlag = True
                else:
                    dataValidFlag = False
                if dataValidFlag:
                    saveSet = {}
                    saveSet["stock_code"] = stock_code 
                    saveSet["report_date"] = dataSet.get("report_date", "") 
                    saveSet["monetary_capital"] = dataSet.get("monetary_capital", "") 
                    saveSet["settlement_provisions"] = dataSet.get("settlement_provisions", "") 
                    saveSet["loans_to_other_banks"] = dataSet.get("loans_to_other_banks", "") 
                    saveSet["trading_financial_assets"] = dataSet.get("trading_financial_assets", "") 
                    saveSet["financial_assets_purchased_for_resale"] = dataSet.get("financial_assets_purchased_for_resale", "") 
                    saveSet["derivative_financial_assets"] = dataSet.get("derivative_financial_assets", "") 
                    saveSet["notes_and_accounts_receivable"] = dataSet.get("notes_and_accounts_receivable", "") 
                    saveSet["notes_receivable"] = dataSet.get("notes_receivable", "") 
                    saveSet["accounts_receivable"] = dataSet.get("accounts_receivable", "") 
                    saveSet["receivables_financing"] = dataSet.get("receivables_financing", "") 
                    saveSet["prepayments"] = dataSet.get("prepayments", "") 
                    saveSet["dividends_receivable"] = dataSet.get("dividends_receivable", "") 
                    saveSet["interest_receivable"] = dataSet.get("interest_receivable", "") 
                    saveSet["insurance_premiums_receivable"] = dataSet.get("insurance_premiums_receivable", "") 
                    saveSet["reinsurance_receivables"] = dataSet.get("reinsurance_receivables", "") 
                    saveSet["reinsurance_contract_reserves_receivable"] = dataSet.get("reinsurance_contract_reserves_receivable", "") 
                    saveSet["export_tax_rebates_receivable"] = dataSet.get("export_tax_rebates_receivable", "") 
                    saveSet["subsidies_receivable"] = dataSet.get("subsidies_receivable", "") 
                    saveSet["deposits_receivable"] = dataSet.get("deposits_receivable", "") 
                    saveSet["internal_receivables"] = dataSet.get("internal_receivables", "") 
                    saveSet["other_receivables"] = dataSet.get("other_receivables", "") 
                    saveSet["other_receivables_total"] = dataSet.get("other_receivables_total", "") 
                    saveSet["inventories"] = dataSet.get("inventories", "") 
                    saveSet["assets_held_for_sale"] = dataSet.get("assets_held_for_sale", "") 
                    saveSet["prepaid_expenses"] = dataSet.get("prepaid_expenses", "") 
                    saveSet["current_assets_pending_disposal"] = dataSet.get("current_assets_pending_disposal", "") 
                    saveSet["non_current_assets_due_within_one_year"] = dataSet.get("non_current_assets_due_within_one_year", "") 
                    saveSet["other_current_assets"] = dataSet.get("other_current_assets", "") 
                    saveSet["total_current_assets"] = dataSet.get("total_current_assets", "") 
                    saveSet["non_current_assets"] = dataSet.get("non_current_assets", "") 
                    saveSet["loans_and_advances"] = dataSet.get("loans_and_advances", "") 
                    saveSet["debt_investments"] = dataSet.get("debt_investments", "") 
                    saveSet["other_debt_investments"] = dataSet.get("other_debt_investments", "") 
                    saveSet["financial_assets_at_fvoci"] = dataSet.get("financial_assets_at_fvoci", "") 
                    saveSet["financial_assets_at_amortized_cost"] = dataSet.get("financial_assets_at_amortized_cost", "") 
                    saveSet["available_for_sale_financial_assets"] = dataSet.get("available_for_sale_financial_assets", "") 
                    saveSet["long_term_equity_investments"] = dataSet.get("long_term_equity_investments", "") 
                    saveSet["investment_property"] = dataSet.get("investment_property", "") 
                    saveSet["long_term_receivables"] = dataSet.get("long_term_receivables", "") 
                    saveSet["other_equity_instrument_investments"] = dataSet.get("other_equity_instrument_investments", "") 
                    saveSet["other_non_current_financial_assets"] = dataSet.get("other_non_current_financial_assets", "") 
                    saveSet["other_long_term_investments"] = dataSet.get("other_long_term_investments", "") 
                    saveSet["fixed_assets_original_value"] = dataSet.get("fixed_assets_original_value", "") 
                    saveSet["accumulated_depreciation"] = dataSet.get("accumulated_depreciation", "") 
                    saveSet["fixed_assets_net_value"] = dataSet.get("fixed_assets_net_value", "") 
                    saveSet["fixed_assets_impairment_provision"] = dataSet.get("fixed_assets_impairment_provision", "") 
                    saveSet["construction_in_progress_total"] = dataSet.get("construction_in_progress_total", "") 
                    saveSet["construction_in_progress"] = dataSet.get("construction_in_progress", "") 
                    saveSet["construction_materials"] = dataSet.get("construction_materials", "") 
                    saveSet["fixed_assets_net"] = dataSet.get("fixed_assets_net", "") 
                    saveSet["fixed_assets_disposal"] = dataSet.get("fixed_assets_disposal", "") 
                    saveSet["fixed_assets_and_disposal_total"] = dataSet.get("fixed_assets_and_disposal_total", "") 
                    saveSet["productive_biological_assets"] = dataSet.get("productive_biological_assets", "") 
                    saveSet["consumptive_biological_assets"] = dataSet.get("consumptive_biological_assets", "") 
                    saveSet["oil_and_gas_assets"] = dataSet.get("oil_and_gas_assets", "") 
                    saveSet["contract_assets"] = dataSet.get("contract_assets", "") 
                    saveSet["right_of_use_assets"] = dataSet.get("right_of_use_assets", "") 
                    saveSet["intangible_assets"] = dataSet.get("intangible_assets", "") 
                    saveSet["development_expenditure"] = dataSet.get("development_expenditure", "") 
                    saveSet["goodwill"] = dataSet.get("goodwill", "") 
                    saveSet["long_term_deferred_expenses"] = dataSet.get("long_term_deferred_expenses", "") 
                    saveSet["split_share_structure_circulation_rights"] = dataSet.get("split_share_structure_circulation_rights", "") 
                    saveSet["deferred_tax_assets"] = dataSet.get("deferred_tax_assets", "") 
                    saveSet["other_non_current_assets"] = dataSet.get("other_non_current_assets", "") 
                    saveSet["total_non_current_assets"] = dataSet.get("total_non_current_assets", "") 
                    saveSet["total_assets"] = dataSet.get("total_assets", "") 
                    saveSet["current_liabilities"] = dataSet.get("current_liabilities", "") 
                    saveSet["short_term_borrowings"] = dataSet.get("short_term_borrowings", "") 
                    saveSet["borrowings_from_central_bank"] = dataSet.get("borrowings_from_central_bank", "") 
                    saveSet["deposits_from_customers_and_banks"] = dataSet.get("deposits_from_customers_and_banks", "") 
                    saveSet["borrowings_from_other_banks"] = dataSet.get("borrowings_from_other_banks", "") 
                    saveSet["trading_financial_liabilities"] = dataSet.get("trading_financial_liabilities", "") 
                    saveSet["derivative_financial_liabilities"] = dataSet.get("derivative_financial_liabilities", "") 
                    saveSet["notes_and_accounts_payable"] = dataSet.get("notes_and_accounts_payable", "") 
                    saveSet["notes_payable"] = dataSet.get("notes_payable", "") 
                    saveSet["accounts_payable"] = dataSet.get("accounts_payable", "") 
                    saveSet["advances_from_customers"] = dataSet.get("advances_from_customers", "") 
                    saveSet["contract_liabilities"] = dataSet.get("contract_liabilities", "") 
                    saveSet["financial_assets_sold_for_repurchase"] = dataSet.get("financial_assets_sold_for_repurchase", "") 
                    saveSet["fees_and_commissions_payable"] = dataSet.get("fees_and_commissions_payable", "") 
                    saveSet["employee_benefits_payable"] = dataSet.get("employee_benefits_payable", "") 
                    saveSet["taxes_payable"] = dataSet.get("taxes_payable", "") 
                    saveSet["interest_payable"] = dataSet.get("interest_payable", "") 
                    saveSet["dividends_payable"] = dataSet.get("dividends_payable", "") 
                    saveSet["deposits_payable"] = dataSet.get("deposits_payable", "") 
                    saveSet["internal_payables"] = dataSet.get("internal_payables", "") 
                    saveSet["other_payables"] = dataSet.get("other_payables", "") 
                    saveSet["other_payables_total"] = dataSet.get("other_payables_total", "") 
                    saveSet["other_taxes_payable"] = dataSet.get("other_taxes_payable", "") 
                    saveSet["guarantee_liability_reserves"] = dataSet.get("guarantee_liability_reserves", "") 
                    saveSet["reinsurance_payables"] = dataSet.get("reinsurance_payables", "") 
                    saveSet["insurance_contract_reserves"] = dataSet.get("insurance_contract_reserves", "") 
                    saveSet["securities_trading_agency_payables"] = dataSet.get("securities_trading_agency_payables", "") 
                    saveSet["securities_underwriting_agency_payables"] = dataSet.get("securities_underwriting_agency_payables", "") 
                    saveSet["international_settlement"] = dataSet.get("international_settlement", "") 
                    saveSet["domestic_settlement"] = dataSet.get("domestic_settlement", "") 
                    saveSet["accrued_expenses"] = dataSet.get("accrued_expenses", "") 
                    saveSet["estimated_current_liabilities"] = dataSet.get("estimated_current_liabilities", "") 
                    saveSet["short_term_bonds_payable"] = dataSet.get("short_term_bonds_payable", "") 
                    saveSet["liabilities_held_for_sale"] = dataSet.get("liabilities_held_for_sale", "") 
                    saveSet["deferred_revenue_due_within_one_year"] = dataSet.get("deferred_revenue_due_within_one_year", "") 
                    saveSet["non_current_liabilities_due_within_one_year"] = dataSet.get("non_current_liabilities_due_within_one_year", "") 
                    saveSet["other_current_liabilities"] = dataSet.get("other_current_liabilities", "") 
                    saveSet["total_current_liabilities"] = dataSet.get("total_current_liabilities", "") 
                    saveSet["non_current_liabilities"] = dataSet.get("non_current_liabilities", "") 
                    saveSet["long_term_borrowings"] = dataSet.get("long_term_borrowings", "") 
                    saveSet["bonds_payable"] = dataSet.get("bonds_payable", "") 
                    saveSet["bonds_payable_preferred_stock"] = dataSet.get("bonds_payable_preferred_stock", "") 
                    saveSet["bonds_payable_perpetual_bonds"] = dataSet.get("bonds_payable_perpetual_bonds", "") 
                    saveSet["lease_liabilities"] = dataSet.get("lease_liabilities", "") 
                    saveSet["long_term_employee_benefits_payable"] = dataSet.get("long_term_employee_benefits_payable", "") 
                    saveSet["long_term_payables"] = dataSet.get("long_term_payables", "") 
                    saveSet["long_term_payables_total"] = dataSet.get("long_term_payables_total", "") 
                    saveSet["special_payables"] = dataSet.get("special_payables", "") 
                    saveSet["estimated_non_current_liabilities"] = dataSet.get("estimated_non_current_liabilities", "") 
                    saveSet["long_term_deferred_revenue"] = dataSet.get("long_term_deferred_revenue", "") 
                    saveSet["deferred_tax_liabilities"] = dataSet.get("deferred_tax_liabilities", "") 
                    saveSet["other_non_current_liabilities"] = dataSet.get("other_non_current_liabilities", "") 
                    saveSet["total_non_current_liabilities"] = dataSet.get("total_non_current_liabilities", "") 
                    saveSet["total_liabilities"] = dataSet.get("total_liabilities", "") 
                    saveSet["owners_equity"] = dataSet.get("owners_equity", "") 
                    saveSet["paid_in_capital"] = dataSet.get("paid_in_capital", "") 
                    saveSet["other_equity_instruments"] = dataSet.get("other_equity_instruments", "") 
                    saveSet["preferred_stock"] = dataSet.get("preferred_stock", "") 
                    saveSet["perpetual_bonds"] = dataSet.get("perpetual_bonds", "") 
                    saveSet["capital_reserve"] = dataSet.get("capital_reserve", "") 
                    saveSet["less_treasury_stock"] = dataSet.get("less_treasury_stock", "") 
                    saveSet["other_comprehensive_income"] = dataSet.get("other_comprehensive_income", "") 
                    saveSet["special_reserve"] = dataSet.get("special_reserve", "") 
                    saveSet["surplus_reserve"] = dataSet.get("surplus_reserve", "") 
                    saveSet["general_risk_reserve"] = dataSet.get("general_risk_reserve", "") 
                    saveSet["unrecognized_investment_losses"] = dataSet.get("unrecognized_investment_losses", "") 
                    saveSet["retained_earnings"] = dataSet.get("retained_earnings", "") 
                    saveSet["proposed_cash_dividends"] = dataSet.get("proposed_cash_dividends", "") 
                    saveSet["foreign_currency_translation_difference"] = dataSet.get("foreign_currency_translation_difference", "") 
                    saveSet["equity_attributable_to_parent_company"] = dataSet.get("equity_attributable_to_parent_company", "") 
                    saveSet["minority_interests"] = dataSet.get("minority_interests", "") 
                    saveSet["total_owners_equity"] = dataSet.get("total_owners_equity", "") 
                    saveSet["total_liabilities_and_owners_equity"] = dataSet.get("total_liabilities_and_owners_equity", "") 
                    saveSet["label1"] = dataSet.get("label1", "") 
                    saveSet["label2"] = dataSet.get("label2", "") 
                    saveSet["label3"] = dataSet.get("label3", "") 
                    saveSet["memo"] = dataSet.get("memo", "") 
                    saveSet["dispFlag"] = dataSet.get("dispFlag", "") 
                    saveSet["delFlag"] = dataSet.get("delFlag", "0") 
                    saveSet["regID"] = loginID
                    saveSet["regYMDHMS"] = misc.getTime()

                    tableName = comMysql.tablename_convertor_balance_sheets()
                    recID = comMysql.insert_balance_sheets(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},saveSet:{saveSet}")
                    else:
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: recID:{recID}")

                    result = rtnData

                else:
                    #data invalid
                    errCode = "BA"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#balance sheet删除代码
def funcBalanceSheetDel(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                id = dataSet.get("id", "")
                tableName = comMysql.tablename_convertor_balance_sheets()
                currDataList = comMysql.query_balance_sheets(tableName,id)
                if len(currDataList) == 1:
                    saveSet = {}
                    saveSet["modifyID"] = loginID
                    saveSet["modifyYMDHMS"] = misc.getTime()
                    #saveSet["delFlag"] = "1"

                    rtn = comMysql.delete_balance_sheets(tableName,id)
                    rtnData["rtn"] = str(rtn)

                    if _DEBUG:
                        _LOG.info(f"D: rtn:{rtn}")

                    result = rtnData

                else:
                    errCode = "CB"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#balance sheet修改代码
def funcBalanceSheetModify(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                id = dataSet.get("id") 
                stock_code = dataSet.get("stock_code") 
                report_date = dataSet.get("report_date") 
                monetary_capital = dataSet.get("monetary_capital") 
                settlement_provisions = dataSet.get("settlement_provisions") 
                loans_to_other_banks = dataSet.get("loans_to_other_banks") 
                trading_financial_assets = dataSet.get("trading_financial_assets") 
                financial_assets_purchased_for_resale = dataSet.get("financial_assets_purchased_for_resale") 
                derivative_financial_assets = dataSet.get("derivative_financial_assets") 
                notes_and_accounts_receivable = dataSet.get("notes_and_accounts_receivable") 
                notes_receivable = dataSet.get("notes_receivable") 
                accounts_receivable = dataSet.get("accounts_receivable") 
                receivables_financing = dataSet.get("receivables_financing") 
                prepayments = dataSet.get("prepayments") 
                dividends_receivable = dataSet.get("dividends_receivable") 
                interest_receivable = dataSet.get("interest_receivable") 
                insurance_premiums_receivable = dataSet.get("insurance_premiums_receivable") 
                reinsurance_receivables = dataSet.get("reinsurance_receivables") 
                reinsurance_contract_reserves_receivable = dataSet.get("reinsurance_contract_reserves_receivable") 
                export_tax_rebates_receivable = dataSet.get("export_tax_rebates_receivable") 
                subsidies_receivable = dataSet.get("subsidies_receivable") 
                deposits_receivable = dataSet.get("deposits_receivable") 
                internal_receivables = dataSet.get("internal_receivables") 
                other_receivables = dataSet.get("other_receivables") 
                other_receivables_total = dataSet.get("other_receivables_total") 
                inventories = dataSet.get("inventories") 
                assets_held_for_sale = dataSet.get("assets_held_for_sale") 
                prepaid_expenses = dataSet.get("prepaid_expenses") 
                current_assets_pending_disposal = dataSet.get("current_assets_pending_disposal") 
                non_current_assets_due_within_one_year = dataSet.get("non_current_assets_due_within_one_year") 
                other_current_assets = dataSet.get("other_current_assets") 
                total_current_assets = dataSet.get("total_current_assets") 
                non_current_assets = dataSet.get("non_current_assets") 
                loans_and_advances = dataSet.get("loans_and_advances") 
                debt_investments = dataSet.get("debt_investments") 
                other_debt_investments = dataSet.get("other_debt_investments") 
                financial_assets_at_fvoci = dataSet.get("financial_assets_at_fvoci") 
                financial_assets_at_amortized_cost = dataSet.get("financial_assets_at_amortized_cost") 
                available_for_sale_financial_assets = dataSet.get("available_for_sale_financial_assets") 
                long_term_equity_investments = dataSet.get("long_term_equity_investments") 
                investment_property = dataSet.get("investment_property") 
                long_term_receivables = dataSet.get("long_term_receivables") 
                other_equity_instrument_investments = dataSet.get("other_equity_instrument_investments") 
                other_non_current_financial_assets = dataSet.get("other_non_current_financial_assets") 
                other_long_term_investments = dataSet.get("other_long_term_investments") 
                fixed_assets_original_value = dataSet.get("fixed_assets_original_value") 
                accumulated_depreciation = dataSet.get("accumulated_depreciation") 
                fixed_assets_net_value = dataSet.get("fixed_assets_net_value") 
                fixed_assets_impairment_provision = dataSet.get("fixed_assets_impairment_provision") 
                construction_in_progress_total = dataSet.get("construction_in_progress_total") 
                construction_in_progress = dataSet.get("construction_in_progress") 
                construction_materials = dataSet.get("construction_materials") 
                fixed_assets_net = dataSet.get("fixed_assets_net") 
                fixed_assets_disposal = dataSet.get("fixed_assets_disposal") 
                fixed_assets_and_disposal_total = dataSet.get("fixed_assets_and_disposal_total") 
                productive_biological_assets = dataSet.get("productive_biological_assets") 
                consumptive_biological_assets = dataSet.get("consumptive_biological_assets") 
                oil_and_gas_assets = dataSet.get("oil_and_gas_assets") 
                contract_assets = dataSet.get("contract_assets") 
                right_of_use_assets = dataSet.get("right_of_use_assets") 
                intangible_assets = dataSet.get("intangible_assets") 
                development_expenditure = dataSet.get("development_expenditure") 
                goodwill = dataSet.get("goodwill") 
                long_term_deferred_expenses = dataSet.get("long_term_deferred_expenses") 
                split_share_structure_circulation_rights = dataSet.get("split_share_structure_circulation_rights") 
                deferred_tax_assets = dataSet.get("deferred_tax_assets") 
                other_non_current_assets = dataSet.get("other_non_current_assets") 
                total_non_current_assets = dataSet.get("total_non_current_assets") 
                total_assets = dataSet.get("total_assets") 
                current_liabilities = dataSet.get("current_liabilities") 
                short_term_borrowings = dataSet.get("short_term_borrowings") 
                borrowings_from_central_bank = dataSet.get("borrowings_from_central_bank") 
                deposits_from_customers_and_banks = dataSet.get("deposits_from_customers_and_banks") 
                borrowings_from_other_banks = dataSet.get("borrowings_from_other_banks") 
                trading_financial_liabilities = dataSet.get("trading_financial_liabilities") 
                derivative_financial_liabilities = dataSet.get("derivative_financial_liabilities") 
                notes_and_accounts_payable = dataSet.get("notes_and_accounts_payable") 
                notes_payable = dataSet.get("notes_payable") 
                accounts_payable = dataSet.get("accounts_payable") 
                advances_from_customers = dataSet.get("advances_from_customers") 
                contract_liabilities = dataSet.get("contract_liabilities") 
                financial_assets_sold_for_repurchase = dataSet.get("financial_assets_sold_for_repurchase") 
                fees_and_commissions_payable = dataSet.get("fees_and_commissions_payable") 
                employee_benefits_payable = dataSet.get("employee_benefits_payable") 
                taxes_payable = dataSet.get("taxes_payable") 
                interest_payable = dataSet.get("interest_payable") 
                dividends_payable = dataSet.get("dividends_payable") 
                deposits_payable = dataSet.get("deposits_payable") 
                internal_payables = dataSet.get("internal_payables") 
                other_payables = dataSet.get("other_payables") 
                other_payables_total = dataSet.get("other_payables_total") 
                other_taxes_payable = dataSet.get("other_taxes_payable") 
                guarantee_liability_reserves = dataSet.get("guarantee_liability_reserves") 
                reinsurance_payables = dataSet.get("reinsurance_payables") 
                insurance_contract_reserves = dataSet.get("insurance_contract_reserves") 
                securities_trading_agency_payables = dataSet.get("securities_trading_agency_payables") 
                securities_underwriting_agency_payables = dataSet.get("securities_underwriting_agency_payables") 
                international_settlement = dataSet.get("international_settlement") 
                domestic_settlement = dataSet.get("domestic_settlement") 
                accrued_expenses = dataSet.get("accrued_expenses") 
                estimated_current_liabilities = dataSet.get("estimated_current_liabilities") 
                short_term_bonds_payable = dataSet.get("short_term_bonds_payable") 
                liabilities_held_for_sale = dataSet.get("liabilities_held_for_sale") 
                deferred_revenue_due_within_one_year = dataSet.get("deferred_revenue_due_within_one_year") 
                non_current_liabilities_due_within_one_year = dataSet.get("non_current_liabilities_due_within_one_year") 
                other_current_liabilities = dataSet.get("other_current_liabilities") 
                total_current_liabilities = dataSet.get("total_current_liabilities") 
                non_current_liabilities = dataSet.get("non_current_liabilities") 
                long_term_borrowings = dataSet.get("long_term_borrowings") 
                bonds_payable = dataSet.get("bonds_payable") 
                bonds_payable_preferred_stock = dataSet.get("bonds_payable_preferred_stock") 
                bonds_payable_perpetual_bonds = dataSet.get("bonds_payable_perpetual_bonds") 
                lease_liabilities = dataSet.get("lease_liabilities") 
                long_term_employee_benefits_payable = dataSet.get("long_term_employee_benefits_payable") 
                long_term_payables = dataSet.get("long_term_payables") 
                long_term_payables_total = dataSet.get("long_term_payables_total") 
                special_payables = dataSet.get("special_payables") 
                estimated_non_current_liabilities = dataSet.get("estimated_non_current_liabilities") 
                long_term_deferred_revenue = dataSet.get("long_term_deferred_revenue") 
                deferred_tax_liabilities = dataSet.get("deferred_tax_liabilities") 
                other_non_current_liabilities = dataSet.get("other_non_current_liabilities") 
                total_non_current_liabilities = dataSet.get("total_non_current_liabilities") 
                total_liabilities = dataSet.get("total_liabilities") 
                owners_equity = dataSet.get("owners_equity") 
                paid_in_capital = dataSet.get("paid_in_capital") 
                other_equity_instruments = dataSet.get("other_equity_instruments") 
                preferred_stock = dataSet.get("preferred_stock") 
                perpetual_bonds = dataSet.get("perpetual_bonds") 
                capital_reserve = dataSet.get("capital_reserve") 
                less_treasury_stock = dataSet.get("less_treasury_stock") 
                other_comprehensive_income = dataSet.get("other_comprehensive_income") 
                special_reserve = dataSet.get("special_reserve") 
                surplus_reserve = dataSet.get("surplus_reserve") 
                general_risk_reserve = dataSet.get("general_risk_reserve") 
                unrecognized_investment_losses = dataSet.get("unrecognized_investment_losses") 
                retained_earnings = dataSet.get("retained_earnings") 
                proposed_cash_dividends = dataSet.get("proposed_cash_dividends") 
                foreign_currency_translation_difference = dataSet.get("foreign_currency_translation_difference") 
                equity_attributable_to_parent_company = dataSet.get("equity_attributable_to_parent_company") 
                minority_interests = dataSet.get("minority_interests") 
                total_owners_equity = dataSet.get("total_owners_equity") 
                total_liabilities_and_owners_equity = dataSet.get("total_liabilities_and_owners_equity") 
                label1 = dataSet.get("label1") 
                label2 = dataSet.get("label2") 
                label3 = dataSet.get("label3") 
                memo = dataSet.get("memo") 
                dispFlag = dataSet.get("dispFlag") 
                delFlag = dataSet.get("delFlag") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    recID = dataSet.get("id", "")

                    tableName = comMysql.tablename_convertor_balance_sheets()
                    currDataList = comMysql.query_balance_sheets(tableName,recID)

                    if len(currDataList) == 1:
                        currDataSet = currDataList[0]

                        #权限或其他检查
                        if errCode == "B0": #

                            saveSet = {}

                            if stock_code != currDataSet.get("stock_code") and stock_code:
                                saveSet["stock_code"] = stock_code

                            if report_date != currDataSet.get("report_date") and report_date:
                                saveSet["report_date"] = report_date

                            if monetary_capital != currDataSet.get("monetary_capital") and monetary_capital:
                                saveSet["monetary_capital"] = monetary_capital

                            if settlement_provisions != currDataSet.get("settlement_provisions") and settlement_provisions:
                                saveSet["settlement_provisions"] = settlement_provisions

                            if loans_to_other_banks != currDataSet.get("loans_to_other_banks") and loans_to_other_banks:
                                saveSet["loans_to_other_banks"] = loans_to_other_banks

                            if trading_financial_assets != currDataSet.get("trading_financial_assets") and trading_financial_assets:
                                saveSet["trading_financial_assets"] = trading_financial_assets

                            if financial_assets_purchased_for_resale != currDataSet.get("financial_assets_purchased_for_resale") and financial_assets_purchased_for_resale:
                                saveSet["financial_assets_purchased_for_resale"] = financial_assets_purchased_for_resale

                            if derivative_financial_assets != currDataSet.get("derivative_financial_assets") and derivative_financial_assets:
                                saveSet["derivative_financial_assets"] = derivative_financial_assets

                            if notes_and_accounts_receivable != currDataSet.get("notes_and_accounts_receivable") and notes_and_accounts_receivable:
                                saveSet["notes_and_accounts_receivable"] = notes_and_accounts_receivable

                            if notes_receivable != currDataSet.get("notes_receivable") and notes_receivable:
                                saveSet["notes_receivable"] = notes_receivable

                            if accounts_receivable != currDataSet.get("accounts_receivable") and accounts_receivable:
                                saveSet["accounts_receivable"] = accounts_receivable

                            if receivables_financing != currDataSet.get("receivables_financing") and receivables_financing:
                                saveSet["receivables_financing"] = receivables_financing

                            if prepayments != currDataSet.get("prepayments") and prepayments:
                                saveSet["prepayments"] = prepayments

                            if dividends_receivable != currDataSet.get("dividends_receivable") and dividends_receivable:
                                saveSet["dividends_receivable"] = dividends_receivable

                            if interest_receivable != currDataSet.get("interest_receivable") and interest_receivable:
                                saveSet["interest_receivable"] = interest_receivable

                            if insurance_premiums_receivable != currDataSet.get("insurance_premiums_receivable") and insurance_premiums_receivable:
                                saveSet["insurance_premiums_receivable"] = insurance_premiums_receivable

                            if reinsurance_receivables != currDataSet.get("reinsurance_receivables") and reinsurance_receivables:
                                saveSet["reinsurance_receivables"] = reinsurance_receivables

                            if reinsurance_contract_reserves_receivable != currDataSet.get("reinsurance_contract_reserves_receivable") and reinsurance_contract_reserves_receivable:
                                saveSet["reinsurance_contract_reserves_receivable"] = reinsurance_contract_reserves_receivable

                            if export_tax_rebates_receivable != currDataSet.get("export_tax_rebates_receivable") and export_tax_rebates_receivable:
                                saveSet["export_tax_rebates_receivable"] = export_tax_rebates_receivable

                            if subsidies_receivable != currDataSet.get("subsidies_receivable") and subsidies_receivable:
                                saveSet["subsidies_receivable"] = subsidies_receivable

                            if deposits_receivable != currDataSet.get("deposits_receivable") and deposits_receivable:
                                saveSet["deposits_receivable"] = deposits_receivable

                            if internal_receivables != currDataSet.get("internal_receivables") and internal_receivables:
                                saveSet["internal_receivables"] = internal_receivables

                            if other_receivables != currDataSet.get("other_receivables") and other_receivables:
                                saveSet["other_receivables"] = other_receivables

                            if other_receivables_total != currDataSet.get("other_receivables_total") and other_receivables_total:
                                saveSet["other_receivables_total"] = other_receivables_total

                            if inventories != currDataSet.get("inventories") and inventories:
                                saveSet["inventories"] = inventories

                            if assets_held_for_sale != currDataSet.get("assets_held_for_sale") and assets_held_for_sale:
                                saveSet["assets_held_for_sale"] = assets_held_for_sale

                            if prepaid_expenses != currDataSet.get("prepaid_expenses") and prepaid_expenses:
                                saveSet["prepaid_expenses"] = prepaid_expenses

                            if current_assets_pending_disposal != currDataSet.get("current_assets_pending_disposal") and current_assets_pending_disposal:
                                saveSet["current_assets_pending_disposal"] = current_assets_pending_disposal

                            if non_current_assets_due_within_one_year != currDataSet.get("non_current_assets_due_within_one_year") and non_current_assets_due_within_one_year:
                                saveSet["non_current_assets_due_within_one_year"] = non_current_assets_due_within_one_year

                            if other_current_assets != currDataSet.get("other_current_assets") and other_current_assets:
                                saveSet["other_current_assets"] = other_current_assets

                            if total_current_assets != currDataSet.get("total_current_assets") and total_current_assets:
                                saveSet["total_current_assets"] = total_current_assets

                            if non_current_assets != currDataSet.get("non_current_assets") and non_current_assets:
                                saveSet["non_current_assets"] = non_current_assets

                            if loans_and_advances != currDataSet.get("loans_and_advances") and loans_and_advances:
                                saveSet["loans_and_advances"] = loans_and_advances

                            if debt_investments != currDataSet.get("debt_investments") and debt_investments:
                                saveSet["debt_investments"] = debt_investments

                            if other_debt_investments != currDataSet.get("other_debt_investments") and other_debt_investments:
                                saveSet["other_debt_investments"] = other_debt_investments

                            if financial_assets_at_fvoci != currDataSet.get("financial_assets_at_fvoci") and financial_assets_at_fvoci:
                                saveSet["financial_assets_at_fvoci"] = financial_assets_at_fvoci

                            if financial_assets_at_amortized_cost != currDataSet.get("financial_assets_at_amortized_cost") and financial_assets_at_amortized_cost:
                                saveSet["financial_assets_at_amortized_cost"] = financial_assets_at_amortized_cost

                            if available_for_sale_financial_assets != currDataSet.get("available_for_sale_financial_assets") and available_for_sale_financial_assets:
                                saveSet["available_for_sale_financial_assets"] = available_for_sale_financial_assets

                            if long_term_equity_investments != currDataSet.get("long_term_equity_investments") and long_term_equity_investments:
                                saveSet["long_term_equity_investments"] = long_term_equity_investments

                            if investment_property != currDataSet.get("investment_property") and investment_property:
                                saveSet["investment_property"] = investment_property

                            if long_term_receivables != currDataSet.get("long_term_receivables") and long_term_receivables:
                                saveSet["long_term_receivables"] = long_term_receivables

                            if other_equity_instrument_investments != currDataSet.get("other_equity_instrument_investments") and other_equity_instrument_investments:
                                saveSet["other_equity_instrument_investments"] = other_equity_instrument_investments

                            if other_non_current_financial_assets != currDataSet.get("other_non_current_financial_assets") and other_non_current_financial_assets:
                                saveSet["other_non_current_financial_assets"] = other_non_current_financial_assets

                            if other_long_term_investments != currDataSet.get("other_long_term_investments") and other_long_term_investments:
                                saveSet["other_long_term_investments"] = other_long_term_investments

                            if fixed_assets_original_value != currDataSet.get("fixed_assets_original_value") and fixed_assets_original_value:
                                saveSet["fixed_assets_original_value"] = fixed_assets_original_value

                            if accumulated_depreciation != currDataSet.get("accumulated_depreciation") and accumulated_depreciation:
                                saveSet["accumulated_depreciation"] = accumulated_depreciation

                            if fixed_assets_net_value != currDataSet.get("fixed_assets_net_value") and fixed_assets_net_value:
                                saveSet["fixed_assets_net_value"] = fixed_assets_net_value

                            if fixed_assets_impairment_provision != currDataSet.get("fixed_assets_impairment_provision") and fixed_assets_impairment_provision:
                                saveSet["fixed_assets_impairment_provision"] = fixed_assets_impairment_provision

                            if construction_in_progress_total != currDataSet.get("construction_in_progress_total") and construction_in_progress_total:
                                saveSet["construction_in_progress_total"] = construction_in_progress_total

                            if construction_in_progress != currDataSet.get("construction_in_progress") and construction_in_progress:
                                saveSet["construction_in_progress"] = construction_in_progress

                            if construction_materials != currDataSet.get("construction_materials") and construction_materials:
                                saveSet["construction_materials"] = construction_materials

                            if fixed_assets_net != currDataSet.get("fixed_assets_net") and fixed_assets_net:
                                saveSet["fixed_assets_net"] = fixed_assets_net

                            if fixed_assets_disposal != currDataSet.get("fixed_assets_disposal") and fixed_assets_disposal:
                                saveSet["fixed_assets_disposal"] = fixed_assets_disposal

                            if fixed_assets_and_disposal_total != currDataSet.get("fixed_assets_and_disposal_total") and fixed_assets_and_disposal_total:
                                saveSet["fixed_assets_and_disposal_total"] = fixed_assets_and_disposal_total

                            if productive_biological_assets != currDataSet.get("productive_biological_assets") and productive_biological_assets:
                                saveSet["productive_biological_assets"] = productive_biological_assets

                            if consumptive_biological_assets != currDataSet.get("consumptive_biological_assets") and consumptive_biological_assets:
                                saveSet["consumptive_biological_assets"] = consumptive_biological_assets

                            if oil_and_gas_assets != currDataSet.get("oil_and_gas_assets") and oil_and_gas_assets:
                                saveSet["oil_and_gas_assets"] = oil_and_gas_assets

                            if contract_assets != currDataSet.get("contract_assets") and contract_assets:
                                saveSet["contract_assets"] = contract_assets

                            if right_of_use_assets != currDataSet.get("right_of_use_assets") and right_of_use_assets:
                                saveSet["right_of_use_assets"] = right_of_use_assets

                            if intangible_assets != currDataSet.get("intangible_assets") and intangible_assets:
                                saveSet["intangible_assets"] = intangible_assets

                            if development_expenditure != currDataSet.get("development_expenditure") and development_expenditure:
                                saveSet["development_expenditure"] = development_expenditure

                            if goodwill != currDataSet.get("goodwill") and goodwill:
                                saveSet["goodwill"] = goodwill

                            if long_term_deferred_expenses != currDataSet.get("long_term_deferred_expenses") and long_term_deferred_expenses:
                                saveSet["long_term_deferred_expenses"] = long_term_deferred_expenses

                            if split_share_structure_circulation_rights != currDataSet.get("split_share_structure_circulation_rights") and split_share_structure_circulation_rights:
                                saveSet["split_share_structure_circulation_rights"] = split_share_structure_circulation_rights

                            if deferred_tax_assets != currDataSet.get("deferred_tax_assets") and deferred_tax_assets:
                                saveSet["deferred_tax_assets"] = deferred_tax_assets

                            if other_non_current_assets != currDataSet.get("other_non_current_assets") and other_non_current_assets:
                                saveSet["other_non_current_assets"] = other_non_current_assets

                            if total_non_current_assets != currDataSet.get("total_non_current_assets") and total_non_current_assets:
                                saveSet["total_non_current_assets"] = total_non_current_assets

                            if total_assets != currDataSet.get("total_assets") and total_assets:
                                saveSet["total_assets"] = total_assets

                            if current_liabilities != currDataSet.get("current_liabilities") and current_liabilities:
                                saveSet["current_liabilities"] = current_liabilities

                            if short_term_borrowings != currDataSet.get("short_term_borrowings") and short_term_borrowings:
                                saveSet["short_term_borrowings"] = short_term_borrowings

                            if borrowings_from_central_bank != currDataSet.get("borrowings_from_central_bank") and borrowings_from_central_bank:
                                saveSet["borrowings_from_central_bank"] = borrowings_from_central_bank

                            if deposits_from_customers_and_banks != currDataSet.get("deposits_from_customers_and_banks") and deposits_from_customers_and_banks:
                                saveSet["deposits_from_customers_and_banks"] = deposits_from_customers_and_banks

                            if borrowings_from_other_banks != currDataSet.get("borrowings_from_other_banks") and borrowings_from_other_banks:
                                saveSet["borrowings_from_other_banks"] = borrowings_from_other_banks

                            if trading_financial_liabilities != currDataSet.get("trading_financial_liabilities") and trading_financial_liabilities:
                                saveSet["trading_financial_liabilities"] = trading_financial_liabilities

                            if derivative_financial_liabilities != currDataSet.get("derivative_financial_liabilities") and derivative_financial_liabilities:
                                saveSet["derivative_financial_liabilities"] = derivative_financial_liabilities

                            if notes_and_accounts_payable != currDataSet.get("notes_and_accounts_payable") and notes_and_accounts_payable:
                                saveSet["notes_and_accounts_payable"] = notes_and_accounts_payable

                            if notes_payable != currDataSet.get("notes_payable") and notes_payable:
                                saveSet["notes_payable"] = notes_payable

                            if accounts_payable != currDataSet.get("accounts_payable") and accounts_payable:
                                saveSet["accounts_payable"] = accounts_payable

                            if advances_from_customers != currDataSet.get("advances_from_customers") and advances_from_customers:
                                saveSet["advances_from_customers"] = advances_from_customers

                            if contract_liabilities != currDataSet.get("contract_liabilities") and contract_liabilities:
                                saveSet["contract_liabilities"] = contract_liabilities

                            if financial_assets_sold_for_repurchase != currDataSet.get("financial_assets_sold_for_repurchase") and financial_assets_sold_for_repurchase:
                                saveSet["financial_assets_sold_for_repurchase"] = financial_assets_sold_for_repurchase

                            if fees_and_commissions_payable != currDataSet.get("fees_and_commissions_payable") and fees_and_commissions_payable:
                                saveSet["fees_and_commissions_payable"] = fees_and_commissions_payable

                            if employee_benefits_payable != currDataSet.get("employee_benefits_payable") and employee_benefits_payable:
                                saveSet["employee_benefits_payable"] = employee_benefits_payable

                            if taxes_payable != currDataSet.get("taxes_payable") and taxes_payable:
                                saveSet["taxes_payable"] = taxes_payable

                            if interest_payable != currDataSet.get("interest_payable") and interest_payable:
                                saveSet["interest_payable"] = interest_payable

                            if dividends_payable != currDataSet.get("dividends_payable") and dividends_payable:
                                saveSet["dividends_payable"] = dividends_payable

                            if deposits_payable != currDataSet.get("deposits_payable") and deposits_payable:
                                saveSet["deposits_payable"] = deposits_payable

                            if internal_payables != currDataSet.get("internal_payables") and internal_payables:
                                saveSet["internal_payables"] = internal_payables

                            if other_payables != currDataSet.get("other_payables") and other_payables:
                                saveSet["other_payables"] = other_payables

                            if other_payables_total != currDataSet.get("other_payables_total") and other_payables_total:
                                saveSet["other_payables_total"] = other_payables_total

                            if other_taxes_payable != currDataSet.get("other_taxes_payable") and other_taxes_payable:
                                saveSet["other_taxes_payable"] = other_taxes_payable

                            if guarantee_liability_reserves != currDataSet.get("guarantee_liability_reserves") and guarantee_liability_reserves:
                                saveSet["guarantee_liability_reserves"] = guarantee_liability_reserves

                            if reinsurance_payables != currDataSet.get("reinsurance_payables") and reinsurance_payables:
                                saveSet["reinsurance_payables"] = reinsurance_payables

                            if insurance_contract_reserves != currDataSet.get("insurance_contract_reserves") and insurance_contract_reserves:
                                saveSet["insurance_contract_reserves"] = insurance_contract_reserves

                            if securities_trading_agency_payables != currDataSet.get("securities_trading_agency_payables") and securities_trading_agency_payables:
                                saveSet["securities_trading_agency_payables"] = securities_trading_agency_payables

                            if securities_underwriting_agency_payables != currDataSet.get("securities_underwriting_agency_payables") and securities_underwriting_agency_payables:
                                saveSet["securities_underwriting_agency_payables"] = securities_underwriting_agency_payables

                            if international_settlement != currDataSet.get("international_settlement") and international_settlement:
                                saveSet["international_settlement"] = international_settlement

                            if domestic_settlement != currDataSet.get("domestic_settlement") and domestic_settlement:
                                saveSet["domestic_settlement"] = domestic_settlement

                            if accrued_expenses != currDataSet.get("accrued_expenses") and accrued_expenses:
                                saveSet["accrued_expenses"] = accrued_expenses

                            if estimated_current_liabilities != currDataSet.get("estimated_current_liabilities") and estimated_current_liabilities:
                                saveSet["estimated_current_liabilities"] = estimated_current_liabilities

                            if short_term_bonds_payable != currDataSet.get("short_term_bonds_payable") and short_term_bonds_payable:
                                saveSet["short_term_bonds_payable"] = short_term_bonds_payable

                            if liabilities_held_for_sale != currDataSet.get("liabilities_held_for_sale") and liabilities_held_for_sale:
                                saveSet["liabilities_held_for_sale"] = liabilities_held_for_sale

                            if deferred_revenue_due_within_one_year != currDataSet.get("deferred_revenue_due_within_one_year") and deferred_revenue_due_within_one_year:
                                saveSet["deferred_revenue_due_within_one_year"] = deferred_revenue_due_within_one_year

                            if non_current_liabilities_due_within_one_year != currDataSet.get("non_current_liabilities_due_within_one_year") and non_current_liabilities_due_within_one_year:
                                saveSet["non_current_liabilities_due_within_one_year"] = non_current_liabilities_due_within_one_year

                            if other_current_liabilities != currDataSet.get("other_current_liabilities") and other_current_liabilities:
                                saveSet["other_current_liabilities"] = other_current_liabilities

                            if total_current_liabilities != currDataSet.get("total_current_liabilities") and total_current_liabilities:
                                saveSet["total_current_liabilities"] = total_current_liabilities

                            if non_current_liabilities != currDataSet.get("non_current_liabilities") and non_current_liabilities:
                                saveSet["non_current_liabilities"] = non_current_liabilities

                            if long_term_borrowings != currDataSet.get("long_term_borrowings") and long_term_borrowings:
                                saveSet["long_term_borrowings"] = long_term_borrowings

                            if bonds_payable != currDataSet.get("bonds_payable") and bonds_payable:
                                saveSet["bonds_payable"] = bonds_payable

                            if bonds_payable_preferred_stock != currDataSet.get("bonds_payable_preferred_stock") and bonds_payable_preferred_stock:
                                saveSet["bonds_payable_preferred_stock"] = bonds_payable_preferred_stock

                            if bonds_payable_perpetual_bonds != currDataSet.get("bonds_payable_perpetual_bonds") and bonds_payable_perpetual_bonds:
                                saveSet["bonds_payable_perpetual_bonds"] = bonds_payable_perpetual_bonds

                            if lease_liabilities != currDataSet.get("lease_liabilities") and lease_liabilities:
                                saveSet["lease_liabilities"] = lease_liabilities

                            if long_term_employee_benefits_payable != currDataSet.get("long_term_employee_benefits_payable") and long_term_employee_benefits_payable:
                                saveSet["long_term_employee_benefits_payable"] = long_term_employee_benefits_payable

                            if long_term_payables != currDataSet.get("long_term_payables") and long_term_payables:
                                saveSet["long_term_payables"] = long_term_payables

                            if long_term_payables_total != currDataSet.get("long_term_payables_total") and long_term_payables_total:
                                saveSet["long_term_payables_total"] = long_term_payables_total

                            if special_payables != currDataSet.get("special_payables") and special_payables:
                                saveSet["special_payables"] = special_payables

                            if estimated_non_current_liabilities != currDataSet.get("estimated_non_current_liabilities") and estimated_non_current_liabilities:
                                saveSet["estimated_non_current_liabilities"] = estimated_non_current_liabilities

                            if long_term_deferred_revenue != currDataSet.get("long_term_deferred_revenue") and long_term_deferred_revenue:
                                saveSet["long_term_deferred_revenue"] = long_term_deferred_revenue

                            if deferred_tax_liabilities != currDataSet.get("deferred_tax_liabilities") and deferred_tax_liabilities:
                                saveSet["deferred_tax_liabilities"] = deferred_tax_liabilities

                            if other_non_current_liabilities != currDataSet.get("other_non_current_liabilities") and other_non_current_liabilities:
                                saveSet["other_non_current_liabilities"] = other_non_current_liabilities

                            if total_non_current_liabilities != currDataSet.get("total_non_current_liabilities") and total_non_current_liabilities:
                                saveSet["total_non_current_liabilities"] = total_non_current_liabilities

                            if total_liabilities != currDataSet.get("total_liabilities") and total_liabilities:
                                saveSet["total_liabilities"] = total_liabilities

                            if owners_equity != currDataSet.get("owners_equity") and owners_equity:
                                saveSet["owners_equity"] = owners_equity

                            if paid_in_capital != currDataSet.get("paid_in_capital") and paid_in_capital:
                                saveSet["paid_in_capital"] = paid_in_capital

                            if other_equity_instruments != currDataSet.get("other_equity_instruments") and other_equity_instruments:
                                saveSet["other_equity_instruments"] = other_equity_instruments

                            if preferred_stock != currDataSet.get("preferred_stock") and preferred_stock:
                                saveSet["preferred_stock"] = preferred_stock

                            if perpetual_bonds != currDataSet.get("perpetual_bonds") and perpetual_bonds:
                                saveSet["perpetual_bonds"] = perpetual_bonds

                            if capital_reserve != currDataSet.get("capital_reserve") and capital_reserve:
                                saveSet["capital_reserve"] = capital_reserve

                            if less_treasury_stock != currDataSet.get("less_treasury_stock") and less_treasury_stock:
                                saveSet["less_treasury_stock"] = less_treasury_stock

                            if other_comprehensive_income != currDataSet.get("other_comprehensive_income") and other_comprehensive_income:
                                saveSet["other_comprehensive_income"] = other_comprehensive_income

                            if special_reserve != currDataSet.get("special_reserve") and special_reserve:
                                saveSet["special_reserve"] = special_reserve

                            if surplus_reserve != currDataSet.get("surplus_reserve") and surplus_reserve:
                                saveSet["surplus_reserve"] = surplus_reserve

                            if general_risk_reserve != currDataSet.get("general_risk_reserve") and general_risk_reserve:
                                saveSet["general_risk_reserve"] = general_risk_reserve

                            if unrecognized_investment_losses != currDataSet.get("unrecognized_investment_losses") and unrecognized_investment_losses:
                                saveSet["unrecognized_investment_losses"] = unrecognized_investment_losses

                            if retained_earnings != currDataSet.get("retained_earnings") and retained_earnings:
                                saveSet["retained_earnings"] = retained_earnings

                            if proposed_cash_dividends != currDataSet.get("proposed_cash_dividends") and proposed_cash_dividends:
                                saveSet["proposed_cash_dividends"] = proposed_cash_dividends

                            if foreign_currency_translation_difference != currDataSet.get("foreign_currency_translation_difference") and foreign_currency_translation_difference:
                                saveSet["foreign_currency_translation_difference"] = foreign_currency_translation_difference

                            if equity_attributable_to_parent_company != currDataSet.get("equity_attributable_to_parent_company") and equity_attributable_to_parent_company:
                                saveSet["equity_attributable_to_parent_company"] = equity_attributable_to_parent_company

                            if minority_interests != currDataSet.get("minority_interests") and minority_interests:
                                saveSet["minority_interests"] = minority_interests

                            if total_owners_equity != currDataSet.get("total_owners_equity") and total_owners_equity:
                                saveSet["total_owners_equity"] = total_owners_equity

                            if total_liabilities_and_owners_equity != currDataSet.get("total_liabilities_and_owners_equity") and total_liabilities_and_owners_equity:
                                saveSet["total_liabilities_and_owners_equity"] = total_liabilities_and_owners_equity

                            if label1 != currDataSet.get("label1") and label1:
                                saveSet["label1"] = label1

                            if label2 != currDataSet.get("label2") and label2:
                                saveSet["label2"] = label2

                            if label3 != currDataSet.get("label3") and label3:
                                saveSet["label3"] = label3

                            if memo != currDataSet.get("memo") and memo:
                                saveSet["memo"] = memo

                            if dispFlag != currDataSet.get("dispFlag") and dispFlag:
                                saveSet["dispFlag"] = dispFlag

                            if delFlag != currDataSet.get("delFlag") and delFlag:
                                saveSet["delFlag"] = delFlag


                            if saveSet:
                                #saveSet["delFlag"] = "0"
                                saveSet["modifyID"] = loginID
                                saveSet["modifyYMDHMS"] = misc.getTime()

                                #保存数据
                                tableName = comMysql.tablename_convertor_balance_sheets()
                                rtn = comMysql.update_balance_sheets(tableName,id,saveSet)
                                rtnData["rtn"] = str(rtn)

                                if rtn < 0:
                                    _LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")
                                else:
                                    if _DEBUG:
                                        pass
                                        _LOG.info(f"D: rtn:{rtn}")

                                result = rtnData

                        else:
                            #BT
                            errCode = "BT"

                    else:
                        #CB
                        errCode = "CB"

                else:
                    #data invalid
                    errCode = "BA"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#balance sheet查询代码
def funcBalanceSheetQry(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查

            if errCode == "B0": #
                #获取查询输入参数
                id = dataSet.get("id", "")

                #houseID = dataSet.get("houseID", "")
                symbol = dataSet.get("symbol", "")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code", "")
                
                stock_name = dataSet.get("stock_name", "")

                report_date = dataSet.get("report_date", "")

                forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记

                searchOption = dataSet.get("searchOption")

                mode = dataSet.get("mode", "full")

                #limitNum = dataSet.get("limitNum",0)

                #权限检查/功能检测

                rightCheckFlag = True

                if rightCheckFlag:

                    #生成indexKey
                    indexKeyDataSet = {} #查询生成index的因素
                    if id:
                        indexKeyDataSet["id"] = id
                    if stock_code:
                        indexKeyDataSet["stock_code"] = stock_code
                    if stock_name:
                        indexKeyDataSet["stock_name"] = stock_name
                    if report_date:
                        indexKeyDataSet["report_date"] = report_date
                    if searchOption:
                        indexKeyDataSet["searchOption"] = searchOption
                    if mode:
                        indexKeyDataSet["mode"] = mode

                    #if limitNum:
                        #indexKeyDataSet["limitNum"] = mode

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) 
                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) 
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) 

                    #判断数据是否在缓冲区:
                    if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:

                        if searchOption:
                            currDataList = []
                            tableName = comMysql.tablename_convertor_balance_sheets()
                            allDataList = comMysql.query_balance_sheets(tableName,mode = mode)
                            allowList = ["description", "label"] #筛选字段
                            serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)
                            if serachResultSet["rtn"] == "B0":
                                currDataList = serachResultSet.get("data", [])
                        else:
                            if id:
                                tableName = comMysql.tablename_convertor_balance_sheets()
                                currDataList = comMysql.query_balance_sheets(tableName,id,mode = mode)
                            else:
                                tableName = comMysql.tablename_convertor_balance_sheets()
                                currDataList = comMysql.query_balance_sheets(tableName,stock_code=stock_code,stock_name=stock_name,report_date=report_date)

                        dataList = []

                        for currDataSet in currDataList:
                            aSet = {}

                            #需要把文件转移到public domain
                            #appendixFileID00 =  currDataSet.get("appendixFileID00", "")
                            #appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)

                            #if mode == "full":
                                #aSet["houseID"] = currDataSet.get("houseID", "")

                            aSet["id"] = currDataSet.get("id","")
                            aSet["stock_code"] = currDataSet.get("stock_code","")
                            aSet["report_date"] = currDataSet.get("report_date","")
                            aSet["monetary_capital"] = currDataSet.get("monetary_capital","")
                            aSet["settlement_provisions"] = currDataSet.get("settlement_provisions","")
                            aSet["loans_to_other_banks"] = currDataSet.get("loans_to_other_banks","")
                            aSet["trading_financial_assets"] = currDataSet.get("trading_financial_assets","")
                            aSet["financial_assets_purchased_for_resale"] = currDataSet.get("financial_assets_purchased_for_resale","")
                            aSet["derivative_financial_assets"] = currDataSet.get("derivative_financial_assets","")
                            aSet["notes_and_accounts_receivable"] = currDataSet.get("notes_and_accounts_receivable","")
                            aSet["notes_receivable"] = currDataSet.get("notes_receivable","")
                            aSet["accounts_receivable"] = currDataSet.get("accounts_receivable","")
                            aSet["receivables_financing"] = currDataSet.get("receivables_financing","")
                            aSet["prepayments"] = currDataSet.get("prepayments","")
                            aSet["dividends_receivable"] = currDataSet.get("dividends_receivable","")
                            aSet["interest_receivable"] = currDataSet.get("interest_receivable","")
                            aSet["insurance_premiums_receivable"] = currDataSet.get("insurance_premiums_receivable","")
                            aSet["reinsurance_receivables"] = currDataSet.get("reinsurance_receivables","")
                            aSet["reinsurance_contract_reserves_receivable"] = currDataSet.get("reinsurance_contract_reserves_receivable","")
                            aSet["export_tax_rebates_receivable"] = currDataSet.get("export_tax_rebates_receivable","")
                            aSet["subsidies_receivable"] = currDataSet.get("subsidies_receivable","")
                            aSet["deposits_receivable"] = currDataSet.get("deposits_receivable","")
                            aSet["internal_receivables"] = currDataSet.get("internal_receivables","")
                            aSet["other_receivables"] = currDataSet.get("other_receivables","")
                            aSet["other_receivables_total"] = currDataSet.get("other_receivables_total","")
                            aSet["inventories"] = currDataSet.get("inventories","")
                            aSet["assets_held_for_sale"] = currDataSet.get("assets_held_for_sale","")
                            aSet["prepaid_expenses"] = currDataSet.get("prepaid_expenses","")
                            aSet["current_assets_pending_disposal"] = currDataSet.get("current_assets_pending_disposal","")
                            aSet["non_current_assets_due_within_one_year"] = currDataSet.get("non_current_assets_due_within_one_year","")
                            aSet["other_current_assets"] = currDataSet.get("other_current_assets","")
                            aSet["total_current_assets"] = currDataSet.get("total_current_assets","")
                            aSet["non_current_assets"] = currDataSet.get("non_current_assets","")
                            aSet["loans_and_advances"] = currDataSet.get("loans_and_advances","")
                            aSet["debt_investments"] = currDataSet.get("debt_investments","")
                            aSet["other_debt_investments"] = currDataSet.get("other_debt_investments","")
                            aSet["financial_assets_at_fvoci"] = currDataSet.get("financial_assets_at_fvoci","")
                            aSet["financial_assets_at_amortized_cost"] = currDataSet.get("financial_assets_at_amortized_cost","")
                            aSet["available_for_sale_financial_assets"] = currDataSet.get("available_for_sale_financial_assets","")
                            aSet["long_term_equity_investments"] = currDataSet.get("long_term_equity_investments","")
                            aSet["investment_property"] = currDataSet.get("investment_property","")
                            aSet["long_term_receivables"] = currDataSet.get("long_term_receivables","")
                            aSet["other_equity_instrument_investments"] = currDataSet.get("other_equity_instrument_investments","")
                            aSet["other_non_current_financial_assets"] = currDataSet.get("other_non_current_financial_assets","")
                            aSet["other_long_term_investments"] = currDataSet.get("other_long_term_investments","")
                            aSet["fixed_assets_original_value"] = currDataSet.get("fixed_assets_original_value","")
                            aSet["accumulated_depreciation"] = currDataSet.get("accumulated_depreciation","")
                            aSet["fixed_assets_net_value"] = currDataSet.get("fixed_assets_net_value","")
                            aSet["fixed_assets_impairment_provision"] = currDataSet.get("fixed_assets_impairment_provision","")
                            aSet["construction_in_progress_total"] = currDataSet.get("construction_in_progress_total","")
                            aSet["construction_in_progress"] = currDataSet.get("construction_in_progress","")
                            aSet["construction_materials"] = currDataSet.get("construction_materials","")
                            aSet["fixed_assets_net"] = currDataSet.get("fixed_assets_net","")
                            aSet["fixed_assets_disposal"] = currDataSet.get("fixed_assets_disposal","")
                            aSet["fixed_assets_and_disposal_total"] = currDataSet.get("fixed_assets_and_disposal_total","")
                            aSet["productive_biological_assets"] = currDataSet.get("productive_biological_assets","")
                            aSet["consumptive_biological_assets"] = currDataSet.get("consumptive_biological_assets","")
                            aSet["oil_and_gas_assets"] = currDataSet.get("oil_and_gas_assets","")
                            aSet["contract_assets"] = currDataSet.get("contract_assets","")
                            aSet["right_of_use_assets"] = currDataSet.get("right_of_use_assets","")
                            aSet["intangible_assets"] = currDataSet.get("intangible_assets","")
                            aSet["development_expenditure"] = currDataSet.get("development_expenditure","")
                            aSet["goodwill"] = currDataSet.get("goodwill","")
                            aSet["long_term_deferred_expenses"] = currDataSet.get("long_term_deferred_expenses","")
                            aSet["split_share_structure_circulation_rights"] = currDataSet.get("split_share_structure_circulation_rights","")
                            aSet["deferred_tax_assets"] = currDataSet.get("deferred_tax_assets","")
                            aSet["other_non_current_assets"] = currDataSet.get("other_non_current_assets","")
                            aSet["total_non_current_assets"] = currDataSet.get("total_non_current_assets","")
                            aSet["total_assets"] = currDataSet.get("total_assets","")
                            aSet["current_liabilities"] = currDataSet.get("current_liabilities","")
                            aSet["short_term_borrowings"] = currDataSet.get("short_term_borrowings","")
                            aSet["borrowings_from_central_bank"] = currDataSet.get("borrowings_from_central_bank","")
                            aSet["deposits_from_customers_and_banks"] = currDataSet.get("deposits_from_customers_and_banks","")
                            aSet["borrowings_from_other_banks"] = currDataSet.get("borrowings_from_other_banks","")
                            aSet["trading_financial_liabilities"] = currDataSet.get("trading_financial_liabilities","")
                            aSet["derivative_financial_liabilities"] = currDataSet.get("derivative_financial_liabilities","")
                            aSet["notes_and_accounts_payable"] = currDataSet.get("notes_and_accounts_payable","")
                            aSet["notes_payable"] = currDataSet.get("notes_payable","")
                            aSet["accounts_payable"] = currDataSet.get("accounts_payable","")
                            aSet["advances_from_customers"] = currDataSet.get("advances_from_customers","")
                            aSet["contract_liabilities"] = currDataSet.get("contract_liabilities","")
                            aSet["financial_assets_sold_for_repurchase"] = currDataSet.get("financial_assets_sold_for_repurchase","")
                            aSet["fees_and_commissions_payable"] = currDataSet.get("fees_and_commissions_payable","")
                            aSet["employee_benefits_payable"] = currDataSet.get("employee_benefits_payable","")
                            aSet["taxes_payable"] = currDataSet.get("taxes_payable","")
                            aSet["interest_payable"] = currDataSet.get("interest_payable","")
                            aSet["dividends_payable"] = currDataSet.get("dividends_payable","")
                            aSet["deposits_payable"] = currDataSet.get("deposits_payable","")
                            aSet["internal_payables"] = currDataSet.get("internal_payables","")
                            aSet["other_payables"] = currDataSet.get("other_payables","")
                            aSet["other_payables_total"] = currDataSet.get("other_payables_total","")
                            aSet["other_taxes_payable"] = currDataSet.get("other_taxes_payable","")
                            aSet["guarantee_liability_reserves"] = currDataSet.get("guarantee_liability_reserves","")
                            aSet["reinsurance_payables"] = currDataSet.get("reinsurance_payables","")
                            aSet["insurance_contract_reserves"] = currDataSet.get("insurance_contract_reserves","")
                            aSet["securities_trading_agency_payables"] = currDataSet.get("securities_trading_agency_payables","")
                            aSet["securities_underwriting_agency_payables"] = currDataSet.get("securities_underwriting_agency_payables","")
                            aSet["international_settlement"] = currDataSet.get("international_settlement","")
                            aSet["domestic_settlement"] = currDataSet.get("domestic_settlement","")
                            aSet["accrued_expenses"] = currDataSet.get("accrued_expenses","")
                            aSet["estimated_current_liabilities"] = currDataSet.get("estimated_current_liabilities","")
                            aSet["short_term_bonds_payable"] = currDataSet.get("short_term_bonds_payable","")
                            aSet["liabilities_held_for_sale"] = currDataSet.get("liabilities_held_for_sale","")
                            aSet["deferred_revenue_due_within_one_year"] = currDataSet.get("deferred_revenue_due_within_one_year","")
                            aSet["non_current_liabilities_due_within_one_year"] = currDataSet.get("non_current_liabilities_due_within_one_year","")
                            aSet["other_current_liabilities"] = currDataSet.get("other_current_liabilities","")
                            aSet["total_current_liabilities"] = currDataSet.get("total_current_liabilities","")
                            aSet["non_current_liabilities"] = currDataSet.get("non_current_liabilities","")
                            aSet["long_term_borrowings"] = currDataSet.get("long_term_borrowings","")
                            aSet["bonds_payable"] = currDataSet.get("bonds_payable","")
                            aSet["bonds_payable_preferred_stock"] = currDataSet.get("bonds_payable_preferred_stock","")
                            aSet["bonds_payable_perpetual_bonds"] = currDataSet.get("bonds_payable_perpetual_bonds","")
                            aSet["lease_liabilities"] = currDataSet.get("lease_liabilities","")
                            aSet["long_term_employee_benefits_payable"] = currDataSet.get("long_term_employee_benefits_payable","")
                            aSet["long_term_payables"] = currDataSet.get("long_term_payables","")
                            aSet["long_term_payables_total"] = currDataSet.get("long_term_payables_total","")
                            aSet["special_payables"] = currDataSet.get("special_payables","")
                            aSet["estimated_non_current_liabilities"] = currDataSet.get("estimated_non_current_liabilities","")
                            aSet["long_term_deferred_revenue"] = currDataSet.get("long_term_deferred_revenue","")
                            aSet["deferred_tax_liabilities"] = currDataSet.get("deferred_tax_liabilities","")
                            aSet["other_non_current_liabilities"] = currDataSet.get("other_non_current_liabilities","")
                            aSet["total_non_current_liabilities"] = currDataSet.get("total_non_current_liabilities","")
                            aSet["total_liabilities"] = currDataSet.get("total_liabilities","")
                            aSet["owners_equity"] = currDataSet.get("owners_equity","")
                            aSet["paid_in_capital"] = currDataSet.get("paid_in_capital","")
                            aSet["other_equity_instruments"] = currDataSet.get("other_equity_instruments","")
                            aSet["preferred_stock"] = currDataSet.get("preferred_stock","")
                            aSet["perpetual_bonds"] = currDataSet.get("perpetual_bonds","")
                            aSet["capital_reserve"] = currDataSet.get("capital_reserve","")
                            aSet["less_treasury_stock"] = currDataSet.get("less_treasury_stock","")
                            aSet["other_comprehensive_income"] = currDataSet.get("other_comprehensive_income","")
                            aSet["special_reserve"] = currDataSet.get("special_reserve","")
                            aSet["surplus_reserve"] = currDataSet.get("surplus_reserve","")
                            aSet["general_risk_reserve"] = currDataSet.get("general_risk_reserve","")
                            aSet["unrecognized_investment_losses"] = currDataSet.get("unrecognized_investment_losses","")
                            aSet["retained_earnings"] = currDataSet.get("retained_earnings","")
                            aSet["proposed_cash_dividends"] = currDataSet.get("proposed_cash_dividends","")
                            aSet["foreign_currency_translation_difference"] = currDataSet.get("foreign_currency_translation_difference","")
                            aSet["equity_attributable_to_parent_company"] = currDataSet.get("equity_attributable_to_parent_company","")
                            aSet["minority_interests"] = currDataSet.get("minority_interests","")
                            aSet["total_owners_equity"] = currDataSet.get("total_owners_equity","")
                            aSet["total_liabilities_and_owners_equity"] = currDataSet.get("total_liabilities_and_owners_equity","")
                            aSet["label1"] = currDataSet.get("label1","")
                            aSet["label2"] = currDataSet.get("label2","")
                            aSet["label3"] = currDataSet.get("label3","")
                            aSet["memo"] = currDataSet.get("memo","")
                            aSet["regID"] = currDataSet.get("regID","")
                            aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                            aSet["modifyID"] = currDataSet.get("modifyID","")
                            aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                            aSet["dispFlag"] = currDataSet.get("dispFlag","")
                            aSet["delFlag"] = currDataSet.get("delFlag","")

                            dataList.append(aSet)

                        #临时缓存机制,改进型, 2023/10/16
                        indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去

                    rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)

                    #rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "BT"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#收入数据(income statements) 增加代码
def funcIncomeStatementsAdd(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                #data validation check
                symbol = dataSet.get("symbol", "")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code", "")

                if stock_code:
                    dataValidFlag = True
                else:
                    davaValidFlag = False

                if dataValidFlag:
                    saveSet = {}
                    saveSet["stock_code"] = stock_code
                    saveSet["report_date"] = dataSet.get("report_date", "") 
                    saveSet["total_operating_revenue"] = dataSet.get("total_operating_revenue", "") 
                    saveSet["operating_revenue"] = dataSet.get("operating_revenue", "") 
                    saveSet["interest_income"] = dataSet.get("interest_income", "") 
                    saveSet["earned_premiums"] = dataSet.get("earned_premiums", "") 
                    saveSet["fees_and_commissions_income"] = dataSet.get("fees_and_commissions_income", "") 
                    saveSet["real_estate_sales_revenue"] = dataSet.get("real_estate_sales_revenue", "") 
                    saveSet["other_business_revenue"] = dataSet.get("other_business_revenue", "") 
                    saveSet["total_operating_costs"] = dataSet.get("total_operating_costs", "") 
                    saveSet["operating_costs"] = dataSet.get("operating_costs", "") 
                    saveSet["fees_and_commissions_expenses"] = dataSet.get("fees_and_commissions_expenses", "") 
                    saveSet["real_estate_sales_costs"] = dataSet.get("real_estate_sales_costs", "") 
                    saveSet["surrender_value"] = dataSet.get("surrender_value", "") 
                    saveSet["net_claims_paid"] = dataSet.get("net_claims_paid", "") 
                    saveSet["net_insurance_contract_reserves"] = dataSet.get("net_insurance_contract_reserves", "") 
                    saveSet["policy_dividend_expenses"] = dataSet.get("policy_dividend_expenses", "") 
                    saveSet["reinsurance_expenses"] = dataSet.get("reinsurance_expenses", "") 
                    saveSet["other_business_costs"] = dataSet.get("other_business_costs", "") 
                    saveSet["taxes_and_surcharges"] = dataSet.get("taxes_and_surcharges", "") 
                    saveSet["rd_expenses"] = dataSet.get("rd_expenses", "") 
                    saveSet["selling_expenses"] = dataSet.get("selling_expenses", "") 
                    saveSet["administrative_expenses"] = dataSet.get("administrative_expenses", "") 
                    saveSet["financial_expenses"] = dataSet.get("financial_expenses", "") 
                    saveSet["interest_expenses"] = dataSet.get("interest_expenses", "") 
                    saveSet["interest_expenditure"] = dataSet.get("interest_expenditure", "") 
                    saveSet["investment_income"] = dataSet.get("investment_income", "") 
                    saveSet["investment_income_from_associates_and_joint_ventures"] = dataSet.get("investment_income_from_associates_and_joint_ventures", "") 
                    saveSet["gain_on_derecognition_of_financial_assets_at_amortized_cost"] = dataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost", "") 
                    saveSet["foreign_exchange_gains"] = dataSet.get("foreign_exchange_gains", "") 
                    saveSet["net_open_hedge_gains"] = dataSet.get("net_open_hedge_gains", "") 
                    saveSet["fair_value_change_gains"] = dataSet.get("fair_value_change_gains", "") 
                    saveSet["futures_gains_losses"] = dataSet.get("futures_gains_losses", "") 
                    saveSet["custody_income"] = dataSet.get("custody_income", "") 
                    saveSet["subsidy_income"] = dataSet.get("subsidy_income", "") 
                    saveSet["other_gains"] = dataSet.get("other_gains", "") 
                    saveSet["asset_impairment_losses"] = dataSet.get("asset_impairment_losses", "") 
                    saveSet["credit_impairment_losses"] = dataSet.get("credit_impairment_losses", "") 
                    saveSet["other_business_profits"] = dataSet.get("other_business_profits", "") 
                    saveSet["asset_disposal_gains"] = dataSet.get("asset_disposal_gains", "") 
                    saveSet["operating_profit"] = dataSet.get("operating_profit", "") 
                    saveSet["non_operating_income"] = dataSet.get("non_operating_income", "") 
                    saveSet["non_current_asset_disposal_gains"] = dataSet.get("non_current_asset_disposal_gains", "") 
                    saveSet["non_operating_expenses"] = dataSet.get("non_operating_expenses", "") 
                    saveSet["non_current_asset_disposal_losses"] = dataSet.get("non_current_asset_disposal_losses", "") 
                    saveSet["total_profit"] = dataSet.get("total_profit", "") 
                    saveSet["income_tax_expense"] = dataSet.get("income_tax_expense", "") 
                    saveSet["unrecognized_investment_losses"] = dataSet.get("unrecognized_investment_losses", "") 
                    saveSet["net_profit"] = dataSet.get("net_profit", "") 
                    saveSet["net_profit_from_continuing_operations"] = dataSet.get("net_profit_from_continuing_operations", "") 
                    saveSet["net_profit_from_discontinued_operations"] = dataSet.get("net_profit_from_discontinued_operations", "") 
                    saveSet["net_profit_attributable_to_parent_company"] = dataSet.get("net_profit_attributable_to_parent_company", "") 
                    saveSet["net_profit_of_acquiree_before_merger"] = dataSet.get("net_profit_of_acquiree_before_merger", "") 
                    saveSet["minority_interests_profit_loss"] = dataSet.get("minority_interests_profit_loss", "") 
                    saveSet["other_comprehensive_income"] = dataSet.get("other_comprehensive_income", "") 
                    saveSet["other_comprehensive_income_attributable_to_parent"] = dataSet.get("other_comprehensive_income_attributable_to_parent", "") 
                    saveSet["oci_not_reclassified_to_profit_loss"] = dataSet.get("oci_not_reclassified_to_profit_loss", "") 
                    saveSet["remeasurement_of_defined_benefit_plans"] = dataSet.get("remeasurement_of_defined_benefit_plans", "") 
                    saveSet["oci_under_equity_method_not_reclassified"] = dataSet.get("oci_under_equity_method_not_reclassified", "") 
                    saveSet["fair_value_change_of_other_equity_instruments"] = dataSet.get("fair_value_change_of_other_equity_instruments", "") 
                    saveSet["fair_value_change_of_own_credit_risk"] = dataSet.get("fair_value_change_of_own_credit_risk", "") 
                    saveSet["oci_reclassified_to_profit_loss"] = dataSet.get("oci_reclassified_to_profit_loss", "") 
                    saveSet["oci_under_equity_method_reclassified"] = dataSet.get("oci_under_equity_method_reclassified", "") 
                    saveSet["fair_value_change_of_afs_financial_assets"] = dataSet.get("fair_value_change_of_afs_financial_assets", "") 
                    saveSet["fair_value_change_of_other_debt_investments"] = dataSet.get("fair_value_change_of_other_debt_investments", "") 
                    saveSet["financial_assets_reclassified_to_oci"] = dataSet.get("financial_assets_reclassified_to_oci", "") 
                    saveSet["credit_impairment_of_other_debt_investments"] = dataSet.get("credit_impairment_of_other_debt_investments", "") 
                    saveSet["htm_reclassified_to_afs_gains_losses"] = dataSet.get("htm_reclassified_to_afs_gains_losses", "") 
                    saveSet["cash_flow_hedge_reserve"] = dataSet.get("cash_flow_hedge_reserve", "") 
                    saveSet["effective_portion_of_cash_flow_hedge"] = dataSet.get("effective_portion_of_cash_flow_hedge", "") 
                    saveSet["foreign_currency_translation_difference"] = dataSet.get("foreign_currency_translation_difference", "") 
                    saveSet["other"] = dataSet.get("other", "") 
                    saveSet["other_comprehensive_income_attributable_to_minority"] = dataSet.get("other_comprehensive_income_attributable_to_minority", "") 
                    saveSet["total_comprehensive_income"] = dataSet.get("total_comprehensive_income", "") 
                    saveSet["total_comprehensive_income_attributable_to_parent"] = dataSet.get("total_comprehensive_income_attributable_to_parent", "") 
                    saveSet["total_comprehensive_income_attributable_to_minority"] = dataSet.get("total_comprehensive_income_attributable_to_minority", "") 
                    saveSet["basic_earnings_per_share"] = dataSet.get("basic_earnings_per_share", "") 
                    saveSet["diluted_earnings_per_share"] = dataSet.get("diluted_earnings_per_share", "") 
                    saveSet["label1"] = dataSet.get("label1", "") 
                    saveSet["label2"] = dataSet.get("label2", "") 
                    saveSet["label3"] = dataSet.get("label3", "") 
                    saveSet["memo"] = dataSet.get("memo", "") 
                    saveSet["dispFlag"] = dataSet.get("dispFlag", "") 
                    saveSet["delFlag"] = dataSet.get("delFlag", "0") 
                    saveSet["regID"] = loginID
                    saveSet["regYMDHMS"] = misc.getTime()

                    tableName = comMysql.tablename_convertor_income_statements()
                    recID = comMysql.insert_income_statements(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},saveSet:{saveSet}")
                    else:
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: recID:{recID}")

                    result = rtnData

                else:
                    #data invalid
                    errCode = "BA"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#收入数据(income statements) 删除代码
def funcIncomeStatementsDel(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                id = dataSet.get("id", "")
                tableName = comMysql.tablename_convertor_income_statements()
                currDataList = comMysql.query_income_statements(tableName,id)
                if len(currDataList) == 1:
                    saveSet = {}
                    saveSet["modifyID"] = loginID
                    saveSet["modifyYMDHMS"] = misc.getTime()
                    #saveSet["delFlag"] = "1"

                    rtn = comMysql.delete_income_statements(tableName,id)
                    rtnData["rtn"] = str(rtn)

                    if _DEBUG:
                        _LOG.info(f"D: rtn:{rtn}")

                    result = rtnData

                else:
                    errCode = "CB"
        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#收入数据(income statements) 修改代码
def funcIncomeStatementsModify(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                stock_code = dataSet.get("stock_code") 
                report_date = dataSet.get("report_date") 
                total_operating_revenue = dataSet.get("total_operating_revenue") 
                operating_revenue = dataSet.get("operating_revenue") 
                interest_income = dataSet.get("interest_income") 
                earned_premiums = dataSet.get("earned_premiums") 
                fees_and_commissions_income = dataSet.get("fees_and_commissions_income") 
                real_estate_sales_revenue = dataSet.get("real_estate_sales_revenue") 
                other_business_revenue = dataSet.get("other_business_revenue") 
                total_operating_costs = dataSet.get("total_operating_costs") 
                operating_costs = dataSet.get("operating_costs") 
                fees_and_commissions_expenses = dataSet.get("fees_and_commissions_expenses") 
                real_estate_sales_costs = dataSet.get("real_estate_sales_costs") 
                surrender_value = dataSet.get("surrender_value") 
                net_claims_paid = dataSet.get("net_claims_paid") 
                net_insurance_contract_reserves = dataSet.get("net_insurance_contract_reserves") 
                policy_dividend_expenses = dataSet.get("policy_dividend_expenses") 
                reinsurance_expenses = dataSet.get("reinsurance_expenses") 
                other_business_costs = dataSet.get("other_business_costs") 
                taxes_and_surcharges = dataSet.get("taxes_and_surcharges") 
                rd_expenses = dataSet.get("rd_expenses") 
                selling_expenses = dataSet.get("selling_expenses") 
                administrative_expenses = dataSet.get("administrative_expenses") 
                financial_expenses = dataSet.get("financial_expenses") 
                interest_expenses = dataSet.get("interest_expenses") 
                interest_expenditure = dataSet.get("interest_expenditure") 
                investment_income = dataSet.get("investment_income") 
                investment_income_from_associates_and_joint_ventures = dataSet.get("investment_income_from_associates_and_joint_ventures") 
                gain_on_derecognition_of_financial_assets_at_amortized_cost = dataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost") 
                foreign_exchange_gains = dataSet.get("foreign_exchange_gains") 
                net_open_hedge_gains = dataSet.get("net_open_hedge_gains") 
                fair_value_change_gains = dataSet.get("fair_value_change_gains") 
                futures_gains_losses = dataSet.get("futures_gains_losses") 
                custody_income = dataSet.get("custody_income") 
                subsidy_income = dataSet.get("subsidy_income") 
                other_gains = dataSet.get("other_gains") 
                asset_impairment_losses = dataSet.get("asset_impairment_losses") 
                credit_impairment_losses = dataSet.get("credit_impairment_losses") 
                other_business_profits = dataSet.get("other_business_profits") 
                asset_disposal_gains = dataSet.get("asset_disposal_gains") 
                operating_profit = dataSet.get("operating_profit") 
                non_operating_income = dataSet.get("non_operating_income") 
                non_current_asset_disposal_gains = dataSet.get("non_current_asset_disposal_gains") 
                non_operating_expenses = dataSet.get("non_operating_expenses") 
                non_current_asset_disposal_losses = dataSet.get("non_current_asset_disposal_losses") 
                total_profit = dataSet.get("total_profit") 
                income_tax_expense = dataSet.get("income_tax_expense") 
                unrecognized_investment_losses = dataSet.get("unrecognized_investment_losses") 
                net_profit = dataSet.get("net_profit") 
                net_profit_from_continuing_operations = dataSet.get("net_profit_from_continuing_operations") 
                net_profit_from_discontinued_operations = dataSet.get("net_profit_from_discontinued_operations") 
                net_profit_attributable_to_parent_company = dataSet.get("net_profit_attributable_to_parent_company") 
                net_profit_of_acquiree_before_merger = dataSet.get("net_profit_of_acquiree_before_merger") 
                minority_interests_profit_loss = dataSet.get("minority_interests_profit_loss") 
                other_comprehensive_income = dataSet.get("other_comprehensive_income") 
                other_comprehensive_income_attributable_to_parent = dataSet.get("other_comprehensive_income_attributable_to_parent") 
                oci_not_reclassified_to_profit_loss = dataSet.get("oci_not_reclassified_to_profit_loss") 
                remeasurement_of_defined_benefit_plans = dataSet.get("remeasurement_of_defined_benefit_plans") 
                oci_under_equity_method_not_reclassified = dataSet.get("oci_under_equity_method_not_reclassified") 
                fair_value_change_of_other_equity_instruments = dataSet.get("fair_value_change_of_other_equity_instruments") 
                fair_value_change_of_own_credit_risk = dataSet.get("fair_value_change_of_own_credit_risk") 
                oci_reclassified_to_profit_loss = dataSet.get("oci_reclassified_to_profit_loss") 
                oci_under_equity_method_reclassified = dataSet.get("oci_under_equity_method_reclassified") 
                fair_value_change_of_afs_financial_assets = dataSet.get("fair_value_change_of_afs_financial_assets") 
                fair_value_change_of_other_debt_investments = dataSet.get("fair_value_change_of_other_debt_investments") 
                financial_assets_reclassified_to_oci = dataSet.get("financial_assets_reclassified_to_oci") 
                credit_impairment_of_other_debt_investments = dataSet.get("credit_impairment_of_other_debt_investments") 
                htm_reclassified_to_afs_gains_losses = dataSet.get("htm_reclassified_to_afs_gains_losses") 
                cash_flow_hedge_reserve = dataSet.get("cash_flow_hedge_reserve") 
                effective_portion_of_cash_flow_hedge = dataSet.get("effective_portion_of_cash_flow_hedge") 
                foreign_currency_translation_difference = dataSet.get("foreign_currency_translation_difference") 
                other = dataSet.get("other") 
                other_comprehensive_income_attributable_to_minority = dataSet.get("other_comprehensive_income_attributable_to_minority") 
                total_comprehensive_income = dataSet.get("total_comprehensive_income") 
                total_comprehensive_income_attributable_to_parent = dataSet.get("total_comprehensive_income_attributable_to_parent") 
                total_comprehensive_income_attributable_to_minority = dataSet.get("total_comprehensive_income_attributable_to_minority") 
                basic_earnings_per_share = dataSet.get("basic_earnings_per_share") 
                diluted_earnings_per_share = dataSet.get("diluted_earnings_per_share") 
                label1 = dataSet.get("label1") 
                label2 = dataSet.get("label2") 
                label3 = dataSet.get("label3") 
                memo = dataSet.get("memo") 
                dispFlag = dataSet.get("dispFlag") 
                delFlag = dataSet.get("delFlag") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    recID = dataSet.get("id", "")

                    tableName = comMysql.tablename_convertor_income_statements()
                    currDataList = comMysql.query_income_statements(tableName,recID)

                    if len(currDataList) == 1:
                        currDataSet = currDataList[0]

                        #权限或其他检查
                        if errCode == "B0": #

                            saveSet = {}

                            if stock_code != currDataSet.get("stock_code") and stock_code:
                                saveSet["stock_code"] = stock_code

                            if report_date != currDataSet.get("report_date") and report_date:
                                saveSet["report_date"] = report_date

                            if total_operating_revenue != currDataSet.get("total_operating_revenue") and total_operating_revenue:
                                saveSet["total_operating_revenue"] = total_operating_revenue

                            if operating_revenue != currDataSet.get("operating_revenue") and operating_revenue:
                                saveSet["operating_revenue"] = operating_revenue

                            if interest_income != currDataSet.get("interest_income") and interest_income:
                                saveSet["interest_income"] = interest_income

                            if earned_premiums != currDataSet.get("earned_premiums") and earned_premiums:
                                saveSet["earned_premiums"] = earned_premiums

                            if fees_and_commissions_income != currDataSet.get("fees_and_commissions_income") and fees_and_commissions_income:
                                saveSet["fees_and_commissions_income"] = fees_and_commissions_income

                            if real_estate_sales_revenue != currDataSet.get("real_estate_sales_revenue") and real_estate_sales_revenue:
                                saveSet["real_estate_sales_revenue"] = real_estate_sales_revenue

                            if other_business_revenue != currDataSet.get("other_business_revenue") and other_business_revenue:
                                saveSet["other_business_revenue"] = other_business_revenue

                            if total_operating_costs != currDataSet.get("total_operating_costs") and total_operating_costs:
                                saveSet["total_operating_costs"] = total_operating_costs

                            if operating_costs != currDataSet.get("operating_costs") and operating_costs:
                                saveSet["operating_costs"] = operating_costs

                            if fees_and_commissions_expenses != currDataSet.get("fees_and_commissions_expenses") and fees_and_commissions_expenses:
                                saveSet["fees_and_commissions_expenses"] = fees_and_commissions_expenses

                            if real_estate_sales_costs != currDataSet.get("real_estate_sales_costs") and real_estate_sales_costs:
                                saveSet["real_estate_sales_costs"] = real_estate_sales_costs

                            if surrender_value != currDataSet.get("surrender_value") and surrender_value:
                                saveSet["surrender_value"] = surrender_value

                            if net_claims_paid != currDataSet.get("net_claims_paid") and net_claims_paid:
                                saveSet["net_claims_paid"] = net_claims_paid

                            if net_insurance_contract_reserves != currDataSet.get("net_insurance_contract_reserves") and net_insurance_contract_reserves:
                                saveSet["net_insurance_contract_reserves"] = net_insurance_contract_reserves

                            if policy_dividend_expenses != currDataSet.get("policy_dividend_expenses") and policy_dividend_expenses:
                                saveSet["policy_dividend_expenses"] = policy_dividend_expenses

                            if reinsurance_expenses != currDataSet.get("reinsurance_expenses") and reinsurance_expenses:
                                saveSet["reinsurance_expenses"] = reinsurance_expenses

                            if other_business_costs != currDataSet.get("other_business_costs") and other_business_costs:
                                saveSet["other_business_costs"] = other_business_costs

                            if taxes_and_surcharges != currDataSet.get("taxes_and_surcharges") and taxes_and_surcharges:
                                saveSet["taxes_and_surcharges"] = taxes_and_surcharges

                            if rd_expenses != currDataSet.get("rd_expenses") and rd_expenses:
                                saveSet["rd_expenses"] = rd_expenses

                            if selling_expenses != currDataSet.get("selling_expenses") and selling_expenses:
                                saveSet["selling_expenses"] = selling_expenses

                            if administrative_expenses != currDataSet.get("administrative_expenses") and administrative_expenses:
                                saveSet["administrative_expenses"] = administrative_expenses

                            if financial_expenses != currDataSet.get("financial_expenses") and financial_expenses:
                                saveSet["financial_expenses"] = financial_expenses

                            if interest_expenses != currDataSet.get("interest_expenses") and interest_expenses:
                                saveSet["interest_expenses"] = interest_expenses

                            if interest_expenditure != currDataSet.get("interest_expenditure") and interest_expenditure:
                                saveSet["interest_expenditure"] = interest_expenditure

                            if investment_income != currDataSet.get("investment_income") and investment_income:
                                saveSet["investment_income"] = investment_income

                            if investment_income_from_associates_and_joint_ventures != currDataSet.get("investment_income_from_associates_and_joint_ventures") and investment_income_from_associates_and_joint_ventures:
                                saveSet["investment_income_from_associates_and_joint_ventures"] = investment_income_from_associates_and_joint_ventures

                            if gain_on_derecognition_of_financial_assets_at_amortized_cost != currDataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost") and gain_on_derecognition_of_financial_assets_at_amortized_cost:
                                saveSet["gain_on_derecognition_of_financial_assets_at_amortized_cost"] = gain_on_derecognition_of_financial_assets_at_amortized_cost

                            if foreign_exchange_gains != currDataSet.get("foreign_exchange_gains") and foreign_exchange_gains:
                                saveSet["foreign_exchange_gains"] = foreign_exchange_gains

                            if net_open_hedge_gains != currDataSet.get("net_open_hedge_gains") and net_open_hedge_gains:
                                saveSet["net_open_hedge_gains"] = net_open_hedge_gains

                            if fair_value_change_gains != currDataSet.get("fair_value_change_gains") and fair_value_change_gains:
                                saveSet["fair_value_change_gains"] = fair_value_change_gains

                            if futures_gains_losses != currDataSet.get("futures_gains_losses") and futures_gains_losses:
                                saveSet["futures_gains_losses"] = futures_gains_losses

                            if custody_income != currDataSet.get("custody_income") and custody_income:
                                saveSet["custody_income"] = custody_income

                            if subsidy_income != currDataSet.get("subsidy_income") and subsidy_income:
                                saveSet["subsidy_income"] = subsidy_income

                            if other_gains != currDataSet.get("other_gains") and other_gains:
                                saveSet["other_gains"] = other_gains

                            if asset_impairment_losses != currDataSet.get("asset_impairment_losses") and asset_impairment_losses:
                                saveSet["asset_impairment_losses"] = asset_impairment_losses

                            if credit_impairment_losses != currDataSet.get("credit_impairment_losses") and credit_impairment_losses:
                                saveSet["credit_impairment_losses"] = credit_impairment_losses

                            if other_business_profits != currDataSet.get("other_business_profits") and other_business_profits:
                                saveSet["other_business_profits"] = other_business_profits

                            if asset_disposal_gains != currDataSet.get("asset_disposal_gains") and asset_disposal_gains:
                                saveSet["asset_disposal_gains"] = asset_disposal_gains

                            if operating_profit != currDataSet.get("operating_profit") and operating_profit:
                                saveSet["operating_profit"] = operating_profit

                            if non_operating_income != currDataSet.get("non_operating_income") and non_operating_income:
                                saveSet["non_operating_income"] = non_operating_income

                            if non_current_asset_disposal_gains != currDataSet.get("non_current_asset_disposal_gains") and non_current_asset_disposal_gains:
                                saveSet["non_current_asset_disposal_gains"] = non_current_asset_disposal_gains

                            if non_operating_expenses != currDataSet.get("non_operating_expenses") and non_operating_expenses:
                                saveSet["non_operating_expenses"] = non_operating_expenses

                            if non_current_asset_disposal_losses != currDataSet.get("non_current_asset_disposal_losses") and non_current_asset_disposal_losses:
                                saveSet["non_current_asset_disposal_losses"] = non_current_asset_disposal_losses

                            if total_profit != currDataSet.get("total_profit") and total_profit:
                                saveSet["total_profit"] = total_profit

                            if income_tax_expense != currDataSet.get("income_tax_expense") and income_tax_expense:
                                saveSet["income_tax_expense"] = income_tax_expense

                            if unrecognized_investment_losses != currDataSet.get("unrecognized_investment_losses") and unrecognized_investment_losses:
                                saveSet["unrecognized_investment_losses"] = unrecognized_investment_losses

                            if net_profit != currDataSet.get("net_profit") and net_profit:
                                saveSet["net_profit"] = net_profit

                            if net_profit_from_continuing_operations != currDataSet.get("net_profit_from_continuing_operations") and net_profit_from_continuing_operations:
                                saveSet["net_profit_from_continuing_operations"] = net_profit_from_continuing_operations

                            if net_profit_from_discontinued_operations != currDataSet.get("net_profit_from_discontinued_operations") and net_profit_from_discontinued_operations:
                                saveSet["net_profit_from_discontinued_operations"] = net_profit_from_discontinued_operations

                            if net_profit_attributable_to_parent_company != currDataSet.get("net_profit_attributable_to_parent_company") and net_profit_attributable_to_parent_company:
                                saveSet["net_profit_attributable_to_parent_company"] = net_profit_attributable_to_parent_company

                            if net_profit_of_acquiree_before_merger != currDataSet.get("net_profit_of_acquiree_before_merger") and net_profit_of_acquiree_before_merger:
                                saveSet["net_profit_of_acquiree_before_merger"] = net_profit_of_acquiree_before_merger

                            if minority_interests_profit_loss != currDataSet.get("minority_interests_profit_loss") and minority_interests_profit_loss:
                                saveSet["minority_interests_profit_loss"] = minority_interests_profit_loss

                            if other_comprehensive_income != currDataSet.get("other_comprehensive_income") and other_comprehensive_income:
                                saveSet["other_comprehensive_income"] = other_comprehensive_income

                            if other_comprehensive_income_attributable_to_parent != currDataSet.get("other_comprehensive_income_attributable_to_parent") and other_comprehensive_income_attributable_to_parent:
                                saveSet["other_comprehensive_income_attributable_to_parent"] = other_comprehensive_income_attributable_to_parent

                            if oci_not_reclassified_to_profit_loss != currDataSet.get("oci_not_reclassified_to_profit_loss") and oci_not_reclassified_to_profit_loss:
                                saveSet["oci_not_reclassified_to_profit_loss"] = oci_not_reclassified_to_profit_loss

                            if remeasurement_of_defined_benefit_plans != currDataSet.get("remeasurement_of_defined_benefit_plans") and remeasurement_of_defined_benefit_plans:
                                saveSet["remeasurement_of_defined_benefit_plans"] = remeasurement_of_defined_benefit_plans

                            if oci_under_equity_method_not_reclassified != currDataSet.get("oci_under_equity_method_not_reclassified") and oci_under_equity_method_not_reclassified:
                                saveSet["oci_under_equity_method_not_reclassified"] = oci_under_equity_method_not_reclassified

                            if fair_value_change_of_other_equity_instruments != currDataSet.get("fair_value_change_of_other_equity_instruments") and fair_value_change_of_other_equity_instruments:
                                saveSet["fair_value_change_of_other_equity_instruments"] = fair_value_change_of_other_equity_instruments

                            if fair_value_change_of_own_credit_risk != currDataSet.get("fair_value_change_of_own_credit_risk") and fair_value_change_of_own_credit_risk:
                                saveSet["fair_value_change_of_own_credit_risk"] = fair_value_change_of_own_credit_risk

                            if oci_reclassified_to_profit_loss != currDataSet.get("oci_reclassified_to_profit_loss") and oci_reclassified_to_profit_loss:
                                saveSet["oci_reclassified_to_profit_loss"] = oci_reclassified_to_profit_loss

                            if oci_under_equity_method_reclassified != currDataSet.get("oci_under_equity_method_reclassified") and oci_under_equity_method_reclassified:
                                saveSet["oci_under_equity_method_reclassified"] = oci_under_equity_method_reclassified

                            if fair_value_change_of_afs_financial_assets != currDataSet.get("fair_value_change_of_afs_financial_assets") and fair_value_change_of_afs_financial_assets:
                                saveSet["fair_value_change_of_afs_financial_assets"] = fair_value_change_of_afs_financial_assets

                            if fair_value_change_of_other_debt_investments != currDataSet.get("fair_value_change_of_other_debt_investments") and fair_value_change_of_other_debt_investments:
                                saveSet["fair_value_change_of_other_debt_investments"] = fair_value_change_of_other_debt_investments

                            if financial_assets_reclassified_to_oci != currDataSet.get("financial_assets_reclassified_to_oci") and financial_assets_reclassified_to_oci:
                                saveSet["financial_assets_reclassified_to_oci"] = financial_assets_reclassified_to_oci

                            if credit_impairment_of_other_debt_investments != currDataSet.get("credit_impairment_of_other_debt_investments") and credit_impairment_of_other_debt_investments:
                                saveSet["credit_impairment_of_other_debt_investments"] = credit_impairment_of_other_debt_investments

                            if htm_reclassified_to_afs_gains_losses != currDataSet.get("htm_reclassified_to_afs_gains_losses") and htm_reclassified_to_afs_gains_losses:
                                saveSet["htm_reclassified_to_afs_gains_losses"] = htm_reclassified_to_afs_gains_losses

                            if cash_flow_hedge_reserve != currDataSet.get("cash_flow_hedge_reserve") and cash_flow_hedge_reserve:
                                saveSet["cash_flow_hedge_reserve"] = cash_flow_hedge_reserve

                            if effective_portion_of_cash_flow_hedge != currDataSet.get("effective_portion_of_cash_flow_hedge") and effective_portion_of_cash_flow_hedge:
                                saveSet["effective_portion_of_cash_flow_hedge"] = effective_portion_of_cash_flow_hedge

                            if foreign_currency_translation_difference != currDataSet.get("foreign_currency_translation_difference") and foreign_currency_translation_difference:
                                saveSet["foreign_currency_translation_difference"] = foreign_currency_translation_difference

                            if other != currDataSet.get("other") and other:
                                saveSet["other"] = other

                            if other_comprehensive_income_attributable_to_minority != currDataSet.get("other_comprehensive_income_attributable_to_minority") and other_comprehensive_income_attributable_to_minority:
                                saveSet["other_comprehensive_income_attributable_to_minority"] = other_comprehensive_income_attributable_to_minority

                            if total_comprehensive_income != currDataSet.get("total_comprehensive_income") and total_comprehensive_income:
                                saveSet["total_comprehensive_income"] = total_comprehensive_income

                            if total_comprehensive_income_attributable_to_parent != currDataSet.get("total_comprehensive_income_attributable_to_parent") and total_comprehensive_income_attributable_to_parent:
                                saveSet["total_comprehensive_income_attributable_to_parent"] = total_comprehensive_income_attributable_to_parent

                            if total_comprehensive_income_attributable_to_minority != currDataSet.get("total_comprehensive_income_attributable_to_minority") and total_comprehensive_income_attributable_to_minority:
                                saveSet["total_comprehensive_income_attributable_to_minority"] = total_comprehensive_income_attributable_to_minority

                            if basic_earnings_per_share != currDataSet.get("basic_earnings_per_share") and basic_earnings_per_share:
                                saveSet["basic_earnings_per_share"] = basic_earnings_per_share

                            if diluted_earnings_per_share != currDataSet.get("diluted_earnings_per_share") and diluted_earnings_per_share:
                                saveSet["diluted_earnings_per_share"] = diluted_earnings_per_share

                            if label1 != currDataSet.get("label1") and label1:
                                saveSet["label1"] = label1

                            if label2 != currDataSet.get("label2") and label2:
                                saveSet["label2"] = label2

                            if label3 != currDataSet.get("label3") and label3:
                                saveSet["label3"] = label3

                            if memo != currDataSet.get("memo") and memo:
                                saveSet["memo"] = memo

                            if regID != currDataSet.get("regID") and regID:
                                saveSet["regID"] = regID

                            if regYMDHMS != currDataSet.get("regYMDHMS") and regYMDHMS:
                                saveSet["regYMDHMS"] = regYMDHMS

                            if modifyID != currDataSet.get("modifyID") and modifyID:
                                saveSet["modifyID"] = modifyID

                            if modifyYMDHMS != currDataSet.get("modifyYMDHMS") and modifyYMDHMS:
                                saveSet["modifyYMDHMS"] = modifyYMDHMS

                            if dispFlag != currDataSet.get("dispFlag") and dispFlag:
                                saveSet["dispFlag"] = dispFlag

                            if delFlag != currDataSet.get("delFlag") and delFlag:
                                saveSet["delFlag"] = delFlag


                            if saveSet:
                                #saveSet["delFlag"] = "0"
                                saveSet["modifyID"] = loginID
                                saveSet["modifyYMDHMS"] = misc.getTime()

                                #保存数据
                                tableName = comMysql.tablename_convertor_income_statements()
                                rtn = comMysql.update_income_statements(tableName,id,saveSet)
                                rtnData["rtn"] = str(rtn)

                                if rtn < 0:
                                    _LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")
                                else:
                                    if _DEBUG:
                                        pass
                                        _LOG.info(f"D: rtn:{rtn}")

                                result = rtnData

                        else:
                            #BT
                            errCode = "BT"

                    else:
                        #CB
                        errCode = "CB"

                else:
                    #data invalid
                    errCode = "BA"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#收入数据(income statements) 查询代码
def funcIncomeStatementsQry(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查

            if errCode == "B0": #
                #获取查询输入参数
                id = dataSet.get("id", "")

                #houseID = dataSet.get("houseID", "")
                symbol = dataSet.get("symbol", "")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code","")
                
                report_date = dataSet.get("report_date","")

                forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记

                searchOption = dataSet.get("searchOption")

                mode = dataSet.get("mode", "full")

                #limitNum = dataSet.get("limitNum",0)

                #权限检查/功能检测

                rightCheckFlag = True

                if rightCheckFlag:

                    #生成indexKey
                    indexKeyDataSet = {} #查询生成index的因素
                    if id:
                        indexKeyDataSet["id"] = id
                    if stock_code:
                        indexKeyDataSet["stock_code"] = stock_code
                    if report_date:
                        indexKeyDataSet["report_date"] = report_date
                    if searchOption:
                        indexKeyDataSet["searchOption"] = searchOption
                    if mode:
                        indexKeyDataSet["mode"] = mode

                    #if limitNum:
                        #indexKeyDataSet["limitNum"] = mode

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) 
                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) 
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) 

                    #判断数据是否在缓冲区:
                    if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:

                        if searchOption:
                            currDataList = []
                            tableName = comMysql.tablename_convertor_income_statements()
                            allDataList = comMysql.query_income_statements(tableName,mode = mode)
                            allowList = ["description", "label"] #筛选字段
                            serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)
                            if serachResultSet["rtn"] == "B0":
                                currDataList = serachResultSet.get("data", [])
                        else:
                            if id:
                                tableName = comMysql.tablename_convertor_income_statements()
                                currDataList = comMysql.query_income_statements(tableName,id,mode = mode)
                            else:
                                tableName = comMysql.tablename_convertor_income_statements()
                                currDataList = comMysql.query_income_statements(tableName,stock_code=stock_code,report_date=report_date,mode = mode)

                        dataList = []

                        for currDataSet in currDataList:
                            aSet = {}

                            #需要把文件转移到public domain
                            #appendixFileID00 =  currDataSet.get("appendixFileID00", "")
                            #appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)

                            #if mode == "full":
                                #aSet["houseID"] = currDataSet.get("houseID", "")

                            aSet["id"] = currDataSet.get("id","")
                            aSet["stock_code"] = currDataSet.get("stock_code","")
                            aSet["report_date"] = currDataSet.get("report_date","")
                            aSet["total_operating_revenue"] = currDataSet.get("total_operating_revenue","")
                            aSet["operating_revenue"] = currDataSet.get("operating_revenue","")
                            aSet["interest_income"] = currDataSet.get("interest_income","")
                            aSet["earned_premiums"] = currDataSet.get("earned_premiums","")
                            aSet["fees_and_commissions_income"] = currDataSet.get("fees_and_commissions_income","")
                            aSet["real_estate_sales_revenue"] = currDataSet.get("real_estate_sales_revenue","")
                            aSet["other_business_revenue"] = currDataSet.get("other_business_revenue","")
                            aSet["total_operating_costs"] = currDataSet.get("total_operating_costs","")
                            aSet["operating_costs"] = currDataSet.get("operating_costs","")
                            aSet["fees_and_commissions_expenses"] = currDataSet.get("fees_and_commissions_expenses","")
                            aSet["real_estate_sales_costs"] = currDataSet.get("real_estate_sales_costs","")
                            aSet["surrender_value"] = currDataSet.get("surrender_value","")
                            aSet["net_claims_paid"] = currDataSet.get("net_claims_paid","")
                            aSet["net_insurance_contract_reserves"] = currDataSet.get("net_insurance_contract_reserves","")
                            aSet["policy_dividend_expenses"] = currDataSet.get("policy_dividend_expenses","")
                            aSet["reinsurance_expenses"] = currDataSet.get("reinsurance_expenses","")
                            aSet["other_business_costs"] = currDataSet.get("other_business_costs","")
                            aSet["taxes_and_surcharges"] = currDataSet.get("taxes_and_surcharges","")
                            aSet["rd_expenses"] = currDataSet.get("rd_expenses","")
                            aSet["selling_expenses"] = currDataSet.get("selling_expenses","")
                            aSet["administrative_expenses"] = currDataSet.get("administrative_expenses","")
                            aSet["financial_expenses"] = currDataSet.get("financial_expenses","")
                            aSet["interest_expenses"] = currDataSet.get("interest_expenses","")
                            aSet["interest_expenditure"] = currDataSet.get("interest_expenditure","")
                            aSet["investment_income"] = currDataSet.get("investment_income","")
                            aSet["investment_income_from_associates_and_joint_ventures"] = currDataSet.get("investment_income_from_associates_and_joint_ventures","")
                            aSet["gain_on_derecognition_of_financial_assets_at_amortized_cost"] = currDataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost","")
                            aSet["foreign_exchange_gains"] = currDataSet.get("foreign_exchange_gains","")
                            aSet["net_open_hedge_gains"] = currDataSet.get("net_open_hedge_gains","")
                            aSet["fair_value_change_gains"] = currDataSet.get("fair_value_change_gains","")
                            aSet["futures_gains_losses"] = currDataSet.get("futures_gains_losses","")
                            aSet["custody_income"] = currDataSet.get("custody_income","")
                            aSet["subsidy_income"] = currDataSet.get("subsidy_income","")
                            aSet["other_gains"] = currDataSet.get("other_gains","")
                            aSet["asset_impairment_losses"] = currDataSet.get("asset_impairment_losses","")
                            aSet["credit_impairment_losses"] = currDataSet.get("credit_impairment_losses","")
                            aSet["other_business_profits"] = currDataSet.get("other_business_profits","")
                            aSet["asset_disposal_gains"] = currDataSet.get("asset_disposal_gains","")
                            aSet["operating_profit"] = currDataSet.get("operating_profit","")
                            aSet["non_operating_income"] = currDataSet.get("non_operating_income","")
                            aSet["non_current_asset_disposal_gains"] = currDataSet.get("non_current_asset_disposal_gains","")
                            aSet["non_operating_expenses"] = currDataSet.get("non_operating_expenses","")
                            aSet["non_current_asset_disposal_losses"] = currDataSet.get("non_current_asset_disposal_losses","")
                            aSet["total_profit"] = currDataSet.get("total_profit","")
                            aSet["income_tax_expense"] = currDataSet.get("income_tax_expense","")
                            aSet["unrecognized_investment_losses"] = currDataSet.get("unrecognized_investment_losses","")
                            aSet["net_profit"] = currDataSet.get("net_profit","")
                            aSet["net_profit_from_continuing_operations"] = currDataSet.get("net_profit_from_continuing_operations","")
                            aSet["net_profit_from_discontinued_operations"] = currDataSet.get("net_profit_from_discontinued_operations","")
                            aSet["net_profit_attributable_to_parent_company"] = currDataSet.get("net_profit_attributable_to_parent_company","")
                            aSet["net_profit_of_acquiree_before_merger"] = currDataSet.get("net_profit_of_acquiree_before_merger","")
                            aSet["minority_interests_profit_loss"] = currDataSet.get("minority_interests_profit_loss","")
                            aSet["other_comprehensive_income"] = currDataSet.get("other_comprehensive_income","")
                            aSet["other_comprehensive_income_attributable_to_parent"] = currDataSet.get("other_comprehensive_income_attributable_to_parent","")
                            aSet["oci_not_reclassified_to_profit_loss"] = currDataSet.get("oci_not_reclassified_to_profit_loss","")
                            aSet["remeasurement_of_defined_benefit_plans"] = currDataSet.get("remeasurement_of_defined_benefit_plans","")
                            aSet["oci_under_equity_method_not_reclassified"] = currDataSet.get("oci_under_equity_method_not_reclassified","")
                            aSet["fair_value_change_of_other_equity_instruments"] = currDataSet.get("fair_value_change_of_other_equity_instruments","")
                            aSet["fair_value_change_of_own_credit_risk"] = currDataSet.get("fair_value_change_of_own_credit_risk","")
                            aSet["oci_reclassified_to_profit_loss"] = currDataSet.get("oci_reclassified_to_profit_loss","")
                            aSet["oci_under_equity_method_reclassified"] = currDataSet.get("oci_under_equity_method_reclassified","")
                            aSet["fair_value_change_of_afs_financial_assets"] = currDataSet.get("fair_value_change_of_afs_financial_assets","")
                            aSet["fair_value_change_of_other_debt_investments"] = currDataSet.get("fair_value_change_of_other_debt_investments","")
                            aSet["financial_assets_reclassified_to_oci"] = currDataSet.get("financial_assets_reclassified_to_oci","")
                            aSet["credit_impairment_of_other_debt_investments"] = currDataSet.get("credit_impairment_of_other_debt_investments","")
                            aSet["htm_reclassified_to_afs_gains_losses"] = currDataSet.get("htm_reclassified_to_afs_gains_losses","")
                            aSet["cash_flow_hedge_reserve"] = currDataSet.get("cash_flow_hedge_reserve","")
                            aSet["effective_portion_of_cash_flow_hedge"] = currDataSet.get("effective_portion_of_cash_flow_hedge","")
                            aSet["foreign_currency_translation_difference"] = currDataSet.get("foreign_currency_translation_difference","")
                            aSet["other"] = currDataSet.get("other","")
                            aSet["other_comprehensive_income_attributable_to_minority"] = currDataSet.get("other_comprehensive_income_attributable_to_minority","")
                            aSet["total_comprehensive_income"] = currDataSet.get("total_comprehensive_income","")
                            aSet["total_comprehensive_income_attributable_to_parent"] = currDataSet.get("total_comprehensive_income_attributable_to_parent","")
                            aSet["total_comprehensive_income_attributable_to_minority"] = currDataSet.get("total_comprehensive_income_attributable_to_minority","")
                            aSet["basic_earnings_per_share"] = currDataSet.get("basic_earnings_per_share","")
                            aSet["diluted_earnings_per_share"] = currDataSet.get("diluted_earnings_per_share","")
                            aSet["label1"] = currDataSet.get("label1","")
                            aSet["label2"] = currDataSet.get("label2","")
                            aSet["label3"] = currDataSet.get("label3","")
                            aSet["memo"] = currDataSet.get("memo","")
                            aSet["regID"] = currDataSet.get("regID","")
                            aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                            aSet["modifyID"] = currDataSet.get("modifyID","")
                            aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                            aSet["dispFlag"] = currDataSet.get("dispFlag","")
                            aSet["delFlag"] = currDataSet.get("delFlag","")

                            dataList.append(aSet)

                        #临时缓存机制,改进型, 2023/10/16
                        indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去

                    rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)

                    #rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "BT"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#cash flow增加代码
def funcCashFlowAdd(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                #data validation check
                symbol = dataSet.get("symbol", "")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code", "")
                if stock_code:
                    dataValidFlag = True
                else:
                    dataValidFlag = False
                if dataValidFlag:
                    saveSet = {}
                    saveSet["stock_code"] = stock_code
                    saveSet["report_date"] = dataSet.get("report_date", "") 
                    saveSet["total_operating_revenue"] = dataSet.get("total_operating_revenue", "") 
                    saveSet["operating_revenue"] = dataSet.get("operating_revenue", "") 
                    saveSet["interest_income"] = dataSet.get("interest_income", "") 
                    saveSet["earned_premiums"] = dataSet.get("earned_premiums", "") 
                    saveSet["fees_and_commissions_income"] = dataSet.get("fees_and_commissions_income", "") 
                    saveSet["real_estate_sales_revenue"] = dataSet.get("real_estate_sales_revenue", "") 
                    saveSet["other_business_revenue"] = dataSet.get("other_business_revenue", "") 
                    saveSet["total_operating_costs"] = dataSet.get("total_operating_costs", "") 
                    saveSet["operating_costs"] = dataSet.get("operating_costs", "") 
                    saveSet["fees_and_commissions_expenses"] = dataSet.get("fees_and_commissions_expenses", "") 
                    saveSet["real_estate_sales_costs"] = dataSet.get("real_estate_sales_costs", "") 
                    saveSet["surrender_value"] = dataSet.get("surrender_value", "") 
                    saveSet["net_claims_paid"] = dataSet.get("net_claims_paid", "") 
                    saveSet["net_insurance_contract_reserves"] = dataSet.get("net_insurance_contract_reserves", "") 
                    saveSet["policy_dividend_expenses"] = dataSet.get("policy_dividend_expenses", "") 
                    saveSet["reinsurance_expenses"] = dataSet.get("reinsurance_expenses", "") 
                    saveSet["other_business_costs"] = dataSet.get("other_business_costs", "") 
                    saveSet["taxes_and_surcharges"] = dataSet.get("taxes_and_surcharges", "") 
                    saveSet["rd_expenses"] = dataSet.get("rd_expenses", "") 
                    saveSet["selling_expenses"] = dataSet.get("selling_expenses", "") 
                    saveSet["administrative_expenses"] = dataSet.get("administrative_expenses", "") 
                    saveSet["financial_expenses"] = dataSet.get("financial_expenses", "") 
                    saveSet["interest_expenses"] = dataSet.get("interest_expenses", "") 
                    saveSet["interest_expenditure"] = dataSet.get("interest_expenditure", "") 
                    saveSet["investment_income"] = dataSet.get("investment_income", "") 
                    saveSet["investment_income_from_associates_and_joint_ventures"] = dataSet.get("investment_income_from_associates_and_joint_ventures", "") 
                    saveSet["gain_on_derecognition_of_financial_assets_at_amortized_cost"] = dataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost", "") 
                    saveSet["foreign_exchange_gains"] = dataSet.get("foreign_exchange_gains", "") 
                    saveSet["net_open_hedge_gains"] = dataSet.get("net_open_hedge_gains", "") 
                    saveSet["fair_value_change_gains"] = dataSet.get("fair_value_change_gains", "") 
                    saveSet["futures_gains_losses"] = dataSet.get("futures_gains_losses", "") 
                    saveSet["custody_income"] = dataSet.get("custody_income", "") 
                    saveSet["subsidy_income"] = dataSet.get("subsidy_income", "") 
                    saveSet["other_gains"] = dataSet.get("other_gains", "") 
                    saveSet["asset_impairment_losses"] = dataSet.get("asset_impairment_losses", "") 
                    saveSet["credit_impairment_losses"] = dataSet.get("credit_impairment_losses", "") 
                    saveSet["other_business_profits"] = dataSet.get("other_business_profits", "") 
                    saveSet["asset_disposal_gains"] = dataSet.get("asset_disposal_gains", "") 
                    saveSet["operating_profit"] = dataSet.get("operating_profit", "") 
                    saveSet["non_operating_income"] = dataSet.get("non_operating_income", "") 
                    saveSet["non_current_asset_disposal_gains"] = dataSet.get("non_current_asset_disposal_gains", "") 
                    saveSet["non_operating_expenses"] = dataSet.get("non_operating_expenses", "") 
                    saveSet["non_current_asset_disposal_losses"] = dataSet.get("non_current_asset_disposal_losses", "") 
                    saveSet["total_profit"] = dataSet.get("total_profit", "") 
                    saveSet["income_tax_expense"] = dataSet.get("income_tax_expense", "") 
                    saveSet["unrecognized_investment_losses"] = dataSet.get("unrecognized_investment_losses", "") 
                    saveSet["net_profit"] = dataSet.get("net_profit", "") 
                    saveSet["net_profit_from_continuing_operations"] = dataSet.get("net_profit_from_continuing_operations", "") 
                    saveSet["net_profit_from_discontinued_operations"] = dataSet.get("net_profit_from_discontinued_operations", "") 
                    saveSet["net_profit_attributable_to_parent_company"] = dataSet.get("net_profit_attributable_to_parent_company", "") 
                    saveSet["net_profit_of_acquiree_before_merger"] = dataSet.get("net_profit_of_acquiree_before_merger", "") 
                    saveSet["minority_interests_profit_loss"] = dataSet.get("minority_interests_profit_loss", "") 
                    saveSet["other_comprehensive_income"] = dataSet.get("other_comprehensive_income", "") 
                    saveSet["other_comprehensive_income_attributable_to_parent"] = dataSet.get("other_comprehensive_income_attributable_to_parent", "") 
                    saveSet["oci_not_reclassified_to_profit_loss"] = dataSet.get("oci_not_reclassified_to_profit_loss", "") 
                    saveSet["remeasurement_of_defined_benefit_plans"] = dataSet.get("remeasurement_of_defined_benefit_plans", "") 
                    saveSet["oci_under_equity_method_not_reclassified"] = dataSet.get("oci_under_equity_method_not_reclassified", "") 
                    saveSet["fair_value_change_of_other_equity_instruments"] = dataSet.get("fair_value_change_of_other_equity_instruments", "") 
                    saveSet["fair_value_change_of_own_credit_risk"] = dataSet.get("fair_value_change_of_own_credit_risk", "") 
                    saveSet["oci_reclassified_to_profit_loss"] = dataSet.get("oci_reclassified_to_profit_loss", "") 
                    saveSet["oci_under_equity_method_reclassified"] = dataSet.get("oci_under_equity_method_reclassified", "") 
                    saveSet["fair_value_change_of_afs_financial_assets"] = dataSet.get("fair_value_change_of_afs_financial_assets", "") 
                    saveSet["fair_value_change_of_other_debt_investments"] = dataSet.get("fair_value_change_of_other_debt_investments", "") 
                    saveSet["financial_assets_reclassified_to_oci"] = dataSet.get("financial_assets_reclassified_to_oci", "") 
                    saveSet["credit_impairment_of_other_debt_investments"] = dataSet.get("credit_impairment_of_other_debt_investments", "") 
                    saveSet["htm_reclassified_to_afs_gains_losses"] = dataSet.get("htm_reclassified_to_afs_gains_losses", "") 
                    saveSet["cash_flow_hedge_reserve"] = dataSet.get("cash_flow_hedge_reserve", "") 
                    saveSet["effective_portion_of_cash_flow_hedge"] = dataSet.get("effective_portion_of_cash_flow_hedge", "") 
                    saveSet["foreign_currency_translation_difference"] = dataSet.get("foreign_currency_translation_difference", "") 
                    saveSet["other"] = dataSet.get("other", "") 
                    saveSet["other_comprehensive_income_attributable_to_minority"] = dataSet.get("other_comprehensive_income_attributable_to_minority", "") 
                    saveSet["total_comprehensive_income"] = dataSet.get("total_comprehensive_income", "") 
                    saveSet["total_comprehensive_income_attributable_to_parent"] = dataSet.get("total_comprehensive_income_attributable_to_parent", "") 
                    saveSet["total_comprehensive_income_attributable_to_minority"] = dataSet.get("total_comprehensive_income_attributable_to_minority", "") 
                    saveSet["basic_earnings_per_share"] = dataSet.get("basic_earnings_per_share", "") 
                    saveSet["diluted_earnings_per_share"] = dataSet.get("diluted_earnings_per_share", "") 
                    saveSet["label1"] = dataSet.get("label1", "") 
                    saveSet["label2"] = dataSet.get("label2", "") 
                    saveSet["label3"] = dataSet.get("label3", "") 
                    saveSet["memo"] = dataSet.get("memo", "") 
                    saveSet["dispFlag"] = dataSet.get("dispFlag", "") 
                    saveSet["delFlag"] = dataSet.get("delFlag", "0") 
                    saveSet["regID"] = loginID
                    saveSet["regYMDHMS"] = misc.getTime()

                    tableName = comMysql.tablename_convertor_cash_flow_statements()
                    recID = comMysql.insert_cash_flow_statements(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},saveSet:{saveSet}")
                    else:
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: recID:{recID}")

                    result = rtnData

                else:
                    #data invalid
                    errCode = "BA"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#cash flow删除代码
def funcCashFlowDel(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                id = dataSet.get("id", "")
                tableName = comMysql.tablename_convertor_cash_flow_statements()
                currDataList = comMysql.query_cash_flow_statements(tableName,id)
                if len(currDataList) == 1:
                    saveSet = {}
                    saveSet["modifyID"] = loginID
                    saveSet["modifyYMDHMS"] = misc.getTime()
                    #saveSet["delFlag"] = "1"

                    rtn = comMysql.delete_cash_flow_statements(tableName,id)
                    rtnData["rtn"] = str(rtn)

                    if _DEBUG:
                        _LOG.info(f"D: rtn:{rtn}")

                    result = rtnData

                else:
                    errCode = "CB"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#cash flow修改代码
def funcCashFlowModify(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                stock_code = dataSet.get("stock_code") 
                report_date = dataSet.get("report_date") 
                total_operating_revenue = dataSet.get("total_operating_revenue") 
                operating_revenue = dataSet.get("operating_revenue") 
                interest_income = dataSet.get("interest_income") 
                earned_premiums = dataSet.get("earned_premiums") 
                fees_and_commissions_income = dataSet.get("fees_and_commissions_income") 
                real_estate_sales_revenue = dataSet.get("real_estate_sales_revenue") 
                other_business_revenue = dataSet.get("other_business_revenue") 
                total_operating_costs = dataSet.get("total_operating_costs") 
                operating_costs = dataSet.get("operating_costs") 
                fees_and_commissions_expenses = dataSet.get("fees_and_commissions_expenses") 
                real_estate_sales_costs = dataSet.get("real_estate_sales_costs") 
                surrender_value = dataSet.get("surrender_value") 
                net_claims_paid = dataSet.get("net_claims_paid") 
                net_insurance_contract_reserves = dataSet.get("net_insurance_contract_reserves") 
                policy_dividend_expenses = dataSet.get("policy_dividend_expenses") 
                reinsurance_expenses = dataSet.get("reinsurance_expenses") 
                other_business_costs = dataSet.get("other_business_costs") 
                taxes_and_surcharges = dataSet.get("taxes_and_surcharges") 
                rd_expenses = dataSet.get("rd_expenses") 
                selling_expenses = dataSet.get("selling_expenses") 
                administrative_expenses = dataSet.get("administrative_expenses") 
                financial_expenses = dataSet.get("financial_expenses") 
                interest_expenses = dataSet.get("interest_expenses") 
                interest_expenditure = dataSet.get("interest_expenditure") 
                investment_income = dataSet.get("investment_income") 
                investment_income_from_associates_and_joint_ventures = dataSet.get("investment_income_from_associates_and_joint_ventures") 
                gain_on_derecognition_of_financial_assets_at_amortized_cost = dataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost") 
                foreign_exchange_gains = dataSet.get("foreign_exchange_gains") 
                net_open_hedge_gains = dataSet.get("net_open_hedge_gains") 
                fair_value_change_gains = dataSet.get("fair_value_change_gains") 
                futures_gains_losses = dataSet.get("futures_gains_losses") 
                custody_income = dataSet.get("custody_income") 
                subsidy_income = dataSet.get("subsidy_income") 
                other_gains = dataSet.get("other_gains") 
                asset_impairment_losses = dataSet.get("asset_impairment_losses") 
                credit_impairment_losses = dataSet.get("credit_impairment_losses") 
                other_business_profits = dataSet.get("other_business_profits") 
                asset_disposal_gains = dataSet.get("asset_disposal_gains") 
                operating_profit = dataSet.get("operating_profit") 
                non_operating_income = dataSet.get("non_operating_income") 
                non_current_asset_disposal_gains = dataSet.get("non_current_asset_disposal_gains") 
                non_operating_expenses = dataSet.get("non_operating_expenses") 
                non_current_asset_disposal_losses = dataSet.get("non_current_asset_disposal_losses") 
                total_profit = dataSet.get("total_profit") 
                income_tax_expense = dataSet.get("income_tax_expense") 
                unrecognized_investment_losses = dataSet.get("unrecognized_investment_losses") 
                net_profit = dataSet.get("net_profit") 
                net_profit_from_continuing_operations = dataSet.get("net_profit_from_continuing_operations") 
                net_profit_from_discontinued_operations = dataSet.get("net_profit_from_discontinued_operations") 
                net_profit_attributable_to_parent_company = dataSet.get("net_profit_attributable_to_parent_company") 
                net_profit_of_acquiree_before_merger = dataSet.get("net_profit_of_acquiree_before_merger") 
                minority_interests_profit_loss = dataSet.get("minority_interests_profit_loss") 
                other_comprehensive_income = dataSet.get("other_comprehensive_income") 
                other_comprehensive_income_attributable_to_parent = dataSet.get("other_comprehensive_income_attributable_to_parent") 
                oci_not_reclassified_to_profit_loss = dataSet.get("oci_not_reclassified_to_profit_loss") 
                remeasurement_of_defined_benefit_plans = dataSet.get("remeasurement_of_defined_benefit_plans") 
                oci_under_equity_method_not_reclassified = dataSet.get("oci_under_equity_method_not_reclassified") 
                fair_value_change_of_other_equity_instruments = dataSet.get("fair_value_change_of_other_equity_instruments") 
                fair_value_change_of_own_credit_risk = dataSet.get("fair_value_change_of_own_credit_risk") 
                oci_reclassified_to_profit_loss = dataSet.get("oci_reclassified_to_profit_loss") 
                oci_under_equity_method_reclassified = dataSet.get("oci_under_equity_method_reclassified") 
                fair_value_change_of_afs_financial_assets = dataSet.get("fair_value_change_of_afs_financial_assets") 
                fair_value_change_of_other_debt_investments = dataSet.get("fair_value_change_of_other_debt_investments") 
                financial_assets_reclassified_to_oci = dataSet.get("financial_assets_reclassified_to_oci") 
                credit_impairment_of_other_debt_investments = dataSet.get("credit_impairment_of_other_debt_investments") 
                htm_reclassified_to_afs_gains_losses = dataSet.get("htm_reclassified_to_afs_gains_losses") 
                cash_flow_hedge_reserve = dataSet.get("cash_flow_hedge_reserve") 
                effective_portion_of_cash_flow_hedge = dataSet.get("effective_portion_of_cash_flow_hedge") 
                foreign_currency_translation_difference = dataSet.get("foreign_currency_translation_difference") 
                other = dataSet.get("other") 
                other_comprehensive_income_attributable_to_minority = dataSet.get("other_comprehensive_income_attributable_to_minority") 
                total_comprehensive_income = dataSet.get("total_comprehensive_income") 
                total_comprehensive_income_attributable_to_parent = dataSet.get("total_comprehensive_income_attributable_to_parent") 
                total_comprehensive_income_attributable_to_minority = dataSet.get("total_comprehensive_income_attributable_to_minority") 
                basic_earnings_per_share = dataSet.get("basic_earnings_per_share") 
                diluted_earnings_per_share = dataSet.get("diluted_earnings_per_share") 
                label1 = dataSet.get("label1") 
                label2 = dataSet.get("label2") 
                label3 = dataSet.get("label3") 
                memo = dataSet.get("memo") 
                dispFlag = dataSet.get("dispFlag") 
                delFlag = dataSet.get("delFlag") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    recID = dataSet.get("id", "")

                    tableName = comMysql.tablename_convertor_cash_flow_statements()
                    currDataList = comMysql.query_cash_flow_statements(tableName,recID)

                    if len(currDataList) == 1:
                        currDataSet = currDataList[0]

                        #权限或其他检查
                        if errCode == "B0": #

                            saveSet = {}

                            if stock_code != currDataSet.get("stock_code") and stock_code:
                                saveSet["stock_code"] = stock_code

                            if report_date != currDataSet.get("report_date") and report_date:
                                saveSet["report_date"] = report_date

                            if total_operating_revenue != currDataSet.get("total_operating_revenue") and total_operating_revenue:
                                saveSet["total_operating_revenue"] = total_operating_revenue

                            if operating_revenue != currDataSet.get("operating_revenue") and operating_revenue:
                                saveSet["operating_revenue"] = operating_revenue

                            if interest_income != currDataSet.get("interest_income") and interest_income:
                                saveSet["interest_income"] = interest_income

                            if earned_premiums != currDataSet.get("earned_premiums") and earned_premiums:
                                saveSet["earned_premiums"] = earned_premiums

                            if fees_and_commissions_income != currDataSet.get("fees_and_commissions_income") and fees_and_commissions_income:
                                saveSet["fees_and_commissions_income"] = fees_and_commissions_income

                            if real_estate_sales_revenue != currDataSet.get("real_estate_sales_revenue") and real_estate_sales_revenue:
                                saveSet["real_estate_sales_revenue"] = real_estate_sales_revenue

                            if other_business_revenue != currDataSet.get("other_business_revenue") and other_business_revenue:
                                saveSet["other_business_revenue"] = other_business_revenue

                            if total_operating_costs != currDataSet.get("total_operating_costs") and total_operating_costs:
                                saveSet["total_operating_costs"] = total_operating_costs

                            if operating_costs != currDataSet.get("operating_costs") and operating_costs:
                                saveSet["operating_costs"] = operating_costs

                            if fees_and_commissions_expenses != currDataSet.get("fees_and_commissions_expenses") and fees_and_commissions_expenses:
                                saveSet["fees_and_commissions_expenses"] = fees_and_commissions_expenses

                            if real_estate_sales_costs != currDataSet.get("real_estate_sales_costs") and real_estate_sales_costs:
                                saveSet["real_estate_sales_costs"] = real_estate_sales_costs

                            if surrender_value != currDataSet.get("surrender_value") and surrender_value:
                                saveSet["surrender_value"] = surrender_value

                            if net_claims_paid != currDataSet.get("net_claims_paid") and net_claims_paid:
                                saveSet["net_claims_paid"] = net_claims_paid

                            if net_insurance_contract_reserves != currDataSet.get("net_insurance_contract_reserves") and net_insurance_contract_reserves:
                                saveSet["net_insurance_contract_reserves"] = net_insurance_contract_reserves

                            if policy_dividend_expenses != currDataSet.get("policy_dividend_expenses") and policy_dividend_expenses:
                                saveSet["policy_dividend_expenses"] = policy_dividend_expenses

                            if reinsurance_expenses != currDataSet.get("reinsurance_expenses") and reinsurance_expenses:
                                saveSet["reinsurance_expenses"] = reinsurance_expenses

                            if other_business_costs != currDataSet.get("other_business_costs") and other_business_costs:
                                saveSet["other_business_costs"] = other_business_costs

                            if taxes_and_surcharges != currDataSet.get("taxes_and_surcharges") and taxes_and_surcharges:
                                saveSet["taxes_and_surcharges"] = taxes_and_surcharges

                            if rd_expenses != currDataSet.get("rd_expenses") and rd_expenses:
                                saveSet["rd_expenses"] = rd_expenses

                            if selling_expenses != currDataSet.get("selling_expenses") and selling_expenses:
                                saveSet["selling_expenses"] = selling_expenses

                            if administrative_expenses != currDataSet.get("administrative_expenses") and administrative_expenses:
                                saveSet["administrative_expenses"] = administrative_expenses

                            if financial_expenses != currDataSet.get("financial_expenses") and financial_expenses:
                                saveSet["financial_expenses"] = financial_expenses

                            if interest_expenses != currDataSet.get("interest_expenses") and interest_expenses:
                                saveSet["interest_expenses"] = interest_expenses

                            if interest_expenditure != currDataSet.get("interest_expenditure") and interest_expenditure:
                                saveSet["interest_expenditure"] = interest_expenditure

                            if investment_income != currDataSet.get("investment_income") and investment_income:
                                saveSet["investment_income"] = investment_income

                            if investment_income_from_associates_and_joint_ventures != currDataSet.get("investment_income_from_associates_and_joint_ventures") and investment_income_from_associates_and_joint_ventures:
                                saveSet["investment_income_from_associates_and_joint_ventures"] = investment_income_from_associates_and_joint_ventures

                            if gain_on_derecognition_of_financial_assets_at_amortized_cost != currDataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost") and gain_on_derecognition_of_financial_assets_at_amortized_cost:
                                saveSet["gain_on_derecognition_of_financial_assets_at_amortized_cost"] = gain_on_derecognition_of_financial_assets_at_amortized_cost

                            if foreign_exchange_gains != currDataSet.get("foreign_exchange_gains") and foreign_exchange_gains:
                                saveSet["foreign_exchange_gains"] = foreign_exchange_gains

                            if net_open_hedge_gains != currDataSet.get("net_open_hedge_gains") and net_open_hedge_gains:
                                saveSet["net_open_hedge_gains"] = net_open_hedge_gains

                            if fair_value_change_gains != currDataSet.get("fair_value_change_gains") and fair_value_change_gains:
                                saveSet["fair_value_change_gains"] = fair_value_change_gains

                            if futures_gains_losses != currDataSet.get("futures_gains_losses") and futures_gains_losses:
                                saveSet["futures_gains_losses"] = futures_gains_losses

                            if custody_income != currDataSet.get("custody_income") and custody_income:
                                saveSet["custody_income"] = custody_income

                            if subsidy_income != currDataSet.get("subsidy_income") and subsidy_income:
                                saveSet["subsidy_income"] = subsidy_income

                            if other_gains != currDataSet.get("other_gains") and other_gains:
                                saveSet["other_gains"] = other_gains

                            if asset_impairment_losses != currDataSet.get("asset_impairment_losses") and asset_impairment_losses:
                                saveSet["asset_impairment_losses"] = asset_impairment_losses

                            if credit_impairment_losses != currDataSet.get("credit_impairment_losses") and credit_impairment_losses:
                                saveSet["credit_impairment_losses"] = credit_impairment_losses

                            if other_business_profits != currDataSet.get("other_business_profits") and other_business_profits:
                                saveSet["other_business_profits"] = other_business_profits

                            if asset_disposal_gains != currDataSet.get("asset_disposal_gains") and asset_disposal_gains:
                                saveSet["asset_disposal_gains"] = asset_disposal_gains

                            if operating_profit != currDataSet.get("operating_profit") and operating_profit:
                                saveSet["operating_profit"] = operating_profit

                            if non_operating_income != currDataSet.get("non_operating_income") and non_operating_income:
                                saveSet["non_operating_income"] = non_operating_income

                            if non_current_asset_disposal_gains != currDataSet.get("non_current_asset_disposal_gains") and non_current_asset_disposal_gains:
                                saveSet["non_current_asset_disposal_gains"] = non_current_asset_disposal_gains

                            if non_operating_expenses != currDataSet.get("non_operating_expenses") and non_operating_expenses:
                                saveSet["non_operating_expenses"] = non_operating_expenses

                            if non_current_asset_disposal_losses != currDataSet.get("non_current_asset_disposal_losses") and non_current_asset_disposal_losses:
                                saveSet["non_current_asset_disposal_losses"] = non_current_asset_disposal_losses

                            if total_profit != currDataSet.get("total_profit") and total_profit:
                                saveSet["total_profit"] = total_profit

                            if income_tax_expense != currDataSet.get("income_tax_expense") and income_tax_expense:
                                saveSet["income_tax_expense"] = income_tax_expense

                            if unrecognized_investment_losses != currDataSet.get("unrecognized_investment_losses") and unrecognized_investment_losses:
                                saveSet["unrecognized_investment_losses"] = unrecognized_investment_losses

                            if net_profit != currDataSet.get("net_profit") and net_profit:
                                saveSet["net_profit"] = net_profit

                            if net_profit_from_continuing_operations != currDataSet.get("net_profit_from_continuing_operations") and net_profit_from_continuing_operations:
                                saveSet["net_profit_from_continuing_operations"] = net_profit_from_continuing_operations

                            if net_profit_from_discontinued_operations != currDataSet.get("net_profit_from_discontinued_operations") and net_profit_from_discontinued_operations:
                                saveSet["net_profit_from_discontinued_operations"] = net_profit_from_discontinued_operations

                            if net_profit_attributable_to_parent_company != currDataSet.get("net_profit_attributable_to_parent_company") and net_profit_attributable_to_parent_company:
                                saveSet["net_profit_attributable_to_parent_company"] = net_profit_attributable_to_parent_company

                            if net_profit_of_acquiree_before_merger != currDataSet.get("net_profit_of_acquiree_before_merger") and net_profit_of_acquiree_before_merger:
                                saveSet["net_profit_of_acquiree_before_merger"] = net_profit_of_acquiree_before_merger

                            if minority_interests_profit_loss != currDataSet.get("minority_interests_profit_loss") and minority_interests_profit_loss:
                                saveSet["minority_interests_profit_loss"] = minority_interests_profit_loss

                            if other_comprehensive_income != currDataSet.get("other_comprehensive_income") and other_comprehensive_income:
                                saveSet["other_comprehensive_income"] = other_comprehensive_income

                            if other_comprehensive_income_attributable_to_parent != currDataSet.get("other_comprehensive_income_attributable_to_parent") and other_comprehensive_income_attributable_to_parent:
                                saveSet["other_comprehensive_income_attributable_to_parent"] = other_comprehensive_income_attributable_to_parent

                            if oci_not_reclassified_to_profit_loss != currDataSet.get("oci_not_reclassified_to_profit_loss") and oci_not_reclassified_to_profit_loss:
                                saveSet["oci_not_reclassified_to_profit_loss"] = oci_not_reclassified_to_profit_loss

                            if remeasurement_of_defined_benefit_plans != currDataSet.get("remeasurement_of_defined_benefit_plans") and remeasurement_of_defined_benefit_plans:
                                saveSet["remeasurement_of_defined_benefit_plans"] = remeasurement_of_defined_benefit_plans

                            if oci_under_equity_method_not_reclassified != currDataSet.get("oci_under_equity_method_not_reclassified") and oci_under_equity_method_not_reclassified:
                                saveSet["oci_under_equity_method_not_reclassified"] = oci_under_equity_method_not_reclassified

                            if fair_value_change_of_other_equity_instruments != currDataSet.get("fair_value_change_of_other_equity_instruments") and fair_value_change_of_other_equity_instruments:
                                saveSet["fair_value_change_of_other_equity_instruments"] = fair_value_change_of_other_equity_instruments

                            if fair_value_change_of_own_credit_risk != currDataSet.get("fair_value_change_of_own_credit_risk") and fair_value_change_of_own_credit_risk:
                                saveSet["fair_value_change_of_own_credit_risk"] = fair_value_change_of_own_credit_risk

                            if oci_reclassified_to_profit_loss != currDataSet.get("oci_reclassified_to_profit_loss") and oci_reclassified_to_profit_loss:
                                saveSet["oci_reclassified_to_profit_loss"] = oci_reclassified_to_profit_loss

                            if oci_under_equity_method_reclassified != currDataSet.get("oci_under_equity_method_reclassified") and oci_under_equity_method_reclassified:
                                saveSet["oci_under_equity_method_reclassified"] = oci_under_equity_method_reclassified

                            if fair_value_change_of_afs_financial_assets != currDataSet.get("fair_value_change_of_afs_financial_assets") and fair_value_change_of_afs_financial_assets:
                                saveSet["fair_value_change_of_afs_financial_assets"] = fair_value_change_of_afs_financial_assets

                            if fair_value_change_of_other_debt_investments != currDataSet.get("fair_value_change_of_other_debt_investments") and fair_value_change_of_other_debt_investments:
                                saveSet["fair_value_change_of_other_debt_investments"] = fair_value_change_of_other_debt_investments

                            if financial_assets_reclassified_to_oci != currDataSet.get("financial_assets_reclassified_to_oci") and financial_assets_reclassified_to_oci:
                                saveSet["financial_assets_reclassified_to_oci"] = financial_assets_reclassified_to_oci

                            if credit_impairment_of_other_debt_investments != currDataSet.get("credit_impairment_of_other_debt_investments") and credit_impairment_of_other_debt_investments:
                                saveSet["credit_impairment_of_other_debt_investments"] = credit_impairment_of_other_debt_investments

                            if htm_reclassified_to_afs_gains_losses != currDataSet.get("htm_reclassified_to_afs_gains_losses") and htm_reclassified_to_afs_gains_losses:
                                saveSet["htm_reclassified_to_afs_gains_losses"] = htm_reclassified_to_afs_gains_losses

                            if cash_flow_hedge_reserve != currDataSet.get("cash_flow_hedge_reserve") and cash_flow_hedge_reserve:
                                saveSet["cash_flow_hedge_reserve"] = cash_flow_hedge_reserve

                            if effective_portion_of_cash_flow_hedge != currDataSet.get("effective_portion_of_cash_flow_hedge") and effective_portion_of_cash_flow_hedge:
                                saveSet["effective_portion_of_cash_flow_hedge"] = effective_portion_of_cash_flow_hedge

                            if foreign_currency_translation_difference != currDataSet.get("foreign_currency_translation_difference") and foreign_currency_translation_difference:
                                saveSet["foreign_currency_translation_difference"] = foreign_currency_translation_difference

                            if other != currDataSet.get("other") and other:
                                saveSet["other"] = other

                            if other_comprehensive_income_attributable_to_minority != currDataSet.get("other_comprehensive_income_attributable_to_minority") and other_comprehensive_income_attributable_to_minority:
                                saveSet["other_comprehensive_income_attributable_to_minority"] = other_comprehensive_income_attributable_to_minority

                            if total_comprehensive_income != currDataSet.get("total_comprehensive_income") and total_comprehensive_income:
                                saveSet["total_comprehensive_income"] = total_comprehensive_income

                            if total_comprehensive_income_attributable_to_parent != currDataSet.get("total_comprehensive_income_attributable_to_parent") and total_comprehensive_income_attributable_to_parent:
                                saveSet["total_comprehensive_income_attributable_to_parent"] = total_comprehensive_income_attributable_to_parent

                            if total_comprehensive_income_attributable_to_minority != currDataSet.get("total_comprehensive_income_attributable_to_minority") and total_comprehensive_income_attributable_to_minority:
                                saveSet["total_comprehensive_income_attributable_to_minority"] = total_comprehensive_income_attributable_to_minority

                            if basic_earnings_per_share != currDataSet.get("basic_earnings_per_share") and basic_earnings_per_share:
                                saveSet["basic_earnings_per_share"] = basic_earnings_per_share

                            if diluted_earnings_per_share != currDataSet.get("diluted_earnings_per_share") and diluted_earnings_per_share:
                                saveSet["diluted_earnings_per_share"] = diluted_earnings_per_share

                            if label1 != currDataSet.get("label1") and label1:
                                saveSet["label1"] = label1

                            if label2 != currDataSet.get("label2") and label2:
                                saveSet["label2"] = label2

                            if label3 != currDataSet.get("label3") and label3:
                                saveSet["label3"] = label3

                            if memo != currDataSet.get("memo") and memo:
                                saveSet["memo"] = memo

                            if dispFlag != currDataSet.get("dispFlag") and dispFlag:
                                saveSet["dispFlag"] = dispFlag

                            if delFlag != currDataSet.get("delFlag") and delFlag:
                                saveSet["delFlag"] = delFlag

                            if saveSet:
                                #saveSet["delFlag"] = "0"
                                saveSet["modifyID"] = loginID
                                saveSet["modifyYMDHMS"] = misc.getTime()

                                #保存数据
                                tableName = comMysql.tablename_convertor_cash_flow_statements()
                                rtn = comMysql.update_cash_flow_statements(tableName,id,saveSet)
                                rtnData["rtn"] = str(rtn)

                                if rtn < 0:
                                    _LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")
                                else:
                                    if _DEBUG:
                                        pass
                                        _LOG.info(f"D: rtn:{rtn}")

                                result = rtnData

                        else:
                            #BT
                            errCode = "BT"

                    else:
                        #CB
                        errCode = "CB"

                else:
                    #data invalid
                    errCode = "BA"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#cash flow查询代码
def funcCashFlowQry(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查

            if errCode == "B0": #
                #获取查询输入参数
                id = dataSet.get("id", "")

                #houseID = dataSet.get("houseID", "")
                symbol = dataSet.get("symbol", "")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code", "")

                report_date = dataSet.get("report_date", "")

                forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记

                searchOption = dataSet.get("searchOption")

                mode = dataSet.get("mode", "full")

                #limitNum = dataSet.get("limitNum",0)

                #权限检查/功能检测

                rightCheckFlag = True

                if rightCheckFlag:

                    #生成indexKey
                    indexKeyDataSet = {} #查询生成index的因素
                    if id:
                        indexKeyDataSet["id"] = id
                    if stock_code:
                        indexKeyDataSet["stock_code"] = stock_code
                    if report_date:
                        indexKeyDataSet["report_date"] = report_date
                    if searchOption:
                        indexKeyDataSet["searchOption"] = searchOption
                    if mode:
                        indexKeyDataSet["mode"] = mode

                    #if limitNum:
                        #indexKeyDataSet["limitNum"] = mode

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) 
                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) 
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) 

                    #判断数据是否在缓冲区:
                    if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:

                        if searchOption:
                            currDataList = []
                            tableName = comMysql.tablename_convertor_cash_flow_statements()
                            allDataList = comMysql.query_cash_flow_statements(tableName,mode = mode)
                            allowList = ["description", "label"] #筛选字段
                            serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)
                            if serachResultSet["rtn"] == "B0":
                                currDataList = serachResultSet.get("data", [])
                        else:
                            if id:
                                tableName = comMysql.tablename_convertor_cash_flow_statements()
                                currDataList = comMysql.query_cash_flow_statements(tableName,id,mode = mode)
                            else:
                                tableName = comMysql.tablename_convertor_cash_flow_statements()
                                currDataList = comMysql.query_cash_flow_statements(tableName,stock_code=stock_code,report_date=report_date)

                        dataList = []

                        for currDataSet in currDataList:
                            aSet = {}

                            #需要把文件转移到public domain
                            #appendixFileID00 =  currDataSet.get("appendixFileID00", "")
                            #appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)

                            #if mode == "full":
                                #aSet["houseID"] = currDataSet.get("houseID", "")

                            aSet["id"] = currDataSet.get("id","")
                            aSet["stock_code"] = currDataSet.get("stock_code","")
                            aSet["report_date"] = currDataSet.get("report_date","")
                            aSet["total_operating_revenue"] = currDataSet.get("total_operating_revenue","")
                            aSet["operating_revenue"] = currDataSet.get("operating_revenue","")
                            aSet["interest_income"] = currDataSet.get("interest_income","")
                            aSet["earned_premiums"] = currDataSet.get("earned_premiums","")
                            aSet["fees_and_commissions_income"] = currDataSet.get("fees_and_commissions_income","")
                            aSet["real_estate_sales_revenue"] = currDataSet.get("real_estate_sales_revenue","")
                            aSet["other_business_revenue"] = currDataSet.get("other_business_revenue","")
                            aSet["total_operating_costs"] = currDataSet.get("total_operating_costs","")
                            aSet["operating_costs"] = currDataSet.get("operating_costs","")
                            aSet["fees_and_commissions_expenses"] = currDataSet.get("fees_and_commissions_expenses","")
                            aSet["real_estate_sales_costs"] = currDataSet.get("real_estate_sales_costs","")
                            aSet["surrender_value"] = currDataSet.get("surrender_value","")
                            aSet["net_claims_paid"] = currDataSet.get("net_claims_paid","")
                            aSet["net_insurance_contract_reserves"] = currDataSet.get("net_insurance_contract_reserves","")
                            aSet["policy_dividend_expenses"] = currDataSet.get("policy_dividend_expenses","")
                            aSet["reinsurance_expenses"] = currDataSet.get("reinsurance_expenses","")
                            aSet["other_business_costs"] = currDataSet.get("other_business_costs","")
                            aSet["taxes_and_surcharges"] = currDataSet.get("taxes_and_surcharges","")
                            aSet["rd_expenses"] = currDataSet.get("rd_expenses","")
                            aSet["selling_expenses"] = currDataSet.get("selling_expenses","")
                            aSet["administrative_expenses"] = currDataSet.get("administrative_expenses","")
                            aSet["financial_expenses"] = currDataSet.get("financial_expenses","")
                            aSet["interest_expenses"] = currDataSet.get("interest_expenses","")
                            aSet["interest_expenditure"] = currDataSet.get("interest_expenditure","")
                            aSet["investment_income"] = currDataSet.get("investment_income","")
                            aSet["investment_income_from_associates_and_joint_ventures"] = currDataSet.get("investment_income_from_associates_and_joint_ventures","")
                            aSet["gain_on_derecognition_of_financial_assets_at_amortized_cost"] = currDataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost","")
                            aSet["foreign_exchange_gains"] = currDataSet.get("foreign_exchange_gains","")
                            aSet["net_open_hedge_gains"] = currDataSet.get("net_open_hedge_gains","")
                            aSet["fair_value_change_gains"] = currDataSet.get("fair_value_change_gains","")
                            aSet["futures_gains_losses"] = currDataSet.get("futures_gains_losses","")
                            aSet["custody_income"] = currDataSet.get("custody_income","")
                            aSet["subsidy_income"] = currDataSet.get("subsidy_income","")
                            aSet["other_gains"] = currDataSet.get("other_gains","")
                            aSet["asset_impairment_losses"] = currDataSet.get("asset_impairment_losses","")
                            aSet["credit_impairment_losses"] = currDataSet.get("credit_impairment_losses","")
                            aSet["other_business_profits"] = currDataSet.get("other_business_profits","")
                            aSet["asset_disposal_gains"] = currDataSet.get("asset_disposal_gains","")
                            aSet["operating_profit"] = currDataSet.get("operating_profit","")
                            aSet["non_operating_income"] = currDataSet.get("non_operating_income","")
                            aSet["non_current_asset_disposal_gains"] = currDataSet.get("non_current_asset_disposal_gains","")
                            aSet["non_operating_expenses"] = currDataSet.get("non_operating_expenses","")
                            aSet["non_current_asset_disposal_losses"] = currDataSet.get("non_current_asset_disposal_losses","")
                            aSet["total_profit"] = currDataSet.get("total_profit","")
                            aSet["income_tax_expense"] = currDataSet.get("income_tax_expense","")
                            aSet["unrecognized_investment_losses"] = currDataSet.get("unrecognized_investment_losses","")
                            aSet["net_profit"] = currDataSet.get("net_profit","")
                            aSet["net_profit_from_continuing_operations"] = currDataSet.get("net_profit_from_continuing_operations","")
                            aSet["net_profit_from_discontinued_operations"] = currDataSet.get("net_profit_from_discontinued_operations","")
                            aSet["net_profit_attributable_to_parent_company"] = currDataSet.get("net_profit_attributable_to_parent_company","")
                            aSet["net_profit_of_acquiree_before_merger"] = currDataSet.get("net_profit_of_acquiree_before_merger","")
                            aSet["minority_interests_profit_loss"] = currDataSet.get("minority_interests_profit_loss","")
                            aSet["other_comprehensive_income"] = currDataSet.get("other_comprehensive_income","")
                            aSet["other_comprehensive_income_attributable_to_parent"] = currDataSet.get("other_comprehensive_income_attributable_to_parent","")
                            aSet["oci_not_reclassified_to_profit_loss"] = currDataSet.get("oci_not_reclassified_to_profit_loss","")
                            aSet["remeasurement_of_defined_benefit_plans"] = currDataSet.get("remeasurement_of_defined_benefit_plans","")
                            aSet["oci_under_equity_method_not_reclassified"] = currDataSet.get("oci_under_equity_method_not_reclassified","")
                            aSet["fair_value_change_of_other_equity_instruments"] = currDataSet.get("fair_value_change_of_other_equity_instruments","")
                            aSet["fair_value_change_of_own_credit_risk"] = currDataSet.get("fair_value_change_of_own_credit_risk","")
                            aSet["oci_reclassified_to_profit_loss"] = currDataSet.get("oci_reclassified_to_profit_loss","")
                            aSet["oci_under_equity_method_reclassified"] = currDataSet.get("oci_under_equity_method_reclassified","")
                            aSet["fair_value_change_of_afs_financial_assets"] = currDataSet.get("fair_value_change_of_afs_financial_assets","")
                            aSet["fair_value_change_of_other_debt_investments"] = currDataSet.get("fair_value_change_of_other_debt_investments","")
                            aSet["financial_assets_reclassified_to_oci"] = currDataSet.get("financial_assets_reclassified_to_oci","")
                            aSet["credit_impairment_of_other_debt_investments"] = currDataSet.get("credit_impairment_of_other_debt_investments","")
                            aSet["htm_reclassified_to_afs_gains_losses"] = currDataSet.get("htm_reclassified_to_afs_gains_losses","")
                            aSet["cash_flow_hedge_reserve"] = currDataSet.get("cash_flow_hedge_reserve","")
                            aSet["effective_portion_of_cash_flow_hedge"] = currDataSet.get("effective_portion_of_cash_flow_hedge","")
                            aSet["foreign_currency_translation_difference"] = currDataSet.get("foreign_currency_translation_difference","")
                            aSet["other"] = currDataSet.get("other","")
                            aSet["other_comprehensive_income_attributable_to_minority"] = currDataSet.get("other_comprehensive_income_attributable_to_minority","")
                            aSet["total_comprehensive_income"] = currDataSet.get("total_comprehensive_income","")
                            aSet["total_comprehensive_income_attributable_to_parent"] = currDataSet.get("total_comprehensive_income_attributable_to_parent","")
                            aSet["total_comprehensive_income_attributable_to_minority"] = currDataSet.get("total_comprehensive_income_attributable_to_minority","")
                            aSet["basic_earnings_per_share"] = currDataSet.get("basic_earnings_per_share","")
                            aSet["diluted_earnings_per_share"] = currDataSet.get("diluted_earnings_per_share","")
                            aSet["label1"] = currDataSet.get("label1","")
                            aSet["label2"] = currDataSet.get("label2","")
                            aSet["label3"] = currDataSet.get("label3","")
                            aSet["memo"] = currDataSet.get("memo","")
                            aSet["regID"] = currDataSet.get("regID","")
                            aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                            aSet["modifyID"] = currDataSet.get("modifyID","")
                            aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                            aSet["dispFlag"] = currDataSet.get("dispFlag","")
                            aSet["delFlag"] = currDataSet.get("delFlag","")

                            dataList.append(aSet)

                        #临时缓存机制,改进型, 2023/10/16
                        indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去

                    rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)

                    #rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "BT"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#indicator增加代码
def funcIndicatorAdd(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                #data validation check
                indicator_name = dataSet.get("indicator_name", "")
                if indicator_name:
                    dataValidFlag = True
                else:
                    dataValidFlag = False
                if dataValidFlag:
                    saveSet = {}
                    saveSet["indicator_name"] = indicator_name 
                    saveSet["report_date"] = dataSet.get("report_date", "") 
                    saveSet["median_value"] = dataSet.get("median_value", "") 
                    saveSet["cache_version"] = dataSet.get("cache_version", "") 
                    saveSet["label1"] = dataSet.get("label1", "") 
                    saveSet["label2"] = dataSet.get("label2", "") 
                    saveSet["label3"] = dataSet.get("label3", "") 
                    saveSet["memo"] = dataSet.get("memo", "") 
                    saveSet["dispFlag"] = dataSet.get("dispFlag", "") 
                    saveSet["delFlag"] = dataSet.get("delFlag", "0") 
                    saveSet["regID"] = loginID
                    saveSet["regYMDHMS"] = misc.getTime()

                    tableName = comMysql.tablename_convertor_indicator_medians()
                    recID = comMysql.insert_indicator_medians(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},saveSet:{saveSet}")
                    else:
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: recID:{recID}")

                    result = rtnData

                else:
                    #data invalid
                    errCode = "BA"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#indicator删除代码
def funcIndicatorDel(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                id = dataSet.get("id", "")
                tableName = comMysql.tablename_convertor_indicator_medians()
                currDataList = comMysql.query_indicator_medians(tableName,id)
                if len(currDataList) == 1:
                    saveSet = {}
                    saveSet["modifyID"] = loginID
                    saveSet["modifyYMDHMS"] = misc.getTime()
                    #saveSet["delFlag"] = "1"

                    rtn = comMysql.delete_indicator_medians(tableName,id)
                    rtnData["rtn"] = str(rtn)

                    if _DEBUG:
                        _LOG.info(f"D: rtn:{rtn}")

                    result = rtnData

                else:
                    errCode = "CB"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#indicator修改代码
def funcIndicatorModify(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                id = dataSet.get("id") 
                indicator_name = dataSet.get("indicator_name") 
                report_date = dataSet.get("report_date") 
                median_value = dataSet.get("median_value") 
                cache_version = dataSet.get("cache_version") 
                label1 = dataSet.get("label1") 
                label2 = dataSet.get("label2") 
                label3 = dataSet.get("label3") 
                memo = dataSet.get("memo") 
                dispFlag = dataSet.get("dispFlag") 
                delFlag = dataSet.get("delFlag") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    recID = dataSet.get("id", "")

                    tableName = comMysql.tablename_convertor_indicator_medians()
                    currDataList = comMysql.query_indicator_medians(tableName,recID)

                    if len(currDataList) == 1:
                        currDataSet = currDataList[0]

                        #权限或其他检查
                        if errCode == "B0": #

                            saveSet = {}

                            if indicator_name != currDataSet.get("indicator_name") and indicator_name:
                                saveSet["indicator_name"] = indicator_name

                            if report_date != currDataSet.get("report_date") and report_date:
                                saveSet["report_date"] = report_date

                            if median_value != currDataSet.get("median_value") and median_value:
                                saveSet["median_value"] = median_value

                            if cache_version != currDataSet.get("cache_version") and cache_version:
                                saveSet["cache_version"] = cache_version

                            if label1 != currDataSet.get("label1") and label1:
                                saveSet["label1"] = label1

                            if label2 != currDataSet.get("label2") and label2:
                                saveSet["label2"] = label2

                            if label3 != currDataSet.get("label3") and label3:
                                saveSet["label3"] = label3

                            if memo != currDataSet.get("memo") and memo:
                                saveSet["memo"] = memo

                            if dispFlag != currDataSet.get("dispFlag") and dispFlag:
                                saveSet["dispFlag"] = dispFlag

                            if delFlag != currDataSet.get("delFlag") and delFlag:
                                saveSet["delFlag"] = delFlag


                            if saveSet:
                                #saveSet["delFlag"] = "0"
                                saveSet["modifyID"] = loginID
                                saveSet["modifyYMDHMS"] = misc.getTime()

                                #保存数据
                                tableName = comMysql.tablename_convertor_indicator_medians()
                                rtn = comMysql.update_indicator_medians(tableName,id,saveSet)
                                rtnData["rtn"] = str(rtn)

                                if rtn < 0:
                                    _LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")
                                else:
                                    if _DEBUG:
                                        pass
                                        _LOG.info(f"D: rtn:{rtn}")

                                result = rtnData

                        else:
                            #BT
                            errCode = "BT"

                    else:
                        #CB
                        errCode = "CB"

                else:
                    #data invalid
                    errCode = "BA"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#indicator查询代码
def funcIndicatorQry(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查

            if errCode == "B0": #
                #获取查询输入参数
                id = dataSet.get("id", "")

                #houseID = dataSet.get("houseID", "")
                indicator_name = dataSet.get("indicator_name", "")
                report_date = dataSet.get("report_date", "")

                forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记

                searchOption = dataSet.get("searchOption")

                mode = dataSet.get("mode", "full")

                #limitNum = dataSet.get("limitNum",0)

                #权限检查/功能检测

                rightCheckFlag = True

                if rightCheckFlag:

                    #生成indexKey
                    indexKeyDataSet = {} #查询生成index的因素
                    if id:
                        indexKeyDataSet["id"] = id
                    if indicator_name:
                        indexKeyDataSet["indicator_name"] = indicator_name
                    if report_date:
                        indexKeyDataSet["report_date"] = report_date
                    if searchOption:
                        indexKeyDataSet["searchOption"] = searchOption
                    if mode:
                        indexKeyDataSet["mode"] = mode

                    #if limitNum:
                        #indexKeyDataSet["limitNum"] = mode

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) 
                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) 
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) 

                    #判断数据是否在缓冲区:
                    if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:

                        if searchOption:
                            currDataList = []
                            tableName = comMysql.tablename_convertor_indicator_medians()
                            allDataList = comMysql.query_indicator_medians(tableName,mode = mode)
                            allowList = ["description", "label"] #筛选字段
                            serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)
                            if serachResultSet["rtn"] == "B0":
                                currDataList = serachResultSet.get("data", [])
                        else:
                            if id:
                                tableName = comMysql.tablename_convertor_indicator_medians()
                                currDataList = comMysql.query_indicator_medians(tableName,id,mode = mode)
                            else:
                                tableName = comMysql.tablename_convertor_indicator_medians()
                                currDataList = comMysql.query_indicator_medians(tableName,indicator_name=indicator_name,report_date=report_date,mode = mode)

                        dataList = []

                        for currDataSet in currDataList:
                            aSet = {}

                            #需要把文件转移到public domain
                            #appendixFileID00 =  currDataSet.get("appendixFileID00", "")
                            #appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)

                            #if mode == "full":
                                #aSet["houseID"] = currDataSet.get("houseID", "")

                            aSet["id"] = currDataSet.get("id","")
                            aSet["indicator_name"] = currDataSet.get("indicator_name","")
                            aSet["report_date"] = currDataSet.get("report_date","")
                            aSet["median_value"] = currDataSet.get("median_value","")
                            aSet["cache_version"] = currDataSet.get("cache_version","")
                            aSet["label1"] = currDataSet.get("label1","")
                            aSet["label2"] = currDataSet.get("label2","")
                            aSet["label3"] = currDataSet.get("label3","")
                            aSet["memo"] = currDataSet.get("memo","")
                            aSet["regID"] = currDataSet.get("regID","")
                            aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                            aSet["modifyID"] = currDataSet.get("modifyID","")
                            aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                            aSet["dispFlag"] = currDataSet.get("dispFlag","")
                            aSet["delFlag"] = currDataSet.get("delFlag","")

                            dataList.append(aSet)

                        #临时缓存机制,改进型, 2023/10/16
                        indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去

                    rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)

                    #rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "BT"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#用户股票列表增加代码
def funcUserStockListAdd(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:
        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                #data validation check
                symbol = dataSet.get("symbol", "")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code", "") 
                if stock_code:
                    dataValidFlag = True
                else:
                    dataValiFlag = False
                if dataValidFlag:
                    saveSet = {}
                    saveSet["loginID"] = loginID
                    saveSet["username"] = dataSet.get("username", "") 
                    saveSet["stock_code"] = stock_code
                    saveSet["stock_name"] = dataSet.get("stock_name", "") 
                    saveSet["initial_weight"] = dataSet.get("initial_weight", "") 
                    saveSet["current_weight"] = dataSet.get("current_weight", "") 
                    saveSet["initial_cap"] = dataSet.get("initial_cap", "") 
                    saveSet["current_cap"] = dataSet.get("current_cap", "") 
                    saveSet["label1"] = dataSet.get("label1", "") 
                    saveSet["label2"] = dataSet.get("label2", "") 
                    saveSet["label3"] = dataSet.get("label3", "") 
                    saveSet["memo"] = dataSet.get("memo", "") 
                    saveSet["dispFlag"] = dataSet.get("dispFlag", "") 
                    saveSet["delFlag"] = dataSet.get("delFlag", "0") 
                    saveSet["regID"] = loginID
                    saveSet["regYMDHMS"] = misc.getTime()

                    tableName = comMysql.tablename_convertor_user_stock_list()
                    recID = comMysql.insert_user_stock_list(tableName,saveSet)
                    rtnData["recID"] = str(recID)

                    if recID <= 0:
                        #记录添加失败
                        errCode = "CG"
                        _LOG.warning(f"rtn:{recID},saveSet:{saveSet}")
                    else:
                        if _DEBUG:
                            pass
                            _LOG.info(f"D: recID:{recID}")

                    result = rtnData

                else:
                    #data invalid
                    errCode = "BA"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#用户股票列表删除代码
def funcUserStockListDel(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID
            #权限检查

            if errCode == "B0": #
                id = dataSet.get("id", "")
                tableName = comMysql.tablename_convertor_user_stock_list()
                currDataList = comMysql.query_user_stock_list(tableName,id)
                if len(currDataList) == 1:
                    saveSet = {}
                    saveSet["modifyID"] = loginID
                    saveSet["modifyYMDHMS"] = misc.getTime()
                    #saveSet["delFlag"] = "1"

                    rtn = comMysql.delete_user_stock_list(tableName,id)
                    rtnData["rtn"] = str(rtn)

                    if _DEBUG:
                        _LOG.info(f"D: rtn:{rtn}")

                    result = rtnData

                else:
                    errCode = "CB"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#用户股票列表修改代码
def funcUserStockListModify(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查/功能检测

            if errCode == "B0": #
                #data validation check
                dataValidFlag = True

                # loginID = loginID 
                username = dataSet.get("username") 
                stock_code = dataSet.get("stock_code") 
                stock_name = dataSet.get("stock_name") 
                initial_weight = dataSet.get("initial_weight") 
                current_weight = dataSet.get("current_weight") 
                initial_cap = dataSet.get("initial_cap") 
                current_cap = dataSet.get("current_cap") 
                label1 = dataSet.get("label1") 
                label2 = dataSet.get("label2") 
                label3 = dataSet.get("label3") 
                memo = dataSet.get("memo") 
                dispFlag = dataSet.get("dispFlag") 
                delFlag = dataSet.get("delFlag") 
                #data valid 检查

                if dataValidFlag:
                    #当前记录获取
                    recID = dataSet.get("id", "")

                    tableName = comMysql.tablename_convertor_user_stock_list()
                    currDataList = comMysql.query_user_stock_list(tableName,recID)

                    if len(currDataList) == 1:
                        currDataSet = currDataList[0]

                        #权限或其他检查
                        if errCode == "B0": #

                            saveSet = {}

                            # if loginID != currDataSet.get("loginID") and loginID:
                            #     saveSet["loginID"] = loginID

                            if username != currDataSet.get("username") and username:
                                saveSet["username"] = username

                            if stock_code != currDataSet.get("stock_code") and stock_code:
                                saveSet["stock_code"] = stock_code

                            if stock_name != currDataSet.get("stock_name") and stock_name:
                                saveSet["stock_name"] = stock_name

                            if initial_weight != currDataSet.get("initial_weight") and initial_weight:
                                saveSet["initial_weight"] = initial_weight

                            if current_weight != currDataSet.get("current_weight") and current_weight:
                                saveSet["current_weight"] = current_weight

                            if initial_cap != currDataSet.get("initial_cap") and initial_cap:
                                saveSet["initial_cap"] = initial_cap

                            if current_cap != currDataSet.get("current_cap") and current_cap:
                                saveSet["current_cap"] = current_cap

                            if label1 != currDataSet.get("label1") and label1:
                                saveSet["label1"] = label1

                            if label2 != currDataSet.get("label2") and label2:
                                saveSet["label2"] = label2

                            if label3 != currDataSet.get("label3") and label3:
                                saveSet["label3"] = label3

                            if memo != currDataSet.get("memo") and memo:
                                saveSet["memo"] = memo

                            if regID != currDataSet.get("regID") and regID:
                                saveSet["regID"] = regID

                            if regYMDHMS != currDataSet.get("regYMDHMS") and regYMDHMS:
                                saveSet["regYMDHMS"] = regYMDHMS

                            if modifyID != currDataSet.get("modifyID") and modifyID:
                                saveSet["modifyID"] = modifyID

                            if modifyYMDHMS != currDataSet.get("modifyYMDHMS") and modifyYMDHMS:
                                saveSet["modifyYMDHMS"] = modifyYMDHMS

                            if dispFlag != currDataSet.get("dispFlag") and dispFlag:
                                saveSet["dispFlag"] = dispFlag

                            if delFlag != currDataSet.get("delFlag") and delFlag:
                                saveSet["delFlag"] = delFlag


                            if saveSet:
                                #saveSet["delFlag"] = "0"
                                saveSet["modifyID"] = loginID
                                saveSet["modifyYMDHMS"] = misc.getTime()

                                #保存数据
                                tableName = comMysql.tablename_convertor_user_stock_list()
                                rtn = comMysql.update_user_stock_list(tableName,id,saveSet)
                                rtnData["rtn"] = str(rtn)

                                if rtn < 0:
                                    _LOG.warning(f"D: rtn:{rtn},saveSet:{saveSet}")
                                else:
                                    if _DEBUG:
                                        pass
                                        _LOG.info(f"D: rtn:{rtn}")

                                result = rtnData

                        else:
                            #BT
                            errCode = "BT"

                    else:
                        #CB
                        errCode = "CB"

                else:
                    #data invalid
                    errCode = "BA"


        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#用户股票列表查询代码
def funcUserStockListQry(CMD,dataSet,sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD
    rtnField = ""
    rtnData = {}

    dataValidFlag = True #数据是否有效的标志
    rtnErrMsgList = [] #数据错误原因

    try:

        lang = dataSet.get("lang", comGD._DEF_DEFAULT_LANGUAGE)
        msgKey = "applicationMsgKey"
        openID = sessionIDSet.get("openID", "")
        roleName = sessionIDSet.get("roleName", "")
        tempUserID = sessionIDSet.get("loginID", "")

        if tempUserID != "":
            loginID = tempUserID

            #权限检查

            if errCode == "B0": #
                #获取查询输入参数
                id = dataSet.get("id", "")

                #houseID = dataSet.get("houseID", "")
                symbol = dataSet.get("symbol","")
                if symbol:
                    stock_code = symbol
                else:
                    stock_code = dataSet.get("stock_code", "") 

                forceFlashFlag = dataSet.get("forceFlashFlag",comGD._CONST_NO) #是否强制查询(刷新)标记

                searchOption = dataSet.get("searchOption")

                mode = dataSet.get("mode", "full")

                # limitNum = dataSet.get("limitNum",0)

                #权限检查/功能检测

                rightCheckFlag = True

                if rightCheckFlag:

                    #生成indexKey
                    indexKeyDataSet = {} #查询生成index的因素
                    if id:
                        indexKeyDataSet["id"] = id
                    if loginID:
                        indexKeyDataSet["loginID"] = loginID
                    if stock_code:
                        indexKeyDataSet["stock_code"] = stock_code
                    if searchOption:
                        indexKeyDataSet["searchOption"] = searchOption
                    if mode:
                        indexKeyDataSet["mode"] = mode

                    # if limitNum:
                    #     indexKeyDataSet["limitNum"] = mode

                    sessionID = sessionIDSet.get("sessionID", "")
                    indexKey = genBufferIndexKey(CMD, sessionID, indexKeyDataSet) 
                    beginNum = int(dataSet.get("beginNum", comGD._DEF_BUFFER_DATA_BEGIN_NUM)) 
                    endNum = int(dataSet.get("endNum", comGD._DEF_BUFFER_DATA_END_NUM)) 

                    #判断数据是否在缓冲区:
                    if not(useQueryBufferFlag and chkBufferExist(indexKey)) or forceFlashFlag == comGD._CONST_YES:

                        if searchOption:
                            currDataList = []
                            tableName = comMysql.tablename_convertor_user_stock_list()
                            allDataList = comMysql.query_user_stock_list(tableName,mode = mode)
                            allowList = ["description", "label"] #筛选字段
                            serachResultSet = comFC.handleSearchOption(searchOption,allowList, allDataList)
                            if serachResultSet["rtn"] == "B0":
                                currDataList = serachResultSet.get("data", [])
                        else:
                            if id:
                                tableName = comMysql.tablename_convertor_user_stock_list()
                                currDataList = comMysql.query_user_stock_list(tableName,id,mode = mode)
                            else:
                                tableName = comMysql.tablename_convertor_user_stock_list()
                                currDataList = comMysql.query_user_stock_list(tableName,loginID=loginID,stock_code=stock_code)

                        dataList = []

                        for currDataSet in currDataList:
                            aSet = {}

                            #需要把文件转移到public domain
                            #appendixFileID00 =  currDataSet.get("appendixFileID00", "")
                            #appendixFileID00 = getTempLocation(appendixFileID00, privateFlag = True)

                            #if mode == "full":
                                #aSet["houseID"] = currDataSet.get("houseID", "")

                            aSet["id"] = currDataSet.get("id","")
                            aSet["loginID"] = currDataSet.get("loginID","")
                            aSet["username"] = currDataSet.get("username","")
                            aSet["stock_code"] = currDataSet.get("stock_code","")
                            aSet["stock_name"] = currDataSet.get("stock_name","")
                            aSet["initial_weight"] = currDataSet.get("initial_weight","")
                            aSet["current_weight"] = currDataSet.get("current_weight","")
                            aSet["initial_cap"] = currDataSet.get("initial_cap","")
                            aSet["current_cap"] = currDataSet.get("current_cap","")
                            aSet["label1"] = currDataSet.get("label1","")
                            aSet["label2"] = currDataSet.get("label2","")
                            aSet["label3"] = currDataSet.get("label3","")
                            aSet["memo"] = currDataSet.get("memo","")
                            aSet["regID"] = currDataSet.get("regID","")
                            aSet["regYMDHMS"] = currDataSet.get("regYMDHMS","")
                            aSet["modifyID"] = currDataSet.get("modifyID","")
                            aSet["modifyYMDHMS"] = currDataSet.get("modifyYMDHMS","")
                            aSet["dispFlag"] = currDataSet.get("dispFlag","")
                            aSet["delFlag"] = currDataSet.get("delFlag","")

                            dataList.append(aSet)

                        #临时缓存机制,改进型, 2023/10/16
                        indexKey = putQuery2Buffer(indexKey, dataList) #存放数据到临时缓冲区去

                    rtnData = getQueryBufferComplte(indexKey, beginNum = beginNum,  endNum = endNum)

                    #rtnData["limitNum"] = limitNum

                    result = rtnData

                else:
                    errCode = "BT"

        else:
            errCode = "B8"

        rtnCMD = CMD
        rtnSet = comFC.rtnMSG(errCode,rtnField, lang, msgKey)
        result["CMD"] = rtnCMD
        result["msgKey"] = msgKey
        result["MSG"] = rtnSet["MSG"]
        result["errCode"] = errCode
        result["MSG"]["content"] += ";"+";".join(rtnErrMsgList)

    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnSet = comFC.rtnMSG("ERR_GENERAL", "ERR_GENERAL", "")
        result = rtnSet

    return result


#application functions end


#===== main entrace ======
urlPathMap = {

    #用户是否存在
    "chkuserexist":funcChkUserExist, 
    #用户注册
    "registration":funcUserRegistration, 
    #用户增加
    "useradd":funcUserAdd, 
    #用户删除
    "userdel":funcUserDelete, 
    #用户修改
    "usermodify":funcUserModify, 
    # #用户查询
    # "usersearch":funcUserSearch, 
    #用户查询 mysql
    "usersearch":funcUserSearchMysql, 
    # #用户信息获取
    # "getuserinfo":funcGetUserInfo, 
    #用户信息获取 mysql
    "getuserinfo":funcGetUserInfoMysql, 
    #用户登录
    "login":funcUserLogin, 
    #用户注销/登出
    "logout":funcUserLogout, 
    #验证请求
    "smsrequest":funcSMSRequest, 
    #验证反馈
    "smsverify":funcSMSVerify, 
    #用户,重置passwd
    "resetpasswd":funcResetPasswd,
    # #用户,用户信息查询
    # "userinfoqry":funcUserInfoQuery,
    #用户,用户信息查询
    "userinfoqry":funcUserSearchMysql,
    #passwd合格检查
    "passwdvalidcheck":funcPasswdValidCheck,

    #用户存储数据 --  G1A0
    "usersavedata":funcUserSaveData, 
    #获取用户存储数据 --  G2A0
    "usergetdata":funcUserGetData, 
    #获取下一批数据 --  G3A0
    "generalnext":funGeneralNext, 

    #系统版本查询
    "serverversionqry":funcServerVersionQry,    

    #sw upgrade related begin
    
    #上传升级软件
    "swupload":funcSWUpload, 

    #硬件信息报告
    "hwinforeport":funcHWInfoReport,
    #获取硬件信息报告
    "gethwinfo":funcGetHWInfo,

    #sw upgrade related begin

    #application functions begin
      
    #获取运维信息
    "getomcinfo":funcGetOmcInfo,

    #stock related begin
    "industryinfoadd":funcIndustryInfoAdd,
    "industryinfodel":funcIndustryInfoDel,
    "industryinfomodify":funcIndustryInfoModify,
    "industryinfoqry":funcIndustryInfoQry,

    "stockinfoadd":funcStockInfoAdd,
    "stockinfodel":funcStockInfoDel,
    "stockinfomodify":funcStockInfoModify,
    "stockinfoqry":funcStockInfoQry,

    "stockhistoryadd":funcStockHistoryAdd,
    "stockhistorydel":funcStockHistoryDel,
    "stockhistorymodify":funcStockHistoryModify,
    "stockhistoryqry":funcStockHistoryQry,

    "stockdividendadd":funcStockDividendAdd,
    "stockdividenddel":funcStockDividendDel,
    "stockdividendmodify":funcStockDividendModify,
    "stockdividendqry":funcStockDividendQry,

    "industryhistoryadd":funcIndustryHistoryAdd,
    "industryhistorydel":funcIndustryHistoryDel,
    "industryhistorymodify":funcIndustryHistoryModify,
    "industryhistoryqry":funcIndustryHistoryQry,

    "balancesheetadd":funcBalanceSheetAdd,
    "balancesheetdel":funcBalanceSheetDel,
    "balancesheetmodify":funcBalanceSheetModify,
    "balancesheetqry":funcBalanceSheetQry,

    "incomestatementsadd":funcIncomeStatementsAdd,
    "incomestatementsdel":funcIncomeStatementsDel,
    "incomestatementsmodify":funcIncomeStatementsModify,
    "incomestatementsqry":funcIncomeStatementsQry,

    "cashflowadd":funcCashFlowAdd,
    "cashflowdel":funcCashFlowDel,
    "cashflowmodify":funcCashFlowModify,
    "cashflowqry":funcCashFlowQry,

    "indicatoradd":funcIndicatorAdd,
    "indicatordel":funcIndicatorDel,
    "indicatormodify":funcIndicatorModify,
    "indicatorqry":funcIndicatorQry,

    "userstocklistadd":funcUserStockListAdd,
    "userstocklistdel":funcUserStockListDel,
    "userstocklistmodify":funcUserStockListModify,
    "userstocklistqry":funcUserStockListQry,
    
    #stock related end

    #application functions end
}


CMDMapKeyList = []


for k, v in urlPathMap.items():
    CMDMapKeyList.append(k)


#数据格式检查
def dataFormatConvertor(dataType,  dataSet):
    result = dataSet
    if dataType == "FORM":
        formData = dataSet.get("formData", {})
        for k, v in formData.items():
            result[k] = v
        result.pop("formData")
    return result


#trust domain 检查
def dataTrustDomainCheck(dataSet):
    validDataFlag = True
    result = {} 
    for key,val in dataSet.items():
        if isinstance(val,list):
            for v in val:
                if not comFC.chkTrustDomain(v):
                    validDataFlag = False
                    break
        elif isinstance(val,dict):
            for k,v in val.items():
                if not comFC.chkTrustDomain(v):
                    validDataFlag = False
                    break
            result[key] = val
        else:
            if not comFC.chkTrustDomain(val):
                validDataFlag = False
                break
        if validDataFlag:
            result[key] = val
        else:
            _LOG.warning(f"W:not trust domain data,{key},{val}")
    return validDataFlag,result


#检查上传数据是否合规
def uploadContentCheck(dataSet):
    errCode = "B0"
    if isinstance(dataSet,list):
        for val in dataSet:
            if not comFC.uploadContentCheck(val):
                errCode = "EL"
                _LOG.warning(f"W:upload content error,{val}")
                break
    elif isinstance(dataSet,dict):
        for key,val in dataSet.items():
            if isinstance(val,list):
                for v in val:
                    if not comFC.uploadContentCheck(v):
                        errCode = "EL"
                        _LOG.warning(f"W:upload content error,{v}")
                        break
            elif isinstance(val,dict):
                for k,v in val.items():
                    if not comFC.uploadContentCheck(v):
                        errCode = "EL"
                        _LOG.warning(f"W:upload content error,{v}")
                        break
            else:
                if not comFC.uploadContentCheck(val):
                    errCode = "EL"
                    _LOG.warning(f"W:upload content error,{val}")
    else:
        if not comFC.uploadContentCheck(dataSet):
            errCode = "EL"
            _LOG.warning(f"W:upload content error,{dataSet}")

    return errCode


def calUserCMDMapKeyList(dataSet,CMDList =[]):
    sessionIDSet = {}
    errCode = "B0"

    #获取 sessionIDSet
    sessionID = dataSet.get("sessionID", "")

    requestData = {}
    requestData["CMD"] = "GAA0"
    requestData["sessionID"] = sessionID
    requestData["CMDMapKeyList"] = []

    url = ACCOUNT_SERVICE_URL
    headers = {'content-type': 'application/json'}
    
    rtnData = {}

    try:
        payload = misc.jsonDumps(requestData)
        r = requests.post(url, data = payload, headers = headers)

        if r.status_code == 200:
            rtnData = misc.jsonLoads(r.text)

    except:
        pass
    
    userErrCode = rtnData.get("errCode","B0")
    if userErrCode == "B0":
        data = rtnData.get("data",{})
        errCode = data.get("errCode","B0")
        if errCode == "B0":
            sessionIDSet = data.get("sessionIDSet",{})
            #设置默认的roleName
            roleName = sessionIDSet.get("roleName")
            if roleName not in settings.ROLE_CMD_LIST:
                sessionIDSet["roleName"] = settings.accountServiceDefaultRoleName

    #获取 userCMDMapKeyList
    CMDList += settings.NO_SESSIONID_CMD_LIST
    if sessionIDSet != {}:
        roleName = sessionIDSet.get("roleName", "")
        sessionIDSet["sessionID"] = sessionID
        aList = settings.ROLE_CMD_LIST.get(roleName, [])
        CMDList =list(set(aList).intersection(set(CMDList)))
    
    #判断权限
    userCMD = dataSet.get("CMD")
    if userCMD not in CMDList:
        errCode = "B8"

    return CMDList, sessionIDSet, errCode
    
    
#程序入口, post 调用
def post(urlPath, dataSet, IP, envSet, appType):
    global gSN
    global _LOG
    global _VERSION
    global gSourceServerAddr
        
    CMD = urlPath.lower()
    errCode = "OK"
    rtnField = ""
    rtnData = {}

    try:
        gSN += 1
        localSN = str(gSN)

        #访问的服务器地址, 用于文件系统
        _x_server_addr = envSet.get("_x_server_addr","")
        _x_server_port = envSet.get("_x_server_port","")
        _x_protocol_used = envSet.get("_x_protocol_used","")

        if _x_server_addr and _x_server_port and _x_protocol_used:
            _source_server_http = _x_protocol_used + "://" + _x_server_addr + ":" + _x_server_port
            dataSet["_source_server_http"] = _source_server_http
            gSourceServerAddr = _source_server_http
        else:
            dataSet["_source_server_http"] = ""
            gSourceServerAddr = ""

        # dataSet["_x_server_addr"] = _x_server_addr
        # dataSet["_x_server_port"] = _x_server_port
        # dataSet["_x_protocol_used"] = _x_protocol_used

        if _DEBUG:
            _LOG.info(f"R: PID: {_processorPID},IP:{IP},SN:{localSN},CMD:{CMD} '{misc.jsonDumps(dataSet)}'")
            
        # if True:
        IP = IP.strip()
        IPCheckFlag = False
        if comDB.chkIPCount(IP,checkFlag = IPCheckFlag):
            if CMD != "": 
                dataSet["CMD"] = CMD
                uploadSN = dataSet.get("SN")
                try:
                    uploadSN = int(uploadSN)
                    localSN = str(uploadSN)
                except:
                    pass

                dataType = dataSet.get("dataType", "")
                if dataType != "":
                    dataSet = dataFormatConvertor(dataType, dataSet)
                
                if CMD in settings.NO_SESSIONID_CMD_LIST:
                    userCMDMapKeyList = settings.NO_SESSIONID_CMD_LIST
                    sessionIDSet = {} # modify here
                    sessionIDSet["loginID"] = settings.accountServiceDefaultLoginID # modify here
                    sessionIDSet["roleName"] = settings.accountServiceDefaultRoleName # modify here
                    errCode = "B0"

                else:
                    userCMDMapKeyList, sessionIDSet, errCode = calUserCMDMapKeyList(dataSet,CMDMapKeyList)

                #用户被停用标记
                activeFlag = sessionIDSet.get("activeFLag",comGD._CONST_YES)
                if activeFlag == comGD._CONST_NO:
                    errCode = "BT"
                
                #信任domain check
                # validDataFlag, dataSet = dataTrustDomainCheck(dataSet)
                # if validDataFlag == False:
                #     errCode = "BA"

                if errCode == "B0":
                    #上传内容格式检查, 主要是是否含html和其他url等
                    # errCode = uploadContentCheck(dataSet)

                    if errCode == "B0":                
                        sessionIDSet["appType"] = appType
                        
                        if CMD in userCMDMapKeyList:
                            dataSet["_IP"] = IP
                            rtnData = urlPathMap[CMD](CMD, dataSet, sessionIDSet)
                            
                        else:
                            rtnData = comFC.rtnMSG("ERR_NOCMD", "ERR_NOCMD")
                    else:
                        rtnData = comFC.rtnMSG(errCode, errCode)
                        rtnData["errCode"] = errCode

                else:
                    # rtnCMD = CMD
                    rtnData = comFC.rtnMSG(errCode, errCode)
                    rtnData["errCode"] = errCode
            else:                
                rtnData = comFC.rtnMSG("ERR_NOCMD", "ERR_NOCMD")
        else:
            rtnData = comFC.rtnMSG("ERR_IPFLOOD", "ERR_IPFLOOD")

        rtnData["SN"] = localSN
        rtnData["YMDHMS"]  = misc.getTime()
        result = rtnData

        if _DEBUG:
            _LOG.info(f"S: PID: {_processorPID},IP:{IP},SN:{localSN},CMD:{CMD},data:{misc.jsonDumps(result)}")
            
    except Exception as e:
        errMsg = f"PID: {_processorPID},CMD:{CMD}, IP:{IP}, post() unknow failure, errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

        rtnData = comFC.rtnMSG("ERROR", "ERR_GENERAL")
        result = rtnData
   
    return result


if __name__ == "__main__":
    IP = "0.0.0.0"
    appType = "chief"
    appType = ""
    envSet = {"CONTENT_LENGTH":100}
    if len(sys.argv) > 1:
        pass
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
            urlPath=sys.argv[1]
            msg = sys.argv[2]
            dataSet = misc.jsonLoads(msg)
            post(urlPath, dataSet, IP, envSet, appType)
            exit(-1)
