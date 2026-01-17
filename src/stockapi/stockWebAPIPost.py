#!/usr/bin/env python3
#encoding: utf-8

#Filename: ntnWebAPIPost.py 
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com/xie_frank@163.com
#Date: 2023-06-29
#Description:  stock web api


_VERSION="20260105"


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

    #accountServiceSyncSessionID 仅供内部使用, 无法在应用使用
    if sessionID == settings.accountServiceSyncSessionID:
        errCode = "B8"

    else:

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
