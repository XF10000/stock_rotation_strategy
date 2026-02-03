#! /usr/bin/env python3
#encoding: utf-8

#Filename: transferStockMysql.py 
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com  
#Date: 2022-08-29
#Description:   服务器侧数据存储Mysql部分

_VERSION = "20260131"

_DEBUG=True

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import traceback
import requests
import pathlib
import copy

#global defintion/common var etc.
from common import globalDefinition as comGD

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#common functions(database operation)
from common import redisCommon as comDB

from common import mysqlCommon as comMysql

# from common import whooshCommon as comSearch

# from common import aliyunOSS as OSS
# from common import tencentCOS as COS

#common functions(funct operation)
from common import funcCommon as comFC

# from common import aliyunSMS as SMS

#from common import codingDecoding as comCD

#setting files
# from config import settings as settings
from config import basicSettings as settings


_processorPID = os.getpid()

if "_LOG" not in dir() or not _LOG:
    _LOG = misc.setLogNew("TRANS", comGD._DEF_LOG_TRANS_MYSQL_LOG)

systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
_LOG.info(f"PID:{_processorPID}, python version:{systemVersion}, main code version:{_VERSION}")


# comSearchWhooshFlag = settings.comSearchWhooshFlag

_SYS_SERVER_NAME = settings._SYS_SERVER_NAME

FILE_SYSTEM_MODE = settings.FILE_SYSTEM_MODE
FASTDFS_SERVER_PATH = settings.FASTDFS_SERVER_PATH
LOCAL_FILE_SERVER_PATH = settings.LOCAL_FILE_SERVER_PATH
LOCAL_FILE_SERVER_BASE = settings.LOCAL_FILE_SERVER_BASE
LOCAL_FILE_TEMP_WEB_DIR = 'web/'

ACCOUNT_SERVICE_URL = settings.ACCOUNT_SERVICE_URL

REGISTRATION_NOTIFICATION_USER_LIST = settings.REGISTRATION_NOTIFICATION_USER_LIST


# command part begin

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


#copy file to private network, and change the url
def save2newLocation(fileID,  objectName=None, requestType = "", prefix = "", privateFlag = False, compressFlag = comGD._CONST_YES):
    result = fileID
    
    if requestType == comGD._DEF_FILE_REQUEST_TYPE_THUMBNAIL:
        delFlag = comGD._CONST_NO
    else:
#        delFlag = comGD._CONST_YES
        delFlag = comGD._CONST_NO
        
    if FILE_SYSTEM_MODE == "FASTDFS":
        #already save to correct position 
        if delFlag:
            comDB.delFileInfo(fileID)

    elif FILE_SYSTEM_MODE == "ALIOSS":
        fileInfoData = comDB.getFileInfo(fileID)
        if fileInfoData == {}:
            #save file and send to /upload
            fileID = urlSaveFileUpload(fileID)

        fileInfoData = comDB.getFileInfo(fileID)
        if _DEBUG:
            _LOG.info(f"DEBUG: save2newLocation <step 1> fileID:{fileID},requestType:{requestType},fileInfoData:{fileInfoData},privateFlag:{privateFlag}")
        if fileInfoData != {}:
            if delFlag == comGD._CONST_YES:
                comDB.delFileInfo(fileID)
            serverName = fileInfoData.get("serverName")
            fileInfoData["CMD"] = "F0A0" #阿里云oss请求
            fileInfoData["fileSystem"] = "ALIOSS" #阿里云oss请求
            if objectName:
                fileInfoData["objectName"] = objectName
            fileInfoData["requestType"] = requestType
            fileInfoData["prefix"] = prefix
            fileInfoData["delFlag"] = delFlag
            fileInfoData["privateFlag"] = privateFlag
            fileInfoData["compressFlag"] = compressFlag
            fileInfoData["YMDHMS"] = misc.getTime()
            fileInfoData["token"] = comFC.genDigest(settings.GEN_DIGIST_KEY, fileInfoData["CMD"], fileInfoData["YMDHMS"])
            rtnSet = comFC.fileServerRequest(serverName, fileInfoData)
            if rtnSet.get("CMD")[2:4] == "B0":
                result = rtnSet.get("fileUrl", "")
        else:
            _LOG.warning(f"DEBUG: save2newLocation <step 2> fileID:{fileID},requestType:{requestType},fileInfoData:{fileInfoData}")

    elif FILE_SYSTEM_MODE == "TENCENT":
        fileInfoData = comDB.getFileInfo(fileID)
        if fileInfoData == {}:
            #save file and send to /upload
            fileID = urlSaveFileUpload(fileID)

        fileInfoData = comDB.getFileInfo(fileID)
        if _DEBUG:
            _LOG.info(f"DEBUG: save2newLocation <step 1> fileID:{fileID},requestType:{requestType},fileInfoData:{fileInfoData},privateFlag:{privateFlag}")
        if fileInfoData != {}:
            if delFlag == comGD._CONST_YES:
                comDB.delFileInfo(fileID)
            serverName = fileInfoData.get("serverName")
            fileInfoData["CMD"] = "F0A0" #腾讯云cos请求
            fileInfoData["fileSystem"] = "TENCENT" #腾讯云cos请求
            if objectName:
                fileInfoData["objectName"] = objectName
            fileInfoData["requestType"] = requestType
            fileInfoData["prefix"] = prefix
            fileInfoData["delFlag"] = delFlag
            fileInfoData["privateFlag"] = privateFlag
            fileInfoData["compressFlag"] = compressFlag
            fileInfoData["YMDHMS"] = misc.getTime()
            fileInfoData["token"] = comFC.genDigest(settings.GEN_DIGIST_KEY, fileInfoData["CMD"], fileInfoData["YMDHMS"])
            rtnSet = comFC.fileServerRequest(serverName, fileInfoData)
            if rtnSet.get("CMD")[2:4] == "B0":
                result = rtnSet.get("fileUrl", "")
        else:
            _LOG.warning(f"DEBUG: save2newLocation <step 2> fileID:{fileID},requestType:{requestType},fileInfoData:{fileInfoData}")

    elif FILE_SYSTEM_MODE == "NGINX":
        fileInfoData = comDB.getFileInfo(fileID)
        if fileInfoData == {}:
            #save file and send to /upload
            fileID = urlSaveFileUpload(fileID)

        fileInfoData = comDB.getFileInfo(fileID)
        if _DEBUG:
            _LOG.info(f"DEBUG: save2newLocation <step 1> fileID:{fileID},requestType:{requestType},fileInfoData:{fileInfoData},privateFlag:{privateFlag}")
        if fileInfoData != {}:
            if delFlag == comGD._CONST_YES:
                comDB.delFileInfo(fileID)
            serverName = fileInfoData.get("serverName")
            fileInfoData["CMD"] = "F0A0" #
            fileInfoData["fileSystem"] = "NGINX" #
            if objectName:
                fileInfoData["objectName"] = objectName
            fileInfoData["requestType"] = requestType
            fileInfoData["prefix"] = prefix
            fileInfoData["delFlag"] = delFlag
            fileInfoData["YMDHMS"] = misc.getTime()
            fileInfoData["token"] = comFC.genDigest(settings.GEN_DIGIST_KEY, fileInfoData["CMD"], fileInfoData["YMDHMS"])
            rtnSet = comFC.fileServerRequest(serverName, fileInfoData)
            if rtnSet.get("CMD")[2:4] == "B0":
                result = rtnSet.get("fileUrl", "")
        else:
            _LOG.warning(f"DEBUG: save2newLocation <step 2> fileID:{fileID},requestType:{requestType},fileInfoData:{fileInfoData}")

    else:
        pass

    if _DEBUG:
        _LOG.info(f"DEBUG: save2newLocation <step 3> FILE_SYSTEM_MODE:{FILE_SYSTEM_MODE},fileID:{fileID},requestTyp:{requestType},result:{result}")
    
    return result
    
  
#copy file to private network, and generate thumbnail photo and change the url, 必须在 save2newLocation之前调用
def generateThumbnail(fileID,  objectName=None,prefix = "", privateFlag = True):
    requestType = comGD._DEF_FILE_REQUEST_TYPE_THUMBNAIL
    return save2newLocation(fileID, objectName, requestType,  prefix, privateFlag)


#上传文件并生成相关保存的文件ID 和 缩略图
def save2newLocationWithThumbail(fileID,  objectName, requestType = "", prefix = "",  privateFlag = True, compressFlag = comGD._CONST_YES):
    thumbnailID = generateThumbnail(fileID, objectName, prefix)
    if compressFlag == comGD._CONST_YES:
        fileID = save2newLocation(fileID=fileID, objectName=objectName, requestType=requestType,  prefix=prefix,  privateFlag = privateFlag )
    else:
        fileID = save2newLocation(fileID=fileID, objectName=objectName, requestType=requestType,  prefix=prefix,  privateFlag = privateFlag , compressFlag = compressFlag )
        
    return (fileID,  thumbnailID)
    

#del files 
def delPermanentFile(fileID, privateFlag=False):
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
def getTempLocation(fileID, privateFlag = True,  localAccess = False):
    result = fileID
    if fileID !="" and fileID:
        if FILE_SYSTEM_MODE == "FASTDFS":
            #already save to correct position 
            pass
        elif FILE_SYSTEM_MODE == "ALIOSS":
            if fileID[0:4] != "http":
                if localAccess == False:
                    #直接访问阿里云
                    result = OSS.genFileTempUrl(fileID) #生成的临时url有时候有问题
                else:
                    #temp solution
                    midDigital = 0
                    for a in fileID:
                        midDigital += ord(a)
                    midDir = str(midDigital % 10)+'/'
                    outfileDir = LOCAL_FILE_SERVER_BASE + LOCAL_FILE_TEMP_WEB_DIR + midDir 
                    outfilePath = os.path.join(outfileDir,  fileID)
                    if os.path.exists(outfilePath) == False:
                        try:
                        #download file from oss
                            rtn = OSS.downloadFile(fileID, outfilePath)
                            if rtn == False:
                                if _DEBUG:
                                    _LOG.warning(f"DEBUG: getTempLocation, FILE_SYSTEM_MODE:[{FILE_SYSTEM_MODE}] fileID:[{fileID}] result:[{rtn}]")                        
                        except Exception as e:
                            errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}, {outfilePath}"
                            _LOG.error(f"getTempLocation,{errMsg}, {traceback.format_exc()}")
                    result = LOCAL_FILE_SERVER_PATH + LOCAL_FILE_TEMP_WEB_DIR + midDir  + fileID
                
        elif FILE_SYSTEM_MODE == "TENCENT":
            if fileID[0:4] != "http":
                if localAccess == False:
                    #直接访问腾讯云
                    if privateFlag == False: 
                        result = COS.genFileTempUrl(fileID, privateFlag=privateFlag) #生成的公开url, bucket是私有写公有读
                    else:
                        result = COS.genFileTempUrl(fileID, privateFlag=privateFlag) #生成的临时url, 
                else:
                    #本地访问方案
                    midDigital = 0
                    for a in fileID:
                        midDigital += ord(a)
                    midDir = str(midDigital % 10)+'/'
                    outfileDir = LOCAL_FILE_SERVER_BASE + LOCAL_FILE_TEMP_WEB_DIR + midDir 
                    outfilePath = os.path.join(outfileDir,  fileID)
                    if os.path.exists(outfilePath) == False:
                        try:
                        #download file from oss
                            rtn = COS.downloadFile(fileID, outfilePath, privateFlag=privateFlag)
                            if rtn == False:
                                if _DEBUG:
                                    _LOG.warning("DEBUG: {0} FILE_SYSTEM_MODE:[{1}] fileID:[{2}] result:[{3}]".format("getTempLocation", FILE_SYSTEM_MODE, fileID, rtn))                        
                        except Exception as e:
                            errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}, {outfilePath}"
                            _LOG.error(f"getTempLocation,{errMsg}, {traceback.format_exc()}")
                    result = LOCAL_FILE_SERVER_PATH + LOCAL_FILE_TEMP_WEB_DIR + midDir  + fileID                
        else:
            pass

    if _DEBUG:
        _LOG.info(f"DEBUG: getTempLocation, FILE_SYSTEM_MODE:[{FILE_SYSTEM_MODE}] fileID:[{fileID}] result:[{result}]")
    
    return result

#upload file to aliyun oss/tencent cos end

#user relate begin
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
                #先复制所以信息
                result = rtnData

                # result["loginID"] = rtnData.get("loginID","")

                #设置默认的roleName
                roleName = rtnData.get("roleName","")
                if roleName not in settings.ROLE_CMD_LIST:
                    roleName = settings.accountServiceDefaultRoleName #修改默认的用户角色, modify default rolename        
                result["roleName"] = roleName

                # result["nickName"] = rtnData.get("nickName","")
                # result["realName"] = rtnData.get("realName","")

                gender = rtnData.get("gender","")
                if gender:
                    gender = rtnData.get("sex","")
                result["gender"] = gender

                # result["avatarID"] = rtnData.get("avatarID","")

                # result["mobilePhoneNo"] = rtnData.get("mobilePhoneNo","")

                # result["province"] = rtnData.get("province","")
                # result["city"] = rtnData.get("city","")
                # result["area"] = rtnData.get("area","")
                # result["address"] = rtnData.get("address","")

                # result["email"] = rtnData.get("email","")

                # result["PID"] = rtnData.get("PID","")
                # result["photoIDFront"] = rtnData.get("photoIDFront","")
                # result["photoIDBack"] = rtnData.get("photoIDBack","")
                # result["photoID"] = rtnData.get("photoID","")

                # result["regID"] = rtnData.get("regID","")
                # result["regYMDHMS"] = rtnData.get("regYMDHMS","")
                # result["updateYMDHMS"] = rtnData.get("updateYMDHMS","")
    except:
        pass
    return result


#写入用户数据库 mysql
def writeUser2UserBasic(dataSet,operatorLoginID):
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


#用户自己注册或者管理员添加,同步到mysql
def funcUserRegistration(dataSet):
    result = 0

    operatorLoginID = dataSet.get("loginID")

    inputData = dataSet.get("data")
    outputData = dataSet.get("rtnData")

    loginID = inputData.get("loginID")
    
    sessionID = outputData.get("sessionID")
    if not sessionID:
        sessionID = inputData.get("sessionID")

    rtnErrCode = outputData.get("errCode")

    try:
        if loginID and rtnErrCode=="B0":
            result = 1

            #同步数据到mysql
            userInfo = getUserInfo(loginID,sessionID)
            if userInfo:
                saveSet = copy.deepcopy(userInfo)
                if "passwd" in userInfo:
                    del userInfo["passwd"]
                if _DEBUG:
                    _LOG.info(f"DEBUG: userInfo:{userInfo}")

                rtn = writeUser2UserBasic(saveSet,operatorLoginID)
                _LOG.info(f"D: writeUser2UserBasic,rtn: {rtn}")
                
            #通知管理员处理
            # for userID in REGISTRATION_NOTIFICATION_USER_LIST:
            #     SMS.infoSMS(userID,loginID)
            #     if _DEBUG:
            #         _LOG.info(f"D: funcUserRegistration, userID:{userID}, loginID:{loginID}")
                pass

    except Exception as e:
        errMsg = f"PID: {_processorPID},dataSet:{dataSet},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result



#用户登录,检测用户数据是否存在, 如果不存在,就同步到mysql
def funcUserLogin(dataSet):
    result = 0

    operatorLoginID = dataSet.get("loginID")

    inputData = dataSet.get("data")
    outputData = dataSet.get("rtnData")

    loginID = inputData.get("loginID")

    sessionID = outputData.get("sessionID")
    if not sessionID:
        sessionID = inputData.get("sessionID")

    rtnErrCode = outputData.get("errCode")

    try:
        if loginID and rtnErrCode=="B0":
            result = 1
            
            #检测用户是否存在(mysql)
            currDataList = comMysql.queryUserBasic(loginID)
            userInfo = getUserInfo(loginID,sessionID)

            #比较一些关键参数
            saveFlag = False
            if userInfo and not currDataList:
                saveFlag = True
            elif userInfo and currDataList:
                currDataSet = currDataList[0]
                #roleName,
                if userInfo.get("roleName") != currDataSet.get("roleName"):
                    saveFlag = True
                elif userInfo.get("realName") != currDataSet.get("realName"):
                    saveFlag = True
                elif userInfo.get("mobilePhoneNo") != currDataSet.get("mobilePhoneNo"):
                    saveFlag = True
            else:
                pass
            
            if saveFlag:
                saveSet = copy.deepcopy(userInfo)
                if "passwd" in userInfo:
                    del userInfo["passwd"]
                if _DEBUG:
                    _LOG.info(f"DEBUG: userInfo:{userInfo}")

                rtn = writeUser2UserBasic(saveSet,operatorLoginID)
                _LOG.info(f"D: writeUser2UserBasic,rtn: {rtn}")
                
                pass

    except Exception as e:
        errMsg = f"PID: {_processorPID},dataSet:{dataSet},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#用户删除,同步到mysql
def funcUserDel(dataSet):
    result = 0

    operatorLoginID = dataSet.get("loginID")

    inputData = dataSet.get("data")
    outputData = dataSet.get("rtnData")

    loginID = inputData.get("loginID")
    sessionID = inputData.get("sessionID")

    rtnErrCode = outputData.get("errCode")

    try:
        if loginID and rtnErrCode=="B0":
            result = 1

            #同步数据到mysql
            userInfo = getUserInfo(loginID,sessionID)
            if userInfo:
                dataList = comMysql.queryUserBasic(loginID = loginID)
                if dataList:
                    #delete
                    rtn = comMysql.deleteUserBasic(loginID)
                    if rtn < 0:
                        _LOG.warning(f"user delete:{loginID}")

    except Exception as e:
        errMsg = f"PID: {_processorPID},dataSet:{dataSet},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#用户数据修改,同步到mysql
def funcUserModify(dataSet):
    result = 0

    operatorLoginID = dataSet.get("loginID")

    inputData = dataSet.get("data")
    outputData = dataSet.get("rtnData")

    loginID = inputData.get("loginID")
    sessionID = inputData.get("sessionID")

    rtnErrCode = outputData.get("errCode")

    try:
        if loginID and rtnErrCode=="B0":
            result = 1

            #同步数据到mysql
            userInfo = getUserInfo(loginID,sessionID)
            if userInfo:
                saveSet = userInfo

                rtn = writeUser2UserBasic(saveSet,operatorLoginID)
                _LOG.info(f"D: writeUser2UserBasic,rtn: {rtn}")

    except Exception as e:
        errMsg = f"PID: {_processorPID},dataSet:{dataSet},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result

#user relate end

# command part end



# buffer相关, 考虑到部分查询数据较多, 因此查询结果先缓存到redis,然后根据用户要求取出,
# 这个是一个异步代码, 保证后台的速度
def funcPutDataBufferList(dataSet):
    indexKey = dataSet.get("indexKey")
    dataList = dataSet.get("data")
    comDB.delDataBuffer(indexKey)
    rtn =  comDB.putAllDataBuffer(indexKey, dataList)
    result = len(dataList)
    return result


def checkDatabaseExists():
    pass


def procTransferData():
    checkDatabaseExists()

    while True: 
        try:
            transSet = comDB.getMsg2Queue(comGD._DEF_STOCK_MYSQL_TITLE)
            if transSet:
                CMD = transSet.get("CMD")

                #transfer data receive
                if _DEBUG:
                    # _LOG.info(f"R: CMD:{CMD}") 
                    _LOG.info(f"R: CMD:{CMD},transSet:{transSet}") 

                if CMD == "registration": 
                    totalHandledData = funcUserRegistration(transSet)
                    if _DEBUG:
                        _LOG.info(f"DEBUG: {transSet}")
                        _LOG.info(f"S: total {CMD} :{totalHandledData}")

                elif CMD == "login": 
                    totalHandledData = funcUserLogin(transSet)
                    if _DEBUG:
                        _LOG.info(f"DEBUG: {transSet}")
                        _LOG.info(f"S: total {CMD} :{totalHandledData}")

                elif CMD == "useradd": 
                    totalHandledData = funcUserRegistration(transSet)
                    if _DEBUG:
                        _LOG.info(f"DEBUG: {transSet}")
                        _LOG.info(f"S: total {CMD} :{totalHandledData}")

                elif CMD == "userdel": 
                    totalHandledData = funcUserDel(transSet)
                    if _DEBUG:
                        _LOG.info(f"DEBUG: {transSet}")
                        _LOG.info(f"S: total {CMD} :{totalHandledData}")

                elif CMD == "usermodify": 
                    totalHandledData = funcUserModify(transSet)
                    if _DEBUG:
                        _LOG.info(f"DEBUG: {transSet}")
                        _LOG.info(f"S: total {CMD} :{totalHandledData}")

                elif CMD == "putdatabufferlist": 
                    totalHandledData = funcPutDataBufferList(transSet)
                    if _DEBUG:
                        _LOG.info(f"S: total {CMD} :{totalHandledData}")

                else:
                    if _DEBUG:
                        _LOG.info(f"S: total {CMD} : nothing to do")
            else:
                misc.time.sleep(0.1) #避免占用CPU资源

        except Exception as e:
            errMsg = f"PID: {_processorPID},CMD:{CMD},errMsg:{str(e)}"
            _LOG.error(f"{errMsg}, {traceback.format_exc()}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        pass
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
    
    procTransferData()
