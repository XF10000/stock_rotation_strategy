#!/usr/bin/env python3
# -*- coding: utf-8 -*-


#Filename: mysqlCommon.py  
#Date: 2020-04-01
#Description:   mysql 处理代码

#mysql数据库信息也存储在这里, 主要是只有部分程序需要处理mysql数据库, 读写已经分离, 目前主要是采用sql语句处理, 已经防止注入攻击. 


_VERSION="20260115"

#add src directory
import os
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
import copy

#global defintion/common var etc.
from common import globalDefinition as comGD

from common import funcCommon as comFC
#code/decode functions
#from common import codingDecoding as comCD

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#setting files
from config import mysqlSettings as mysqlSettings

from config import basicSettings as settings

HOME_DIR = settings._HOME_DIR

if "_LOG" not in dir() or not _LOG:
    logfilepath = os.path.join(HOME_DIR, comGD._DEF_GENRAL_MYSQL_LOG_NAME)
#    _LOG = misc.setLogNew(comGD._DEF_XJY_MYSQL_TITLE, logfilepath)
    
_DEBUG = settings._DEBUG

auto_increment_default_value = 10000

SYS_DEFAULT_AUTO_LOGINID = settings.SYS_DEFAULT_AUTO_LOGINID

if "mysqlDB" not in dir() or not mysqlDB:
    mysqlDB = mysqlSettings.mysqlDB

database_name = mysqlSettings.MYSQL_READ_DB

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
    rtn = mysqlDB.executeRead(sqlStr, (database_name, tableName))
    if rtn > 0:
        result = True
    return result


def dropTableGeneral(tableName):
    result = False
    try:
        sqlStr = "DROP TABLE %s;" % tableName
        rtn = mysqlDB.executeWrite(sqlStr)
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
        rtn = mysqlDB.executeWrite(sqlStr, tuple(valuesList))
#        if _DEBUG:
#            if rtn <=0:
#                _LOG.warning("M: %d %s" % (rtn,  sqlStr)) 
        
        if rtn > 0:
            if selfDefinedPrimaryKey == comGD._CONST_NO:
                #result = mysqlDB.lastrowid
                result = mysqlDB.insertID()
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
        rtn = mysqlDB.executeWrite(sqlStr, tuple(valuesList))
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
    databaseName = mysqlSettings.MYSQL_READ_DB

    valuesList = []
    sqlStr =  f"SELECT TABLE_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{databaseName}';" 

    try:
        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            for data in dataList:
                aSet = {}
                aSet["tableName"] = data["TABLE_NAME"]
                aSet["tableRows"] = data["TABLE_ROWS"]
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
    
    
#user family begin
def createUserBasic():
    tableName = "USER_BASIC"
    aList  = ["CREATE TABLE IF NOT EXISTS %s("
    "loginID VARCHAR(32) PRIMARY KEY COMMENT '用户登录号',",
    "passwd VARCHAR(80) COMMENT '用户密码',",
    "openID VARCHAR(40) COMMENT '微信openID',",
    "roleName VARCHAR(16) COMMENT '角色名称',",
    "nickName VARCHAR(40) COMMENT '昵称',",
    "realName VARCHAR(40) COMMENT '用户真实姓名' ,",
    "gender CHAR(1) COMMENT '性别',",
    "avatarID VARCHAR(200) COMMENT '头像ID',",
    "mobilePhoneNo VARCHAR(32) COMMENT '手机号',",
    "masterID VARCHAR(32) COMMENT '用户主号',",
    "province VARCHAR(32) COMMENT '省',",
    "city VARCHAR(32) COMMENT '市',",
    "area VARCHAR(32) COMMENT '地区',",
    "address VARCHAR(200) COMMENT '地址',",
    "email VARCHAR(100) COMMENT '用户邮箱',",
    "PID VARCHAR(20) COMMENT '用户身份证号',",
    "photoIDFront VARCHAR(128) COMMENT '用户身份证头像侧',",
    "photoIDBack VARCHAR(128) COMMENT '用户身份证背面',",
    "photoID VARCHAR(128) COMMENT '用户照片',",
    "delFlag CHAR(1) COMMENT '删除标记',",
    "activeFlag CHAR(1) COMMENT '活动标记',",
    "regPosition VARCHAR(80) COMMENT '注册位置',",
    "regID VARCHAR(32) COMMENT '注册ID',",
    "regYMDHMS VARCHAR(16) COMMENT '注册年月日',",
    "updateYMDHMS VARCHAR(16) COMMENT '数据更新日期',",
    "lastOpenID VARCHAR(40) COMMENT '用户最后一次登录openID',",
    "lastLoginYMDHMS VARCHAR(16) COMMENT '用户最后一次登录年月日',",
    "modifyID VARCHAR(32) COMMENT '修改用户ID',",
    "modifyYMDHMS VARCHAR(16) COMMENT '修改年月日',",
    "passwdYMDHMS VARCHAR(16) COMMENT '密码修改年月日',",
    "extStartYMDHMS VARCHAR(16) COMMENT '扩展开始年月日',",
    "extLeaveYMDHMS VARCHAR(16) COMMENT '扩展停止年月日',",
    "extJobPosition VARCHAR(100) COMMENT '扩展职位',",
    "extDepartment VARCHAR(100) COMMENT '扩展部门',",
    "extOrgName VARCHAR(300) COMMENT '扩展组织名称',",
    "extOrgID INT COMMENT '扩展组织ID',",
    "extInService VARCHAR(1) COMMENT '扩展是否在职',",
    "extJobLabel VARCHAR(16) COMMENT '扩展职位身份标签_注册用户_区域用户_后台用户',",
    "extJobDetail VARCHAR(64) COMMENT '扩展职位细节_例如是工信厅卫生局等',",
    "extBrief VARCHAR(1000) COMMENT '扩展人员简介_专家简介_区域管理员类别',",
    "extManualTagList VARCHAR(500) COMMENT '扩展标签列表',",
    "extManagementAreaList VARCHAR(500) COMMENT '扩展管理区域',",
    "extMemo VARCHAR(100) COMMENT '扩展备注'"
    ")  ENGINE=INNODB DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;" , 
    ]
    tempStr = "".join(aList)
    sqlStr = tempStr % (tableName)
    rtn = mysqlDB.executeWrite(sqlStr)
    result = chkTableExist(tableName)
    if result:
        pass
        sqlStr = "CREATE INDEX {1} ON {0}({1})".format(tableName, "roleName")
        rtn = mysqlDB.executeWrite(sqlStr)
        sqlStr = "CREATE INDEX {1} ON {0}({1})".format(tableName, "extJobLabel")
        rtn = mysqlDB.executeWrite(sqlStr)

    return result


def dropUserBasic():
    tableName = "USER_BASIC"
    result = dropTableGeneral(tableName)
    return result


#以后这个是标准写法,利用fetchMany来处理数据
#SELECT * FROM USER_BASIC WHERE loginID = "13910710766";
def queryUserBasic(loginID = "" , name = "", mobile = "", manualTag = "",jobLabel="",
                   searchOption = {},  roleName = "", roleNameList = [],  keyword="",
                   mode = "normal", beginYMD = "",endYMD = "", 
                   order="create", limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    tableName = "USER_BASIC"

    if mode =="short":
        columns = "loginID,nickName,realName,roleName, avatarID"
    elif mode =="normal":
        columns = "loginID,nickName,realName,roleName,avatarID,openID,masterID,mobilePhoneNo,province,city,area, address, email, regYMDHMS, updateYMDHMS"
    else:
        columns = "*"
    valuesList = [] 
    sqlStr =   "SELECT %s FROM %s " % (columns, tableName )

    try:
        if loginID:
            sqlStr += " WHERE loginID = %s"
            valuesList = [loginID]
        else:
            valuesList = []
            #以下 searchOption,roleName,roleNameList是并列关系,只能选一个
            if searchOption:
                if valuesList:
                    whereStr = " AND "
                else:
                    whereStr = " WHERE "
                logic = searchOption.get("logic", "AND")
                optionList = searchOption.get("optionList", [])
                count = 0
                for optionSet in optionList:
                    if count > 0:
                        whereStr += " " + logic + " "
                    if "realName" in optionSet:
                        whereStr += " realName = %s" 
                        valuesList.append(optionSet["realName"])
                    if "nickName" in optionSet:
                        whereStr += " nickName = %s" 
                        valuesList.append(optionSet["nickName"])
                    if "loginID" in optionSet:
                        whereStr += " loginID = %s" 
                        valuesList.append(optionSet["loginID"])
                    if "roleName" in optionSet:
                        whereStr += " roleName = %s" 
                        valuesList.append(optionSet["roleName"])
                    if "province" in optionSet:
                        whereStr += " province = %s" 
                        valuesList.append(optionSet["province"])
                    count += 1
                sqlStr += whereStr 
                
            elif jobLabel:
                if valuesList:
                    whereStr = " AND extJobLabel = %s "
                else:
                    whereStr = " WHERE extJobLabel = %s "
                valuesList.append(jobLabel)
                sqlStr += whereStr 

            elif roleName:
                if valuesList:
                    whereStr = " AND roleName = %s "
                else:
                    whereStr = " WHERE roleName = %s "
                valuesList.append(roleName)
                sqlStr += whereStr 

            elif roleNameList:
                if valuesList:
                    whereStr = " AND " + genOrList(roleNameList, "roleName")
                else:
                    whereStr = " WHERE " + genOrList(roleNameList, "roleName")
                valuesList += roleNameList
                sqlStr += whereStr 
            
            if keyword:
                if valuesList:
                    sqlStr =  sqlStr + " AND (locate(%s,realName) OR locate(%s,nickName) OR locate(%s,extJobPosition) OR locate(%s,extJobDetail) OR locate(%s,extOrgName) OR locate(%s,loginID) )" 
                else:
                    sqlStr =  sqlStr + " WHERE (locate(%s,realName) OR locate(%s,nickName) OR locate(%s,extJobPosition) OR locate(%s,extJobDetail) OR locate(%s,extOrgName) OR locate(%s,loginID) )" 
                valuesList.append(keyword)
                valuesList.append(keyword)
                valuesList.append(keyword)
                valuesList.append(keyword)
                valuesList.append(keyword)
                valuesList.append(keyword)

            # beginYMD 和 endYMD可以和上面混用
            if beginYMD  and endYMD :
                beginYMDHMS = beginYMD + "000000"
                endYMDHMS = endYMD + "240000"
                if valuesList:
                    whereStr = " AND regYMDHMS >= %s and regYMDHMS <= %s "
                else:
                    whereStr = " WHERE regYMDHMS >= %s and regYMDHMS <= %s "
                sqlStr = sqlStr + " WHERE regYMDHMS >= %s and regYMDHMS <= %s " 
                valuesList += [beginYMDHMS, endYMDHMS] 
                                
        if order == "create":
            sqlStr += " ORDER BY regYMDHMS DESC"
        
        #其他过滤数据的在这里
        if name or mobile or manualTag:
            #分批次获取数据并挑选数据
            rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
            if rtn:
                batchNum = 1000
                total = 0
                while True:
                    dataList = mysqlDB.fetchMany(batchNum)
                    dataList = dataFormatConvert(dataList)
                
                    for data in dataList:
                        matchFlag = False

                        if mobile:
                            keyword = mobile
                            keyList = ["mobilePhoneNo","loginID"]
                            for key in keyList:
                                currVal = data.get(key)
                                if currVal:
                                    if currVal.find(keyword) >= 0:
                                        matchFlag = True
                                        break
                        if name:
                            keyword = name
                            keyList = ["roleName","nickName","realName"]
                            for key in keyList:
                                currVal = data.get(key)
                                if currVal:
                                    if currVal.find(keyword) >= 0:
                                        matchFlag = True
                                        break

                        if matchFlag:
                            result.append(data)

                    #final
                    #如果取不到更多数据就退出
                    currDataLen = len(dataList)
                    if currDataLen < batchNum:
                        break
                    
                    #如果取到的数据满足要求也退出,limitNUm = 0 是提取全部满足条件数据
                    if limitNum > 0:
                        total = len(result)
                        if total >= limitNum:
                            result = result[0:limitNum]
                            break
        else:
            if limitNum > 0:
                sqlStr += " LIMIT {0}".format(limitNum)

            rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
            if rtn > 0:
                dataList = mysqlDB.fetchAll()
                dataList = dataFormatConvert(dataList)

                result = dataList

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
#        if _DEBUG:
#            _LOG.error(f"{errMsg}")

    return result

    
#SELECT * FROM USER_BASIC WHERE loginID = "13910710766";
def deleteUserBasic(loginID):
    result = 0
    tableName = "USER_BASIC"
    try:
    
        sqlStr = "DELETE FROM %s WHERE loginID = \"%s\";" % (tableName, loginID)
        rtn = mysqlDB.executeWrite(sqlStr)
        result = rtn

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
#        if _DEBUG:
#            _LOG.error(f"{errMsg}")

    return result
    
    
def insertUserBasic(loginID, dataSet):
    result = 0
    tableName = "USER_BASIC"
    try:
        saveSet = {}
        saveSet["loginID"]  = loginID

        saveSet["passwd"] = dataSet.get("passwd", "") 

        saveSet["openID"] = dataSet.get("openID", "") 

        saveSet["roleName"] = dataSet.get("roleName", "") 

        saveSet["nickName"] = dataSet.get("nickName", "") 

        saveSet["realName"] = dataSet.get("realName", "") 

        saveSet["gender"] = dataSet.get("gender", "") 

        saveSet["avatarID"] = dataSet.get("avatarID", "") 

        saveSet["mobilePhoneNo"] = dataSet.get("mobilePhoneNo", "") 

        saveSet["masterID"] = dataSet.get("masterID", "") 

        saveSet["province"] = dataSet.get("province", "") 

        saveSet["city"] = dataSet.get("city", "") 

        saveSet["area"] = dataSet.get("area", "") 

        saveSet["address"] = dataSet.get("address", "") 

        saveSet["email"] = dataSet.get("email", "") 

        saveSet["PID"] = dataSet.get("PID", "") 

        saveSet["photoIDFront"] = dataSet.get("photoIDFront", "") 

        saveSet["photoIDBack"] = dataSet.get("photoIDBack", "") 

        saveSet["photoID"] = dataSet.get("photoID", "") 

        saveSet["delFlag"] = dataSet.get("delFlag", "0") 

        saveSet["activeFlag"] = dataSet.get("activeFlag", comGD._CONST_YES) 

        regPosition = dataSet.get("regPosition", {})
        if (regPosition != {}):
            #双引号的特殊处理
            saveSet["regPosition"]  = misc.jsonDumps(regPosition).replace("\"", "'")
        else:
            saveSet["regPosition"] = misc.jsonDumps({})

        saveSet["regID"] = dataSet.get("regID", "") 

        saveSet["regYMDHMS"] = dataSet.get("regYMDHMS", "") 

        saveSet["updateYMDHMS"] = dataSet.get("updateYMDHMS", "") 

        saveSet["lastOpenID"] = dataSet.get("lastOpenID", "") 

        saveSet["lastLoginYMDHMS"] = dataSet.get("lastLoginYMDHMS", "") 

        saveSet["passwdYMDHMS"] = dataSet.get("passwdYMDHMS", "") 

        # extend items begin, per project
        saveSet["extStartYMDHMS"] = dataSet.get("extStartYMDHMS", "") 

        saveSet["extLeaveYMDHMS"] = dataSet.get("extLeaveYMDHMS", "") 

        saveSet["extJobPosition"] = dataSet.get("extJobPosition", "") 

        saveSet["extDepartment"] = dataSet.get("extDepartment", "") 

        saveSet["extOrgName"] = dataSet.get("extOrgName", "") 

        try:
            extOrgID = int(dataSet.get("extOrgID")) 
        except:
            extOrgID = 0 
        saveSet["extOrgID"] = extOrgID

        saveSet["extInService"] = dataSet.get("extInService", "") 

        saveSet["extJobLabel"] = dataSet.get("extJobLabel", "") 

        saveSet["extJobDetail"] = dataSet.get("extJobDetail", "") 

        saveSet["extBrief"] = dataSet.get("extBrief", "") 

        saveSet["extManualTagList"] = dataSet.get("extManualTagList", "") 

        saveSet["extManagementAreaList"] = dataSet.get("extManagementAreaList", "") 

        saveSet["extMemo"] = dataSet.get("extMemo", "") 
        # extend items end, per project

        result = insertTableGeneral(tableName, saveSet, selfDefinedPrimaryKey = comGD._CONST_YES)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
#        if _DEBUG:
#            _LOG.error(f"{errMsg}")

    return result
    
    
def updateUserBasic(loginID, dataSet):
    result = 0
    tableName = "USER_BASIC"
    try:
        saveSet = {}
        
        passwd = dataSet.get("passwd")
        if passwd != "" and passwd:
            saveSet["passwd"] = passwd
            
        openID = dataSet.get("openID", "")
        if openID != "":
            saveSet["openID"] = openID

        roleName = dataSet.get("roleName", "")
        if roleName != "":
            saveSet["roleName"] = roleName

        nickName = dataSet.get("nickName", "")
        if nickName != "":
            saveSet["nickName"] = nickName

        realName = dataSet.get("realName", "")
        if realName != "":
            saveSet["realName"] = realName

        gender = dataSet.get("gender", "")
        if gender != "":
            saveSet["gender"] = gender

        avatarID = dataSet.get("avatarID", "")
        if avatarID != "":
            saveSet["avatarID"] = avatarID

        mobilePhoneNo = dataSet.get("mobilePhoneNo", "")
        if mobilePhoneNo != "":
            saveSet["mobilePhoneNo"] = mobilePhoneNo

        province = dataSet.get("province", "")
        if province != "":
            saveSet["province"] = province
            
        masterID = dataSet.get("masterID", "")
        if masterID != "":
            saveSet["masterID"] = masterID
            
        city = dataSet.get("city", "")
        if city != "":
            saveSet["city"] = city
            
        area = dataSet.get("area", "")
        if area != "":
            saveSet["area"] = area
            
        address = dataSet.get("address", "")
        if address != "":
            saveSet["address"] = address
            
        email = dataSet.get("email", "")
        if email != "":
            saveSet["email"] = email
            
        PID = dataSet.get("PID", "")
        if PID != "":
            saveSet["PID"] = PID
            
        photoIDFront = dataSet.get("photoIDFront", "")
        if photoIDFront != "":
            saveSet["photoIDFront"] = photoIDFront
            
        photoIDBack = dataSet.get("photoIDBack", "")
        if photoIDBack != "":
            saveSet["photoIDBack"] = photoIDBack
            
        photoIDBack = dataSet.get("photoIDBack", "")
        if photoIDBack != "":
            saveSet["photoIDBack"] = photoIDBack
            
        photoID = dataSet.get("photoID", "")
        if photoID != "":
            saveSet["photoID"] = photoID

        delFlag = dataSet.get("delFlag") 
        if delFlag:
            if delFlag != "1":
                delFlag = "0"
            saveSet["delFlag"] = delFlag

        activeFlag = dataSet.get("activeFlag")
        if activeFlag:
            saveSet["activeFlag"] = activeFlag

        regPosition = dataSet.get("regPosition", {})
        if (regPosition != {}):
            #双引号的特殊处理
            saveSet["regPosition"] = misc.jsonDumps(regPosition).replace("\"", "'")

        updateYMDHMS = dataSet.get("updateYMDHMS", "")
        if updateYMDHMS != "":
            saveSet["updateYMDHMS"] = updateYMDHMS
        lastOpenID = dataSet.get("lastOpenID", "")

        if lastOpenID != "":
            saveSet["lastOpenID"] = lastOpenID

        lastLoginYMDHMS = dataSet.get("lastLoginYMDHMS", "")
        if lastLoginYMDHMS != "":
            saveSet["lastLoginYMDHMS"] = lastLoginYMDHMS

        modifyID = dataSet.get("modifyID")
        if modifyID != "":
            saveSet["modifyID"] = modifyID

        modifyYMDHMS = dataSet.get("modifyYMDHMS", "")
        if modifyYMDHMS != "":
            saveSet["modifyYMDHMS"] = modifyYMDHMS

        passwdYMDHMS = dataSet.get("passwdYMDHMS", "")
        if passwdYMDHMS != "":
            saveSet["passwdYMDHMS"] = passwdYMDHMS
    
        # extend items begin, per project

        extStartYMDHMS = dataSet.get("extStartYMDHMS") 
        if extStartYMDHMS:
            saveSet["extStartYMDHMS"] = extStartYMDHMS

        extLeaveYMDHMS = dataSet.get("extLeaveYMDHMS") 
        if extLeaveYMDHMS:
            saveSet["extLeaveYMDHMS"] = extLeaveYMDHMS

        extJobPosition = dataSet.get("extJobPosition") 
        if extJobPosition:
            saveSet["extJobPosition"] = extJobPosition

        extDepartment = dataSet.get("extDepartment") 
        if extDepartment:
            saveSet["extDepartment"] = extDepartment

        extOrgName = dataSet.get("extOrgName") 
        if extOrgName:
            saveSet["extOrgName"] = extOrgName

        extOrgID = dataSet.get("extOrgID") 
        if extOrgID:
            try:
                extOrgID = int(dataSet.get("extOrgID")) 
                saveSet["extOrgID"] = extOrgID
            except:
                pass

        extInService = dataSet.get("extInService") 
        if extInService:
            saveSet["extInService"] = extInService

        extJobLabel = dataSet.get("extJobLabel") 
        if extJobLabel:
            saveSet["extJobLabel"] = extJobLabel

        extJobDetail = dataSet.get("extJobDetail") 
        if extJobDetail:
            saveSet["extJobDetail"] = extJobDetail

        extBrief = dataSet.get("extBrief") 
        if extBrief:
            saveSet["extBrief"] = extBrief

        extManualTagList = dataSet.get("extManualTagList") 
        if extManualTagList:
            saveSet["extManualTagList"] = extManualTagList

        extManagementAreaList = dataSet.get("extManagementAreaList") 
        if extManagementAreaList:
            saveSet["extManagementAreaList"] = extManagementAreaList

        extMemo = dataSet.get("extMemo") 
        if extMemo:
            saveSet["extMemo"] = extMemo

        # extend items end, per project

        keySqlstr = "loginID = %s" 
        keyValues = [loginID]
        
        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)
        
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
#        if _DEBUG:
#            _LOG.error(f"{errMsg}")

    return result


#获取本地用户信息mysql
def getUserInfoMysql(loginID):
    result = {}
    try:
        mode = "full"
        currDataList = queryUserBasic(loginID,mode = mode)

        if currDataList:
            currDataSet = currDataList[0]

            aSet = {}

            aSet["loginID"] = currDataSet.get("loginID","")
            # aSet["openID"] = currDataSet.get("openID","")
            aSet["roleName"] = currDataSet.get("roleName","")
            aSet["nickName"] = currDataSet.get("nickName","")
            aSet["realName"] = currDataSet.get("realName","")
            aSet["gender"] = currDataSet.get("gender","")

            aSet["avatarID"] = currDataSet.get("avatarID","")

            aSet["mobilePhoneNo"] = currDataSet.get("mobilePhoneNo","")
            aSet["masterID"] = currDataSet.get("masterID","")
            aSet["province"] = currDataSet.get("province","")
            aSet["city"] = currDataSet.get("city","")
            aSet["area"] = currDataSet.get("area","")
            aSet["address"] = currDataSet.get("address","")
            aSet["email"] = currDataSet.get("email","")
            aSet["PID"] = currDataSet.get("PID","")
            aSet["activeFlag"] = currDataSet.get("activeFlag","")

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

            aSet["extInService"] = currDataSet.get("extInService","")
            # aSet["extInService"] = chkIsInService(aSet["extInService"],aSet["activeFlag"])

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

            result = aSet
    except:
        pass
    return result


#champion only 
def statUserBasic(statBy = "roleName", beginYMD = "",endYMD = ""):
    result = []
    tableName = "USER_BASIC"

    valuesList = []

    try:
        if statBy == "roleName":
            sqlStr = f"SELECT count(loginID) as total, roleName FROM {tableName} "

            if beginYMD:
                beginYMDHMS = beginYMD + "000000"
                if valuesList:
                    sqlStr += " AND regYMDHMS >= % " 
                else:
                    sqlStr += " WHERE regYMDHMS >= % " 
                valuesList.append(beginYMDHMS)

            if endYMD:
                endYMDHMS = endYMD + "240000"
                if valuesList:
                    sqlStr += " AND regYMDHMS <= % " 
                else:
                    sqlStr += " WHERE regYMDHMS <= % " 
                valuesList.append(endYMDHMS)
            
            sqlStr += " GROUP BY roleName"
        else:
            sqlStr = f"SELECT count(loginID) as total FROM {tableName}"

            if beginYMD:
                beginYMDHMS = beginYMD + "000000"
                if valuesList:
                    sqlStr += " AND regYMDHMS >= % " 
                else:
                    sqlStr += " WHERE regYMDHMS >= % " 
                valuesList.append(beginYMDHMS)

            if endYMD:
                endYMDHMS = endYMD + "240000"
                if valuesList:
                    sqlStr += " AND regYMDHMS <= % " 
                else:
                    sqlStr += " WHERE regYMDHMS <= % " 
                valuesList.append(endYMDHMS)
            
        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = dataList

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
#        if _DEBUG:
#            _LOG.error(f"{errMsg}")

    return result

#user family end


#wechat code  family begin
def createUserWechatCode():
    tableName = "USER_weChatCode"
    aList  = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY,", 
    "loginID VARCHAR(32) NOT NULL,", 
    "openID VARCHAR(40) NOT NULL,", 
    "YMDHMS VARCHAR(16) ,", 
    "index (loginID, openID)", 
    ")  ENGINE=INNODB DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;" , 
    ]
    tempStr = "".join(aList)
    sqlStr = tempStr % (tableName)
    rtn = mysqlDB.executeWrite(sqlStr)
    result = chkTableExist(tableName)
    return result


def dropUserWechatCode():
    tableName = "USER_weChatCode"
    result = dropTableGeneral(tableName)
    return result
    

#SELECT * FROM USER_weChatCode WHERE loginID = "13910710766";
def queryUserWechatCode(loginID,  openID = ""):
    result = []
    tableName = "USER_weChatCode"
    try:

        if loginID == "" and openID != "":
            sqlStr = "SELECT * FROM %s WHERE openID = \"%s\";" % (tableName, openID)              
        elif loginID != "" and openID != "":
            sqlStr = "SELECT * FROM %s WHERE loginID = \"%s\" and openID = \"%s\";" % (tableName, loginID, openID) 
        else:
            sqlStr = "SELECT * FROM %s WHERE loginID = \"%s\";" % (tableName, loginID)
            
        rtn = mysqlDB.executeRead(sqlStr)
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)        
                
    except:
        pass
    return result
    
    
#DELETE  FROM USER_weChatCode WHERE loginID = "13910710766";
def deleteUserWechatCode(loginID, openID = ""):
    result = 0
    tableName = "USER_weChatCode"
    try:

        if openID == "":
            sqlStr = "DELETE FROM %s WHERE loginID = \"%s\";" % (tableName, loginID)
        else:
            sqlStr = "DELETE FROM %s WHERE loginID = \"%s\" and openID = \"%s\";" % (tableName, loginID, openID)                
        rtn = mysqlDB.executeWrite(sqlStr)
        result = rtn

    except:
        pass
    return result
    
    
def insertUserWechatCode(loginID, dataSet):
    result = 0
    tableName = "USER_weChatCode"
    try:
        saveSet = {}
        saveSet["loginID"]  = loginID

        openID = dataSet.get("openID", "")
        saveSet["openID"]  = openID

        YMDHMS = dataSet.get("YMDHMS", "")
        if (YMDHMS != ""):
            saveSet["YMDHMS"]  = YMDHMS

        result = insertTableGeneral(tableName, saveSet)
    except:
        pass
    return result
    
    
def updateUserWechatCode(loginID, dataSet):
    result = 0
    tableName = "USER_weChatCode"
    try:
        saveSet = {}
        
        openID = dataSet.get("openID", "")

        YMDHMS = dataSet.get("YMDHMS","")
        if YMDHMS != "":
            saveSet["YMDHMS"] = YMDHMS
            
        keyOption = ("loginID = \"%s\" and openID = \"%s\"" %(loginID, openID))
            
        result = updateTableGeneral(tableName, keyOption , saveSet)
    except:
        pass
    return result
#wechat code  family end


#application begin


#hwinfo_report_record begin 

def tablename_convertor_hwinfo_report_record(dataSource=""):
    if dataSource:
        tableName = "hwinfo_report_record" + "_" + dataSource
    else:
        tableName = "hwinfo_report_record"
    tableName = tableName.lower()
    return tableName


def decode_tablename_hwinfo_report_record(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建hwinfo_report_record表
def create_hwinfo_report_record(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "recID INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "hostName VARCHAR(64) COMMENT '主机名称',",
    "description VARCHAR(64) COMMENT '主机描述',",
    "IP VARCHAR(16) COMMENT 'IP地址',",
    "IPs VARCHAR(1000) COMMENT 'IP地址集合',",
    "os VARCHAR(32) COMMENT '操作系统',",
    "osVersion VARCHAR(64) COMMENT '操作系统版本',",
    "mac VARCHAR(20) COMMENT 'mac地址',",
    "cpuCount INT COMMENT 'CPU核心数',",
    "cpuLoad INT COMMENT 'CPU占用百分比',",
    "RAMTotal VARCHAR(8) COMMENT 'RAM total',",
    "RAMUsed VARCHAR(8) COMMENT 'RAM used',",
    "RAMFree VARCHAR(8) COMMENT 'RAM free',",
    "RAMPercent INT COMMENT 'RAM占用百分比',",
    "disk VARCHAR(1000) COMMENT 'disk描述',",
    "diskTotal VARCHAR(8) COMMENT '硬盘总容量',",
    "diskUsed VARCHAR(8) COMMENT '硬盘使用量',",
    "diskPercent INT COMMENT '硬盘使用百分比',",
    "processorInfo VARCHAR(1000) COMMENT '进程描述',",
    "addtionalInfo VARCHAR(5000) COMMENT '额外信息',",
    "YMDHMS VARCHAR(16) COMMENT '数据年月日',",
    "label1 VARCHAR(32) NULL,",
    "label2 VARCHAR(32) NULL,",
    "label3 VARCHAR(32) NULL,",
    "memo VARCHAR(200) NULL,",
    "regID VARCHAR(32) NOT NULL COMMENT '注册ID',",
    "regYMDHMS VARCHAR(16) NOT NULL COMMENT '注册年月日',",
    "modifyID VARCHAR(32) COMMENT '修改ID',",
    "modifyYMDHMS VARCHAR(16) COMMENT '数据修改年月日',",
    "dispFlag VARCHAR(1) COMMENT '是否显示标记',",
    "delFlag VARCHAR(1) COMMENT '是否删除标记'"
    ")  ENGINE=INNODB DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    ]
    tempStr = "".join(aList)
    sqlStr = tempStr % (tableName)
    rtn = mysqlDB.executeWrite(sqlStr)
    result = chkTableExist(tableName)
    if result:
        pass
        #sqlStr = "CREATE INDEX {1} ON {0}({1}) ".format(tableName, "indexKey")
        #rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除hwinfo_report_record表
def drop_hwinfo_report_record(tableName):
    result = dropTableGeneral(tableName)
    return result


#hwinfo_report_record 删除记录
def delete_hwinfo_report_record(tableName,recID):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE recID = %s"
        valuesList = [recID] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#hwinfo_report_record 增加记录
def insert_hwinfo_report_record(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["hostName"] = dataSet.get("hostName", "") 

        saveSet["description"] = dataSet.get("description", "") 

        saveSet["IP"] = dataSet.get("IP", "") 

        saveSet["IPs"] = dataSet.get("IPs", "") 

        saveSet["os"] = dataSet.get("os", "") 

        saveSet["osVersion"] = dataSet.get("osVersion", "") 

        saveSet["mac"] = dataSet.get("mac", "") 

        try:
            cpuCount = int(dataSet.get("cpuCount")) 
        except:
            cpuCount = 0 
        saveSet["cpuCount"] = cpuCount

        try:
            cpuLoad = int(dataSet.get("cpuLoad")) 
        except:
            cpuLoad = 0 
        saveSet["cpuLoad"] = cpuLoad

        saveSet["RAMTotal"] = dataSet.get("RAMTotal", "") 

        saveSet["RAMUsed"] = dataSet.get("RAMUsed", "") 

        saveSet["RAMFree"] = dataSet.get("RAMFree", "") 

        try:
            RAMPercent = int(dataSet.get("RAMPercent")) 
        except:
            RAMPercent = 0 
        saveSet["RAMPercent"] = RAMPercent

        saveSet["disk"] = dataSet.get("disk", "") 

        saveSet["diskTotal"] = dataSet.get("diskTotal", "") 

        saveSet["diskUsed"] = dataSet.get("diskUsed", "") 

        try:
            diskPercent = int(dataSet.get("diskPercent")) 
        except:
            diskPercent = 0 
        saveSet["diskPercent"] = diskPercent

        saveSet["processorInfo"] = dataSet.get("processorInfo", "") 

        saveSet["addtionalInfo"] = dataSet.get("addtionalInfo", "") 

        saveSet["YMDHMS"] = dataSet.get("YMDHMS", "") 

        saveSet["label1"] = dataSet.get("label1", "") 

        saveSet["label2"] = dataSet.get("label2", "") 

        saveSet["label3"] = dataSet.get("label3", "") 

        saveSet["memo"] = dataSet.get("memo", "") 

        saveSet["regID"] = dataSet.get("regID", "") 

        saveSet["regYMDHMS"] = dataSet.get("regYMDHMS", "") 

        saveSet["dispFlag"] = dataSet.get("dispFlag", "") 

        saveSet["delFlag"] = dataSet.get("delFlag", "0") 

        result = insertTableGeneral(tableName, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#hwinfo_report_record 修改记录
def update_hwinfo_report_record(tableName,recID,dataSet):
    result = -2
    try:
        saveSet = {}

        hostName = dataSet.get("hostName") 
        if hostName:
            saveSet["hostName"] = hostName

        description = dataSet.get("description") 
        if description:
            saveSet["description"] = description

        IP = dataSet.get("IP") 
        if IP:
            saveSet["IP"] = IP

        IPs = dataSet.get("IPs") 
        if IPs:
            saveSet["IPs"] = IPs

        os = dataSet.get("os") 
        if os:
            saveSet["os"] = os

        osVersion = dataSet.get("osVersion") 
        if osVersion:
            saveSet["osVersion"] = osVersion

        mac = dataSet.get("mac") 
        if mac:
            saveSet["mac"] = mac

        cpuCount = dataSet.get("cpuCount") 
        if cpuCount:
            try:
                cpuCount = int(dataSet.get("cpuCount")) 
                saveSet["cpuCount"] = cpuCount
            except:
                pass

        cpuLoad = dataSet.get("cpuLoad") 
        if cpuLoad:
            try:
                cpuLoad = int(dataSet.get("cpuLoad")) 
                saveSet["cpuLoad"] = cpuLoad
            except:
                pass

        RAMTotal = dataSet.get("RAMTotal") 
        if RAMTotal:
            saveSet["RAMTotal"] = RAMTotal

        RAMUsed = dataSet.get("RAMUsed") 
        if RAMUsed:
            saveSet["RAMUsed"] = RAMUsed

        RAMFree = dataSet.get("RAMFree") 
        if RAMFree:
            saveSet["RAMFree"] = RAMFree

        RAMPercent = dataSet.get("RAMPercent") 
        if RAMPercent:
            try:
                RAMPercent = int(dataSet.get("RAMPercent")) 
                saveSet["RAMPercent"] = RAMPercent
            except:
                pass

        disk = dataSet.get("disk") 
        if disk:
            saveSet["disk"] = disk

        diskTotal = dataSet.get("diskTotal") 
        if diskTotal:
            saveSet["diskTotal"] = diskTotal

        diskUsed = dataSet.get("diskUsed") 
        if diskUsed:
            saveSet["diskUsed"] = diskUsed

        diskPercent = dataSet.get("diskPercent") 
        if diskPercent:
            try:
                diskPercent = int(dataSet.get("diskPercent")) 
                saveSet["diskPercent"] = diskPercent
            except:
                pass

        processorInfo = dataSet.get("processorInfo") 
        if processorInfo:
            saveSet["processorInfo"] = processorInfo

        addtionalInfo = dataSet.get("addtionalInfo") 
        if addtionalInfo:
            saveSet["addtionalInfo"] = addtionalInfo

        YMDHMS = dataSet.get("YMDHMS") 
        if YMDHMS:
            saveSet["YMDHMS"] = YMDHMS

        label1 = dataSet.get("label1") 
        if label1:
            saveSet["label1"] = label1

        label2 = dataSet.get("label2") 
        if label2:
            saveSet["label2"] = label2

        label3 = dataSet.get("label3") 
        if label3:
            saveSet["label3"] = label3

        memo = dataSet.get("memo") 
        if memo:
            saveSet["memo"] = memo

        modifyID = dataSet.get("modifyID") 
        if modifyID:
            saveSet["modifyID"] = modifyID

        modifyYMDHMS = dataSet.get("modifyYMDHMS") 
        if modifyYMDHMS:
            saveSet["modifyYMDHMS"] = modifyYMDHMS

        dispFlag = dataSet.get("dispFlag") 
        if dispFlag:
            saveSet["dispFlag"] = dispFlag

        delFlag = dataSet.get("delFlag") 
        if delFlag:
            saveSet["delFlag"] = delFlag

        keySqlstr = "recID = %s"
        keyValues = [recID]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#hwinfo_report_record 查询记录
def query_hwinfo_report_record(tableName,recID = "0", hostName = "", YMDHMS="", sortFlag = "DESC",
                               delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            recID = int(recID)
        except:
            recID = 0

        if recID > 0:
            sqlStr =  sqlStr + " WHERE recID = %s" 
            valuesList = [recID]  
        else:
            if hostName:
                sqlStr =  sqlStr + " WHERE hostName = %s" 
                valuesList = [hostName]  
            elif YMDHMS:
                sqlStr =  sqlStr + " WHERE regYMDHMS <= %s"
                valuesList = [YMDHMS]  
            
            if sortFlag == "DESC":
                sqlStr = sqlStr + " ORDER BY recID DESC"

            if limitNum > 0:
                sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            if dataList:
                dataList = dataFormatConvert(dataList)
                result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#hwinfo_report_record end 


#application end


def checkMySqlDataBase():
    YMDHMS = misc.getTime()
    currYear = YMDHMS[0:4]
    YM = YMDHMS[0:6]
    YMD = YMDHMS[0:8]
    
    #user basic
    tableName = "USER_BASIC"
    if chkTableExist(tableName) == False:
        rtn = createUserBasic()

    # tableName = "USER_weChatCode"
    # if chkTableExist(tableName) == False:
    #     createUserWechatCode()

    tableName = tablename_convertor_hwinfo_report_record()
    if chkTableExist(tableName) == False:
        rtn = create_hwinfo_report_record(tableName)


def dropMySqlDataBase():
    YMDHMS = misc.getTime()
    currYear = YMDHMS[0:4]
    YM = YMDHMS[0:6]
    YMD = YMDHMS[0:8]
    
    #user basic
    tableName = "USER_BASIC"
    if chkTableExist(tableName):
        rtn = dropUserBasic()

    # tableName = "USER_weChatCode"
    # if chkTableExist(tableName) == False:
    #     createUserWechatCode()

    tableName = tablename_convertor_hwinfo_report_record()
    if chkTableExist(tableName):
        rtn = drop_hwinfo_report_record(tableName)


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

