#!/usr/bin/env python3
# -*- coding: utf-8 -*-


#Filename: mysqlCommon.py  
#Date: 2020-04-01
#Description:   mysql 处理代码

#mysql数据库信息也存储在这里, 主要是只有部分程序需要处理mysql数据库, 读写已经分离, 目前主要是采用sql语句处理, 已经防止注入攻击. 


_VERSION="20260428"

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
    "extSessionID VARCHAR(48) COMMENT '扩展用户sessionID',",
    "extCapital float COMMENT '扩展用户资金',",
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
        
        saveSet["extSessionID"] = dataSet.get("extSessionID", "") 

        try:
            extCapital = float(dataSet.get("extCapital")) 
        except:
            extCapital = 0.0 
        saveSet["extCapital"] = extCapital

        saveSet["extStartYMDHMS"] = dataSet.get("extStartYMDHMS", "") 

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

        extSessionID = dataSet.get("extSessionID") 
        if extSessionID:
            saveSet["extSessionID"] = extSessionID
            
        try:
            extCapital = float(dataSet.get("extCapital")) 
        except:
            extCapital = 0.0 
        saveSet["extCapital"] = extCapital

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
            aSet["extSessionID"] = currDataSet.get("extSessionID","")
            
            try:
                extCapital = float(currDataSet.get("extCapital")) 
            except:
                extCapital = 0.0 
            aSet["extCapital"] = extCapital
            
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

        try:
            cpuCount = int(dataSet.get("cpuCount")) 
            saveSet["cpuCount"] = cpuCount
        except:
            pass

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
def query_hwinfo_report_record(tableName,recID = "0", hostName = "", YMDHMS="", beginYMDHMS="", endYMDHMS="", sortFlag = "DESC",
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
                if valuesList:
                    sqlStr =  sqlStr + " AND hostName = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE hostName = %s" 
                valuesList.append(hostName)
            if YMDHMS:
                if valuesList:
                    sqlStr =  sqlStr + " AND YMDHMS <= %s"
                else:
                    sqlStr =  sqlStr + " WHERE YMDHMS <= %s"
                valuesList.append(YMDHMS)            
            if beginYMDHMS:
                if valuesList:
                    sqlStr =  sqlStr + " AND YMDHMS >= %s"
                else:
                    sqlStr =  sqlStr + " WHERE YMDHMS >= %s"
                valuesList.append(beginYMDHMS)            
            if endYMDHMS:
                if valuesList:
                    sqlStr =  sqlStr + " AND YMDHMS < %s"
                else:
                    sqlStr =  sqlStr + " WHERE YMDHMS < %s"
                valuesList.append(endYMDHMS)            

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

#industry_info begin 

def tablename_convertor_industry_info():
    tableName = "industry_info"
    tableName = tableName.lower()
    return tableName


def decode_tablename_industry_info(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建industry_info表
def create_industry_info(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "industry_code VARCHAR(10) COMMENT '行业代码',",
    "industry_code_sw VARCHAR(12) COMMENT '行业代码别名',",
    "industry_name VARCHAR(48) COMMENT '行业名称',",
    "industry_name_sw VARCHAR(48) COMMENT '行业名称申银万国',",
    "industry_name_em VARCHAR(48) COMMENT '行业名称东方财富',",
    "parenet_industry VARCHAR(48) COMMENT '上级行业',",
    "parenet_industry_sw VARCHAR(48) COMMENT '上级行业申银万国',",
    "parenet_industry_em VARCHAR(48) COMMENT '上级行业东方财富',",
    "industry_level_sw VARCHAR(2) COMMENT '行业级别申银万国',",
    "industry_level_em VARCHAR(2) COMMENT '行业级别东方财富',",
    "num_of_constituents float COMMENT '成份个数',",
    "static_PE_ratio float COMMENT '静态市盈率',",
    "TTM_PE_ratio float COMMENT 'TTM(滚动)市盈率',",
    "PB_ratio float COMMENT '市净率',",
    "static_divident_yield float COMMENT '静态股息率',",
    "volatility_rating VARCHAR(4) COMMENT '波动率评级',",
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
        sqlStr = "CREATE UNIQUE INDEX {1} ON {0}({1}) ".format(tableName, "industry_code")
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除industry_info表
def drop_industry_info(tableName):
    result = dropTableGeneral(tableName)
    return result


#industry_info 删除记录
def delete_industry_info(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#industry_info 增加记录
def insert_industry_info(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["industry_code"] = dataSet.get("industry_code", "") 

        saveSet["industry_code_sw"] = dataSet.get("industry_code_sw", "") 

        saveSet["industry_name"] = dataSet.get("industry_name", "") 

        saveSet["industry_name_sw"] = dataSet.get("industry_name_sw", "") 

        saveSet["industry_name_em"] = dataSet.get("industry_name_em", "") 

        saveSet["parenet_industry"] = dataSet.get("parenet_industry", "") 

        saveSet["parenet_industry_sw"] = dataSet.get("parenet_industry_sw", "") 

        saveSet["parenet_industry_em"] = dataSet.get("parenet_industry_em", "") 

        saveSet["industry_level_sw"] = dataSet.get("industry_level_sw", "") 

        saveSet["industry_level_em"] = dataSet.get("industry_level_em", "") 

        try:
            num_of_constituents = float(dataSet.get("num_of_constituents")) 
        except:
            num_of_constituents = 0 
        saveSet["num_of_constituents"] = num_of_constituents

        try:
            static_PE_ratio = float(dataSet.get("static_PE_ratio")) 
        except:
            static_PE_ratio = 0 
        saveSet["static_PE_ratio"] = static_PE_ratio

        try:
            TTM_PE_ratio = float(dataSet.get("TTM_PE_ratio")) 
        except:
            TTM_PE_ratio = 0 
        saveSet["TTM_PE_ratio"] = TTM_PE_ratio

        try:
            PB_ratio = float(dataSet.get("PB_ratio")) 
        except:
            PB_ratio = 0 
        saveSet["PB_ratio"] = PB_ratio

        try:
            static_divident_yield = float(dataSet.get("static_divident_yield")) 
        except:
            static_divident_yield = 0 
        saveSet["static_divident_yield"] = static_divident_yield

        saveSet["volatility_rating"] = dataSet.get("volatility_rating", "") 

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


#industry_info 修改记录
def update_industry_info(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        industry_code = dataSet.get("industry_code") 
        if industry_code:
            saveSet["industry_code"] = industry_code

        industry_code_sw = dataSet.get("industry_code_sw") 
        if industry_code_sw:
            saveSet["industry_code_sw"] = industry_code_sw

        industry_name = dataSet.get("industry_name") 
        if industry_name:
            saveSet["industry_name"] = industry_name

        industry_name_sw = dataSet.get("industry_name_sw") 
        if industry_name_sw:
            saveSet["industry_name_sw"] = industry_name_sw

        industry_name_em = dataSet.get("industry_name_em") 
        if industry_name_em:
            saveSet["industry_name_em"] = industry_name_em

        parenet_industry = dataSet.get("parenet_industry") 
        if parenet_industry:
            saveSet["parenet_industry"] = parenet_industry

        parenet_industry_sw = dataSet.get("parenet_industry_sw") 
        if parenet_industry_sw:
            saveSet["parenet_industry_sw"] = parenet_industry_sw

        parenet_industry_em = dataSet.get("parenet_industry_em") 
        if parenet_industry_em:
            saveSet["parenet_industry_em"] = parenet_industry_em

        industry_level_sw = dataSet.get("industry_level_sw") 
        if industry_level_sw:
            saveSet["industry_level_sw"] = industry_level_sw

        industry_level_em = dataSet.get("industry_level_em") 
        if industry_level_em:
            saveSet["industry_level_em"] = industry_level_em

        try:
            num_of_constituents = float(dataSet.get("num_of_constituents")) 
            saveSet["num_of_constituents"] = num_of_constituents
        except:
            pass

        try:
            static_PE_ratio = float(dataSet.get("static_PE_ratio")) 
            saveSet["static_PE_ratio"] = static_PE_ratio
        except:
            pass

        try:
            TTM_PE_ratio = float(dataSet.get("TTM_PE_ratio")) 
            saveSet["TTM_PE_ratio"] = TTM_PE_ratio
        except:
            pass

        try:
            PB_ratio = float(dataSet.get("PB_ratio")) 
            saveSet["PB_ratio"] = PB_ratio
        except:
            pass

        try:
            static_divident_yield = float(dataSet.get("static_divident_yield")) 
            saveSet["static_divident_yield"] = static_divident_yield
        except:
            pass

        volatility_rating = dataSet.get("volatility_rating") 
        if volatility_rating:
            saveSet["volatility_rating"] = volatility_rating

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#industry_info 查询记录
def query_industry_info(tableName,id = "0", industry_code = "", industry_name = "",
                        delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)

        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if industry_code:
                if valuesList:
                    sqlStr =  sqlStr + " AND industry_code = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE industry_code = %s" 
                valuesList.append(industry_code)

            if industry_name:
                if valuesList:
                    sqlStr =  sqlStr + " AND industry_name = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE industry_name = %s" 

                valuesList.append(industry_name)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#industry_info end 


#industry_history_data begin 

def tablename_convertor_industry_history_data(period = "day",old=""):
    if old:
        tableName = f"industry_history_data_{period}_{old}"
    else:
        tableName = f"industry_history_data_{period}"
    tableName = tableName.lower()
    return tableName


def decode_tablename_industry_history_data(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建industry_history_data表
def create_industry_history_data(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "industry_code VARCHAR(10) COMMENT '行业代码',",
    "`date` VARCHAR(10) COMMENT '日期',",
    "`open` float COMMENT '开盘',",
    "`close` float COMMENT '收盘',",
    "high float COMMENT '最高',",
    "low float COMMENT '最低',",
    "volume float COMMENT '成交量',",
    "amount float COMMENT '成交额',",
    "amplitude float COMMENT '振幅',",
    "pct_change float COMMENT '涨跌幅',",
    "price_change float COMMENT '涨跌额',",
    "turnover_rate float COMMENT '换手率',",
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
        index_name = f"{tableName}_industry_date_index"
        sqlStr = f"CREATE UNIQUE INDEX {index_name} ON {tableName} ({'industry_code'},{'date'}) "
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除industry_history_data表
def drop_industry_history_data(tableName):
    result = dropTableGeneral(tableName)
    return result


#industry_history_data 删除记录
def delete_industry_history_data(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#industry_history_data 增加记录
def insert_industry_history_data(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["industry_code"] = dataSet.get("industry_code", "") 

        saveSet["date"] = dataSet.get("date", "") 

        try:
            open = float(dataSet.get("open")) 
        except:
            open = 0 
        saveSet["open"] = open

        try:
            close = float(dataSet.get("close")) 
        except:
            close = 0 
        saveSet["close"] = close

        try:
            high = float(dataSet.get("high")) 
        except:
            high = 0 
        saveSet["high"] = high

        try:
            low = float(dataSet.get("low")) 
        except:
            low = 0 
        saveSet["low"] = low

        try:
            volume = float(dataSet.get("volume")) 
        except:
            volume = 0 
        saveSet["volume"] = volume

        try:
            amount = float(dataSet.get("amount")) 
        except:
            amount = 0 
        saveSet["amount"] = amount

        try:
            amplitude = float(dataSet.get("amplitude")) 
        except:
            amplitude = 0 
        saveSet["amplitude"] = amplitude

        try:
            pct_change = float(dataSet.get("pct_change")) 
        except:
            pct_change = 0 
        saveSet["pct_change"] = pct_change

        try:
            price_change = float(dataSet.get("price_change")) 
        except:
            price_change = 0 
        saveSet["price_change"] = price_change

        try:
            turnover_rate = float(dataSet.get("turnover_rate")) 
        except:
            turnover_rate = 0 
        saveSet["turnover_rate"] = turnover_rate

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


#industry_history_data 修改记录
def update_industry_history_data(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        industry_code = dataSet.get("industry_code") 
        if industry_code:
            saveSet["industry_code"] = industry_code

        date = dataSet.get("date") 
        if date:
            saveSet["date"] = date

        try:
            open = float(dataSet.get("open")) 
            saveSet["open"] = open
        except:
            pass

        try:
            close = float(dataSet.get("close")) 
            saveSet["close"] = close
        except:
            pass

        try:
            high = float(dataSet.get("high")) 
            saveSet["high"] = high
        except:
            pass

        try:
            low = float(dataSet.get("low")) 
            saveSet["low"] = low
        except:
            pass

        try:
            volume = float(dataSet.get("volume")) 
            saveSet["volume"] = volume
        except:
            pass

        try:
            amount = float(dataSet.get("amount")) 
            saveSet["amount"] = amount
        except:
            pass

        try:
            amplitude = float(dataSet.get("amplitude")) 
            saveSet["amplitude"] = amplitude
        except:
            pass

        try:
            pct_change = float(dataSet.get("pct_change")) 
            saveSet["pct_change"] = pct_change
        except:
            pass

        try:
            price_change = float(dataSet.get("price_change")) 
            saveSet["price_change"] = price_change
        except:
            pass

        try:
            turnover_rate = float(dataSet.get("turnover_rate")) 
            saveSet["turnover_rate"] = turnover_rate
        except:
            pass

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#industry_history_data 查询记录
def query_industry_history_data(tableName,id = "0", industry_code = "",start_date = "",end_date = "",date = "",
                                delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)
        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if industry_code:
                if valuesList:
                    sqlStr += " AND industry_code = %s"
                else:
                    sqlStr += " WHERE industry_code = %s"
                valuesList.append(industry_code)
            if start_date:
                if valuesList:
                    sqlStr += " AND date >= %s"
                else:
                    sqlStr += " WHERE date >= %s"
                valuesList.append(start_date)
            if end_date:
                if valuesList:
                    sqlStr += " AND date < %s"
                else:
                    sqlStr += " WHERE date < %s"
                valuesList.append(end_date)
            if date:
                if valuesList:
                    sqlStr += " AND date = %s"
                else:
                    sqlStr += " WHERE date = %s"
                valuesList.append(date)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#industry_history_data end 


#stock_info begin 

def tablename_convertor_stock_info():
    tableName = "stock_info"
    tableName = tableName.lower()
    return tableName


def decode_tablename_stock_info(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建stock_info表
def create_stock_info(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "stock_code VARCHAR(10) COMMENT '股票代码',",
    "stock_name VARCHAR(12) COMMENT '股票名称',",
    "total_shares_outstanding double COMMENT '总股本',",
    "public_float double COMMENT '流通股',",
    "market_cap double COMMENT '总市值',",
    "free_market_cap double COMMENT '流通市值',",
    "dcf_value_per_share float COMMENT '每股贴现现金流价值',",
    "industry_code VARCHAR(10) COMMENT '行业代码',",
    "industry_name VARCHAR(48) COMMENT '行业名称',",
    "industry_name_sw VARCHAR(48) COMMENT '行业申银万国',",
    "industry_name_em VARCHAR(48) COMMENT '行业东方财富',",
    "area VARCHAR(40) COMMENT '地点',",
    "market VARCHAR(10) COMMENT '市场区域',",
    "ipo_date VARCHAR(10) COMMENT '上市时间',",
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
        sqlStr = "CREATE UNIQUE INDEX {1} ON {0}({1}) ".format(tableName, "stock_code")
        rtn = mysqlDB.executeWrite(sqlStr)
        sqlStr = "CREATE INDEX {1} ON {0}({1}) ".format(tableName, "industry_code")
        rtn = mysqlDB.executeWrite(sqlStr)
        sqlStr = "CREATE INDEX {1} ON {0}({1}) ".format(tableName, "industry_name")
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除stock_info表
def drop_stock_info(tableName):
    result = dropTableGeneral(tableName)
    return result


#stock_info 删除记录
def delete_stock_info(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#stock_info 增加记录
def insert_stock_info(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["stock_code"] = dataSet.get("stock_code", "") 

        saveSet["stock_name"] = dataSet.get("stock_name", "") 

        try:
            total_shares_outstanding = float(dataSet.get("total_shares_outstanding")) 
        except:
            total_shares_outstanding = 0 
        saveSet["total_shares_outstanding"] = total_shares_outstanding

        try:
            public_float = float(dataSet.get("public_float")) 
        except:
            public_float = 0 
        saveSet["public_float"] = public_float

        try:
            market_cap = float(dataSet.get("market_cap")) 
        except:
            market_cap = 0 
        saveSet["market_cap"] = market_cap

        try:
            free_market_cap = float(dataSet.get("free_market_cap")) 
        except:
            free_market_cap = 0 
        saveSet["free_market_cap"] = free_market_cap

        try:
            dcf_value_per_share = float(dataSet.get("dcf_value_per_share")) 
        except:
            dcf_value_per_share = 0 
        saveSet["dcf_value_per_share"] = dcf_value_per_share

        saveSet["industry_code"] = dataSet.get("industry_code", "") 

        saveSet["industry_name"] = dataSet.get("industry_name", "") 

        saveSet["industry_name_sw"] = dataSet.get("industry_name_sw", "") 

        saveSet["industry_name_em"] = dataSet.get("industry_name_em", "") 

        saveSet["area"] = dataSet.get("area", "") 

        saveSet["market"] = dataSet.get("market", "") 

        saveSet["ipo_date"] = dataSet.get("ipo_date", "") 

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


#stock_info 修改记录
def update_stock_info(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        stock_code = dataSet.get("stock_code") 
        if stock_code:
            saveSet["stock_code"] = stock_code

        stock_name = dataSet.get("stock_name") 
        if stock_name:
            saveSet["stock_name"] = stock_name

        try:
            total_shares_outstanding = float(dataSet.get("total_shares_outstanding")) 
            saveSet["total_shares_outstanding"] = total_shares_outstanding
        except:
            pass

        try:
            public_float = float(dataSet.get("public_float")) 
            saveSet["public_float"] = public_float
        except:
            pass

        try:
            market_cap = float(dataSet.get("market_cap")) 
            saveSet["market_cap"] = market_cap
        except:
            pass

        try:
            free_market_cap = float(dataSet.get("free_market_cap")) 
            saveSet["free_market_cap"] = free_market_cap
        except:
            pass

        try:
            dcf_value_per_share = float(dataSet.get("dcf_value_per_share")) 
            saveSet["dcf_value_per_share"] = dcf_value_per_share
        except:
            pass

        industry_code = dataSet.get("industry_code") 
        if industry_code:
            saveSet["industry_code"] = industry_code

        industry_name = dataSet.get("industry_name") 
        if industry_name:
            saveSet["industry_name"] = industry_name

        industry_name_sw = dataSet.get("industry_name_sw") 
        if industry_name_sw:
            saveSet["industry_name_sw"] = industry_name_sw

        industry_name_em = dataSet.get("industry_name_em") 
        if industry_name_em:
            saveSet["industry_name_em"] = industry_name_em

        area = dataSet.get("area") 
        if area:
            saveSet["area"] = area

        market = dataSet.get("market") 
        if market:
            saveSet["market"] = market

        ipo_date = dataSet.get("ipo_date") 
        if ipo_date:
            saveSet["ipo_date"] = ipo_date

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#stock_info 查询记录
def query_stock_info(tableName,id = "0",stock_code = "",stock_name = "",industry_code = "",industry_name = "",
                 searchKey="",delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)

        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if stock_code:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_code = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE stock_code = %s" 
                valuesList.append(stock_code)

            if industry_code:
                if valuesList:
                    sqlStr =  sqlStr + " AND industry_code = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE industry_code = %s" 
                valuesList.append(industry_code)

            if stock_name:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_name = %s" 
                else:   
                    sqlStr =  sqlStr + " WHERE stock_name = %s" 
                valuesList.append(stock_name)

            if industry_name:
                if valuesList:
                    sqlStr =  sqlStr + " AND industry_name = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE industry_name = %s" 
                valuesList.append(industry_name)    
            
            if searchKey:
                if valuesList:
                    sqlStr =  sqlStr + " AND (stock_code LIKE %s OR stock_name LIKE %s ) " 
                else:
                    sqlStr =  sqlStr + " WHERE (stock_code LIKE %s OR stock_name LIKE %s ) " 
                aKey = "%" + searchKey + "%"
                valuesList.append(aKey)
                valuesList.append(aKey)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#query stock dcf data
def query_user_stock_dcf_value_per_share(tableName,id = "0",stock_code = "",stock_name = "",userID="",
                 searchKey="",delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    tableName = tablename_convertor_stock_info()
    tableName2 = tablename_convertor_user_stock_list()
    if mode == "full":
        columns = "T1.*"
    else:
        columns = "T1.id,T1.stock_code,T1.stock_name,T1.dcf_value_per_share"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName} as T1,{tableName2} as T2 WHERE T1.stock_code=T2.stock_code AND T2.user_id={userID}"

    try:

        if stock_code:
            sqlStr =  sqlStr + " AND T1.stock_code = %s" 
            valuesList.append(stock_code)

        if stock_name:
            sqlStr =  sqlStr + " AND T1.stock_name = %s" 
            valuesList.append(stock_name)
       
        if searchKey:
            sqlStr =  sqlStr + " AND (T1.stock_code LIKE %s OR T1.stock_name LIKE %s ) " 
            aKey = "%" + searchKey + "%"
            valuesList.append(aKey)
            valuesList.append(aKey)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#stock_info end 


#stock_history_data begin 
#period: day,week,month
def tablename_convertor_stock_history_data(period = "day",adjust="",old=""):
    if old:
        if adjust:
            tableName = f"stock_history_data_{period}_{adjust}_{old}"
        else:
            tableName = f"stock_history_data_{period}_{old}"
    else:
        if adjust:
            tableName = f"stock_history_data_{period}_{adjust}"
        else:
            tableName = f"stock_history_data_{period}"
    tableName = tableName.lower()
    return tableName


def decode_tablename_stock_history_data(tableName):
    result = {}
    aList = tableName.split("_")
    if len(aList) == 3:
        result["period"] = aList[2]
        result["old"] = ""
    elif len(aList) == 4:
        result["period"] = aList[2]
        result["old"] = aList[3]
    return result


#创建stock_history_data表
def create_stock_history_data(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "stock_code VARCHAR(10) COMMENT '股票代码',",
    "`date` VARCHAR(10) COMMENT '日期',",
    "`open` float COMMENT '开盘',",
    "`close` float COMMENT '收盘',",
    "`high`float COMMENT '最高',",
    "`low` float COMMENT '最低',",
    "volume float COMMENT '成交量',",
    "amount float COMMENT '成交额',",
    "amplitude float COMMENT '振幅',",
    "pct_change float COMMENT '涨跌幅',",
    "price_change float COMMENT '涨跌额',",
    "turnover_rate float COMMENT '换手率',",
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
        index_name = f"{tableName}_stock_date_idx"
        sqlStr = f"CREATE UNIQUE INDEX {index_name} ON {tableName} ({'stock_code'},{'date'}) "
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除stock_history_data表
def drop_stock_history_data(tableName):
    result = dropTableGeneral(tableName)
    return result


#stock_history_data 删除记录
def delete_stock_history_data(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#stock_history_data 增加记录
def insert_stock_history_data(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["stock_code"] = dataSet.get("stock_code", "") 

        saveSet["date"] = dataSet.get("date", "") 

        try:
            open = float(dataSet.get("open")) 
        except:
            open = 0 
        saveSet["open"] = open

        try:
            close = float(dataSet.get("close")) 
        except:
            close = 0 
        saveSet["close"] = close

        try:
            high = float(dataSet.get("high")) 
        except:
            high = 0 
        saveSet["high"] = high

        try:
            low = float(dataSet.get("low")) 
        except:
            low = 0 
        saveSet["low"] = low

        try:
            volume = float(dataSet.get("volume")) 
        except:
            volume = 0 
        saveSet["volume"] = volume

        try:
            amount = float(dataSet.get("amount")) 
        except:
            amount = 0 
        saveSet["amount"] = amount

        try:
            amplitude = float(dataSet.get("amplitude")) 
        except:
            amplitude = 0 
        saveSet["amplitude"] = amplitude

        try:
            pct_change = float(dataSet.get("pct_change")) 
        except:
            pct_change = 0 
        saveSet["pct_change"] = pct_change

        try:
            price_change = float(dataSet.get("price_change")) 
        except:
            price_change = 0 
        saveSet["price_change"] = price_change

        try:
            turnover_rate = float(dataSet.get("turnover_rate")) 
        except:
            turnover_rate = 0 
        saveSet["turnover_rate"] = turnover_rate

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


#stock_history_data 修改记录
def update_stock_history_data(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        stock_code = dataSet.get("stock_code") 
        if stock_code:
            saveSet["stock_code"] = stock_code

        date = dataSet.get("date") 
        if date:
            saveSet["date"] = date

        try:
            open = float(dataSet.get("open")) 
            saveSet["open"] = open
        except:
            pass

        try:
            close = float(dataSet.get("close")) 
            saveSet["close"] = close
        except:
            pass

        try:
            high = float(dataSet.get("high")) 
            saveSet["high"] = high
        except:
            pass

        try:
            low = float(dataSet.get("low")) 
            saveSet["low"] = low
        except:
            pass

        try:
            volume = float(dataSet.get("volume")) 
            saveSet["volume"] = volume
        except:
            pass

        try:
            amount = float(dataSet.get("amount")) 
            saveSet["amount"] = amount
        except:
            pass

        try:
            amplitude = float(dataSet.get("amplitude")) 
            saveSet["amplitude"] = amplitude
        except:
            pass

        try:
            pct_change = float(dataSet.get("pct_change")) 
            saveSet["pct_change"] = pct_change
        except:
            pass

        try:
            price_change = float(dataSet.get("price_change")) 
            saveSet["price_change"] = price_change
        except:
            pass

        try:
            turnover_rate = float(dataSet.get("turnover_rate")) 
            saveSet["turnover_rate"] = turnover_rate
        except:
            pass

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#stock_history_data 查询记录
def query_stock_history_data(tableName,id = "0", stock_code="",stock_name="",start_date="",end_date="",date="",
                            delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    if mode == "full":
        columns = "*"
    else:
        columns = "id,date,stock_code"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:
        try:
            id = int(id)
        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if stock_code:
                if valuesList:
                    sqlStr += " AND stock_code = %s"
                else:
                    sqlStr += " WHERE stock_code = %s"
                valuesList.append(stock_code)
            if stock_name:
                if valuesList:
                    sqlStr += " AND stock_name = %s"
                else:
                    sqlStr += " WHERE stock_name = %s"
                valuesList.append(stock_name)
            if date:
                if valuesList:
                    sqlStr += " AND date = %s"
                else:
                    sqlStr += " WHERE date = %s"
                valuesList.append(date)
            if start_date:
                if valuesList:
                    sqlStr += " AND date >= %s"
                else:
                    sqlStr += " WHERE date >= %s"
                valuesList.append(start_date)
            if end_date:
                if valuesList:
                    sqlStr += " AND date < %s"
                else:
                    sqlStr += " WHERE date < %s"
                valuesList.append(end_date)

        sqlStr =  sqlStr + " ORDER BY date" # 按日期排序,从小到大排序

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#stock_history_data end 


#technical_indicators begin 

def tablename_convertor_technical_indicators(period = "day",adjust="",old="",industry=False):
    if industry:
        if old:
            tableName = f"industry_indicators_{period}_{old}"
        else:
            tableName = f"industry_indicators_{period}"
    else:
        if old:
            if adjust:
                tableName = f"technical_indicators_{period}_{adjust}_{old}"
            else:
                tableName = f"technical_indicators_{period}_{old}"
        else:
            if adjust:
                tableName = f"technical_indicators_{period}_{adjust}"
            else:
                tableName = f"technical_indicators_{period}"
    tableName = tableName.lower()
    return tableName


def decode_tablename_technical_indicators(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建technical_indicators表
def create_technical_indicators(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "stock_code VARCHAR(10) NOT NULL COMMENT '股票代码',",
    "`date` VARCHAR(10) NOT NULL COMMENT '交易日期',",
    "`close` FLOAT COMMENT '收盘',",
    "ma_5 FLOAT COMMENT '5日均线',",
    "ma_10 FLOAT COMMENT '10日均线',",
    "ma_20 FLOAT COMMENT '20日均线',",
    "ma_60 FLOAT COMMENT '60日均线',",
    "macd_line FLOAT COMMENT 'MACD快线(DIF)',",
    "macd_signal FLOAT COMMENT 'MACD慢线(DEA)',",
    "macd_histogram FLOAT COMMENT 'MACD柱状图',",
    "macd_line_long FLOAT COMMENT 'MACD快线(DIF)长线',",
    "macd_signal_long FLOAT COMMENT 'MACD慢线(DEA)长线',",
    "macd_histogram_long FLOAT COMMENT 'MACD柱状图长线',",
    "boll_upper FLOAT COMMENT '布林上轨',",
    "boll_mid FLOAT COMMENT '布林中轨',",
    "boll_lower FLOAT COMMENT '布林下轨',",
    "ene_upper FLOAT COMMENT 'ENE上轨' ,",
    "ene_mid FLOAT COMMENT 'ENE中轨',",
    "ene_lower FLOAT COMMENT 'ENE下轨',",
    "dmi_pdi FLOAT COMMENT '上升方向线',",
    "dmi_mdi FLOAT COMMENT '下降方向线',",
    "dmi_adx FLOAT COMMENT '趋向平均值',",
    "dma_line FLOAT COMMENT 'DMA线（白线）',",
    "ama_line FLOAT COMMENT 'AMA线（黄线）',",
    "sar FLOAT COMMENT 'SAR停损点（抛物线指标）',",
    "kdj_k FLOAT COMMENT 'KDJ-K值',",
    "kdj_d FLOAT COMMENT 'KDJ-D值',",
    "kdj_j FLOAT COMMENT 'KDJ-J值',",
    "rsi_6 FLOAT COMMENT '6日RSI',",
    "rsi_12 FLOAT COMMENT '12日RSI',",
    "rsi_24 FLOAT COMMENT '24日RSI',",
    "cci FLOAT COMMENT 'CCI指标',",
    "bias_5 FLOAT COMMENT '5日乖离率(百分比)',",
    "bias_10 FLOAT COMMENT '10日乖离率(百分比)',",
    "bias_20 FLOAT COMMENT '20日乖离率(百分比)',",
    "wr_6 FLOAT COMMENT '6日威廉指标(百分比)',",
    "wr_14 FLOAT COMMENT '14日威廉指标(百分比)',",
    "volume BIGINT COMMENT '成交量(股)',",
    "turnover_rate FLOAT COMMENT '换手率(百分比)',",
    "obv FLOAT COMMENT '能量潮指标',",
    "hashval VARCHAR(32) NULL COMMENT 'hash值',",
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
        index_name = f"{tableName}_stock_date_idx"
        sqlStr = f"CREATE UNIQUE INDEX {index_name} ON {tableName} ({'stock_code'},{'date'}) "
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "CREATE INDEX {1} ON {0}({1}) ".format(tableName, "indexKey")
        #rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除technical_indicators表
def drop_technical_indicators(tableName):
    result = dropTableGeneral(tableName)
    return result


#technical_indicators 删除记录
def delete_technical_indicators(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#technical_indicators 增加记录
def insert_technical_indicators(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["stock_code"] = dataSet.get("stock_code", "") 

        saveSet["date"] = dataSet.get("date", "") 

        try:
            close = float(dataSet.get("close")) 
        except:
            close = 0 
        saveSet["close"] = close

        try:
            ma_5 = float(dataSet.get("ma_5")) 
        except:
            ma_5 = 0 
        saveSet["ma_5"] = ma_5

        try:
            ma_10 = float(dataSet.get("ma_10")) 
        except:
            ma_10 = 0 
        saveSet["ma_10"] = ma_10

        try:
            ma_20 = float(dataSet.get("ma_20")) 
        except:
            ma_20 = 0 
        saveSet["ma_20"] = ma_20

        try:
            ma_60 = float(dataSet.get("ma_60")) 
        except:
            ma_60 = 0 
        saveSet["ma_60"] = ma_60

        try:
            macd_line = float(dataSet.get("macd_line")) 
        except:
            macd_line = 0 
        saveSet["macd_line"] = macd_line

        try:
            macd_signal = float(dataSet.get("macd_signal")) 
        except:
            macd_signal = 0 
        saveSet["macd_signal"] = macd_signal

        try:
            macd_histogram = float(dataSet.get("macd_histogram")) 
        except:
            macd_histogram = 0 
        saveSet["macd_histogram"] = macd_histogram

        try:
            macd_line_long = float(dataSet.get("macd_line_long")) 
        except:
            macd_line_long = 0 
        saveSet["macd_line_long"] = macd_line_long

        try:
            macd_signal_long = float(dataSet.get("macd_signal_long")) 
        except:
            macd_signal_long = 0 
        saveSet["macd_signal_long"] = macd_signal_long

        try:
            macd_histogram_long = float(dataSet.get("macd_histogram_long")) 
        except:
            macd_histogram_long = 0 
        saveSet["macd_histogram_long"] = macd_histogram_long

        try:
            boll_upper = float(dataSet.get("boll_upper")) 
        except:
            boll_upper = 0 
        saveSet["boll_upper"] = boll_upper

        try:
            boll_mid = float(dataSet.get("boll_mid")) 
        except:
            boll_mid = 0 
        saveSet["boll_mid"] = boll_mid

        try:
            boll_lower = float(dataSet.get("boll_lower")) 
        except:
            boll_lower = 0 
        saveSet["boll_lower"] = boll_lower

        try:
            ene_upper = float(dataSet.get("ene_upper")) 
        except:
            ene_upper = 0 
        saveSet["ene_upper"] = ene_upper

        try:
            ene_mid = float(dataSet.get("ene_mid")) 
        except:
            ene_mid = 0 
        saveSet["ene_mid"] = ene_mid

        try:
            ene_lower = float(dataSet.get("ene_lower")) 
        except:
            ene_lower = 0 
        saveSet["ene_lower"] = ene_lower

        try:
            dmi_pdi = float(dataSet.get("dmi_pdi")) 
        except:
            dmi_pdi = 0 
        saveSet["dmi_pdi"] = dmi_pdi

        try:
            dmi_mdi = float(dataSet.get("dmi_mdi")) 
        except:
            dmi_mdi = 0 
        saveSet["dmi_mdi"] = dmi_mdi

        try:
            dmi_adx = float(dataSet.get("dmi_adx")) 
        except:
            dmi_adx = 0 
        saveSet["dmi_adx"] = dmi_adx

        try:
            dma_line = float(dataSet.get("dma_line")) 
        except:
            dma_line = 0 
        saveSet["dma_line"] = dma_line

        try:
            ama_line = float(dataSet.get("ama_line")) 
        except:
            ama_line = 0 
        saveSet["ama_line"] = ama_line

        try:
            sar = float(dataSet.get("sar")) 
        except:
            sar = 0 
        saveSet["sar"] = sar

        try:
            kdj_k = float(dataSet.get("kdj_k")) 
        except:
            kdj_k = 0 
        saveSet["kdj_k"] = kdj_k

        try:
            kdj_d = float(dataSet.get("kdj_d")) 
        except:
            kdj_d = 0 
        saveSet["kdj_d"] = kdj_d

        try:
            kdj_j = float(dataSet.get("kdj_j")) 
        except:
            kdj_j = 0 
        saveSet["kdj_j"] = kdj_j

        try:
            rsi_6 = float(dataSet.get("rsi_6")) 
        except:
            rsi_6 = 0 
        saveSet["rsi_6"] = rsi_6

        try:
            rsi_12 = float(dataSet.get("rsi_12")) 
        except:
            rsi_12 = 0 
        saveSet["rsi_12"] = rsi_12

        try:
            rsi_24 = float(dataSet.get("rsi_24")) 
        except:
            rsi_24 = 0 
        saveSet["rsi_24"] = rsi_24

        try:
            cci = float(dataSet.get("cci")) 
        except:
            cci = 0 
        saveSet["cci"] = cci

        try:
            bias_5 = float(dataSet.get("bias_5")) 
        except:
            bias_5 = 0 
        saveSet["bias_5"] = bias_5

        try:
            bias_10 = float(dataSet.get("bias_10")) 
        except:
            bias_10 = 0 
        saveSet["bias_10"] = bias_10

        try:
            bias_20 = float(dataSet.get("bias_20")) 
        except:
            bias_20 = 0 
        saveSet["bias_20"] = bias_20

        try:
            wr_6 = float(dataSet.get("wr_6")) 
        except:
            wr_6 = 0 
        saveSet["wr_6"] = wr_6

        try:
            wr_14 = float(dataSet.get("wr_14")) 
        except:
            wr_14 = 0 
        saveSet["wr_14"] = wr_14

        try:
            volume = int(dataSet.get("volume")) 
        except:
            volume = 0 
        saveSet["volume"] = volume

        try:
            turnover_rate = float(dataSet.get("turnover_rate")) 
        except:
            turnover_rate = 0 
        saveSet["turnover_rate"] = turnover_rate

        try:
            obv = float(dataSet.get("obv")) 
        except:
            obv = 0 
        saveSet["obv"] = obv

        saveSet["hashval"] = dataSet.get("hashval", "") 

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


#technical_indicators 修改记录
def update_technical_indicators(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        stock_code = dataSet.get("stock_code") 
        if stock_code:
            saveSet["stock_code"] = stock_code

        date = dataSet.get("date") 
        if date:
            saveSet["date"] = date

        try:
            close = float(dataSet.get("close")) 
            saveSet["close"] = close
        except:
            pass

        try:
            ma_5 = float(dataSet.get("ma_5")) 
            saveSet["ma_5"] = ma_5
        except:
            pass

        try:
            ma_10 = float(dataSet.get("ma_10")) 
            saveSet["ma_10"] = ma_10
        except:
            pass

        try:
            ma_20 = float(dataSet.get("ma_20")) 
            saveSet["ma_20"] = ma_20
        except:
            pass

        try:
            ma_60 = float(dataSet.get("ma_60")) 
            saveSet["ma_60"] = ma_60
        except:
            pass

        try:
            macd_line = float(dataSet.get("macd_line")) 
            saveSet["macd_line"] = macd_line
        except:
            pass

        try:
            macd_signal = float(dataSet.get("macd_signal")) 
            saveSet["macd_signal"] = macd_signal
        except:
            pass

        try:
            macd_histogram = float(dataSet.get("macd_histogram")) 
            saveSet["macd_histogram"] = macd_histogram
        except:
            pass

        try:
            macd_line_long = float(dataSet.get("macd_line_long")) 
            saveSet["macd_line_long"] = macd_line_long
        except:
            pass

        try:
            macd_signal_long = float(dataSet.get("macd_signal_long")) 
            saveSet["macd_signal_long"] = macd_signal_long
        except:
            pass

        try:
            macd_histogram_long = float(dataSet.get("macd_histogram_long")) 
            saveSet["macd_histogram_long"] = macd_histogram_long
        except:
            pass

        try:
            boll_upper = float(dataSet.get("boll_upper")) 
            saveSet["boll_upper"] = boll_upper
        except:
            pass

        try:
            boll_mid = float(dataSet.get("boll_mid")) 
            saveSet["boll_mid"] = boll_mid
        except:
            pass

        try:
            boll_lower = float(dataSet.get("boll_lower")) 
            saveSet["boll_lower"] = boll_lower
        except:
            pass

        try:
            ene_upper = float(dataSet.get("ene_upper")) 
            saveSet["ene_upper"] = ene_upper
        except:
            pass

        try:
            ene_mid = float(dataSet.get("ene_mid")) 
            saveSet["ene_mid"] = ene_mid
        except:
            pass

        try:
            ene_lower = float(dataSet.get("ene_lower")) 
            saveSet["ene_lower"] = ene_lower
        except:
            pass

        try:
            dmi_pdi = float(dataSet.get("dmi_pdi")) 
            saveSet["dmi_pdi"] = dmi_pdi
        except:
            pass

        try:
            dmi_mdi = float(dataSet.get("dmi_mdi")) 
            saveSet["dmi_mdi"] = dmi_mdi
        except:
            pass

        try:
            dmi_adx = float(dataSet.get("dmi_adx")) 
            saveSet["dmi_adx"] = dmi_adx
        except:
            pass

        try:
            dma_line = float(dataSet.get("dma_line")) 
            saveSet["dma_line"] = dma_line
        except:
            pass

        try:
            ama_line = float(dataSet.get("ama_line")) 
            saveSet["ama_line"] = ama_line
        except:
            pass

        try:
            sar = float(dataSet.get("sar")) 
            saveSet["sar"] = sar
        except:
            pass

        try:
            kdj_k = float(dataSet.get("kdj_k")) 
            saveSet["kdj_k"] = kdj_k
        except:
            pass

        try:
            kdj_d = float(dataSet.get("kdj_d")) 
            saveSet["kdj_d"] = kdj_d
        except:
            pass

        try:
            kdj_j = float(dataSet.get("kdj_j")) 
            saveSet["kdj_j"] = kdj_j
        except:
            pass

        try:
            rsi_6 = float(dataSet.get("rsi_6")) 
            saveSet["rsi_6"] = rsi_6
        except:
            pass

        try:
            rsi_12 = float(dataSet.get("rsi_12")) 
            saveSet["rsi_12"] = rsi_12
        except:
            pass

        try:
            rsi_24 = float(dataSet.get("rsi_24")) 
            saveSet["rsi_24"] = rsi_24
        except:
            pass

        try:
            cci = float(dataSet.get("cci")) 
            saveSet["cci"] = cci
        except:
            pass

        try:
            bias_5 = float(dataSet.get("bias_5")) 
            saveSet["bias_5"] = bias_5
        except:
            pass

        try:
            bias_10 = float(dataSet.get("bias_10")) 
            saveSet["bias_10"] = bias_10
        except:
            pass

        try:
            bias_20 = float(dataSet.get("bias_20")) 
            saveSet["bias_20"] = bias_20
        except:
            pass

        try:
            wr_6 = float(dataSet.get("wr_6")) 
            saveSet["wr_6"] = wr_6
        except:
            pass

        try:
            wr_14 = float(dataSet.get("wr_14")) 
            saveSet["wr_14"] = wr_14
        except:
            pass

        try:
            volume = int(dataSet.get("volume")) 
            saveSet["volume"] = volume
        except:
            pass

        try:
            turnover_rate = float(dataSet.get("turnover_rate")) 
            saveSet["turnover_rate"] = turnover_rate
        except:
            pass

        try:
            obv = float(dataSet.get("obv")) 
            saveSet["obv"] = obv
        except:
            pass

        hashval = dataSet.get("hashval") 
        if hashval:
            saveSet["hashval"] = hashval

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#technical_indicators 查询记录
def query_technical_indicators(tableName,id = "0", stock_code="",date="",start_date="",end_date="",
                                delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    if mode == "full":
        columns = "*"
    else:
        columns = "id,date,stock_code "
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)
        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if stock_code:
                if valuesList:
                    sqlStr += " AND stock_code = %s"
                else:
                    sqlStr += " WHERE stock_code = %s"
                valuesList.append(stock_code)
            if date:
                if valuesList:
                    sqlStr += " AND date = %s"
                else:
                    sqlStr += " WHERE date = %s"
                valuesList.append(date)
            if start_date:
                if valuesList:
                    sqlStr += " AND date >= %s "
                else:
                    sqlStr += " WHERE date >= %s"
                valuesList.append(start_date)
            if end_date:    
                if valuesList:
                    sqlStr += " AND date < %s"
                else:
                    sqlStr += " WHERE date < %s"
                valuesList.append(end_date)
        
        sqlStr =  sqlStr + " ORDER BY date" 

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


# technical_indicators 和 history 数据合并查询
def query_history_technical_indicator(stock_code="",date="",start_date="",end_date="",period="day",adjust="",limitNum=0):
    result = []
    columns = ""
    valuesList = []
    historyTableName = tablename_convertor_stock_history_data(period,adjust)
    indicatorTableName = tablename_convertor_technical_indicators(period,adjust)
    sqlStr = f"SELECT T2.*, T1.open as open, T1.high as high, T1.low as low,T1.amount as amount, \
        T1.amplitude as amplitude FROM {historyTableName} as T1,\
        {indicatorTableName} as T2 WHERE T1.stock_code = T2.stock_code AND T1.date = T2.date"

    try:
        if stock_code:
            sqlStr += " AND T1.stock_code = %s"
            valuesList.append(stock_code)
        if date:
            sqlStr += " AND T1.date = %s"
            valuesList.append(date)
        if start_date:
            sqlStr += " AND T1.date >= %s"
            valuesList.append(start_date)
        if end_date:    
            sqlStr += " AND T1.date < %s"
            valuesList.append(end_date)
        sqlStr += " ORDER BY T1.date"

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)
        pass
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result



#technical_indicators end 

#stock_dividend_data begin 

def tablename_convertor_stock_dividend_data():
    tableName = "stock_dividend_data"
    tableName = tableName.lower()
    return tableName


def decode_tablename_stock_dividend_data(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建stock_dividend_data表
def create_stock_dividend_data(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "stock_code VARCHAR(10) COMMENT '股票代码',",
    "stock_name VARCHAR(12) COMMENT '股票名称',",
    "ipo_date VARCHAR(10) COMMENT '上市时间',",
    "cumulative_dividend double COMMENT '累计股息',",
    "annual_dividend double COMMENT '年均股息',",
    "dividend_count float COMMENT '分红次数',",
    "total_financing double COMMENT '融资总额',",
    "financing_count float COMMENT '融资次数',",
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
        sqlStr = "CREATE UNIQUE INDEX {1} ON {0}({1}) ".format(tableName, "stock_code")
        rtn = mysqlDB.executeWrite(sqlStr)
        sqlStr = "CREATE INDEX {1} ON {0}({1}) ".format(tableName, "stock_name")
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除stock_dividend_data表
def drop_stock_dividend_data(tableName):
    result = dropTableGeneral(tableName)
    return result


#stock_dividend_data 删除记录
def delete_stock_dividend_data(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#stock_dividend_data 增加记录
def insert_stock_dividend_data(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["stock_code"] = dataSet.get("stock_code", "") 

        saveSet["stock_name"] = dataSet.get("stock_name", "") 

        saveSet["ipo_date"] = dataSet.get("ipo_date", "") 

        try:
            cumulative_dividend = float(dataSet.get("cumulative_dividend")) 
        except:
            cumulative_dividend = 0 
        saveSet["cumulative_dividend"] = cumulative_dividend

        try:
            annual_dividend = float(dataSet.get("annual_dividend")) 
        except:
            annual_dividend = 0 
        saveSet["annual_dividend"] = annual_dividend

        try:
            dividend_count = float(dataSet.get("dividend_count")) 
        except:
            dividend_count = 0 
        saveSet["dividend_count"] = dividend_count

        try:
            total_financing = float(dataSet.get("total_financing")) 
        except:
            total_financing = 0 
        saveSet["total_financing"] = total_financing

        try:
            financing_count = float(dataSet.get("financing_count")) 
        except:
            financing_count = 0 
        saveSet["financing_count"] = financing_count

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


#stock_dividend_data 修改记录
def update_stock_dividend_data(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        stock_code = dataSet.get("stock_code") 
        if stock_code:
            saveSet["stock_code"] = stock_code

        stock_name = dataSet.get("stock_name") 
        if stock_name:
            saveSet["stock_name"] = stock_name

        ipo_date = dataSet.get("ipo_date") 
        if ipo_date:
            saveSet["ipo_date"] = ipo_date

        try:
            cumulative_dividend = float(dataSet.get("cumulative_dividend")) 
            saveSet["cumulative_dividend"] = cumulative_dividend
        except:
            pass

        try:
            annual_dividend = float(dataSet.get("annual_dividend")) 
            saveSet["annual_dividend"] = annual_dividend
        except:
            pass

        try:
            dividend_count = float(dataSet.get("dividend_count")) 
            saveSet["dividend_count"] = dividend_count
        except:
            pass

        try:
            total_financing = float(dataSet.get("total_financing")) 
            saveSet["total_financing"] = total_financing
        except:
            pass

        try:
            financing_count = float(dataSet.get("financing_count")) 
            saveSet["financing_count"] = financing_count
        except:
            pass

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#stock_dividend_data 查询记录
def query_stock_dividend_data(tableName,id = "0", stock_code = "", stock_name = "",
                            delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)

        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
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

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#stock_dividend_data end 


#balance_sheets begin 

def tablename_convertor_balance_sheets():
    tableName = "balance_sheets"
    tableName = tableName.lower()
    return tableName


def decode_tablename_balance_sheets(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建balance_sheets表
def create_balance_sheets(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "stock_code VARCHAR(10) COMMENT '股票代码',",
    "report_date VARCHAR(10) NULL,",
    "monetary_capital FLOAT NULL,",
    "settlement_provisions FLOAT NULL,",
    "loans_to_other_banks FLOAT NULL,",
    "trading_financial_assets FLOAT NULL,",
    "financial_assets_purchased_for_resale FLOAT NULL,",
    "derivative_financial_assets FLOAT NULL,",
    "notes_and_accounts_receivable FLOAT NULL,",
    "notes_receivable FLOAT NULL,",
    "accounts_receivable FLOAT NULL,",
    "receivables_financing FLOAT NULL,",
    "prepayments FLOAT NULL,",
    "dividends_receivable FLOAT NULL,",
    "interest_receivable FLOAT NULL,",
    "insurance_premiums_receivable FLOAT NULL,",
    "reinsurance_receivables FLOAT NULL,",
    "reinsurance_contract_reserves_receivable FLOAT NULL,",
    "export_tax_rebates_receivable FLOAT NULL,",
    "subsidies_receivable FLOAT NULL,",
    "deposits_receivable FLOAT NULL,",
    "internal_receivables FLOAT NULL,",
    "other_receivables FLOAT NULL,",
    "other_receivables_total FLOAT NULL,",
    "inventories FLOAT NULL,",
    "assets_held_for_sale FLOAT NULL,",
    "prepaid_expenses FLOAT NULL,",
    "current_assets_pending_disposal FLOAT NULL,",
    "non_current_assets_due_within_one_year FLOAT NULL,",
    "other_current_assets FLOAT NULL,",
    "total_current_assets FLOAT NULL,",
    "non_current_assets FLOAT NULL,",
    "loans_and_advances FLOAT NULL,",
    "debt_investments FLOAT NULL,",
    "other_debt_investments FLOAT NULL,",
    "financial_assets_at_fvoci FLOAT NULL,",
    "financial_assets_at_amortized_cost FLOAT NULL,",
    "available_for_sale_financial_assets FLOAT NULL,",
    "long_term_equity_investments FLOAT NULL,",
    "investment_property FLOAT NULL,",
    "long_term_receivables FLOAT NULL,",
    "other_equity_instrument_investments FLOAT NULL,",
    "other_non_current_financial_assets FLOAT NULL,",
    "other_long_term_investments FLOAT NULL,",
    "fixed_assets_original_value FLOAT NULL,",
    "accumulated_depreciation FLOAT NULL,",
    "fixed_assets_net_value FLOAT NULL,",
    "fixed_assets_impairment_provision FLOAT NULL,",
    "construction_in_progress_total FLOAT NULL,",
    "construction_in_progress FLOAT NULL,",
    "construction_materials FLOAT NULL,",
    "fixed_assets_net FLOAT NULL,",
    "fixed_assets_disposal FLOAT NULL,",
    "fixed_assets_and_disposal_total FLOAT NULL,",
    "productive_biological_assets FLOAT NULL,",
    "consumptive_biological_assets FLOAT NULL,",
    "oil_and_gas_assets FLOAT NULL,",
    "contract_assets FLOAT NULL,",
    "right_of_use_assets FLOAT NULL,",
    "intangible_assets FLOAT NULL,",
    "development_expenditure FLOAT NULL,",
    "goodwill FLOAT NULL,",
    "long_term_deferred_expenses FLOAT NULL,",
    "split_share_structure_circulation_rights FLOAT NULL,",
    "deferred_tax_assets FLOAT NULL,",
    "other_non_current_assets FLOAT NULL,",
    "total_non_current_assets FLOAT NULL,",
    "total_assets FLOAT NULL,",
    "current_liabilities FLOAT NULL,",
    "short_term_borrowings FLOAT NULL,",
    "borrowings_from_central_bank FLOAT NULL,",
    "deposits_from_customers_and_banks FLOAT NULL,",
    "borrowings_from_other_banks FLOAT NULL,",
    "trading_financial_liabilities FLOAT NULL,",
    "derivative_financial_liabilities FLOAT NULL,",
    "notes_and_accounts_payable FLOAT NULL,",
    "notes_payable FLOAT NULL,",
    "accounts_payable FLOAT NULL,",
    "advances_from_customers FLOAT NULL,",
    "contract_liabilities FLOAT NULL,",
    "financial_assets_sold_for_repurchase FLOAT NULL,",
    "fees_and_commissions_payable FLOAT NULL,",
    "employee_benefits_payable FLOAT NULL,",
    "taxes_payable FLOAT NULL,",
    "interest_payable FLOAT NULL,",
    "dividends_payable FLOAT NULL,",
    "deposits_payable FLOAT NULL,",
    "internal_payables FLOAT NULL,",
    "other_payables FLOAT NULL,",
    "other_payables_total FLOAT NULL,",
    "other_taxes_payable FLOAT NULL,",
    "guarantee_liability_reserves FLOAT NULL,",
    "reinsurance_payables FLOAT NULL,",
    "insurance_contract_reserves FLOAT NULL,",
    "securities_trading_agency_payables FLOAT NULL,",
    "securities_underwriting_agency_payables FLOAT NULL,",
    "international_settlement FLOAT NULL,",
    "domestic_settlement FLOAT NULL,",
    "accrued_expenses FLOAT NULL,",
    "estimated_current_liabilities FLOAT NULL,",
    "short_term_bonds_payable FLOAT NULL,",
    "liabilities_held_for_sale FLOAT NULL,",
    "deferred_revenue_due_within_one_year FLOAT NULL,",
    "non_current_liabilities_due_within_one_year FLOAT NULL,",
    "other_current_liabilities FLOAT NULL,",
    "total_current_liabilities FLOAT NULL,",
    "non_current_liabilities FLOAT NULL,",
    "long_term_borrowings FLOAT NULL,",
    "bonds_payable FLOAT NULL,",
    "bonds_payable_preferred_stock FLOAT NULL,",
    "bonds_payable_perpetual_bonds FLOAT NULL,",
    "lease_liabilities FLOAT NULL,",
    "long_term_employee_benefits_payable FLOAT NULL,",
    "long_term_payables FLOAT NULL,",
    "long_term_payables_total FLOAT NULL,",
    "special_payables FLOAT NULL,",
    "estimated_non_current_liabilities FLOAT NULL,",
    "long_term_deferred_revenue FLOAT NULL,",
    "deferred_tax_liabilities FLOAT NULL,",
    "other_non_current_liabilities FLOAT NULL,",
    "total_non_current_liabilities FLOAT NULL,",
    "total_liabilities FLOAT NULL,",
    "owners_equity FLOAT NULL,",
    "paid_in_capital FLOAT NULL,",
    "other_equity_instruments FLOAT NULL,",
    "preferred_stock FLOAT NULL,",
    "perpetual_bonds FLOAT NULL,",
    "capital_reserve FLOAT NULL,",
    "less_treasury_stock FLOAT NULL,",
    "other_comprehensive_income FLOAT NULL,",
    "special_reserve FLOAT NULL,",
    "surplus_reserve FLOAT NULL,",
    "general_risk_reserve FLOAT NULL,",
    "unrecognized_investment_losses FLOAT NULL,",
    "retained_earnings FLOAT NULL,",
    "proposed_cash_dividends FLOAT NULL,",
    "foreign_currency_translation_difference FLOAT NULL,",
    "equity_attributable_to_parent_company FLOAT NULL,",
    "minority_interests FLOAT NULL,",
    "total_owners_equity FLOAT NULL,",
    "total_liabilities_and_owners_equity FLOAT NULL,",
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
        sqlStr = "CREATE UNIQUE INDEX stock_report_index ON {0} ({1},{2}) ".format(tableName, "stock_code","report_date")
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除balance_sheets表
def drop_balance_sheets(tableName):
    result = dropTableGeneral(tableName)
    return result


#balance_sheets 删除记录
def delete_balance_sheets(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#balance_sheets 增加记录
def insert_balance_sheets(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["stock_code"] = dataSet.get("stock_code", "") 

        saveSet["report_date"] = dataSet.get("report_date", "") 

        try:
            monetary_capital = float(dataSet.get("monetary_capital")) 
        except:
            monetary_capital = 0 
        saveSet["monetary_capital"] = monetary_capital

        try:
            settlement_provisions = float(dataSet.get("settlement_provisions")) 
        except:
            settlement_provisions = 0 
        saveSet["settlement_provisions"] = settlement_provisions

        try:
            loans_to_other_banks = float(dataSet.get("loans_to_other_banks")) 
        except:
            loans_to_other_banks = 0 
        saveSet["loans_to_other_banks"] = loans_to_other_banks

        try:
            trading_financial_assets = float(dataSet.get("trading_financial_assets")) 
        except:
            trading_financial_assets = 0 
        saveSet["trading_financial_assets"] = trading_financial_assets

        try:
            financial_assets_purchased_for_resale = float(dataSet.get("financial_assets_purchased_for_resale")) 
        except:
            financial_assets_purchased_for_resale = 0 
        saveSet["financial_assets_purchased_for_resale"] = financial_assets_purchased_for_resale

        try:
            derivative_financial_assets = float(dataSet.get("derivative_financial_assets")) 
        except:
            derivative_financial_assets = 0 
        saveSet["derivative_financial_assets"] = derivative_financial_assets

        try:
            notes_and_accounts_receivable = float(dataSet.get("notes_and_accounts_receivable")) 
        except:
            notes_and_accounts_receivable = 0 
        saveSet["notes_and_accounts_receivable"] = notes_and_accounts_receivable

        try:
            notes_receivable = float(dataSet.get("notes_receivable")) 
        except:
            notes_receivable = 0 
        saveSet["notes_receivable"] = notes_receivable

        try:
            accounts_receivable = float(dataSet.get("accounts_receivable")) 
        except:
            accounts_receivable = 0 
        saveSet["accounts_receivable"] = accounts_receivable

        try:
            receivables_financing = float(dataSet.get("receivables_financing")) 
        except:
            receivables_financing = 0 
        saveSet["receivables_financing"] = receivables_financing

        try:
            prepayments = float(dataSet.get("prepayments")) 
        except:
            prepayments = 0 
        saveSet["prepayments"] = prepayments

        try:
            dividends_receivable = float(dataSet.get("dividends_receivable")) 
        except:
            dividends_receivable = 0 
        saveSet["dividends_receivable"] = dividends_receivable

        try:
            interest_receivable = float(dataSet.get("interest_receivable")) 
        except:
            interest_receivable = 0 
        saveSet["interest_receivable"] = interest_receivable

        try:
            insurance_premiums_receivable = float(dataSet.get("insurance_premiums_receivable")) 
        except:
            insurance_premiums_receivable = 0 
        saveSet["insurance_premiums_receivable"] = insurance_premiums_receivable

        try:
            reinsurance_receivables = float(dataSet.get("reinsurance_receivables")) 
        except:
            reinsurance_receivables = 0 
        saveSet["reinsurance_receivables"] = reinsurance_receivables

        try:
            reinsurance_contract_reserves_receivable = float(dataSet.get("reinsurance_contract_reserves_receivable")) 
        except:
            reinsurance_contract_reserves_receivable = 0 
        saveSet["reinsurance_contract_reserves_receivable"] = reinsurance_contract_reserves_receivable

        try:
            export_tax_rebates_receivable = float(dataSet.get("export_tax_rebates_receivable")) 
        except:
            export_tax_rebates_receivable = 0 
        saveSet["export_tax_rebates_receivable"] = export_tax_rebates_receivable

        try:
            subsidies_receivable = float(dataSet.get("subsidies_receivable")) 
        except:
            subsidies_receivable = 0 
        saveSet["subsidies_receivable"] = subsidies_receivable

        try:
            deposits_receivable = float(dataSet.get("deposits_receivable")) 
        except:
            deposits_receivable = 0 
        saveSet["deposits_receivable"] = deposits_receivable

        try:
            internal_receivables = float(dataSet.get("internal_receivables")) 
        except:
            internal_receivables = 0 
        saveSet["internal_receivables"] = internal_receivables

        try:
            other_receivables = float(dataSet.get("other_receivables")) 
        except:
            other_receivables = 0 
        saveSet["other_receivables"] = other_receivables

        try:
            other_receivables_total = float(dataSet.get("other_receivables_total")) 
        except:
            other_receivables_total = 0 
        saveSet["other_receivables_total"] = other_receivables_total

        try:
            inventories = float(dataSet.get("inventories")) 
        except:
            inventories = 0 
        saveSet["inventories"] = inventories

        try:
            assets_held_for_sale = float(dataSet.get("assets_held_for_sale")) 
        except:
            assets_held_for_sale = 0 
        saveSet["assets_held_for_sale"] = assets_held_for_sale

        try:
            prepaid_expenses = float(dataSet.get("prepaid_expenses")) 
        except:
            prepaid_expenses = 0 
        saveSet["prepaid_expenses"] = prepaid_expenses

        try:
            current_assets_pending_disposal = float(dataSet.get("current_assets_pending_disposal")) 
        except:
            current_assets_pending_disposal = 0 
        saveSet["current_assets_pending_disposal"] = current_assets_pending_disposal

        try:
            non_current_assets_due_within_one_year = float(dataSet.get("non_current_assets_due_within_one_year")) 
        except:
            non_current_assets_due_within_one_year = 0 
        saveSet["non_current_assets_due_within_one_year"] = non_current_assets_due_within_one_year

        try:
            other_current_assets = float(dataSet.get("other_current_assets")) 
        except:
            other_current_assets = 0 
        saveSet["other_current_assets"] = other_current_assets

        try:
            total_current_assets = float(dataSet.get("total_current_assets")) 
        except:
            total_current_assets = 0 
        saveSet["total_current_assets"] = total_current_assets

        try:
            non_current_assets = float(dataSet.get("non_current_assets")) 
        except:
            non_current_assets = 0 
        saveSet["non_current_assets"] = non_current_assets

        try:
            loans_and_advances = float(dataSet.get("loans_and_advances")) 
        except:
            loans_and_advances = 0 
        saveSet["loans_and_advances"] = loans_and_advances

        try:
            debt_investments = float(dataSet.get("debt_investments")) 
        except:
            debt_investments = 0 
        saveSet["debt_investments"] = debt_investments

        try:
            other_debt_investments = float(dataSet.get("other_debt_investments")) 
        except:
            other_debt_investments = 0 
        saveSet["other_debt_investments"] = other_debt_investments

        try:
            financial_assets_at_fvoci = float(dataSet.get("financial_assets_at_fvoci")) 
        except:
            financial_assets_at_fvoci = 0 
        saveSet["financial_assets_at_fvoci"] = financial_assets_at_fvoci

        try:
            financial_assets_at_amortized_cost = float(dataSet.get("financial_assets_at_amortized_cost")) 
        except:
            financial_assets_at_amortized_cost = 0 
        saveSet["financial_assets_at_amortized_cost"] = financial_assets_at_amortized_cost

        try:
            available_for_sale_financial_assets = float(dataSet.get("available_for_sale_financial_assets")) 
        except:
            available_for_sale_financial_assets = 0 
        saveSet["available_for_sale_financial_assets"] = available_for_sale_financial_assets

        try:
            long_term_equity_investments = float(dataSet.get("long_term_equity_investments")) 
        except:
            long_term_equity_investments = 0 
        saveSet["long_term_equity_investments"] = long_term_equity_investments

        try:
            investment_property = float(dataSet.get("investment_property")) 
        except:
            investment_property = 0 
        saveSet["investment_property"] = investment_property

        try:
            long_term_receivables = float(dataSet.get("long_term_receivables")) 
        except:
            long_term_receivables = 0 
        saveSet["long_term_receivables"] = long_term_receivables

        try:
            other_equity_instrument_investments = float(dataSet.get("other_equity_instrument_investments")) 
        except:
            other_equity_instrument_investments = 0 
        saveSet["other_equity_instrument_investments"] = other_equity_instrument_investments

        try:
            other_non_current_financial_assets = float(dataSet.get("other_non_current_financial_assets")) 
        except:
            other_non_current_financial_assets = 0 
        saveSet["other_non_current_financial_assets"] = other_non_current_financial_assets

        try:
            other_long_term_investments = float(dataSet.get("other_long_term_investments")) 
        except:
            other_long_term_investments = 0 
        saveSet["other_long_term_investments"] = other_long_term_investments

        try:
            fixed_assets_original_value = float(dataSet.get("fixed_assets_original_value")) 
        except:
            fixed_assets_original_value = 0 
        saveSet["fixed_assets_original_value"] = fixed_assets_original_value

        try:
            accumulated_depreciation = float(dataSet.get("accumulated_depreciation")) 
        except:
            accumulated_depreciation = 0 
        saveSet["accumulated_depreciation"] = accumulated_depreciation

        try:
            fixed_assets_net_value = float(dataSet.get("fixed_assets_net_value")) 
        except:
            fixed_assets_net_value = 0 
        saveSet["fixed_assets_net_value"] = fixed_assets_net_value

        try:
            fixed_assets_impairment_provision = float(dataSet.get("fixed_assets_impairment_provision")) 
        except:
            fixed_assets_impairment_provision = 0 
        saveSet["fixed_assets_impairment_provision"] = fixed_assets_impairment_provision

        try:
            construction_in_progress_total = float(dataSet.get("construction_in_progress_total")) 
        except:
            construction_in_progress_total = 0 
        saveSet["construction_in_progress_total"] = construction_in_progress_total

        try:
            construction_in_progress = float(dataSet.get("construction_in_progress")) 
        except:
            construction_in_progress = 0 
        saveSet["construction_in_progress"] = construction_in_progress

        try:
            construction_materials = float(dataSet.get("construction_materials")) 
        except:
            construction_materials = 0 
        saveSet["construction_materials"] = construction_materials

        try:
            fixed_assets_net = float(dataSet.get("fixed_assets_net")) 
        except:
            fixed_assets_net = 0 
        saveSet["fixed_assets_net"] = fixed_assets_net

        try:
            fixed_assets_disposal = float(dataSet.get("fixed_assets_disposal")) 
        except:
            fixed_assets_disposal = 0 
        saveSet["fixed_assets_disposal"] = fixed_assets_disposal

        try:
            fixed_assets_and_disposal_total = float(dataSet.get("fixed_assets_and_disposal_total")) 
        except:
            fixed_assets_and_disposal_total = 0 
        saveSet["fixed_assets_and_disposal_total"] = fixed_assets_and_disposal_total

        try:
            productive_biological_assets = float(dataSet.get("productive_biological_assets")) 
        except:
            productive_biological_assets = 0 
        saveSet["productive_biological_assets"] = productive_biological_assets

        try:
            consumptive_biological_assets = float(dataSet.get("consumptive_biological_assets")) 
        except:
            consumptive_biological_assets = 0 
        saveSet["consumptive_biological_assets"] = consumptive_biological_assets

        try:
            oil_and_gas_assets = float(dataSet.get("oil_and_gas_assets")) 
        except:
            oil_and_gas_assets = 0 
        saveSet["oil_and_gas_assets"] = oil_and_gas_assets

        try:
            contract_assets = float(dataSet.get("contract_assets")) 
        except:
            contract_assets = 0 
        saveSet["contract_assets"] = contract_assets

        try:
            right_of_use_assets = float(dataSet.get("right_of_use_assets")) 
        except:
            right_of_use_assets = 0 
        saveSet["right_of_use_assets"] = right_of_use_assets

        try:
            intangible_assets = float(dataSet.get("intangible_assets")) 
        except:
            intangible_assets = 0 
        saveSet["intangible_assets"] = intangible_assets

        try:
            development_expenditure = float(dataSet.get("development_expenditure")) 
        except:
            development_expenditure = 0 
        saveSet["development_expenditure"] = development_expenditure

        try:
            goodwill = float(dataSet.get("goodwill")) 
        except:
            goodwill = 0 
        saveSet["goodwill"] = goodwill

        try:
            long_term_deferred_expenses = float(dataSet.get("long_term_deferred_expenses")) 
        except:
            long_term_deferred_expenses = 0 
        saveSet["long_term_deferred_expenses"] = long_term_deferred_expenses

        try:
            split_share_structure_circulation_rights = float(dataSet.get("split_share_structure_circulation_rights")) 
        except:
            split_share_structure_circulation_rights = 0 
        saveSet["split_share_structure_circulation_rights"] = split_share_structure_circulation_rights

        try:
            deferred_tax_assets = float(dataSet.get("deferred_tax_assets")) 
        except:
            deferred_tax_assets = 0 
        saveSet["deferred_tax_assets"] = deferred_tax_assets

        try:
            other_non_current_assets = float(dataSet.get("other_non_current_assets")) 
        except:
            other_non_current_assets = 0 
        saveSet["other_non_current_assets"] = other_non_current_assets

        try:
            total_non_current_assets = float(dataSet.get("total_non_current_assets")) 
        except:
            total_non_current_assets = 0 
        saveSet["total_non_current_assets"] = total_non_current_assets

        try:
            total_assets = float(dataSet.get("total_assets")) 
        except:
            total_assets = 0 
        saveSet["total_assets"] = total_assets

        try:
            current_liabilities = float(dataSet.get("current_liabilities")) 
        except:
            current_liabilities = 0 
        saveSet["current_liabilities"] = current_liabilities

        try:
            short_term_borrowings = float(dataSet.get("short_term_borrowings")) 
        except:
            short_term_borrowings = 0 
        saveSet["short_term_borrowings"] = short_term_borrowings

        try:
            borrowings_from_central_bank = float(dataSet.get("borrowings_from_central_bank")) 
        except:
            borrowings_from_central_bank = 0 
        saveSet["borrowings_from_central_bank"] = borrowings_from_central_bank

        try:
            deposits_from_customers_and_banks = float(dataSet.get("deposits_from_customers_and_banks")) 
        except:
            deposits_from_customers_and_banks = 0 
        saveSet["deposits_from_customers_and_banks"] = deposits_from_customers_and_banks

        try:
            borrowings_from_other_banks = float(dataSet.get("borrowings_from_other_banks")) 
        except:
            borrowings_from_other_banks = 0 
        saveSet["borrowings_from_other_banks"] = borrowings_from_other_banks

        try:
            trading_financial_liabilities = float(dataSet.get("trading_financial_liabilities")) 
        except:
            trading_financial_liabilities = 0 
        saveSet["trading_financial_liabilities"] = trading_financial_liabilities

        try:
            derivative_financial_liabilities = float(dataSet.get("derivative_financial_liabilities")) 
        except:
            derivative_financial_liabilities = 0 
        saveSet["derivative_financial_liabilities"] = derivative_financial_liabilities

        try:
            notes_and_accounts_payable = float(dataSet.get("notes_and_accounts_payable")) 
        except:
            notes_and_accounts_payable = 0 
        saveSet["notes_and_accounts_payable"] = notes_and_accounts_payable

        try:
            notes_payable = float(dataSet.get("notes_payable")) 
        except:
            notes_payable = 0 
        saveSet["notes_payable"] = notes_payable

        try:
            accounts_payable = float(dataSet.get("accounts_payable")) 
        except:
            accounts_payable = 0 
        saveSet["accounts_payable"] = accounts_payable

        try:
            advances_from_customers = float(dataSet.get("advances_from_customers")) 
        except:
            advances_from_customers = 0 
        saveSet["advances_from_customers"] = advances_from_customers

        try:
            contract_liabilities = float(dataSet.get("contract_liabilities")) 
        except:
            contract_liabilities = 0 
        saveSet["contract_liabilities"] = contract_liabilities

        try:
            financial_assets_sold_for_repurchase = float(dataSet.get("financial_assets_sold_for_repurchase")) 
        except:
            financial_assets_sold_for_repurchase = 0 
        saveSet["financial_assets_sold_for_repurchase"] = financial_assets_sold_for_repurchase

        try:
            fees_and_commissions_payable = float(dataSet.get("fees_and_commissions_payable")) 
        except:
            fees_and_commissions_payable = 0 
        saveSet["fees_and_commissions_payable"] = fees_and_commissions_payable

        try:
            employee_benefits_payable = float(dataSet.get("employee_benefits_payable")) 
        except:
            employee_benefits_payable = 0 
        saveSet["employee_benefits_payable"] = employee_benefits_payable

        try:
            taxes_payable = float(dataSet.get("taxes_payable")) 
        except:
            taxes_payable = 0 
        saveSet["taxes_payable"] = taxes_payable

        try:
            interest_payable = float(dataSet.get("interest_payable")) 
        except:
            interest_payable = 0 
        saveSet["interest_payable"] = interest_payable

        try:
            dividends_payable = float(dataSet.get("dividends_payable")) 
        except:
            dividends_payable = 0 
        saveSet["dividends_payable"] = dividends_payable

        try:
            deposits_payable = float(dataSet.get("deposits_payable")) 
        except:
            deposits_payable = 0 
        saveSet["deposits_payable"] = deposits_payable

        try:
            internal_payables = float(dataSet.get("internal_payables")) 
        except:
            internal_payables = 0 
        saveSet["internal_payables"] = internal_payables

        try:
            other_payables = float(dataSet.get("other_payables")) 
        except:
            other_payables = 0 
        saveSet["other_payables"] = other_payables

        try:
            other_payables_total = float(dataSet.get("other_payables_total")) 
        except:
            other_payables_total = 0 
        saveSet["other_payables_total"] = other_payables_total

        try:
            other_taxes_payable = float(dataSet.get("other_taxes_payable")) 
        except:
            other_taxes_payable = 0 
        saveSet["other_taxes_payable"] = other_taxes_payable

        try:
            guarantee_liability_reserves = float(dataSet.get("guarantee_liability_reserves")) 
        except:
            guarantee_liability_reserves = 0 
        saveSet["guarantee_liability_reserves"] = guarantee_liability_reserves

        try:
            reinsurance_payables = float(dataSet.get("reinsurance_payables")) 
        except:
            reinsurance_payables = 0 
        saveSet["reinsurance_payables"] = reinsurance_payables

        try:
            insurance_contract_reserves = float(dataSet.get("insurance_contract_reserves")) 
        except:
            insurance_contract_reserves = 0 
        saveSet["insurance_contract_reserves"] = insurance_contract_reserves

        try:
            securities_trading_agency_payables = float(dataSet.get("securities_trading_agency_payables")) 
        except:
            securities_trading_agency_payables = 0 
        saveSet["securities_trading_agency_payables"] = securities_trading_agency_payables

        try:
            securities_underwriting_agency_payables = float(dataSet.get("securities_underwriting_agency_payables")) 
        except:
            securities_underwriting_agency_payables = 0 
        saveSet["securities_underwriting_agency_payables"] = securities_underwriting_agency_payables

        try:
            international_settlement = float(dataSet.get("international_settlement")) 
        except:
            international_settlement = 0 
        saveSet["international_settlement"] = international_settlement

        try:
            domestic_settlement = float(dataSet.get("domestic_settlement")) 
        except:
            domestic_settlement = 0 
        saveSet["domestic_settlement"] = domestic_settlement

        try:
            accrued_expenses = float(dataSet.get("accrued_expenses")) 
        except:
            accrued_expenses = 0 
        saveSet["accrued_expenses"] = accrued_expenses

        try:
            estimated_current_liabilities = float(dataSet.get("estimated_current_liabilities")) 
        except:
            estimated_current_liabilities = 0 
        saveSet["estimated_current_liabilities"] = estimated_current_liabilities

        try:
            short_term_bonds_payable = float(dataSet.get("short_term_bonds_payable")) 
        except:
            short_term_bonds_payable = 0 
        saveSet["short_term_bonds_payable"] = short_term_bonds_payable

        try:
            liabilities_held_for_sale = float(dataSet.get("liabilities_held_for_sale")) 
        except:
            liabilities_held_for_sale = 0 
        saveSet["liabilities_held_for_sale"] = liabilities_held_for_sale

        try:
            deferred_revenue_due_within_one_year = float(dataSet.get("deferred_revenue_due_within_one_year")) 
        except:
            deferred_revenue_due_within_one_year = 0 
        saveSet["deferred_revenue_due_within_one_year"] = deferred_revenue_due_within_one_year

        try:
            non_current_liabilities_due_within_one_year = float(dataSet.get("non_current_liabilities_due_within_one_year")) 
        except:
            non_current_liabilities_due_within_one_year = 0 
        saveSet["non_current_liabilities_due_within_one_year"] = non_current_liabilities_due_within_one_year

        try:
            other_current_liabilities = float(dataSet.get("other_current_liabilities")) 
        except:
            other_current_liabilities = 0 
        saveSet["other_current_liabilities"] = other_current_liabilities

        try:
            total_current_liabilities = float(dataSet.get("total_current_liabilities")) 
        except:
            total_current_liabilities = 0 
        saveSet["total_current_liabilities"] = total_current_liabilities

        try:
            non_current_liabilities = float(dataSet.get("non_current_liabilities")) 
        except:
            non_current_liabilities = 0 
        saveSet["non_current_liabilities"] = non_current_liabilities

        try:
            long_term_borrowings = float(dataSet.get("long_term_borrowings")) 
        except:
            long_term_borrowings = 0 
        saveSet["long_term_borrowings"] = long_term_borrowings

        try:
            bonds_payable = float(dataSet.get("bonds_payable")) 
        except:
            bonds_payable = 0 
        saveSet["bonds_payable"] = bonds_payable

        try:
            bonds_payable_preferred_stock = float(dataSet.get("bonds_payable_preferred_stock")) 
        except:
            bonds_payable_preferred_stock = 0 
        saveSet["bonds_payable_preferred_stock"] = bonds_payable_preferred_stock

        try:
            bonds_payable_perpetual_bonds = float(dataSet.get("bonds_payable_perpetual_bonds")) 
        except:
            bonds_payable_perpetual_bonds = 0 
        saveSet["bonds_payable_perpetual_bonds"] = bonds_payable_perpetual_bonds

        try:
            lease_liabilities = float(dataSet.get("lease_liabilities")) 
        except:
            lease_liabilities = 0 
        saveSet["lease_liabilities"] = lease_liabilities

        try:
            long_term_employee_benefits_payable = float(dataSet.get("long_term_employee_benefits_payable")) 
        except:
            long_term_employee_benefits_payable = 0 
        saveSet["long_term_employee_benefits_payable"] = long_term_employee_benefits_payable

        try:
            long_term_payables = float(dataSet.get("long_term_payables")) 
        except:
            long_term_payables = 0 
        saveSet["long_term_payables"] = long_term_payables

        try:
            long_term_payables_total = float(dataSet.get("long_term_payables_total")) 
        except:
            long_term_payables_total = 0 
        saveSet["long_term_payables_total"] = long_term_payables_total

        try:
            special_payables = float(dataSet.get("special_payables")) 
        except:
            special_payables = 0 
        saveSet["special_payables"] = special_payables

        try:
            estimated_non_current_liabilities = float(dataSet.get("estimated_non_current_liabilities")) 
        except:
            estimated_non_current_liabilities = 0 
        saveSet["estimated_non_current_liabilities"] = estimated_non_current_liabilities

        try:
            long_term_deferred_revenue = float(dataSet.get("long_term_deferred_revenue")) 
        except:
            long_term_deferred_revenue = 0 
        saveSet["long_term_deferred_revenue"] = long_term_deferred_revenue

        try:
            deferred_tax_liabilities = float(dataSet.get("deferred_tax_liabilities")) 
        except:
            deferred_tax_liabilities = 0 
        saveSet["deferred_tax_liabilities"] = deferred_tax_liabilities

        try:
            other_non_current_liabilities = float(dataSet.get("other_non_current_liabilities")) 
        except:
            other_non_current_liabilities = 0 
        saveSet["other_non_current_liabilities"] = other_non_current_liabilities

        try:
            total_non_current_liabilities = float(dataSet.get("total_non_current_liabilities")) 
        except:
            total_non_current_liabilities = 0 
        saveSet["total_non_current_liabilities"] = total_non_current_liabilities

        try:
            total_liabilities = float(dataSet.get("total_liabilities")) 
        except:
            total_liabilities = 0 
        saveSet["total_liabilities"] = total_liabilities

        try:
            owners_equity = float(dataSet.get("owners_equity")) 
        except:
            owners_equity = 0 
        saveSet["owners_equity"] = owners_equity

        try:
            paid_in_capital = float(dataSet.get("paid_in_capital")) 
        except:
            paid_in_capital = 0 
        saveSet["paid_in_capital"] = paid_in_capital

        try:
            other_equity_instruments = float(dataSet.get("other_equity_instruments")) 
        except:
            other_equity_instruments = 0 
        saveSet["other_equity_instruments"] = other_equity_instruments

        try:
            preferred_stock = float(dataSet.get("preferred_stock")) 
        except:
            preferred_stock = 0 
        saveSet["preferred_stock"] = preferred_stock

        try:
            perpetual_bonds = float(dataSet.get("perpetual_bonds")) 
        except:
            perpetual_bonds = 0 
        saveSet["perpetual_bonds"] = perpetual_bonds

        try:
            capital_reserve = float(dataSet.get("capital_reserve")) 
        except:
            capital_reserve = 0 
        saveSet["capital_reserve"] = capital_reserve

        try:
            less_treasury_stock = float(dataSet.get("less_treasury_stock")) 
        except:
            less_treasury_stock = 0 
        saveSet["less_treasury_stock"] = less_treasury_stock

        try:
            other_comprehensive_income = float(dataSet.get("other_comprehensive_income")) 
        except:
            other_comprehensive_income = 0 
        saveSet["other_comprehensive_income"] = other_comprehensive_income

        try:
            special_reserve = float(dataSet.get("special_reserve")) 
        except:
            special_reserve = 0 
        saveSet["special_reserve"] = special_reserve

        try:
            surplus_reserve = float(dataSet.get("surplus_reserve")) 
        except:
            surplus_reserve = 0 
        saveSet["surplus_reserve"] = surplus_reserve

        try:
            general_risk_reserve = float(dataSet.get("general_risk_reserve")) 
        except:
            general_risk_reserve = 0 
        saveSet["general_risk_reserve"] = general_risk_reserve

        try:
            unrecognized_investment_losses = float(dataSet.get("unrecognized_investment_losses")) 
        except:
            unrecognized_investment_losses = 0 
        saveSet["unrecognized_investment_losses"] = unrecognized_investment_losses

        try:
            retained_earnings = float(dataSet.get("retained_earnings")) 
        except:
            retained_earnings = 0 
        saveSet["retained_earnings"] = retained_earnings

        try:
            proposed_cash_dividends = float(dataSet.get("proposed_cash_dividends")) 
        except:
            proposed_cash_dividends = 0 
        saveSet["proposed_cash_dividends"] = proposed_cash_dividends

        try:
            foreign_currency_translation_difference = float(dataSet.get("foreign_currency_translation_difference")) 
        except:
            foreign_currency_translation_difference = 0 
        saveSet["foreign_currency_translation_difference"] = foreign_currency_translation_difference

        try:
            equity_attributable_to_parent_company = float(dataSet.get("equity_attributable_to_parent_company")) 
        except:
            equity_attributable_to_parent_company = 0 
        saveSet["equity_attributable_to_parent_company"] = equity_attributable_to_parent_company

        try:
            minority_interests = float(dataSet.get("minority_interests")) 
        except:
            minority_interests = 0 
        saveSet["minority_interests"] = minority_interests

        try:
            total_owners_equity = float(dataSet.get("total_owners_equity")) 
        except:
            total_owners_equity = 0 
        saveSet["total_owners_equity"] = total_owners_equity

        try:
            total_liabilities_and_owners_equity = float(dataSet.get("total_liabilities_and_owners_equity")) 
        except:
            total_liabilities_and_owners_equity = 0 
        saveSet["total_liabilities_and_owners_equity"] = total_liabilities_and_owners_equity

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


#balance_sheets 修改记录
def update_balance_sheets(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        stock_code = dataSet.get("stock_code") 
        if stock_code:
            saveSet["stock_code"] = stock_code

        report_date = dataSet.get("report_date") 
        if report_date:
            saveSet["report_date"] = report_date

        try:
            monetary_capital = float(dataSet.get("monetary_capital")) 
            saveSet["monetary_capital"] = monetary_capital
        except:
            pass

        try:
            settlement_provisions = float(dataSet.get("settlement_provisions")) 
            saveSet["settlement_provisions"] = settlement_provisions
        except:
            pass

        try:
            loans_to_other_banks = float(dataSet.get("loans_to_other_banks")) 
            saveSet["loans_to_other_banks"] = loans_to_other_banks
        except:
            pass

        try:
            trading_financial_assets = float(dataSet.get("trading_financial_assets")) 
            saveSet["trading_financial_assets"] = trading_financial_assets
        except:
            pass

        try:
            financial_assets_purchased_for_resale = float(dataSet.get("financial_assets_purchased_for_resale")) 
            saveSet["financial_assets_purchased_for_resale"] = financial_assets_purchased_for_resale
        except:
            pass

        try:
            derivative_financial_assets = float(dataSet.get("derivative_financial_assets")) 
            saveSet["derivative_financial_assets"] = derivative_financial_assets
        except:
            pass

        try:
            notes_and_accounts_receivable = float(dataSet.get("notes_and_accounts_receivable")) 
            saveSet["notes_and_accounts_receivable"] = notes_and_accounts_receivable
        except:
            pass

        try:
            notes_receivable = float(dataSet.get("notes_receivable")) 
            saveSet["notes_receivable"] = notes_receivable
        except:
            pass

        try:
            accounts_receivable = float(dataSet.get("accounts_receivable")) 
            saveSet["accounts_receivable"] = accounts_receivable
        except:
            pass

        try:
            receivables_financing = float(dataSet.get("receivables_financing")) 
            saveSet["receivables_financing"] = receivables_financing
        except:
            pass

        try:
            prepayments = float(dataSet.get("prepayments")) 
            saveSet["prepayments"] = prepayments
        except:
            pass

        try:
            dividends_receivable = float(dataSet.get("dividends_receivable")) 
            saveSet["dividends_receivable"] = dividends_receivable
        except:
            pass

        try:
            interest_receivable = float(dataSet.get("interest_receivable")) 
            saveSet["interest_receivable"] = interest_receivable
        except:
            pass

        try:
            insurance_premiums_receivable = float(dataSet.get("insurance_premiums_receivable")) 
            saveSet["insurance_premiums_receivable"] = insurance_premiums_receivable
        except:
            pass

        try:
            reinsurance_receivables = float(dataSet.get("reinsurance_receivables")) 
            saveSet["reinsurance_receivables"] = reinsurance_receivables
        except:
            pass

        try:
            reinsurance_contract_reserves_receivable = float(dataSet.get("reinsurance_contract_reserves_receivable")) 
            saveSet["reinsurance_contract_reserves_receivable"] = reinsurance_contract_reserves_receivable
        except:
            pass

        try:
            export_tax_rebates_receivable = float(dataSet.get("export_tax_rebates_receivable")) 
            saveSet["export_tax_rebates_receivable"] = export_tax_rebates_receivable
        except:
            pass

        try:
            subsidies_receivable = float(dataSet.get("subsidies_receivable")) 
            saveSet["subsidies_receivable"] = subsidies_receivable
        except:
            pass

        try:
            deposits_receivable = float(dataSet.get("deposits_receivable")) 
            saveSet["deposits_receivable"] = deposits_receivable
        except:
            pass

        try:
            internal_receivables = float(dataSet.get("internal_receivables")) 
            saveSet["internal_receivables"] = internal_receivables
        except:
            pass

        try:
            other_receivables = float(dataSet.get("other_receivables")) 
            saveSet["other_receivables"] = other_receivables
        except:
            pass

        try:
            other_receivables_total = float(dataSet.get("other_receivables_total")) 
            saveSet["other_receivables_total"] = other_receivables_total
        except:
            pass

        try:
            inventories = float(dataSet.get("inventories")) 
            saveSet["inventories"] = inventories
        except:
            pass

        try:
            assets_held_for_sale = float(dataSet.get("assets_held_for_sale")) 
            saveSet["assets_held_for_sale"] = assets_held_for_sale
        except:
            pass

        try:
            prepaid_expenses = float(dataSet.get("prepaid_expenses")) 
            saveSet["prepaid_expenses"] = prepaid_expenses
        except:
            pass

        try:
            current_assets_pending_disposal = float(dataSet.get("current_assets_pending_disposal")) 
            saveSet["current_assets_pending_disposal"] = current_assets_pending_disposal
        except:
            pass

        try:
            non_current_assets_due_within_one_year = float(dataSet.get("non_current_assets_due_within_one_year")) 
            saveSet["non_current_assets_due_within_one_year"] = non_current_assets_due_within_one_year
        except:
            pass

        try:
            other_current_assets = float(dataSet.get("other_current_assets")) 
            saveSet["other_current_assets"] = other_current_assets
        except:
            pass

        try:
            total_current_assets = float(dataSet.get("total_current_assets")) 
            saveSet["total_current_assets"] = total_current_assets
        except:
            pass

        try:
            non_current_assets = float(dataSet.get("non_current_assets")) 
            saveSet["non_current_assets"] = non_current_assets
        except:
            pass

        try:
            loans_and_advances = float(dataSet.get("loans_and_advances")) 
            saveSet["loans_and_advances"] = loans_and_advances
        except:
            pass

        try:
            debt_investments = float(dataSet.get("debt_investments")) 
            saveSet["debt_investments"] = debt_investments
        except:
            pass

        try:
            other_debt_investments = float(dataSet.get("other_debt_investments")) 
            saveSet["other_debt_investments"] = other_debt_investments
        except:
            pass

        try:
            financial_assets_at_fvoci = float(dataSet.get("financial_assets_at_fvoci")) 
            saveSet["financial_assets_at_fvoci"] = financial_assets_at_fvoci
        except:
            pass

        try:
            financial_assets_at_amortized_cost = float(dataSet.get("financial_assets_at_amortized_cost")) 
            saveSet["financial_assets_at_amortized_cost"] = financial_assets_at_amortized_cost
        except:
            pass

        try:
            available_for_sale_financial_assets = float(dataSet.get("available_for_sale_financial_assets")) 
            saveSet["available_for_sale_financial_assets"] = available_for_sale_financial_assets
        except:
            pass

        try:
            long_term_equity_investments = float(dataSet.get("long_term_equity_investments")) 
            saveSet["long_term_equity_investments"] = long_term_equity_investments
        except:
            pass

        try:
            investment_property = float(dataSet.get("investment_property")) 
            saveSet["investment_property"] = investment_property
        except:
            pass

        try:
            long_term_receivables = float(dataSet.get("long_term_receivables")) 
            saveSet["long_term_receivables"] = long_term_receivables
        except:
            pass

        try:
            other_equity_instrument_investments = float(dataSet.get("other_equity_instrument_investments")) 
            saveSet["other_equity_instrument_investments"] = other_equity_instrument_investments
        except:
            pass

        try:
            other_non_current_financial_assets = float(dataSet.get("other_non_current_financial_assets")) 
            saveSet["other_non_current_financial_assets"] = other_non_current_financial_assets
        except:
            pass

        try:
            other_long_term_investments = float(dataSet.get("other_long_term_investments")) 
            saveSet["other_long_term_investments"] = other_long_term_investments
        except:
            pass

        try:
            fixed_assets_original_value = float(dataSet.get("fixed_assets_original_value")) 
            saveSet["fixed_assets_original_value"] = fixed_assets_original_value
        except:
            pass

        try:
            accumulated_depreciation = float(dataSet.get("accumulated_depreciation")) 
            saveSet["accumulated_depreciation"] = accumulated_depreciation
        except:
            pass

        try:
            fixed_assets_net_value = float(dataSet.get("fixed_assets_net_value")) 
            saveSet["fixed_assets_net_value"] = fixed_assets_net_value
        except:
            pass

        try:
            fixed_assets_impairment_provision = float(dataSet.get("fixed_assets_impairment_provision")) 
            saveSet["fixed_assets_impairment_provision"] = fixed_assets_impairment_provision
        except:
            pass

        try:
            construction_in_progress_total = float(dataSet.get("construction_in_progress_total")) 
            saveSet["construction_in_progress_total"] = construction_in_progress_total
        except:
            pass

        try:
            construction_in_progress = float(dataSet.get("construction_in_progress")) 
            saveSet["construction_in_progress"] = construction_in_progress
        except:
            pass

        try:
            construction_materials = float(dataSet.get("construction_materials")) 
            saveSet["construction_materials"] = construction_materials
        except:
            pass

        try:
            fixed_assets_net = float(dataSet.get("fixed_assets_net")) 
            saveSet["fixed_assets_net"] = fixed_assets_net
        except:
            pass

        try:
            fixed_assets_disposal = float(dataSet.get("fixed_assets_disposal")) 
            saveSet["fixed_assets_disposal"] = fixed_assets_disposal
        except:
            pass

        try:
            fixed_assets_and_disposal_total = float(dataSet.get("fixed_assets_and_disposal_total")) 
            saveSet["fixed_assets_and_disposal_total"] = fixed_assets_and_disposal_total
        except:
            pass

        try:
            productive_biological_assets = float(dataSet.get("productive_biological_assets")) 
            saveSet["productive_biological_assets"] = productive_biological_assets
        except:
            pass

        try:
            consumptive_biological_assets = float(dataSet.get("consumptive_biological_assets")) 
            saveSet["consumptive_biological_assets"] = consumptive_biological_assets
        except:
            pass

        try:
            oil_and_gas_assets = float(dataSet.get("oil_and_gas_assets")) 
            saveSet["oil_and_gas_assets"] = oil_and_gas_assets
        except:
            pass

        try:
            contract_assets = float(dataSet.get("contract_assets")) 
            saveSet["contract_assets"] = contract_assets
        except:
            pass

        try:
            right_of_use_assets = float(dataSet.get("right_of_use_assets")) 
            saveSet["right_of_use_assets"] = right_of_use_assets
        except:
            pass

        try:
            intangible_assets = float(dataSet.get("intangible_assets")) 
            saveSet["intangible_assets"] = intangible_assets
        except:
            pass

        try:
            development_expenditure = float(dataSet.get("development_expenditure")) 
            saveSet["development_expenditure"] = development_expenditure
        except:
            pass

        try:
            goodwill = float(dataSet.get("goodwill")) 
            saveSet["goodwill"] = goodwill
        except:
            pass

        try:
            long_term_deferred_expenses = float(dataSet.get("long_term_deferred_expenses")) 
            saveSet["long_term_deferred_expenses"] = long_term_deferred_expenses
        except:
            pass

        try:
            split_share_structure_circulation_rights = float(dataSet.get("split_share_structure_circulation_rights")) 
            saveSet["split_share_structure_circulation_rights"] = split_share_structure_circulation_rights
        except:
            pass

        try:
            deferred_tax_assets = float(dataSet.get("deferred_tax_assets")) 
            saveSet["deferred_tax_assets"] = deferred_tax_assets
        except:
            pass

        try:
            other_non_current_assets = float(dataSet.get("other_non_current_assets")) 
            saveSet["other_non_current_assets"] = other_non_current_assets
        except:
            pass

        try:
            total_non_current_assets = float(dataSet.get("total_non_current_assets")) 
            saveSet["total_non_current_assets"] = total_non_current_assets
        except:
            pass

        try:
            total_assets = float(dataSet.get("total_assets")) 
            saveSet["total_assets"] = total_assets
        except:
            pass

        try:
            current_liabilities = float(dataSet.get("current_liabilities")) 
            saveSet["current_liabilities"] = current_liabilities
        except:
            pass

        try:
            short_term_borrowings = float(dataSet.get("short_term_borrowings")) 
            saveSet["short_term_borrowings"] = short_term_borrowings
        except:
            pass

        try:
            borrowings_from_central_bank = float(dataSet.get("borrowings_from_central_bank")) 
            saveSet["borrowings_from_central_bank"] = borrowings_from_central_bank
        except:
            pass

        try:
            deposits_from_customers_and_banks = float(dataSet.get("deposits_from_customers_and_banks")) 
            saveSet["deposits_from_customers_and_banks"] = deposits_from_customers_and_banks
        except:
            pass

        try:
            borrowings_from_other_banks = float(dataSet.get("borrowings_from_other_banks")) 
            saveSet["borrowings_from_other_banks"] = borrowings_from_other_banks
        except:
            pass

        try:
            trading_financial_liabilities = float(dataSet.get("trading_financial_liabilities")) 
            saveSet["trading_financial_liabilities"] = trading_financial_liabilities
        except:
            pass

        try:
            derivative_financial_liabilities = float(dataSet.get("derivative_financial_liabilities")) 
            saveSet["derivative_financial_liabilities"] = derivative_financial_liabilities
        except:
            pass

        try:
            notes_and_accounts_payable = float(dataSet.get("notes_and_accounts_payable")) 
            saveSet["notes_and_accounts_payable"] = notes_and_accounts_payable
        except:
            pass

        try:
            notes_payable = float(dataSet.get("notes_payable")) 
            saveSet["notes_payable"] = notes_payable
        except:
            pass

        try:
            accounts_payable = float(dataSet.get("accounts_payable")) 
            saveSet["accounts_payable"] = accounts_payable
        except:
            pass

        try:
            advances_from_customers = float(dataSet.get("advances_from_customers")) 
            saveSet["advances_from_customers"] = advances_from_customers
        except:
            pass

        try:
            contract_liabilities = float(dataSet.get("contract_liabilities")) 
            saveSet["contract_liabilities"] = contract_liabilities
        except:
            pass

        try:
            financial_assets_sold_for_repurchase = float(dataSet.get("financial_assets_sold_for_repurchase")) 
            saveSet["financial_assets_sold_for_repurchase"] = financial_assets_sold_for_repurchase
        except:
            pass

        try:
            fees_and_commissions_payable = float(dataSet.get("fees_and_commissions_payable")) 
            saveSet["fees_and_commissions_payable"] = fees_and_commissions_payable
        except:
            pass

        try:
            employee_benefits_payable = float(dataSet.get("employee_benefits_payable")) 
            saveSet["employee_benefits_payable"] = employee_benefits_payable
        except:
            pass

        try:
            taxes_payable = float(dataSet.get("taxes_payable")) 
            saveSet["taxes_payable"] = taxes_payable
        except:
            pass

        try:
            interest_payable = float(dataSet.get("interest_payable")) 
            saveSet["interest_payable"] = interest_payable
        except:
            pass

        try:
            dividends_payable = float(dataSet.get("dividends_payable")) 
            saveSet["dividends_payable"] = dividends_payable
        except:
            pass

        try:
            deposits_payable = float(dataSet.get("deposits_payable")) 
            saveSet["deposits_payable"] = deposits_payable
        except:
            pass

        try:
            internal_payables = float(dataSet.get("internal_payables")) 
            saveSet["internal_payables"] = internal_payables
        except:
            pass

        try:
            other_payables = float(dataSet.get("other_payables")) 
            saveSet["other_payables"] = other_payables
        except:
            pass

        try:
            other_payables_total = float(dataSet.get("other_payables_total")) 
            saveSet["other_payables_total"] = other_payables_total
        except:
            pass

        try:
            other_taxes_payable = float(dataSet.get("other_taxes_payable")) 
            saveSet["other_taxes_payable"] = other_taxes_payable
        except:
            pass

        try:
            guarantee_liability_reserves = float(dataSet.get("guarantee_liability_reserves")) 
            saveSet["guarantee_liability_reserves"] = guarantee_liability_reserves
        except:
            pass

        try:
            reinsurance_payables = float(dataSet.get("reinsurance_payables")) 
            saveSet["reinsurance_payables"] = reinsurance_payables
        except:
            pass

        try:
            insurance_contract_reserves = float(dataSet.get("insurance_contract_reserves")) 
            saveSet["insurance_contract_reserves"] = insurance_contract_reserves
        except:
            pass

        try:
            securities_trading_agency_payables = float(dataSet.get("securities_trading_agency_payables")) 
            saveSet["securities_trading_agency_payables"] = securities_trading_agency_payables
        except:
            pass

        try:
            securities_underwriting_agency_payables = float(dataSet.get("securities_underwriting_agency_payables")) 
            saveSet["securities_underwriting_agency_payables"] = securities_underwriting_agency_payables
        except:
            pass

        try:
            international_settlement = float(dataSet.get("international_settlement")) 
            saveSet["international_settlement"] = international_settlement
        except:
            pass

        try:
            domestic_settlement = float(dataSet.get("domestic_settlement")) 
            saveSet["domestic_settlement"] = domestic_settlement
        except:
            pass

        try:
            accrued_expenses = float(dataSet.get("accrued_expenses")) 
            saveSet["accrued_expenses"] = accrued_expenses
        except:
            pass

        try:
            estimated_current_liabilities = float(dataSet.get("estimated_current_liabilities")) 
            saveSet["estimated_current_liabilities"] = estimated_current_liabilities
        except:
            pass

        try:
            short_term_bonds_payable = float(dataSet.get("short_term_bonds_payable")) 
            saveSet["short_term_bonds_payable"] = short_term_bonds_payable
        except:
            pass

        try:
            liabilities_held_for_sale = float(dataSet.get("liabilities_held_for_sale")) 
            saveSet["liabilities_held_for_sale"] = liabilities_held_for_sale
        except:
            pass

        try:
            deferred_revenue_due_within_one_year = float(dataSet.get("deferred_revenue_due_within_one_year")) 
            saveSet["deferred_revenue_due_within_one_year"] = deferred_revenue_due_within_one_year
        except:
            pass

        try:
            non_current_liabilities_due_within_one_year = float(dataSet.get("non_current_liabilities_due_within_one_year")) 
            saveSet["non_current_liabilities_due_within_one_year"] = non_current_liabilities_due_within_one_year
        except:
            pass


        try:
            other_current_liabilities = float(dataSet.get("other_current_liabilities")) 
            saveSet["other_current_liabilities"] = other_current_liabilities
        except:
            pass

        try:
            total_current_liabilities = float(dataSet.get("total_current_liabilities")) 
            saveSet["total_current_liabilities"] = total_current_liabilities
        except:
            pass

        try:
            non_current_liabilities = float(dataSet.get("non_current_liabilities")) 
            saveSet["non_current_liabilities"] = non_current_liabilities
        except:
            pass

        try:
            long_term_borrowings = float(dataSet.get("long_term_borrowings")) 
            saveSet["long_term_borrowings"] = long_term_borrowings
        except:
            pass

        try:
            bonds_payable = float(dataSet.get("bonds_payable")) 
            saveSet["bonds_payable"] = bonds_payable
        except:
            pass

        try:
            bonds_payable_preferred_stock = float(dataSet.get("bonds_payable_preferred_stock")) 
            saveSet["bonds_payable_preferred_stock"] = bonds_payable_preferred_stock
        except:
            pass

        try:
            bonds_payable_perpetual_bonds = float(dataSet.get("bonds_payable_perpetual_bonds")) 
            saveSet["bonds_payable_perpetual_bonds"] = bonds_payable_perpetual_bonds
        except:
            pass

        try:
            lease_liabilities = float(dataSet.get("lease_liabilities")) 
            saveSet["lease_liabilities"] = lease_liabilities
        except:
            pass

        try:
            long_term_employee_benefits_payable = float(dataSet.get("long_term_employee_benefits_payable")) 
            saveSet["long_term_employee_benefits_payable"] = long_term_employee_benefits_payable
        except:
            pass

        try:
            long_term_payables = float(dataSet.get("long_term_payables")) 
            saveSet["long_term_payables"] = long_term_payables
        except:
            pass

        try:
            long_term_payables_total = float(dataSet.get("long_term_payables_total")) 
            saveSet["long_term_payables_total"] = long_term_payables_total
        except:
            pass

        try:
            special_payables = float(dataSet.get("special_payables")) 
            saveSet["special_payables"] = special_payables
        except:
            pass

        try:
            estimated_non_current_liabilities = float(dataSet.get("estimated_non_current_liabilities")) 
            saveSet["estimated_non_current_liabilities"] = estimated_non_current_liabilities
        except:
            pass

        try:
            long_term_deferred_revenue = float(dataSet.get("long_term_deferred_revenue")) 
            saveSet["long_term_deferred_revenue"] = long_term_deferred_revenue
        except:
            pass

        try:
            deferred_tax_liabilities = float(dataSet.get("deferred_tax_liabilities")) 
            saveSet["deferred_tax_liabilities"] = deferred_tax_liabilities
        except:
            pass

        try:
            other_non_current_liabilities = float(dataSet.get("other_non_current_liabilities")) 
            saveSet["other_non_current_liabilities"] = other_non_current_liabilities
        except:
            pass

        try:
            total_non_current_liabilities = float(dataSet.get("total_non_current_liabilities")) 
            saveSet["total_non_current_liabilities"] = total_non_current_liabilities
        except:
            pass

        try:
            total_liabilities = float(dataSet.get("total_liabilities")) 
            saveSet["total_liabilities"] = total_liabilities
        except:
            pass

        try:
            owners_equity = float(dataSet.get("owners_equity")) 
            saveSet["owners_equity"] = owners_equity
        except:
            pass

        try:
            paid_in_capital = float(dataSet.get("paid_in_capital")) 
            saveSet["paid_in_capital"] = paid_in_capital
        except:
            pass

        try:
            other_equity_instruments = float(dataSet.get("other_equity_instruments")) 
            saveSet["other_equity_instruments"] = other_equity_instruments
        except:
            pass

        try:
            preferred_stock = float(dataSet.get("preferred_stock")) 
            saveSet["preferred_stock"] = preferred_stock
        except:
            pass

        try:
            perpetual_bonds = float(dataSet.get("perpetual_bonds")) 
            saveSet["perpetual_bonds"] = perpetual_bonds
        except:
            pass

        try:
            capital_reserve = float(dataSet.get("capital_reserve")) 
            saveSet["capital_reserve"] = capital_reserve
        except:
            pass

        try:
            less_treasury_stock = float(dataSet.get("less_treasury_stock")) 
            saveSet["less_treasury_stock"] = less_treasury_stock
        except:
            pass

        try:
            other_comprehensive_income = float(dataSet.get("other_comprehensive_income")) 
            saveSet["other_comprehensive_income"] = other_comprehensive_income
        except:
            pass

        try:
            special_reserve = float(dataSet.get("special_reserve")) 
            saveSet["special_reserve"] = special_reserve
        except:
            pass

        try:
            surplus_reserve = float(dataSet.get("surplus_reserve")) 
            saveSet["surplus_reserve"] = surplus_reserve
        except:
            pass

        try:
            general_risk_reserve = float(dataSet.get("general_risk_reserve")) 
            saveSet["general_risk_reserve"] = general_risk_reserve
        except:
            pass

        try:
            unrecognized_investment_losses = float(dataSet.get("unrecognized_investment_losses")) 
            saveSet["unrecognized_investment_losses"] = unrecognized_investment_losses
        except:
            pass

        try:
            retained_earnings = float(dataSet.get("retained_earnings")) 
            saveSet["retained_earnings"] = retained_earnings
        except:
            pass

        try:
            proposed_cash_dividends = float(dataSet.get("proposed_cash_dividends")) 
            saveSet["proposed_cash_dividends"] = proposed_cash_dividends
        except:
            pass

        try:
            foreign_currency_translation_difference = float(dataSet.get("foreign_currency_translation_difference")) 
            saveSet["foreign_currency_translation_difference"] = foreign_currency_translation_difference
        except:
            pass

        try:
            equity_attributable_to_parent_company = float(dataSet.get("equity_attributable_to_parent_company")) 
            saveSet["equity_attributable_to_parent_company"] = equity_attributable_to_parent_company
        except:
            pass

        try:
            minority_interests = float(dataSet.get("minority_interests")) 
            saveSet["minority_interests"] = minority_interests
        except:
            pass

        try:
            total_owners_equity = float(dataSet.get("total_owners_equity")) 
            saveSet["total_owners_equity"] = total_owners_equity
        except:
            pass

        try:
            total_liabilities_and_owners_equity = float(dataSet.get("total_liabilities_and_owners_equity")) 
            saveSet["total_liabilities_and_owners_equity"] = total_liabilities_and_owners_equity
        except:
            pass

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#balance_sheets 查询记录
def query_balance_sheets(tableName,id = "0", stock_code = "",report_date = "",stock_name = "",
                        delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)
        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if stock_code:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_code = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE stock_code = %s" 
                valuesList.append(stock_code)
            if report_date:
                if valuesList:
                    sqlStr =  sqlStr + " AND report_date = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE report_date = %s" 
                valuesList.append(report_date)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#balance_sheets end 


#income_statements begin 

def tablename_convertor_income_statements():
    tableName = "income_statements"
    tableName = tableName.lower()
    return tableName


def decode_tablename_income_statements(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建income_statements表
def create_income_statements(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "stock_code VARCHAR(10) COMMENT '股票代码',",
    "report_date VARCHAR(10) COMMENT '报告日期',",
    "total_operating_revenue FLOAT NULL,",
    "operating_revenue FLOAT NULL,",
    "interest_income FLOAT NULL,",
    "earned_premiums FLOAT NULL,",
    "fees_and_commissions_income FLOAT NULL,",
    "real_estate_sales_revenue FLOAT NULL,",
    "other_business_revenue FLOAT NULL,",
    "total_operating_costs FLOAT NULL,",
    "operating_costs FLOAT NULL,",
    "fees_and_commissions_expenses FLOAT NULL,",
    "real_estate_sales_costs FLOAT NULL,",
    "surrender_value FLOAT NULL,",
    "net_claims_paid FLOAT NULL,",
    "net_insurance_contract_reserves FLOAT NULL,",
    "policy_dividend_expenses FLOAT NULL,",
    "reinsurance_expenses FLOAT NULL,",
    "other_business_costs FLOAT NULL,",
    "taxes_and_surcharges FLOAT NULL,",
    "rd_expenses FLOAT NULL,",
    "selling_expenses FLOAT NULL,",
    "administrative_expenses FLOAT NULL,",
    "financial_expenses FLOAT NULL,",
    "interest_expenses FLOAT NULL,",
    "interest_expenditure FLOAT NULL,",
    "investment_income FLOAT NULL,",
    "investment_income_from_associates_and_joint_ventures FLOAT NULL,",
    "gain_on_derecognition_of_financial_assets_at_amortized_cost FLOAT NULL,",
    "foreign_exchange_gains FLOAT NULL,",
    "net_open_hedge_gains FLOAT NULL,",
    "fair_value_change_gains FLOAT NULL,",
    "futures_gains_losses FLOAT NULL,",
    "custody_income FLOAT NULL,",
    "subsidy_income FLOAT NULL,",
    "other_gains FLOAT NULL,",
    "asset_impairment_losses FLOAT NULL,",
    "credit_impairment_losses FLOAT NULL,",
    "other_business_profits FLOAT NULL,",
    "asset_disposal_gains FLOAT NULL,",
    "operating_profit FLOAT NULL,",
    "non_operating_income FLOAT NULL,",
    "non_current_asset_disposal_gains FLOAT NULL,",
    "non_operating_expenses FLOAT NULL,",
    "non_current_asset_disposal_losses FLOAT NULL,",
    "total_profit FLOAT NULL,",
    "income_tax_expense FLOAT NULL,",
    "unrecognized_investment_losses FLOAT NULL,",
    "net_profit FLOAT NULL,",
    "net_profit_from_continuing_operations FLOAT NULL,",
    "net_profit_from_discontinued_operations FLOAT NULL,",
    "net_profit_attributable_to_parent_company FLOAT NULL,",
    "net_profit_of_acquiree_before_merger FLOAT NULL,",
    "minority_interests_profit_loss FLOAT NULL,",
    "other_comprehensive_income FLOAT NULL,",
    "other_comprehensive_income_attributable_to_parent FLOAT NULL,",
    "oci_not_reclassified_to_profit_loss FLOAT NULL,",
    "remeasurement_of_defined_benefit_plans FLOAT NULL,",
    "oci_under_equity_method_not_reclassified FLOAT NULL,",
    "fair_value_change_of_other_equity_instruments FLOAT NULL,",
    "fair_value_change_of_own_credit_risk FLOAT NULL,",
    "oci_reclassified_to_profit_loss FLOAT NULL,",
    "oci_under_equity_method_reclassified FLOAT NULL,",
    "fair_value_change_of_afs_financial_assets FLOAT NULL,",
    "fair_value_change_of_other_debt_investments FLOAT NULL,",
    "financial_assets_reclassified_to_oci FLOAT NULL,",
    "credit_impairment_of_other_debt_investments FLOAT NULL,",
    "htm_reclassified_to_afs_gains_losses FLOAT NULL,",
    "cash_flow_hedge_reserve FLOAT NULL,",
    "effective_portion_of_cash_flow_hedge FLOAT NULL,",
    "foreign_currency_translation_difference FLOAT NULL,",
    "other FLOAT NULL,",
    "other_comprehensive_income_attributable_to_minority FLOAT NULL,",
    "total_comprehensive_income FLOAT NULL,",
    "total_comprehensive_income_attributable_to_parent FLOAT NULL,",
    "total_comprehensive_income_attributable_to_minority FLOAT NULL,",
    "basic_earnings_per_share FLOAT NULL,",
    "diluted_earnings_per_share FLOAT NULL,",
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
        sqlStr = "CREATE UNIQUE INDEX income_stock_report_index ON {0} ({1},{2}) ".format(tableName, "stock_code","report_date")
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除income_statements表
def drop_income_statements(tableName):
    result = dropTableGeneral(tableName)
    return result


#income_statements 删除记录
def delete_income_statements(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#income_statements 增加记录
def insert_income_statements(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["stock_code"] = dataSet.get("stock_code", "") 

        saveSet["report_date"] = dataSet.get("report_date", "") 

        try:
            total_operating_revenue = float(dataSet.get("total_operating_revenue")) 
        except:
            total_operating_revenue = 0 
        saveSet["total_operating_revenue"] = total_operating_revenue

        try:
            operating_revenue = float(dataSet.get("operating_revenue")) 
        except:
            operating_revenue = 0 
        saveSet["operating_revenue"] = operating_revenue

        try:
            interest_income = float(dataSet.get("interest_income")) 
        except:
            interest_income = 0 
        saveSet["interest_income"] = interest_income

        try:
            earned_premiums = float(dataSet.get("earned_premiums")) 
        except:
            earned_premiums = 0 
        saveSet["earned_premiums"] = earned_premiums

        try:
            fees_and_commissions_income = float(dataSet.get("fees_and_commissions_income")) 
        except:
            fees_and_commissions_income = 0 
        saveSet["fees_and_commissions_income"] = fees_and_commissions_income

        try:
            real_estate_sales_revenue = float(dataSet.get("real_estate_sales_revenue")) 
        except:
            real_estate_sales_revenue = 0 
        saveSet["real_estate_sales_revenue"] = real_estate_sales_revenue

        try:
            other_business_revenue = float(dataSet.get("other_business_revenue")) 
        except:
            other_business_revenue = 0 
        saveSet["other_business_revenue"] = other_business_revenue

        try:
            total_operating_costs = float(dataSet.get("total_operating_costs")) 
        except:
            total_operating_costs = 0 
        saveSet["total_operating_costs"] = total_operating_costs

        try:
            operating_costs = float(dataSet.get("operating_costs")) 
        except:
            operating_costs = 0 
        saveSet["operating_costs"] = operating_costs

        try:
            fees_and_commissions_expenses = float(dataSet.get("fees_and_commissions_expenses")) 
        except:
            fees_and_commissions_expenses = 0 
        saveSet["fees_and_commissions_expenses"] = fees_and_commissions_expenses

        try:
            real_estate_sales_costs = float(dataSet.get("real_estate_sales_costs")) 
        except:
            real_estate_sales_costs = 0 
        saveSet["real_estate_sales_costs"] = real_estate_sales_costs

        try:
            surrender_value = float(dataSet.get("surrender_value")) 
        except:
            surrender_value = 0 
        saveSet["surrender_value"] = surrender_value

        try:
            net_claims_paid = float(dataSet.get("net_claims_paid")) 
        except:
            net_claims_paid = 0 
        saveSet["net_claims_paid"] = net_claims_paid

        try:
            net_insurance_contract_reserves = float(dataSet.get("net_insurance_contract_reserves")) 
        except:
            net_insurance_contract_reserves = 0 
        saveSet["net_insurance_contract_reserves"] = net_insurance_contract_reserves

        try:
            policy_dividend_expenses = float(dataSet.get("policy_dividend_expenses")) 
        except:
            policy_dividend_expenses = 0 
        saveSet["policy_dividend_expenses"] = policy_dividend_expenses

        try:
            reinsurance_expenses = float(dataSet.get("reinsurance_expenses")) 
        except:
            reinsurance_expenses = 0 
        saveSet["reinsurance_expenses"] = reinsurance_expenses

        try:
            other_business_costs = float(dataSet.get("other_business_costs")) 
        except:
            other_business_costs = 0 
        saveSet["other_business_costs"] = other_business_costs

        try:
            taxes_and_surcharges = float(dataSet.get("taxes_and_surcharges")) 
        except:
            taxes_and_surcharges = 0 
        saveSet["taxes_and_surcharges"] = taxes_and_surcharges

        try:
            rd_expenses = float(dataSet.get("rd_expenses")) 
        except:
            rd_expenses = 0 
        saveSet["rd_expenses"] = rd_expenses

        try:
            selling_expenses = float(dataSet.get("selling_expenses")) 
        except:
            selling_expenses = 0 
        saveSet["selling_expenses"] = selling_expenses

        try:
            administrative_expenses = float(dataSet.get("administrative_expenses")) 
        except:
            administrative_expenses = 0 
        saveSet["administrative_expenses"] = administrative_expenses

        try:
            financial_expenses = float(dataSet.get("financial_expenses")) 
        except:
            financial_expenses = 0 
        saveSet["financial_expenses"] = financial_expenses

        try:
            interest_expenses = float(dataSet.get("interest_expenses")) 
        except:
            interest_expenses = 0 
        saveSet["interest_expenses"] = interest_expenses

        try:
            interest_expenditure = float(dataSet.get("interest_expenditure")) 
        except:
            interest_expenditure = 0 
        saveSet["interest_expenditure"] = interest_expenditure

        try:
            investment_income = float(dataSet.get("investment_income")) 
        except:
            investment_income = 0 
        saveSet["investment_income"] = investment_income

        try:
            investment_income_from_associates_and_joint_ventures = float(dataSet.get("investment_income_from_associates_and_joint_ventures")) 
        except:
            investment_income_from_associates_and_joint_ventures = 0 
        saveSet["investment_income_from_associates_and_joint_ventures"] = investment_income_from_associates_and_joint_ventures

        try:
            gain_on_derecognition_of_financial_assets_at_amortized_cost = float(dataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost")) 
        except:
            gain_on_derecognition_of_financial_assets_at_amortized_cost = 0 
        saveSet["gain_on_derecognition_of_financial_assets_at_amortized_cost"] = gain_on_derecognition_of_financial_assets_at_amortized_cost

        try:
            foreign_exchange_gains = float(dataSet.get("foreign_exchange_gains")) 
        except:
            foreign_exchange_gains = 0 
        saveSet["foreign_exchange_gains"] = foreign_exchange_gains

        try:
            net_open_hedge_gains = float(dataSet.get("net_open_hedge_gains")) 
        except:
            net_open_hedge_gains = 0 
        saveSet["net_open_hedge_gains"] = net_open_hedge_gains

        try:
            fair_value_change_gains = float(dataSet.get("fair_value_change_gains")) 
        except:
            fair_value_change_gains = 0 
        saveSet["fair_value_change_gains"] = fair_value_change_gains

        try:
            futures_gains_losses = float(dataSet.get("futures_gains_losses")) 
        except:
            futures_gains_losses = 0 
        saveSet["futures_gains_losses"] = futures_gains_losses

        try:
            custody_income = float(dataSet.get("custody_income")) 
        except:
            custody_income = 0 
        saveSet["custody_income"] = custody_income

        try:
            subsidy_income = float(dataSet.get("subsidy_income")) 
        except:
            subsidy_income = 0 
        saveSet["subsidy_income"] = subsidy_income

        try:
            other_gains = float(dataSet.get("other_gains")) 
        except:
            other_gains = 0 
        saveSet["other_gains"] = other_gains

        try:
            asset_impairment_losses = float(dataSet.get("asset_impairment_losses")) 
        except:
            asset_impairment_losses = 0 
        saveSet["asset_impairment_losses"] = asset_impairment_losses

        try:
            credit_impairment_losses = float(dataSet.get("credit_impairment_losses")) 
        except:
            credit_impairment_losses = 0 
        saveSet["credit_impairment_losses"] = credit_impairment_losses

        try:
            other_business_profits = float(dataSet.get("other_business_profits")) 
        except:
            other_business_profits = 0 
        saveSet["other_business_profits"] = other_business_profits

        try:
            asset_disposal_gains = float(dataSet.get("asset_disposal_gains")) 
        except:
            asset_disposal_gains = 0 
        saveSet["asset_disposal_gains"] = asset_disposal_gains

        try:
            operating_profit = float(dataSet.get("operating_profit")) 
        except:
            operating_profit = 0 
        saveSet["operating_profit"] = operating_profit

        try:
            non_operating_income = float(dataSet.get("non_operating_income")) 
        except:
            non_operating_income = 0 
        saveSet["non_operating_income"] = non_operating_income

        try:
            non_current_asset_disposal_gains = float(dataSet.get("non_current_asset_disposal_gains")) 
        except:
            non_current_asset_disposal_gains = 0 
        saveSet["non_current_asset_disposal_gains"] = non_current_asset_disposal_gains

        try:
            non_operating_expenses = float(dataSet.get("non_operating_expenses")) 
        except:
            non_operating_expenses = 0 
        saveSet["non_operating_expenses"] = non_operating_expenses

        try:
            non_current_asset_disposal_losses = float(dataSet.get("non_current_asset_disposal_losses")) 
        except:
            non_current_asset_disposal_losses = 0 
        saveSet["non_current_asset_disposal_losses"] = non_current_asset_disposal_losses

        try:
            total_profit = float(dataSet.get("total_profit")) 
        except:
            total_profit = 0 
        saveSet["total_profit"] = total_profit

        try:
            income_tax_expense = float(dataSet.get("income_tax_expense")) 
        except:
            income_tax_expense = 0 
        saveSet["income_tax_expense"] = income_tax_expense

        try:
            unrecognized_investment_losses = float(dataSet.get("unrecognized_investment_losses")) 
        except:
            unrecognized_investment_losses = 0 
        saveSet["unrecognized_investment_losses"] = unrecognized_investment_losses

        try:
            net_profit = float(dataSet.get("net_profit")) 
        except:
            net_profit = 0 
        saveSet["net_profit"] = net_profit

        try:
            net_profit_from_continuing_operations = float(dataSet.get("net_profit_from_continuing_operations")) 
        except:
            net_profit_from_continuing_operations = 0 
        saveSet["net_profit_from_continuing_operations"] = net_profit_from_continuing_operations

        try:
            net_profit_from_discontinued_operations = float(dataSet.get("net_profit_from_discontinued_operations")) 
        except:
            net_profit_from_discontinued_operations = 0 
        saveSet["net_profit_from_discontinued_operations"] = net_profit_from_discontinued_operations

        try:
            net_profit_attributable_to_parent_company = float(dataSet.get("net_profit_attributable_to_parent_company")) 
        except:
            net_profit_attributable_to_parent_company = 0 
        saveSet["net_profit_attributable_to_parent_company"] = net_profit_attributable_to_parent_company

        try:
            net_profit_of_acquiree_before_merger = float(dataSet.get("net_profit_of_acquiree_before_merger")) 
        except:
            net_profit_of_acquiree_before_merger = 0 
        saveSet["net_profit_of_acquiree_before_merger"] = net_profit_of_acquiree_before_merger

        try:
            minority_interests_profit_loss = float(dataSet.get("minority_interests_profit_loss")) 
        except:
            minority_interests_profit_loss = 0 
        saveSet["minority_interests_profit_loss"] = minority_interests_profit_loss

        try:
            other_comprehensive_income = float(dataSet.get("other_comprehensive_income")) 
        except:
            other_comprehensive_income = 0 
        saveSet["other_comprehensive_income"] = other_comprehensive_income

        try:
            other_comprehensive_income_attributable_to_parent = float(dataSet.get("other_comprehensive_income_attributable_to_parent")) 
        except:
            other_comprehensive_income_attributable_to_parent = 0 
        saveSet["other_comprehensive_income_attributable_to_parent"] = other_comprehensive_income_attributable_to_parent

        try:
            oci_not_reclassified_to_profit_loss = float(dataSet.get("oci_not_reclassified_to_profit_loss")) 
        except:
            oci_not_reclassified_to_profit_loss = 0 
        saveSet["oci_not_reclassified_to_profit_loss"] = oci_not_reclassified_to_profit_loss

        try:
            remeasurement_of_defined_benefit_plans = float(dataSet.get("remeasurement_of_defined_benefit_plans")) 
        except:
            remeasurement_of_defined_benefit_plans = 0 
        saveSet["remeasurement_of_defined_benefit_plans"] = remeasurement_of_defined_benefit_plans

        try:
            oci_under_equity_method_not_reclassified = float(dataSet.get("oci_under_equity_method_not_reclassified")) 
        except:
            oci_under_equity_method_not_reclassified = 0 
        saveSet["oci_under_equity_method_not_reclassified"] = oci_under_equity_method_not_reclassified

        try:
            fair_value_change_of_other_equity_instruments = float(dataSet.get("fair_value_change_of_other_equity_instruments")) 
        except:
            fair_value_change_of_other_equity_instruments = 0 
        saveSet["fair_value_change_of_other_equity_instruments"] = fair_value_change_of_other_equity_instruments

        try:
            fair_value_change_of_own_credit_risk = float(dataSet.get("fair_value_change_of_own_credit_risk")) 
        except:
            fair_value_change_of_own_credit_risk = 0 
        saveSet["fair_value_change_of_own_credit_risk"] = fair_value_change_of_own_credit_risk

        try:
            oci_reclassified_to_profit_loss = float(dataSet.get("oci_reclassified_to_profit_loss")) 
        except:
            oci_reclassified_to_profit_loss = 0 
        saveSet["oci_reclassified_to_profit_loss"] = oci_reclassified_to_profit_loss

        try:
            oci_under_equity_method_reclassified = float(dataSet.get("oci_under_equity_method_reclassified")) 
        except:
            oci_under_equity_method_reclassified = 0 
        saveSet["oci_under_equity_method_reclassified"] = oci_under_equity_method_reclassified

        try:
            fair_value_change_of_afs_financial_assets = float(dataSet.get("fair_value_change_of_afs_financial_assets")) 
        except:
            fair_value_change_of_afs_financial_assets = 0 
        saveSet["fair_value_change_of_afs_financial_assets"] = fair_value_change_of_afs_financial_assets

        try:
            fair_value_change_of_other_debt_investments = float(dataSet.get("fair_value_change_of_other_debt_investments")) 
        except:
            fair_value_change_of_other_debt_investments = 0 
        saveSet["fair_value_change_of_other_debt_investments"] = fair_value_change_of_other_debt_investments

        try:
            financial_assets_reclassified_to_oci = float(dataSet.get("financial_assets_reclassified_to_oci")) 
        except:
            financial_assets_reclassified_to_oci = 0 
        saveSet["financial_assets_reclassified_to_oci"] = financial_assets_reclassified_to_oci

        try:
            credit_impairment_of_other_debt_investments = float(dataSet.get("credit_impairment_of_other_debt_investments")) 
        except:
            credit_impairment_of_other_debt_investments = 0 
        saveSet["credit_impairment_of_other_debt_investments"] = credit_impairment_of_other_debt_investments

        try:
            htm_reclassified_to_afs_gains_losses = float(dataSet.get("htm_reclassified_to_afs_gains_losses")) 
        except:
            htm_reclassified_to_afs_gains_losses = 0 
        saveSet["htm_reclassified_to_afs_gains_losses"] = htm_reclassified_to_afs_gains_losses

        try:
            cash_flow_hedge_reserve = float(dataSet.get("cash_flow_hedge_reserve")) 
        except:
            cash_flow_hedge_reserve = 0 
        saveSet["cash_flow_hedge_reserve"] = cash_flow_hedge_reserve

        try:
            effective_portion_of_cash_flow_hedge = float(dataSet.get("effective_portion_of_cash_flow_hedge")) 
        except:
            effective_portion_of_cash_flow_hedge = 0 
        saveSet["effective_portion_of_cash_flow_hedge"] = effective_portion_of_cash_flow_hedge

        try:
            foreign_currency_translation_difference = float(dataSet.get("foreign_currency_translation_difference")) 
        except:
            foreign_currency_translation_difference = 0 
        saveSet["foreign_currency_translation_difference"] = foreign_currency_translation_difference

        try:
            other = float(dataSet.get("other")) 
        except:
            other = 0 
        saveSet["other"] = other

        try:
            other_comprehensive_income_attributable_to_minority = float(dataSet.get("other_comprehensive_income_attributable_to_minority")) 
        except:
            other_comprehensive_income_attributable_to_minority = 0 
        saveSet["other_comprehensive_income_attributable_to_minority"] = other_comprehensive_income_attributable_to_minority

        try:
            total_comprehensive_income = float(dataSet.get("total_comprehensive_income")) 
        except:
            total_comprehensive_income = 0 
        saveSet["total_comprehensive_income"] = total_comprehensive_income

        try:
            total_comprehensive_income_attributable_to_parent = float(dataSet.get("total_comprehensive_income_attributable_to_parent")) 
        except:
            total_comprehensive_income_attributable_to_parent = 0 
        saveSet["total_comprehensive_income_attributable_to_parent"] = total_comprehensive_income_attributable_to_parent

        try:
            total_comprehensive_income_attributable_to_minority = float(dataSet.get("total_comprehensive_income_attributable_to_minority")) 
        except:
            total_comprehensive_income_attributable_to_minority = 0 
        saveSet["total_comprehensive_income_attributable_to_minority"] = total_comprehensive_income_attributable_to_minority

        try:
            basic_earnings_per_share = float(dataSet.get("basic_earnings_per_share")) 
        except:
            basic_earnings_per_share = 0 
        saveSet["basic_earnings_per_share"] = basic_earnings_per_share

        try:
            diluted_earnings_per_share = float(dataSet.get("diluted_earnings_per_share")) 
        except:
            diluted_earnings_per_share = 0 
        saveSet["diluted_earnings_per_share"] = diluted_earnings_per_share

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


#income_statements 修改记录
def update_income_statements(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        stock_code = dataSet.get("stock_code") 
        if stock_code:
            saveSet["stock_code"] = stock_code

        report_date = dataSet.get("report_date") 
        if report_date:
            saveSet["report_date"] = report_date

        try:
            total_operating_revenue = float(dataSet.get("total_operating_revenue")) 
            saveSet["total_operating_revenue"] = total_operating_revenue
        except:
            pass

        try:
            operating_revenue = float(dataSet.get("operating_revenue")) 
            saveSet["operating_revenue"] = operating_revenue
        except:
            pass

        try:
            interest_income = float(dataSet.get("interest_income")) 
            saveSet["interest_income"] = interest_income
        except:
            pass

        try:
            earned_premiums = float(dataSet.get("earned_premiums")) 
            saveSet["earned_premiums"] = earned_premiums
        except:
            pass

        try:
            fees_and_commissions_income = float(dataSet.get("fees_and_commissions_income")) 
            saveSet["fees_and_commissions_income"] = fees_and_commissions_income
        except:
            pass

        try:
            real_estate_sales_revenue = float(dataSet.get("real_estate_sales_revenue")) 
            saveSet["real_estate_sales_revenue"] = real_estate_sales_revenue
        except:
            pass

        try:
            other_business_revenue = float(dataSet.get("other_business_revenue")) 
            saveSet["other_business_revenue"] = other_business_revenue
        except:
            pass

        try:
            total_operating_costs = float(dataSet.get("total_operating_costs")) 
            saveSet["total_operating_costs"] = total_operating_costs
        except:
            pass

        try:
            operating_costs = float(dataSet.get("operating_costs")) 
            saveSet["operating_costs"] = operating_costs
        except:
            pass

        try:
            fees_and_commissions_expenses = float(dataSet.get("fees_and_commissions_expenses")) 
            saveSet["fees_and_commissions_expenses"] = fees_and_commissions_expenses
        except:
            pass

        try:
            real_estate_sales_costs = float(dataSet.get("real_estate_sales_costs")) 
            saveSet["real_estate_sales_costs"] = real_estate_sales_costs
        except:
            pass

        try:
            surrender_value = float(dataSet.get("surrender_value")) 
            saveSet["surrender_value"] = surrender_value
        except:
            pass

        try:
            net_claims_paid = float(dataSet.get("net_claims_paid")) 
            saveSet["net_claims_paid"] = net_claims_paid
        except:
            pass

        try:
            net_insurance_contract_reserves = float(dataSet.get("net_insurance_contract_reserves")) 
            saveSet["net_insurance_contract_reserves"] = net_insurance_contract_reserves
        except:
            pass

        try:
            policy_dividend_expenses = float(dataSet.get("policy_dividend_expenses")) 
            saveSet["policy_dividend_expenses"] = policy_dividend_expenses
        except:
            pass

        try:
            reinsurance_expenses = float(dataSet.get("reinsurance_expenses")) 
            saveSet["reinsurance_expenses"] = reinsurance_expenses
        except:
            pass

        try:
            other_business_costs = float(dataSet.get("other_business_costs")) 
            saveSet["other_business_costs"] = other_business_costs
        except:
            pass

        try:
            taxes_and_surcharges = float(dataSet.get("taxes_and_surcharges")) 
            saveSet["taxes_and_surcharges"] = taxes_and_surcharges
        except:
            pass

        try:
            rd_expenses = float(dataSet.get("rd_expenses")) 
            saveSet["rd_expenses"] = rd_expenses
        except:
            pass

        try:
            selling_expenses = float(dataSet.get("selling_expenses")) 
            saveSet["selling_expenses"] = selling_expenses
        except:
            pass

        try:
            administrative_expenses = float(dataSet.get("administrative_expenses")) 
            saveSet["administrative_expenses"] = administrative_expenses
        except:
            pass

        try:
            financial_expenses = float(dataSet.get("financial_expenses")) 
            saveSet["financial_expenses"] = financial_expenses
        except:
            pass

        try:
            interest_expenses = float(dataSet.get("interest_expenses")) 
            saveSet["interest_expenses"] = interest_expenses
        except:
            pass

        try:
            interest_expenditure = float(dataSet.get("interest_expenditure")) 
            saveSet["interest_expenditure"] = interest_expenditure
        except:
            pass

        try:
            investment_income = float(dataSet.get("investment_income")) 
            saveSet["investment_income"] = investment_income
        except:
            pass

        try:
            investment_income_from_associates_and_joint_ventures = float(dataSet.get("investment_income_from_associates_and_joint_ventures")) 
            saveSet["investment_income_from_associates_and_joint_ventures"] = investment_income_from_associates_and_joint_ventures
        except:
            pass

        try:
            gain_on_derecognition_of_financial_assets_at_amortized_cost = float(dataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost")) 
            saveSet["gain_on_derecognition_of_financial_assets_at_amortized_cost"] = gain_on_derecognition_of_financial_assets_at_amortized_cost
        except:
            pass

        try:
            foreign_exchange_gains = float(dataSet.get("foreign_exchange_gains")) 
            saveSet["foreign_exchange_gains"] = foreign_exchange_gains
        except:
            pass

        try:
            net_open_hedge_gains = float(dataSet.get("net_open_hedge_gains")) 
            saveSet["net_open_hedge_gains"] = net_open_hedge_gains
        except:
            pass

        try:
            fair_value_change_gains = float(dataSet.get("fair_value_change_gains")) 
            saveSet["fair_value_change_gains"] = fair_value_change_gains
        except:
            pass

        try:
            futures_gains_losses = float(dataSet.get("futures_gains_losses")) 
            saveSet["futures_gains_losses"] = futures_gains_losses
        except:
            pass

        try:
            custody_income = float(dataSet.get("custody_income")) 
            saveSet["custody_income"] = custody_income
        except:
            pass

        try:
            subsidy_income = float(dataSet.get("subsidy_income")) 
            saveSet["subsidy_income"] = subsidy_income
        except:
            pass

        try:
            other_gains = float(dataSet.get("other_gains")) 
            saveSet["other_gains"] = other_gains
        except:
            pass

        try:
            asset_impairment_losses = float(dataSet.get("asset_impairment_losses")) 
            saveSet["asset_impairment_losses"] = asset_impairment_losses
        except:
            pass

        try:
            credit_impairment_losses = float(dataSet.get("credit_impairment_losses")) 
            saveSet["credit_impairment_losses"] = credit_impairment_losses
        except:
            pass

        try:
            other_business_profits = float(dataSet.get("other_business_profits")) 
            saveSet["other_business_profits"] = other_business_profits
        except:
            pass

        try:
            asset_disposal_gains = float(dataSet.get("asset_disposal_gains")) 
            saveSet["asset_disposal_gains"] = asset_disposal_gains
        except:
            pass

        try:
            operating_profit = float(dataSet.get("operating_profit")) 
            saveSet["operating_profit"] = operating_profit
        except:
            pass

        try:
            non_operating_income = float(dataSet.get("non_operating_income")) 
            saveSet["non_operating_income"] = non_operating_income
        except:
            pass

        try:
            non_current_asset_disposal_gains = float(dataSet.get("non_current_asset_disposal_gains")) 
            saveSet["non_current_asset_disposal_gains"] = non_current_asset_disposal_gains
        except:
            pass

        try:
            non_operating_expenses = float(dataSet.get("non_operating_expenses")) 
            saveSet["non_operating_expenses"] = non_operating_expenses
        except:
            pass

        try:
            non_current_asset_disposal_losses = float(dataSet.get("non_current_asset_disposal_losses")) 
            saveSet["non_current_asset_disposal_losses"] = non_current_asset_disposal_losses
        except:
            pass

        try:
            total_profit = float(dataSet.get("total_profit")) 
            saveSet["total_profit"] = total_profit
        except:
            pass

        try:
            income_tax_expense = float(dataSet.get("income_tax_expense")) 
            saveSet["income_tax_expense"] = income_tax_expense
        except:
            pass

        try:
            unrecognized_investment_losses = float(dataSet.get("unrecognized_investment_losses")) 
            saveSet["unrecognized_investment_losses"] = unrecognized_investment_losses
        except:
            pass

        try:
            net_profit = float(dataSet.get("net_profit")) 
            saveSet["net_profit"] = net_profit
        except:
            pass

        try:
            net_profit_from_continuing_operations = float(dataSet.get("net_profit_from_continuing_operations")) 
            saveSet["net_profit_from_continuing_operations"] = net_profit_from_continuing_operations
        except:
            pass

        try:
            net_profit_from_discontinued_operations = float(dataSet.get("net_profit_from_discontinued_operations")) 
            saveSet["net_profit_from_discontinued_operations"] = net_profit_from_discontinued_operations
        except:
            pass

        try:
            net_profit_attributable_to_parent_company = float(dataSet.get("net_profit_attributable_to_parent_company")) 
            saveSet["net_profit_attributable_to_parent_company"] = net_profit_attributable_to_parent_company
        except:
            pass

        try:
            net_profit_of_acquiree_before_merger = float(dataSet.get("net_profit_of_acquiree_before_merger")) 
            saveSet["net_profit_of_acquiree_before_merger"] = net_profit_of_acquiree_before_merger
        except:
            pass

        try:
            minority_interests_profit_loss = float(dataSet.get("minority_interests_profit_loss")) 
            saveSet["minority_interests_profit_loss"] = minority_interests_profit_loss
        except:
            pass

        try:
            other_comprehensive_income = float(dataSet.get("other_comprehensive_income")) 
            saveSet["other_comprehensive_income"] = other_comprehensive_income
        except:
            pass

        try:
            other_comprehensive_income_attributable_to_parent = float(dataSet.get("other_comprehensive_income_attributable_to_parent")) 
            saveSet["other_comprehensive_income_attributable_to_parent"] = other_comprehensive_income_attributable_to_parent
        except:
            pass

        try:
            oci_not_reclassified_to_profit_loss = float(dataSet.get("oci_not_reclassified_to_profit_loss")) 
            saveSet["oci_not_reclassified_to_profit_loss"] = oci_not_reclassified_to_profit_loss
        except:
            pass

        try:
            remeasurement_of_defined_benefit_plans = float(dataSet.get("remeasurement_of_defined_benefit_plans")) 
            saveSet["remeasurement_of_defined_benefit_plans"] = remeasurement_of_defined_benefit_plans
        except:
            pass

        try:
            oci_under_equity_method_not_reclassified = float(dataSet.get("oci_under_equity_method_not_reclassified")) 
            saveSet["oci_under_equity_method_not_reclassified"] = oci_under_equity_method_not_reclassified
        except:
            pass

        try:
            fair_value_change_of_other_equity_instruments = float(dataSet.get("fair_value_change_of_other_equity_instruments")) 
            saveSet["fair_value_change_of_other_equity_instruments"] = fair_value_change_of_other_equity_instruments
        except:
            pass

        try:
            fair_value_change_of_own_credit_risk = float(dataSet.get("fair_value_change_of_own_credit_risk")) 
            saveSet["fair_value_change_of_own_credit_risk"] = fair_value_change_of_own_credit_risk
        except:
            pass

        try:
            oci_reclassified_to_profit_loss = float(dataSet.get("oci_reclassified_to_profit_loss")) 
            saveSet["oci_reclassified_to_profit_loss"] = oci_reclassified_to_profit_loss
        except:
            pass

        try:
            oci_under_equity_method_reclassified = float(dataSet.get("oci_under_equity_method_reclassified")) 
            saveSet["oci_under_equity_method_reclassified"] = oci_under_equity_method_reclassified
        except:
            pass

        try:
            fair_value_change_of_afs_financial_assets = float(dataSet.get("fair_value_change_of_afs_financial_assets")) 
            saveSet["fair_value_change_of_afs_financial_assets"] = fair_value_change_of_afs_financial_assets
        except:
            pass

        try:
            fair_value_change_of_other_debt_investments = float(dataSet.get("fair_value_change_of_other_debt_investments")) 
            saveSet["fair_value_change_of_other_debt_investments"] = fair_value_change_of_other_debt_investments
        except:
            pass

        try:
            financial_assets_reclassified_to_oci = float(dataSet.get("financial_assets_reclassified_to_oci")) 
            saveSet["financial_assets_reclassified_to_oci"] = financial_assets_reclassified_to_oci
        except:
            pass

        try:
            credit_impairment_of_other_debt_investments = float(dataSet.get("credit_impairment_of_other_debt_investments")) 
            saveSet["credit_impairment_of_other_debt_investments"] = credit_impairment_of_other_debt_investments
        except:
            pass

        try:
            htm_reclassified_to_afs_gains_losses = float(dataSet.get("htm_reclassified_to_afs_gains_losses")) 
            saveSet["htm_reclassified_to_afs_gains_losses"] = htm_reclassified_to_afs_gains_losses
        except:
            pass

        try:
            cash_flow_hedge_reserve = float(dataSet.get("cash_flow_hedge_reserve")) 
            saveSet["cash_flow_hedge_reserve"] = cash_flow_hedge_reserve
        except:
            pass

        try:
            effective_portion_of_cash_flow_hedge = float(dataSet.get("effective_portion_of_cash_flow_hedge")) 
            saveSet["effective_portion_of_cash_flow_hedge"] = effective_portion_of_cash_flow_hedge
        except:
            pass

        try:
            foreign_currency_translation_difference = float(dataSet.get("foreign_currency_translation_difference")) 
            saveSet["foreign_currency_translation_difference"] = foreign_currency_translation_difference
        except:
            pass

        try:
            other = float(dataSet.get("other")) 
            saveSet["other"] = other
        except:
            pass

        try:
            other_comprehensive_income_attributable_to_minority = float(dataSet.get("other_comprehensive_income_attributable_to_minority")) 
            saveSet["other_comprehensive_income_attributable_to_minority"] = other_comprehensive_income_attributable_to_minority
        except:
            pass

        try:
            total_comprehensive_income = float(dataSet.get("total_comprehensive_income")) 
            saveSet["total_comprehensive_income"] = total_comprehensive_income
        except:
            pass

        try:
            total_comprehensive_income_attributable_to_parent = float(dataSet.get("total_comprehensive_income_attributable_to_parent")) 
            saveSet["total_comprehensive_income_attributable_to_parent"] = total_comprehensive_income_attributable_to_parent
        except:
            pass

        try:
            total_comprehensive_income_attributable_to_minority = float(dataSet.get("total_comprehensive_income_attributable_to_minority")) 
            saveSet["total_comprehensive_income_attributable_to_minority"] = total_comprehensive_income_attributable_to_minority
        except:
            pass

        try:
            basic_earnings_per_share = float(dataSet.get("basic_earnings_per_share")) 
            saveSet["basic_earnings_per_share"] = basic_earnings_per_share
        except:
            pass

        try:
            diluted_earnings_per_share = float(dataSet.get("diluted_earnings_per_share")) 
            saveSet["diluted_earnings_per_share"] = diluted_earnings_per_share
        except:
            pass

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#income_statements 查询记录
def query_income_statements(tableName,id = "0", stock_code="",report_date="",
                            delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)

        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if stock_code:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_code = %s " 
                else:
                    sqlStr =  sqlStr + " WHERE stock_code = %s " 
                valuesList.append(stock_code)
            if report_date:
                if valuesList:
                    sqlStr =  sqlStr + " AND report_date = %s " 
                else:
                    sqlStr =  sqlStr + " WHERE report_date = %s " 
                valuesList.append(report_date)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#income_statements end 


#cash_flow_statements begin 

def tablename_convertor_cash_flow_statements():
    tableName = "cash_flow_statements"
    tableName = tableName.lower()
    return tableName


def decode_tablename_cash_flow_statements(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建cash_flow_statements表
def create_cash_flow_statements(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "stock_code VARCHAR(10) COMMENT '股票代码',",
    "report_date VARCHAR(10) NULL,",
    "total_operating_revenue FLOAT NULL,",
    "operating_revenue FLOAT NULL,",
    "interest_income FLOAT NULL,",
    "earned_premiums FLOAT NULL,",
    "fees_and_commissions_income FLOAT NULL,",
    "real_estate_sales_revenue FLOAT NULL,",
    "other_business_revenue FLOAT NULL,",
    "total_operating_costs FLOAT NULL,",
    "operating_costs FLOAT NULL,",
    "fees_and_commissions_expenses FLOAT NULL,",
    "real_estate_sales_costs FLOAT NULL,",
    "surrender_value FLOAT NULL,",
    "net_claims_paid FLOAT NULL,",
    "net_insurance_contract_reserves FLOAT NULL,",
    "policy_dividend_expenses FLOAT NULL,",
    "reinsurance_expenses FLOAT NULL,",
    "other_business_costs FLOAT NULL,",
    "taxes_and_surcharges FLOAT NULL,",
    "rd_expenses FLOAT NULL,",
    "selling_expenses FLOAT NULL,",
    "administrative_expenses FLOAT NULL,",
    "financial_expenses FLOAT NULL,",
    "interest_expenses FLOAT NULL,",
    "interest_expenditure FLOAT NULL,",
    "investment_income FLOAT NULL,",
    "investment_income_from_associates_and_joint_ventures FLOAT NULL,",
    "gain_on_derecognition_of_financial_assets_at_amortized_cost FLOAT NULL,",
    "foreign_exchange_gains FLOAT NULL,",
    "net_open_hedge_gains FLOAT NULL,",
    "fair_value_change_gains FLOAT NULL,",
    "futures_gains_losses FLOAT NULL,",
    "custody_income FLOAT NULL,",
    "subsidy_income FLOAT NULL,",
    "other_gains FLOAT NULL,",
    "asset_impairment_losses FLOAT NULL,",
    "credit_impairment_losses FLOAT NULL,",
    "other_business_profits FLOAT NULL,",
    "asset_disposal_gains FLOAT NULL,",
    "operating_profit FLOAT NULL,",
    "non_operating_income FLOAT NULL,",
    "non_current_asset_disposal_gains FLOAT NULL,",
    "non_operating_expenses FLOAT NULL,",
    "non_current_asset_disposal_losses FLOAT NULL,",
    "total_profit FLOAT NULL,",
    "income_tax_expense FLOAT NULL,",
    "unrecognized_investment_losses FLOAT NULL,",
    "net_profit FLOAT NULL,",
    "net_profit_from_continuing_operations FLOAT NULL,",
    "net_profit_from_discontinued_operations FLOAT NULL,",
    "net_profit_attributable_to_parent_company FLOAT NULL,",
    "net_profit_of_acquiree_before_merger FLOAT NULL,",
    "minority_interests_profit_loss FLOAT NULL,",
    "other_comprehensive_income FLOAT NULL,",
    "other_comprehensive_income_attributable_to_parent FLOAT NULL,",
    "oci_not_reclassified_to_profit_loss FLOAT NULL,",
    "remeasurement_of_defined_benefit_plans FLOAT NULL,",
    "oci_under_equity_method_not_reclassified FLOAT NULL,",
    "fair_value_change_of_other_equity_instruments FLOAT NULL,",
    "fair_value_change_of_own_credit_risk FLOAT NULL,",
    "oci_reclassified_to_profit_loss FLOAT NULL,",
    "oci_under_equity_method_reclassified FLOAT NULL,",
    "fair_value_change_of_afs_financial_assets FLOAT NULL,",
    "fair_value_change_of_other_debt_investments FLOAT NULL,",
    "financial_assets_reclassified_to_oci FLOAT NULL,",
    "credit_impairment_of_other_debt_investments FLOAT NULL,",
    "htm_reclassified_to_afs_gains_losses FLOAT NULL,",
    "cash_flow_hedge_reserve FLOAT NULL,",
    "effective_portion_of_cash_flow_hedge FLOAT NULL,",
    "foreign_currency_translation_difference FLOAT NULL,",
    "other FLOAT NULL,",
    "other_comprehensive_income_attributable_to_minority FLOAT NULL,",
    "total_comprehensive_income FLOAT NULL,",
    "total_comprehensive_income_attributable_to_parent FLOAT NULL,",
    "total_comprehensive_income_attributable_to_minority FLOAT NULL,",
    "basic_earnings_per_share FLOAT NULL,",
    "diluted_earnings_per_share FLOAT NULL,",
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
        sqlStr = "CREATE UNIQUE INDEX stock_report_index ON {0} ({1},{2}) ".format(tableName, "stock_code","report_date")
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "CREATE INDEX {1} ON {0}({1}) ".format(tableName, "indexKey")
        #rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除cash_flow_statements表
def drop_cash_flow_statements(tableName):
    result = dropTableGeneral(tableName)
    return result


#cash_flow_statements 删除记录
def delete_cash_flow_statements(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#cash_flow_statements 增加记录
def insert_cash_flow_statements(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["stock_code"] = dataSet.get("stock_code", "") 

        saveSet["report_date"] = dataSet.get("report_date", "") 

        try:
            total_operating_revenue = float(dataSet.get("total_operating_revenue")) 
        except:
            total_operating_revenue = 0 
        saveSet["total_operating_revenue"] = total_operating_revenue

        try:
            operating_revenue = float(dataSet.get("operating_revenue")) 
        except:
            operating_revenue = 0 
        saveSet["operating_revenue"] = operating_revenue

        try:
            interest_income = float(dataSet.get("interest_income")) 
        except:
            interest_income = 0 
        saveSet["interest_income"] = interest_income

        try:
            earned_premiums = float(dataSet.get("earned_premiums")) 
        except:
            earned_premiums = 0 
        saveSet["earned_premiums"] = earned_premiums

        try:
            fees_and_commissions_income = float(dataSet.get("fees_and_commissions_income")) 
        except:
            fees_and_commissions_income = 0 
        saveSet["fees_and_commissions_income"] = fees_and_commissions_income

        try:
            real_estate_sales_revenue = float(dataSet.get("real_estate_sales_revenue")) 
        except:
            real_estate_sales_revenue = 0 
        saveSet["real_estate_sales_revenue"] = real_estate_sales_revenue

        try:
            other_business_revenue = float(dataSet.get("other_business_revenue")) 
        except:
            other_business_revenue = 0 
        saveSet["other_business_revenue"] = other_business_revenue

        try:
            total_operating_costs = float(dataSet.get("total_operating_costs")) 
        except:
            total_operating_costs = 0 
        saveSet["total_operating_costs"] = total_operating_costs

        try:
            operating_costs = float(dataSet.get("operating_costs")) 
        except:
            operating_costs = 0 
        saveSet["operating_costs"] = operating_costs

        try:
            fees_and_commissions_expenses = float(dataSet.get("fees_and_commissions_expenses")) 
        except:
            fees_and_commissions_expenses = 0 
        saveSet["fees_and_commissions_expenses"] = fees_and_commissions_expenses

        try:
            real_estate_sales_costs = float(dataSet.get("real_estate_sales_costs")) 
        except:
            real_estate_sales_costs = 0 
        saveSet["real_estate_sales_costs"] = real_estate_sales_costs

        try:
            surrender_value = float(dataSet.get("surrender_value")) 
        except:
            surrender_value = 0 
        saveSet["surrender_value"] = surrender_value

        try:
            net_claims_paid = float(dataSet.get("net_claims_paid")) 
        except:
            net_claims_paid = 0 
        saveSet["net_claims_paid"] = net_claims_paid

        try:
            net_insurance_contract_reserves = float(dataSet.get("net_insurance_contract_reserves")) 
        except:
            net_insurance_contract_reserves = 0 
        saveSet["net_insurance_contract_reserves"] = net_insurance_contract_reserves

        try:
            policy_dividend_expenses = float(dataSet.get("policy_dividend_expenses")) 
        except:
            policy_dividend_expenses = 0 
        saveSet["policy_dividend_expenses"] = policy_dividend_expenses

        try:
            reinsurance_expenses = float(dataSet.get("reinsurance_expenses")) 
        except:
            reinsurance_expenses = 0 
        saveSet["reinsurance_expenses"] = reinsurance_expenses

        try:
            other_business_costs = float(dataSet.get("other_business_costs")) 
        except:
            other_business_costs = 0 
        saveSet["other_business_costs"] = other_business_costs

        try:
            taxes_and_surcharges = float(dataSet.get("taxes_and_surcharges")) 
        except:
            taxes_and_surcharges = 0 
        saveSet["taxes_and_surcharges"] = taxes_and_surcharges

        try:
            rd_expenses = float(dataSet.get("rd_expenses")) 
        except:
            rd_expenses = 0 
        saveSet["rd_expenses"] = rd_expenses

        try:
            selling_expenses = float(dataSet.get("selling_expenses")) 
        except:
            selling_expenses = 0 
        saveSet["selling_expenses"] = selling_expenses

        try:
            administrative_expenses = float(dataSet.get("administrative_expenses")) 
        except:
            administrative_expenses = 0 
        saveSet["administrative_expenses"] = administrative_expenses

        try:
            financial_expenses = float(dataSet.get("financial_expenses")) 
        except:
            financial_expenses = 0 
        saveSet["financial_expenses"] = financial_expenses

        try:
            interest_expenses = float(dataSet.get("interest_expenses")) 
        except:
            interest_expenses = 0 
        saveSet["interest_expenses"] = interest_expenses

        try:
            interest_expenditure = float(dataSet.get("interest_expenditure")) 
        except:
            interest_expenditure = 0 
        saveSet["interest_expenditure"] = interest_expenditure

        try:
            investment_income = float(dataSet.get("investment_income")) 
        except:
            investment_income = 0 
        saveSet["investment_income"] = investment_income

        try:
            investment_income_from_associates_and_joint_ventures = float(dataSet.get("investment_income_from_associates_and_joint_ventures")) 
        except:
            investment_income_from_associates_and_joint_ventures = 0 
        saveSet["investment_income_from_associates_and_joint_ventures"] = investment_income_from_associates_and_joint_ventures

        try:
            gain_on_derecognition_of_financial_assets_at_amortized_cost = float(dataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost")) 
        except:
            gain_on_derecognition_of_financial_assets_at_amortized_cost = 0 
        saveSet["gain_on_derecognition_of_financial_assets_at_amortized_cost"] = gain_on_derecognition_of_financial_assets_at_amortized_cost

        try:
            foreign_exchange_gains = float(dataSet.get("foreign_exchange_gains")) 
        except:
            foreign_exchange_gains = 0 
        saveSet["foreign_exchange_gains"] = foreign_exchange_gains

        try:
            net_open_hedge_gains = float(dataSet.get("net_open_hedge_gains")) 
        except:
            net_open_hedge_gains = 0 
        saveSet["net_open_hedge_gains"] = net_open_hedge_gains

        try:
            fair_value_change_gains = float(dataSet.get("fair_value_change_gains")) 
        except:
            fair_value_change_gains = 0 
        saveSet["fair_value_change_gains"] = fair_value_change_gains

        try:
            futures_gains_losses = float(dataSet.get("futures_gains_losses")) 
        except:
            futures_gains_losses = 0 
        saveSet["futures_gains_losses"] = futures_gains_losses

        try:
            custody_income = float(dataSet.get("custody_income")) 
        except:
            custody_income = 0 
        saveSet["custody_income"] = custody_income

        try:
            subsidy_income = float(dataSet.get("subsidy_income")) 
        except:
            subsidy_income = 0 
        saveSet["subsidy_income"] = subsidy_income

        try:
            other_gains = float(dataSet.get("other_gains")) 
        except:
            other_gains = 0 
        saveSet["other_gains"] = other_gains

        try:
            asset_impairment_losses = float(dataSet.get("asset_impairment_losses")) 
        except:
            asset_impairment_losses = 0 
        saveSet["asset_impairment_losses"] = asset_impairment_losses

        try:
            credit_impairment_losses = float(dataSet.get("credit_impairment_losses")) 
        except:
            credit_impairment_losses = 0 
        saveSet["credit_impairment_losses"] = credit_impairment_losses

        try:
            other_business_profits = float(dataSet.get("other_business_profits")) 
        except:
            other_business_profits = 0 
        saveSet["other_business_profits"] = other_business_profits

        try:
            asset_disposal_gains = float(dataSet.get("asset_disposal_gains")) 
        except:
            asset_disposal_gains = 0 
        saveSet["asset_disposal_gains"] = asset_disposal_gains

        try:
            operating_profit = float(dataSet.get("operating_profit")) 
        except:
            operating_profit = 0 
        saveSet["operating_profit"] = operating_profit

        try:
            non_operating_income = float(dataSet.get("non_operating_income")) 
        except:
            non_operating_income = 0 
        saveSet["non_operating_income"] = non_operating_income

        try:
            non_current_asset_disposal_gains = float(dataSet.get("non_current_asset_disposal_gains")) 
        except:
            non_current_asset_disposal_gains = 0 
        saveSet["non_current_asset_disposal_gains"] = non_current_asset_disposal_gains

        try:
            non_operating_expenses = float(dataSet.get("non_operating_expenses")) 
        except:
            non_operating_expenses = 0 
        saveSet["non_operating_expenses"] = non_operating_expenses

        try:
            non_current_asset_disposal_losses = float(dataSet.get("non_current_asset_disposal_losses")) 
        except:
            non_current_asset_disposal_losses = 0 
        saveSet["non_current_asset_disposal_losses"] = non_current_asset_disposal_losses

        try:
            total_profit = float(dataSet.get("total_profit")) 
        except:
            total_profit = 0 
        saveSet["total_profit"] = total_profit

        try:
            income_tax_expense = float(dataSet.get("income_tax_expense")) 
        except:
            income_tax_expense = 0 
        saveSet["income_tax_expense"] = income_tax_expense

        try:
            unrecognized_investment_losses = float(dataSet.get("unrecognized_investment_losses")) 
        except:
            unrecognized_investment_losses = 0 
        saveSet["unrecognized_investment_losses"] = unrecognized_investment_losses

        try:
            net_profit = float(dataSet.get("net_profit")) 
        except:
            net_profit = 0 
        saveSet["net_profit"] = net_profit

        try:
            net_profit_from_continuing_operations = float(dataSet.get("net_profit_from_continuing_operations")) 
        except:
            net_profit_from_continuing_operations = 0 
        saveSet["net_profit_from_continuing_operations"] = net_profit_from_continuing_operations

        try:
            net_profit_from_discontinued_operations = float(dataSet.get("net_profit_from_discontinued_operations")) 
        except:
            net_profit_from_discontinued_operations = 0 
        saveSet["net_profit_from_discontinued_operations"] = net_profit_from_discontinued_operations

        try:
            net_profit_attributable_to_parent_company = float(dataSet.get("net_profit_attributable_to_parent_company")) 
        except:
            net_profit_attributable_to_parent_company = 0 
        saveSet["net_profit_attributable_to_parent_company"] = net_profit_attributable_to_parent_company

        try:
            net_profit_of_acquiree_before_merger = float(dataSet.get("net_profit_of_acquiree_before_merger")) 
        except:
            net_profit_of_acquiree_before_merger = 0 
        saveSet["net_profit_of_acquiree_before_merger"] = net_profit_of_acquiree_before_merger

        try:
            minority_interests_profit_loss = float(dataSet.get("minority_interests_profit_loss")) 
        except:
            minority_interests_profit_loss = 0 
        saveSet["minority_interests_profit_loss"] = minority_interests_profit_loss

        try:
            other_comprehensive_income = float(dataSet.get("other_comprehensive_income")) 
        except:
            other_comprehensive_income = 0 
        saveSet["other_comprehensive_income"] = other_comprehensive_income

        try:
            other_comprehensive_income_attributable_to_parent = float(dataSet.get("other_comprehensive_income_attributable_to_parent")) 
        except:
            other_comprehensive_income_attributable_to_parent = 0 
        saveSet["other_comprehensive_income_attributable_to_parent"] = other_comprehensive_income_attributable_to_parent

        try:
            oci_not_reclassified_to_profit_loss = float(dataSet.get("oci_not_reclassified_to_profit_loss")) 
        except:
            oci_not_reclassified_to_profit_loss = 0 
        saveSet["oci_not_reclassified_to_profit_loss"] = oci_not_reclassified_to_profit_loss

        try:
            remeasurement_of_defined_benefit_plans = float(dataSet.get("remeasurement_of_defined_benefit_plans")) 
        except:
            remeasurement_of_defined_benefit_plans = 0 
        saveSet["remeasurement_of_defined_benefit_plans"] = remeasurement_of_defined_benefit_plans

        try:
            oci_under_equity_method_not_reclassified = float(dataSet.get("oci_under_equity_method_not_reclassified")) 
        except:
            oci_under_equity_method_not_reclassified = 0 
        saveSet["oci_under_equity_method_not_reclassified"] = oci_under_equity_method_not_reclassified

        try:
            fair_value_change_of_other_equity_instruments = float(dataSet.get("fair_value_change_of_other_equity_instruments")) 
        except:
            fair_value_change_of_other_equity_instruments = 0 
        saveSet["fair_value_change_of_other_equity_instruments"] = fair_value_change_of_other_equity_instruments

        try:
            fair_value_change_of_own_credit_risk = float(dataSet.get("fair_value_change_of_own_credit_risk")) 
        except:
            fair_value_change_of_own_credit_risk = 0 
        saveSet["fair_value_change_of_own_credit_risk"] = fair_value_change_of_own_credit_risk

        try:
            oci_reclassified_to_profit_loss = float(dataSet.get("oci_reclassified_to_profit_loss")) 
        except:
            oci_reclassified_to_profit_loss = 0 
        saveSet["oci_reclassified_to_profit_loss"] = oci_reclassified_to_profit_loss

        try:
            oci_under_equity_method_reclassified = float(dataSet.get("oci_under_equity_method_reclassified")) 
        except:
            oci_under_equity_method_reclassified = 0 
        saveSet["oci_under_equity_method_reclassified"] = oci_under_equity_method_reclassified

        try:
            fair_value_change_of_afs_financial_assets = float(dataSet.get("fair_value_change_of_afs_financial_assets")) 
        except:
            fair_value_change_of_afs_financial_assets = 0 
        saveSet["fair_value_change_of_afs_financial_assets"] = fair_value_change_of_afs_financial_assets

        try:
            fair_value_change_of_other_debt_investments = float(dataSet.get("fair_value_change_of_other_debt_investments")) 
        except:
            fair_value_change_of_other_debt_investments = 0 
        saveSet["fair_value_change_of_other_debt_investments"] = fair_value_change_of_other_debt_investments

        try:
            financial_assets_reclassified_to_oci = float(dataSet.get("financial_assets_reclassified_to_oci")) 
        except:
            financial_assets_reclassified_to_oci = 0 
        saveSet["financial_assets_reclassified_to_oci"] = financial_assets_reclassified_to_oci

        try:
            credit_impairment_of_other_debt_investments = float(dataSet.get("credit_impairment_of_other_debt_investments")) 
        except:
            credit_impairment_of_other_debt_investments = 0 
        saveSet["credit_impairment_of_other_debt_investments"] = credit_impairment_of_other_debt_investments

        try:
            htm_reclassified_to_afs_gains_losses = float(dataSet.get("htm_reclassified_to_afs_gains_losses")) 
        except:
            htm_reclassified_to_afs_gains_losses = 0 
        saveSet["htm_reclassified_to_afs_gains_losses"] = htm_reclassified_to_afs_gains_losses

        try:
            cash_flow_hedge_reserve = float(dataSet.get("cash_flow_hedge_reserve")) 
        except:
            cash_flow_hedge_reserve = 0 
        saveSet["cash_flow_hedge_reserve"] = cash_flow_hedge_reserve

        try:
            effective_portion_of_cash_flow_hedge = float(dataSet.get("effective_portion_of_cash_flow_hedge")) 
        except:
            effective_portion_of_cash_flow_hedge = 0 
        saveSet["effective_portion_of_cash_flow_hedge"] = effective_portion_of_cash_flow_hedge

        try:
            foreign_currency_translation_difference = float(dataSet.get("foreign_currency_translation_difference")) 
        except:
            foreign_currency_translation_difference = 0 
        saveSet["foreign_currency_translation_difference"] = foreign_currency_translation_difference

        try:
            other = float(dataSet.get("other")) 
        except:
            other = 0 
        saveSet["other"] = other

        try:
            other_comprehensive_income_attributable_to_minority = float(dataSet.get("other_comprehensive_income_attributable_to_minority")) 
        except:
            other_comprehensive_income_attributable_to_minority = 0 
        saveSet["other_comprehensive_income_attributable_to_minority"] = other_comprehensive_income_attributable_to_minority

        try:
            total_comprehensive_income = float(dataSet.get("total_comprehensive_income")) 
        except:
            total_comprehensive_income = 0 
        saveSet["total_comprehensive_income"] = total_comprehensive_income

        try:
            total_comprehensive_income_attributable_to_parent = float(dataSet.get("total_comprehensive_income_attributable_to_parent")) 
        except:
            total_comprehensive_income_attributable_to_parent = 0 
        saveSet["total_comprehensive_income_attributable_to_parent"] = total_comprehensive_income_attributable_to_parent

        try:
            total_comprehensive_income_attributable_to_minority = float(dataSet.get("total_comprehensive_income_attributable_to_minority")) 
        except:
            total_comprehensive_income_attributable_to_minority = 0 
        saveSet["total_comprehensive_income_attributable_to_minority"] = total_comprehensive_income_attributable_to_minority

        try:
            basic_earnings_per_share = float(dataSet.get("basic_earnings_per_share")) 
        except:
            basic_earnings_per_share = 0 
        saveSet["basic_earnings_per_share"] = basic_earnings_per_share

        try:
            diluted_earnings_per_share = float(dataSet.get("diluted_earnings_per_share")) 
        except:
            diluted_earnings_per_share = 0 
        saveSet["diluted_earnings_per_share"] = diluted_earnings_per_share

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


#cash_flow_statements 修改记录
def update_cash_flow_statements(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        stock_code = dataSet.get("stock_code") 
        if stock_code:
            saveSet["stock_code"] = stock_code

        report_date = dataSet.get("report_date") 
        if report_date:
            saveSet["report_date"] = report_date

        try:
            total_operating_revenue = float(dataSet.get("total_operating_revenue")) 
            saveSet["total_operating_revenue"] = total_operating_revenue
        except:
            pass

        try:
            operating_revenue = float(dataSet.get("operating_revenue")) 
            saveSet["operating_revenue"] = operating_revenue
        except:
            pass

        try:
            interest_income = float(dataSet.get("interest_income")) 
            saveSet["interest_income"] = interest_income
        except:
            pass

        try:
            earned_premiums = float(dataSet.get("earned_premiums")) 
            saveSet["earned_premiums"] = earned_premiums
        except:
            pass

        try:
            fees_and_commissions_income = float(dataSet.get("fees_and_commissions_income")) 
            saveSet["fees_and_commissions_income"] = fees_and_commissions_income
        except:
            pass

        try:
            real_estate_sales_revenue = float(dataSet.get("real_estate_sales_revenue")) 
            saveSet["real_estate_sales_revenue"] = real_estate_sales_revenue
        except:
            pass

        try:
            other_business_revenue = float(dataSet.get("other_business_revenue")) 
            saveSet["other_business_revenue"] = other_business_revenue
        except:
            pass

        try:
            total_operating_costs = float(dataSet.get("total_operating_costs")) 
            saveSet["total_operating_costs"] = total_operating_costs
        except:
            pass

        try:
            operating_costs = float(dataSet.get("operating_costs")) 
            saveSet["operating_costs"] = operating_costs
        except:
            pass

        try:
            fees_and_commissions_expenses = float(dataSet.get("fees_and_commissions_expenses")) 
            saveSet["fees_and_commissions_expenses"] = fees_and_commissions_expenses
        except:
            pass

        try:
            real_estate_sales_costs = float(dataSet.get("real_estate_sales_costs")) 
            saveSet["real_estate_sales_costs"] = real_estate_sales_costs
        except:
            pass

        try:
            surrender_value = float(dataSet.get("surrender_value")) 
            saveSet["surrender_value"] = surrender_value
        except:
            pass

        try:
            net_claims_paid = float(dataSet.get("net_claims_paid")) 
            saveSet["net_claims_paid"] = net_claims_paid
        except:
            pass

        try:
            net_insurance_contract_reserves = float(dataSet.get("net_insurance_contract_reserves")) 
            saveSet["net_insurance_contract_reserves"] = net_insurance_contract_reserves
        except:
            pass

        try:
            policy_dividend_expenses = float(dataSet.get("policy_dividend_expenses")) 
            saveSet["policy_dividend_expenses"] = policy_dividend_expenses
        except:
            pass

        try:
            reinsurance_expenses = float(dataSet.get("reinsurance_expenses")) 
            saveSet["reinsurance_expenses"] = reinsurance_expenses
        except:
            pass

        try:
            other_business_costs = float(dataSet.get("other_business_costs")) 
            saveSet["other_business_costs"] = other_business_costs
        except:
            pass

        try:
            taxes_and_surcharges = float(dataSet.get("taxes_and_surcharges")) 
            saveSet["taxes_and_surcharges"] = taxes_and_surcharges
        except:
            pass

        try:
            rd_expenses = float(dataSet.get("rd_expenses")) 
            saveSet["rd_expenses"] = rd_expenses
        except:
            pass

        try:
            selling_expenses = float(dataSet.get("selling_expenses")) 
            saveSet["selling_expenses"] = selling_expenses
        except:
            pass

        try:
            administrative_expenses = float(dataSet.get("administrative_expenses")) 
            saveSet["administrative_expenses"] = administrative_expenses
        except:
            pass

        try:
            financial_expenses = float(dataSet.get("financial_expenses")) 
            saveSet["financial_expenses"] = financial_expenses
        except:
            pass

        try:
            interest_expenses = float(dataSet.get("interest_expenses")) 
            saveSet["interest_expenses"] = interest_expenses
        except:
            pass

        try:
            interest_expenditure = float(dataSet.get("interest_expenditure")) 
            saveSet["interest_expenditure"] = interest_expenditure
        except:
            pass

        try:
            investment_income = float(dataSet.get("investment_income")) 
            saveSet["investment_income"] = investment_income
        except:
            pass

        try:
            investment_income_from_associates_and_joint_ventures = float(dataSet.get("investment_income_from_associates_and_joint_ventures")) 
            saveSet["investment_income_from_associates_and_joint_ventures"] = investment_income_from_associates_and_joint_ventures
        except:
            pass

        try:
            gain_on_derecognition_of_financial_assets_at_amortized_cost = float(dataSet.get("gain_on_derecognition_of_financial_assets_at_amortized_cost")) 
            saveSet["gain_on_derecognition_of_financial_assets_at_amortized_cost"] = gain_on_derecognition_of_financial_assets_at_amortized_cost
        except:
            pass

        try:
            foreign_exchange_gains = float(dataSet.get("foreign_exchange_gains")) 
            saveSet["foreign_exchange_gains"] = foreign_exchange_gains
        except:
            pass

        try:
            net_open_hedge_gains = float(dataSet.get("net_open_hedge_gains")) 
            saveSet["net_open_hedge_gains"] = net_open_hedge_gains
        except:
            pass

        try:
            fair_value_change_gains = float(dataSet.get("fair_value_change_gains")) 
            saveSet["fair_value_change_gains"] = fair_value_change_gains
        except:
            pass

        try:
            futures_gains_losses = float(dataSet.get("futures_gains_losses")) 
            saveSet["futures_gains_losses"] = futures_gains_losses
        except:
            pass

        try:
            custody_income = float(dataSet.get("custody_income")) 
            saveSet["custody_income"] = custody_income
        except:
            pass

        try:
            subsidy_income = float(dataSet.get("subsidy_income")) 
            saveSet["subsidy_income"] = subsidy_income
        except:
            pass

        try:
            other_gains = float(dataSet.get("other_gains")) 
            saveSet["other_gains"] = other_gains
        except:
            pass

        try:
            asset_impairment_losses = float(dataSet.get("asset_impairment_losses")) 
            saveSet["asset_impairment_losses"] = asset_impairment_losses
        except:
            pass

        try:
            credit_impairment_losses = float(dataSet.get("credit_impairment_losses")) 
            saveSet["credit_impairment_losses"] = credit_impairment_losses
        except:
            pass

        try:
            other_business_profits = float(dataSet.get("other_business_profits")) 
            saveSet["other_business_profits"] = other_business_profits
        except:
            pass

        try:
            asset_disposal_gains = float(dataSet.get("asset_disposal_gains")) 
            saveSet["asset_disposal_gains"] = asset_disposal_gains
        except:
            pass

        try:
            operating_profit = float(dataSet.get("operating_profit")) 
            saveSet["operating_profit"] = operating_profit
        except:
            pass

        try:
            non_operating_income = float(dataSet.get("non_operating_income")) 
            saveSet["non_operating_income"] = non_operating_income
        except:
            pass

        try:
            non_current_asset_disposal_gains = float(dataSet.get("non_current_asset_disposal_gains")) 
            saveSet["non_current_asset_disposal_gains"] = non_current_asset_disposal_gains
        except:
            pass

        try:
            non_operating_expenses = float(dataSet.get("non_operating_expenses")) 
            saveSet["non_operating_expenses"] = non_operating_expenses
        except:
            pass

        try:
            non_current_asset_disposal_losses = float(dataSet.get("non_current_asset_disposal_losses")) 
            saveSet["non_current_asset_disposal_losses"] = non_current_asset_disposal_losses
        except:
            pass

        try:
            total_profit = float(dataSet.get("total_profit")) 
            saveSet["total_profit"] = total_profit
        except:
            pass

        try:
            income_tax_expense = float(dataSet.get("income_tax_expense")) 
            saveSet["income_tax_expense"] = income_tax_expense
        except:
            pass

        try:
            unrecognized_investment_losses = float(dataSet.get("unrecognized_investment_losses")) 
            saveSet["unrecognized_investment_losses"] = unrecognized_investment_losses
        except:
            pass

        try:
            net_profit = float(dataSet.get("net_profit")) 
            saveSet["net_profit"] = net_profit
        except:
            pass

        try:
            net_profit_from_continuing_operations = float(dataSet.get("net_profit_from_continuing_operations")) 
            saveSet["net_profit_from_continuing_operations"] = net_profit_from_continuing_operations
        except:
            pass

        try:
            net_profit_from_discontinued_operations = float(dataSet.get("net_profit_from_discontinued_operations")) 
            saveSet["net_profit_from_discontinued_operations"] = net_profit_from_discontinued_operations
        except:
            pass

        try:
            net_profit_attributable_to_parent_company = float(dataSet.get("net_profit_attributable_to_parent_company")) 
            saveSet["net_profit_attributable_to_parent_company"] = net_profit_attributable_to_parent_company
        except:
            pass

        try:
            net_profit_of_acquiree_before_merger = float(dataSet.get("net_profit_of_acquiree_before_merger")) 
            saveSet["net_profit_of_acquiree_before_merger"] = net_profit_of_acquiree_before_merger
        except:
            pass

        try:
            minority_interests_profit_loss = float(dataSet.get("minority_interests_profit_loss")) 
            saveSet["minority_interests_profit_loss"] = minority_interests_profit_loss
        except:
            pass

        try:
            other_comprehensive_income = float(dataSet.get("other_comprehensive_income")) 
            saveSet["other_comprehensive_income"] = other_comprehensive_income
        except:
            pass

        try:
            other_comprehensive_income_attributable_to_parent = float(dataSet.get("other_comprehensive_income_attributable_to_parent")) 
            saveSet["other_comprehensive_income_attributable_to_parent"] = other_comprehensive_income_attributable_to_parent
        except:
            pass

        try:
            oci_not_reclassified_to_profit_loss = float(dataSet.get("oci_not_reclassified_to_profit_loss")) 
            saveSet["oci_not_reclassified_to_profit_loss"] = oci_not_reclassified_to_profit_loss
        except:
            pass

        try:
            remeasurement_of_defined_benefit_plans = float(dataSet.get("remeasurement_of_defined_benefit_plans")) 
            saveSet["remeasurement_of_defined_benefit_plans"] = remeasurement_of_defined_benefit_plans
        except:
            pass

        try:
            oci_under_equity_method_not_reclassified = float(dataSet.get("oci_under_equity_method_not_reclassified")) 
            saveSet["oci_under_equity_method_not_reclassified"] = oci_under_equity_method_not_reclassified
        except:
            pass

        try:
            fair_value_change_of_other_equity_instruments = float(dataSet.get("fair_value_change_of_other_equity_instruments")) 
            saveSet["fair_value_change_of_other_equity_instruments"] = fair_value_change_of_other_equity_instruments
        except:
            pass

        try:
            fair_value_change_of_own_credit_risk = float(dataSet.get("fair_value_change_of_own_credit_risk")) 
            saveSet["fair_value_change_of_own_credit_risk"] = fair_value_change_of_own_credit_risk
        except:
            pass

        try:
            oci_reclassified_to_profit_loss = float(dataSet.get("oci_reclassified_to_profit_loss")) 
            saveSet["oci_reclassified_to_profit_loss"] = oci_reclassified_to_profit_loss
        except:
            pass

        try:
            oci_under_equity_method_reclassified = float(dataSet.get("oci_under_equity_method_reclassified")) 
            saveSet["oci_under_equity_method_reclassified"] = oci_under_equity_method_reclassified
        except:
            pass

        try:
            fair_value_change_of_afs_financial_assets = float(dataSet.get("fair_value_change_of_afs_financial_assets")) 
            saveSet["fair_value_change_of_afs_financial_assets"] = fair_value_change_of_afs_financial_assets
        except:
            pass

        try:
            fair_value_change_of_other_debt_investments = float(dataSet.get("fair_value_change_of_other_debt_investments")) 
            saveSet["fair_value_change_of_other_debt_investments"] = fair_value_change_of_other_debt_investments
        except:
            pass

        try:
            financial_assets_reclassified_to_oci = float(dataSet.get("financial_assets_reclassified_to_oci")) 
            saveSet["financial_assets_reclassified_to_oci"] = financial_assets_reclassified_to_oci
        except:
            pass

        try:
            credit_impairment_of_other_debt_investments = float(dataSet.get("credit_impairment_of_other_debt_investments")) 
            saveSet["credit_impairment_of_other_debt_investments"] = credit_impairment_of_other_debt_investments
        except:
            pass

        try:
            htm_reclassified_to_afs_gains_losses = float(dataSet.get("htm_reclassified_to_afs_gains_losses")) 
            saveSet["htm_reclassified_to_afs_gains_losses"] = htm_reclassified_to_afs_gains_losses
        except:
            pass

        try:
            cash_flow_hedge_reserve = float(dataSet.get("cash_flow_hedge_reserve")) 
            saveSet["cash_flow_hedge_reserve"] = cash_flow_hedge_reserve
        except:
            pass

        try:
            effective_portion_of_cash_flow_hedge = float(dataSet.get("effective_portion_of_cash_flow_hedge")) 
            saveSet["effective_portion_of_cash_flow_hedge"] = effective_portion_of_cash_flow_hedge
        except:
            pass

        try:
            foreign_currency_translation_difference = float(dataSet.get("foreign_currency_translation_difference")) 
            saveSet["foreign_currency_translation_difference"] = foreign_currency_translation_difference
        except:
            pass

        try:
            other = float(dataSet.get("other")) 
            saveSet["other"] = other
        except:
            pass

        try:
            other_comprehensive_income_attributable_to_minority = float(dataSet.get("other_comprehensive_income_attributable_to_minority")) 
            saveSet["other_comprehensive_income_attributable_to_minority"] = other_comprehensive_income_attributable_to_minority
        except:
            pass

        try:
            total_comprehensive_income = float(dataSet.get("total_comprehensive_income")) 
            saveSet["total_comprehensive_income"] = total_comprehensive_income
        except:
            pass

        try:
            total_comprehensive_income_attributable_to_parent = float(dataSet.get("total_comprehensive_income_attributable_to_parent")) 
            saveSet["total_comprehensive_income_attributable_to_parent"] = total_comprehensive_income_attributable_to_parent
        except:
            pass

        try:
            total_comprehensive_income_attributable_to_minority = float(dataSet.get("total_comprehensive_income_attributable_to_minority")) 
            saveSet["total_comprehensive_income_attributable_to_minority"] = total_comprehensive_income_attributable_to_minority
        except:
            pass

        try:
            basic_earnings_per_share = float(dataSet.get("basic_earnings_per_share")) 
            saveSet["basic_earnings_per_share"] = basic_earnings_per_share
        except:
            pass

        try:
            diluted_earnings_per_share = float(dataSet.get("diluted_earnings_per_share")) 
            saveSet["diluted_earnings_per_share"] = diluted_earnings_per_share
        except:
            pass

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#cash_flow_statements 查询记录
def query_cash_flow_statements(tableName,id = "0", stock_code="",report_date="",
                                delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)

        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if stock_code:
                if valuesList:
                    sqlStr = sqlStr + " AND stock_code = %s"
                else:
                    sqlStr = sqlStr + " WHERE stock_code = %s"
                valuesList.append(stock_code)
            if report_date:
                if valuesList:
                    sqlStr = sqlStr + " AND report_date = %s"
                else:
                    sqlStr = sqlStr + " WHERE report_date = %s"
                valuesList.append(report_date)

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#cash_flow_statements end 


#indicator_medians begin 

def tablename_convertor_indicator_medians():
    tableName = "indicator_medians"
    tableName = tableName.lower()
    return tableName


def decode_tablename_indicator_medians(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建indicator_medians表
def create_indicator_medians(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "indicator_name  VARCHAR(50) NULL,",
    "report_date  VARCHAR(10) NULL,",
    "median_value float NULL,",
    "cache_version  VARCHAR(50) NULL,",
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
        sqlStr = "CREATE UNIQUE INDEX indicator_report ON {0} ({1},{2}) ".format(tableName, "indicator_name","report_date")
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除indicator_medians表
def drop_indicator_medians(tableName):
    result = dropTableGeneral(tableName)
    return result


#indicator_medians 删除记录
def delete_indicator_medians(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#indicator_medians 增加记录
def insert_indicator_medians(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["indicator_name"] = dataSet.get("indicator_name", "") 

        saveSet["report_date"] = dataSet.get("report_date", "") 

        try:
            median_value = float(dataSet.get("median_value")) 
        except:
            median_value = 0 
        saveSet["median_value"] = median_value

        saveSet["cache_version"] = dataSet.get("cache_version", "") 

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


#indicator_medians 修改记录
def update_indicator_medians(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        indicator_name = dataSet.get("indicator_name") 
        if indicator_name:
            saveSet["indicator_name"] = indicator_name

        report_date = dataSet.get("report_date") 
        if report_date:
            saveSet["report_date"] = report_date

        try:
            median_value = float(dataSet.get("median_value")) 
            saveSet["median_value"] = median_value
        except:
            pass

        cache_version = dataSet.get("cache_version") 
        if cache_version:
            saveSet["cache_version"] = cache_version

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#indicator_medians 查询记录
def query_indicator_medians(tableName,id = "0",indicator_name = "",report_date = "", delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)

        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
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

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#indicator_medians end 


#user_stock_list begin 

def tablename_convertor_user_stock_list():
    tableName = "user_stock_list"
    tableName = tableName.lower()
    return tableName


def decode_tablename_user_stock_list(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建user_stock_list表
def create_user_stock_list(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "userID VARCHAR(32) COMMENT '用户ID',",
    "username VARCHAR(48) COMMENT '用户名称',",
    "stock_code VARCHAR(10) COMMENT '股票代码',",
    "stock_name VARCHAR(12) COMMENT '股票名称',",
    "user_plan VARCHAR(8) COMMENT '用户计划',",
    "plan_status VARCHAR(1) COMMENT '用户计划状态',",
    "history_update VARCHAR(1) COMMENT '历史数据更新',",
    "history_start_date VARCHAR(10) COMMENT '历史数据更新起始日期',",
    "history_end_date VARCHAR(10) COMMENT '历史数据更新结束日期',",
    "initial_weight FLOAT COMMENT '初始占比',",
    "current_weight FLOAT COMMENT '当前占比',",
    "initial_cap DOUBLE COMMENT '初始总额',",
    "current_cap DOUBLE COMMENT '当前总额',",
    "initial_volume INT COMMENT '初始股数',",
    "current_volume INT COMMENT '当前股数',",
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
        sqlStr = "CREATE UNIQUE INDEX idx_user_stock_plan ON {0} ({1},{2},{3}) ".format(tableName, "userID","stock_code","user_plan")
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除user_stock_list表
def drop_user_stock_list(tableName):
    result = dropTableGeneral(tableName)
    return result


#user_stock_list 删除记录
def delete_user_stock_list(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#user_stock_list 增加记录
def insert_user_stock_list(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["userID"] = dataSet.get("userID", "") 

        saveSet["username"] = dataSet.get("username", "") 

        saveSet["stock_code"] = dataSet.get("stock_code", "") 

        saveSet["stock_name"] = dataSet.get("stock_name", "") 

        saveSet["user_plan"] = dataSet.get("user_plan", "") 

        saveSet["plan_status"] = dataSet.get("plan_status", "") 

        saveSet["history_update"] = dataSet.get("history_update", "") 

        saveSet["history_start_date"] = dataSet.get("history_start_date", "") 

        saveSet["history_end_date"] = dataSet.get("history_end_date", "") 

        try:
            initial_weight = float(dataSet.get("initial_weight")) 
        except:
            initial_weight = 0 
        saveSet["initial_weight"] = initial_weight

        try:
            current_weight = float(dataSet.get("current_weight")) 
        except:
            current_weight = 0 
        saveSet["current_weight"] = current_weight

        try:
            initial_cap = float(dataSet.get("initial_cap")) 
        except:
            initial_cap = 0 
        saveSet["initial_cap"] = initial_cap

        try:
            current_cap = float(dataSet.get("current_cap")) 
        except:
            current_cap = 0 
        saveSet["current_cap"] = current_cap

        try:
            initial_volume = int(dataSet.get("initial_volume")) 
        except:
            initial_volume = 0 
        saveSet["initial_volume"] = initial_volume

        try:
            current_volume = int(dataSet.get("current_volume")) 
        except:
            current_volume = 0 
        saveSet["current_volume"] = current_volume

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


#user_stock_list 修改记录
def update_user_stock_list(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        userID = dataSet.get("userID") 
        if userID:
            saveSet["userID"] = userID

        username = dataSet.get("username") 
        if username:
            saveSet["username"] = username

        stock_code = dataSet.get("stock_code") 
        if stock_code:
            saveSet["stock_code"] = stock_code

        stock_name = dataSet.get("stock_name") 
        if stock_name:
            saveSet["stock_name"] = stock_name

        user_plan = dataSet.get("user_plan") 
        if user_plan:
            saveSet["user_plan"] = user_plan

        plan_status = dataSet.get("plan_status") 
        if plan_status:
            saveSet["plan_status"] = plan_status

        history_update = dataSet.get("history_update") 
        if history_update:
            saveSet["history_update"] = history_update

        history_start_date = dataSet.get("history_start_date") 
        if history_start_date:
            saveSet["history_start_date"] = history_start_date

        history_end_date = dataSet.get("history_end_date") 
        if history_end_date:
            saveSet["history_end_date"] = history_end_date

        try:
            initial_weight = float(dataSet.get("initial_weight")) 
            saveSet["initial_weight"] = initial_weight
        except:
            pass

        try:
            current_weight = float(dataSet.get("current_weight")) 
            saveSet["current_weight"] = current_weight
        except:
            pass

        try:
            initial_cap = float(dataSet.get("initial_cap")) 
            saveSet["initial_cap"] = initial_cap
        except:
            pass

        try:
            current_cap = float(dataSet.get("current_cap")) 
            saveSet["current_cap"] = current_cap
        except:
            pass

        try:
            initial_volume = int(dataSet.get("initial_volume")) 
            saveSet["initial_volume"] = initial_volume
        except:
            pass

        try:
            current_volume = int(dataSet.get("current_volume")) 
            saveSet["current_volume"] = current_volume
        except:
            pass

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#user_stock_list 查询记录
def query_user_stock_list(tableName,id = "0", userID = "", stock_code = "", searchKey = "", user_plan = "default" ,plan_status = "Y",history_update = "",
                            history_start_date="", history_end_date="", delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)

        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if userID:
                if valuesList:
                    sqlStr =  sqlStr + " AND userID = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE userID = %s" 
                valuesList.append(userID)
            if user_plan:
                if valuesList:
                    sqlStr =  sqlStr + " AND user_plan = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE user_plan = %s" 
                valuesList.append(user_plan)
            if searchKey:
                if valuesList:
                    sqlStr =  sqlStr + " AND (stock_code LIKE %s OR stock_name LIKE %s ) " 
                else:
                    sqlStr =  sqlStr + " WHERE (stock_code LIKE %s OR stock_name LIKE %s ) " 
                aKey = "%" + searchKey + "%"
                valuesList.append(aKey)
                valuesList.append(aKey)
            if plan_status:
                if valuesList:
                    sqlStr =  sqlStr + " AND plan_status = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE plan_status = %s" 
                valuesList.append(plan_status)
            if history_update:
                if valuesList:
                    sqlStr =  sqlStr + " AND history_update = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE history_update = %s" 
                valuesList.append(history_update)
            if history_start_date:
                if valuesList:
                    sqlStr =  sqlStr + " AND history_update >= %s" 
                else:
                    sqlStr =  sqlStr + " WHERE history_update >= %s" 
                valuesList.append(history_start_date)
            if history_end_date:
                if valuesList:
                    sqlStr =  sqlStr + " AND history_update < %s" 
                else:
                    sqlStr =  sqlStr + " WHERE history_update < %s" 
                valuesList.append(history_end_date)
            if stock_code:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_code = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE stock_code = %s" 
                valuesList.append(stock_code)
            if stock_code:
                if valuesList:
                    sqlStr =  sqlStr + " AND stock_code = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE stock_code = %s" 
                valuesList.append(stock_code)

        # if limitNum > 0:
        #     sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#user_stock_list 查询记录

#获取唯一的所有用户持仓股票代码
def get_unique_user_stock_list():
    result = []
    columns = "*"
    valuesList = []
    tableName = tablename_convertor_user_stock_list()
    sqlStr = f"SELECT DISTINCT stock_code FROM {tableName}"

    try:
        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            for data in dataList:
                stock_code = data.get("stock_code","")
                if stock_code:
                    result.append(stock_code)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#user_stock_list end 


#data_check_log begin 

def tablename_convertor_data_check_log():
    tableName = "data_check_log"
    tableName = tableName.lower()
    return tableName


def decode_tablename_data_check_log(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建data_check_log表
def create_data_check_log(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "check_processor VARCHAR(32) COMMENT '检查进程',",
    "check_type VARCHAR(32) COMMENT '检查类型',",
    "report_date VARCHAR(10) COMMENT '报告日期YMD',",
    "`description` VARCHAR(100) COMMENT '描述',",
    "start_date VARCHAR(20) COMMENT '开始时间',",
    "end_date VARCHAR(20) COMMENT '结束时间',",
    "error_desc VARCHAR(48) COMMENT '错误描述',",
    "proc_num INT COMMENT '处理个数',",
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
        sqlStr = "CREATE INDEX {1} ON {0}({1}) ".format(tableName, "check_processor")
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除data_check_log表
def drop_data_check_log(tableName):
    result = dropTableGeneral(tableName)
    return result


#data_check_log 删除记录
def delete_data_check_log(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#data_check_log 增加记录
def insert_data_check_log(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["check_processor"] = dataSet.get("check_processor", "") 

        saveSet["check_type"] = dataSet.get("check_type", "") 

        saveSet["report_date"] = dataSet.get("report_date", "") 

        saveSet["description"] = dataSet.get("description", "") 

        saveSet["start_date"] = dataSet.get("start_date", "") 

        saveSet["end_date"] = dataSet.get("end_date", "") 

        saveSet["error_desc"] = dataSet.get("error_desc", "") 

        try:
            proc_num = int(dataSet.get("proc_num")) 
        except:
            proc_num = 0 
        saveSet["proc_num"] = proc_num

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


#data_check_log 修改记录
def update_data_check_log(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        check_processor = dataSet.get("check_processor") 
        if check_processor:
            saveSet["check_processor"] = check_processor

        check_type = dataSet.get("check_type") 
        if check_type:
            saveSet["check_type"] = check_type

        report_date = dataSet.get("report_date") 
        if report_date:
            saveSet["report_date"] = report_date

        description = dataSet.get("description") 
        if description:
            saveSet["description"] = description

        start_date = dataSet.get("start_date") 
        if start_date:
            saveSet["start_date"] = start_date

        end_date = dataSet.get("end_date") 
        if end_date:
            saveSet["end_date"] = end_date

        error_desc = dataSet.get("error_desc") 
        if error_desc:
            saveSet["error_desc"] = error_desc

        try:
            proc_num = int(dataSet.get("proc_num")) 
            saveSet["proc_num"] = proc_num
        except:
            pass

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#data_check_log 查询记录
def query_data_check_log(tableName,id = "0", date = "", start_date = "", end_date = "", delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)
        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if date:
                if valuesList:
                    sqlStr =  sqlStr + " AND report_date = %s" 
                else:
                    sqlStr =  sqlStr + " WHERE report_date = %s" 
                valuesList.append(date)

            if start_date:
                if valuesList:
                    sqlStr =  sqlStr + " AND report_date >= %s" 
                else:
                    sqlStr =  sqlStr + " WHERE report_date >= %s" 
                valuesList.append(start_date)
            if end_date:
                if valuesList:
                    sqlStr =  sqlStr + " AND report_date < %s" 
                else:
                    sqlStr =  sqlStr + " WHERE report_date < %s" 
                valuesList.append(end_date)

        #if limitNum > 0:
            #sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#data_check_log end 

#trade_day_record begin 

def tablename_convertor_trade_day_record():
    tableName = "trade_day_record"
    tableName = tableName.lower()
    return tableName


def decode_tablename_trade_day_record(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建trade_day_record表
def create_trade_day_record(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "trade_day VARCHAR(10) COMMENT '交易日',",
    "`description` VARCHAR(50) COMMENT '检查类型',",
    "stock_day VARCHAR(1) COMMENT '股票日数据',",
    "stock_day_qfq VARCHAR(1) COMMENT '股票日数据前复权',",
    "stock_day_hfq VARCHAR(1) COMMENT '股票后数据后复权',",
    "stock_week VARCHAR(1) COMMENT '股票周数据',",
    "stock_week_qfq VARCHAR(1) COMMENT '股票周数据前复权',",
    "stock_week_hfq VARCHAR(1) COMMENT '股票周数据后复权',",
    "stock_month VARCHAR(1) COMMENT '股票月数据',",
    "stock_month_qfq VARCHAR(1) COMMENT '股票月数据前复权',",
    "stock_month_hfq VARCHAR(1) COMMENT '股票月数据后复权',",
    "industry_day VARCHAR(1) COMMENT '行业日数据',",
    "industry_week VARCHAR(1) COMMENT '行业周数据',",
    "industry_month VARCHAR(1) COMMENT '行业月数据',",
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
        sqlStr = "CREATE UNIQUE INDEX {1} ON {0}({1}) ".format(tableName, "trade_day")
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除trade_day_record表
def drop_trade_day_record(tableName):
    result = dropTableGeneral(tableName)
    return result


#trade_day_record 删除记录
def delete_trade_day_record(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#trade_day_record 增加记录
def insert_trade_day_record(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["trade_day"] = dataSet.get("trade_day", "") 

        saveSet["description"] = dataSet.get("description", "") 

        saveSet["stock_day"] = dataSet.get("stock_day", "") 

        saveSet["stock_day_qfq"] = dataSet.get("stock_day_qfq", "") 

        saveSet["stock_day_hfq"] = dataSet.get("stock_day_hfq", "") 

        saveSet["stock_week"] = dataSet.get("stock_week", "") 

        saveSet["stock_week_qfq"] = dataSet.get("stock_week_qfq", "") 

        saveSet["stock_week_hfq"] = dataSet.get("stock_week_hfq", "") 

        saveSet["stock_month"] = dataSet.get("stock_month", "") 

        saveSet["stock_month_qfq"] = dataSet.get("stock_month_qfq", "") 

        saveSet["stock_month_hfq"] = dataSet.get("stock_month_hfq", "") 

        saveSet["industry_day"] = dataSet.get("industry_day", "") 

        saveSet["industry_week"] = dataSet.get("industry_week", "") 

        saveSet["industry_month"] = dataSet.get("industry_month", "") 

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


#trade_day_record 修改记录
def update_trade_day_record(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        trade_day = dataSet.get("trade_day") 
        if trade_day:
            saveSet["trade_day"] = trade_day

        description = dataSet.get("description") 
        if description:
            saveSet["description"] = description

        stock_day = dataSet.get("stock_day") 
        if stock_day:
            saveSet["stock_day"] = stock_day

        stock_day_qfq = dataSet.get("stock_day_qfq") 
        if stock_day_qfq:
            saveSet["stock_day_qfq"] = stock_day_qfq

        stock_day_hfq = dataSet.get("stock_day_hfq") 
        if stock_day_hfq:
            saveSet["stock_day_hfq"] = stock_day_hfq

        stock_week = dataSet.get("stock_week") 
        if stock_week:
            saveSet["stock_week"] = stock_week

        stock_week_qfq = dataSet.get("stock_week_qfq") 
        if stock_week_qfq:
            saveSet["stock_week_qfq"] = stock_week_qfq

        stock_week_hfq = dataSet.get("stock_week_hfq") 
        if stock_week_hfq:
            saveSet["stock_week_hfq"] = stock_week_hfq

        stock_month = dataSet.get("stock_month") 
        if stock_month:
            saveSet["stock_month"] = stock_month

        stock_month_qfq = dataSet.get("stock_month_qfq") 
        if stock_month_qfq:
            saveSet["stock_month_qfq"] = stock_month_qfq

        stock_month_hfq = dataSet.get("stock_month_hfq") 
        if stock_month_hfq:
            saveSet["stock_month_hfq"] = stock_month_hfq

        industry_day = dataSet.get("industry_day") 
        if industry_day:
            saveSet["industry_day"] = industry_day

        industry_week = dataSet.get("industry_week") 
        if industry_week:
            saveSet["industry_week"] = industry_week

        industry_month = dataSet.get("industry_month") 
        if industry_month:
            saveSet["industry_month"] = industry_month

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#trade_day_record 查询记录
def query_trade_day_record(tableName,id = "0", beginDate="",endDate="",tradeDay="",delFlag = "0", mode = "full",limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)
        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        else:
            if tradeDay:
                if valuesList:
                    sqlStr += " AND trade_day = %s"
                else:
                    sqlStr += " WHERE trade_day = %s"
                valuesList.append(tradeDay)

            if beginDate:
                if valuesList:
                    sqlStr += " AND trade_day >= %s"
                else:
                    sqlStr += " WHERE trade_day >= %s"
                valuesList.append(beginDate)

            if endDate:
                if valuesList:
                    sqlStr += " AND trade_day < %s"
                else:
                    sqlStr += " WHERE trade_day < %s"
                valuesList.append(endDate)

        sqlStr += " ORDER BY trade_day"

        #if limitNum > 0:
            #sqlStr += " LIMIT {0}".format(limitNum)       

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#trade_day_record end 

#technical_signal begin 

def tablename_convertor_technical_signal():
    tableName = "technical_signal"
    tableName = tableName.lower()
    return tableName


def decode_tablename_technical_signal(tableName):
    result = {}
    aList = tableName.split("_")
    
    return result


#创建technical_signal表
def create_technical_signal(tableName):
    aList = ["CREATE TABLE IF NOT EXISTS %s("
    "id INT AUTO_INCREMENT PRIMARY KEY COMMENT '记录号',",
    "stock_code VARCHAR(10) NOT NULL COMMENT '股票代码',",
    "`date` VARCHAR(10) NOT NULL COMMENT '交易日期',",
    "`action` VARCHAR(5) NOT NULL COMMENT '交易信号',",
    "adjust VARCHAR(3) COMMENT '数据调整方式',",
    "period VARCHAR(6) COMMENT '数据周期',",
    "indicator VARCHAR(4) COMMENT '信号来源',",
    "subtype VARCHAR(20) COMMENT '信号子类型',",
    "`description` VARCHAR(500) COMMENT '描述',",
    "description2 VARCHAR(500) COMMENT '描述2',",
    "market_trend VARCHAR(10) COMMENT '市场趋势',",
    "market_status VARCHAR(10) COMMENT '震荡单边',",
    "calc_result VARCHAR(6000) NULL COMMENT '计算结果json值',",
    "readFlag VARCHAR(1) COMMENT '是否阅读标记',",
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
        index_name = f"{tableName}_unique_multi_idx"
        sqlStr = f"CREATE UNIQUE INDEX {index_name} ON {tableName} ({'stock_code'},{'date'},{'indicator'},{'adjust'},{'period'}) "
        rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "CREATE INDEX {1} ON {0}({1}) ".format(tableName, "indexKey")
        #rtn = mysqlDB.executeWrite(sqlStr)
        #sqlStr = "ALTER TABLE {0} auto_increment = {1} ".format(tableName,auto_increment_default_value)
        #rtn = mysqlDB.executeWrite(sqlStr)

    return result


#删除technical_signal表
def drop_technical_signal(tableName):
    result = dropTableGeneral(tableName)
    return result


#technical_signal 删除记录
def delete_technical_signal(tableName,id):
    result = 0
    sqlStr = f"DELETE FROM {tableName}"
    try:

        sqlStr += " WHERE id = %s"
        valuesList = [id] 
        result = mysqlDB.executeWrite(sqlStr,tuple(valuesList))

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#technical_signal 增加记录
def insert_technical_signal(tableName,dataSet):
    result = 0
    try:

        saveSet = {}

        saveSet["stock_code"] = dataSet.get("stock_code", "") 

        saveSet["date"] = dataSet.get("date", "") 

        saveSet["action"] = dataSet.get("action", "") 

        saveSet["adjust"] = dataSet.get("adjust", "") 

        saveSet["period"] = dataSet.get("period", "") 

        saveSet["indicator"] = dataSet.get("indicator", "") 

        saveSet["subtype"] = dataSet.get("subtype", "") 

        saveSet["description"] = dataSet.get("description", "") 

        saveSet["description2"] = dataSet.get("description2", "") 

        saveSet["market_trend"] = dataSet.get("market_trend", "") 

        saveSet["market_status"] = dataSet.get("market_status", "") 

        saveSet["calc_result"] = dataSet.get("calc_result", "") 

        saveSet["readFlag"] = dataSet.get("readFlag", "") 

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


#technical_signal 修改记录
def update_technical_signal(tableName,id,dataSet):
    result = -2
    try:
        saveSet = {}

        stock_code = dataSet.get("stock_code") 
        if stock_code:
            saveSet["stock_code"] = stock_code

        date = dataSet.get("date") 
        if date:
            saveSet["date"] = date

        action = dataSet.get("action") 
        if action:
            saveSet["action"] = action

        adjust = dataSet.get("adjust") 
        if adjust:
            saveSet["adjust"] = adjust

        period = dataSet.get("period") 
        if period:
            saveSet["period"] = period

        indicator = dataSet.get("indicator") 
        if indicator:
            saveSet["indicator"] = indicator

        subtype = dataSet.get("subtype") 
        if subtype:
            saveSet["subtype"] = subtype

        description = dataSet.get("description") 
        if description:
            saveSet["description"] = description

        description2 = dataSet.get("description2") 
        if description2:
            saveSet["description2"] = description2

        market_trend = dataSet.get("market_trend") 
        if market_trend:
            saveSet["market_trend"] = market_trend

        market_status = dataSet.get("market_status") 
        if market_status:
            saveSet["market_status"] = market_status

        calc_result = dataSet.get("calc_result") 
        if calc_result:
            saveSet["calc_result"] = calc_result

        readFlag = dataSet.get("readFlag") 
        if readFlag:
            saveSet["readFlag"] = readFlag

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

        keySqlstr = "id = %s"
        keyValues = [id]

        result = updateTableGeneral(tableName, keySqlstr,  keyValues, saveSet)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result


#technical_signal 查询记录
def query_technical_signal(tableName,id = "0", stock_code="", sortFlag="DESC", action="", date="", start_date="", end_date="",readFlag="", adjust="", period="", indicator="", delFlag = "0", limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT {columns} FROM {tableName}"

    try:

        try:
            id = int(id)
        except:
            id = 0

        if id > 0:
            sqlStr =  sqlStr + " WHERE id = %s" 
            valuesList = [id]  
        if stock_code:
            if valuesList:
                sqlStr += " AND stock_code = %s"
            else:
                sqlStr += " WHERE stock_code = %s"
            valuesList.append(stock_code)
        if action:
            if valuesList:
                sqlStr += " AND action = %s"
            else:
                sqlStr += " WHERE action = %s"
            valuesList.append(action)
        if readFlag:
            if valuesList:
                sqlStr += " AND readFlag = %s"
            else:
                sqlStr += " WHERE readFlag = %s"
            valuesList.append(readFlag)
        if adjust:
            if valuesList:
                sqlStr += " AND adjust = %s"
            else:
                sqlStr += " WHERE adjust = %s"
            valuesList.append(adjust)
        if period:
            if valuesList:
                sqlStr += " AND period = %s"
            else:
                sqlStr += " WHERE period = %s"
            valuesList.append(period)
        if indicator:
            if valuesList:
                sqlStr += " AND indicator = %s"
            else:
                sqlStr += " WHERE indicator = %s"
            valuesList.append(indicator)

        if date:
            if valuesList:
                sqlStr += " AND date = %s"
            else:
                sqlStr += " WHERE date = %s"
            valuesList.append(date)

        if start_date:
            if valuesList:
                sqlStr += " AND date >= %s"
            else:
                sqlStr += " WHERE date >= %s"
            valuesList.append(start_date)
        if end_date:
            if valuesList:
                sqlStr += " AND date < %s"
            else:
                sqlStr += " WHERE date < %s"
            valuesList.append(end_date)

        if sortFlag:
            sqlStr += " ORDER BY date DESC"

        if limitNum > 0:
            sqlStr += " LIMIT {0}".format(limitNum)

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result

#technical_signal and user_stock_list查询记录
def query_user_technical_signal(stock_code="",stock_name="", sortFlag="DESC", action="", date="", start_date="", end_date="",readFlag="", adjust="", period="", indicator="", userID = "", limitNum = comGD._DEF_MAX_QUERY_LIMIT_NUM):
    result = []
    tableName = tablename_convertor_technical_signal()
    tableName2 = tablename_convertor_user_stock_list()
    columns = "*"
    valuesList = []
    sqlStr = f"SELECT T1.*, T2.userID as userID, T2.username as username,T2.stock_name as stock_name FROM {tableName} as T1, {tableName2} as T2 WHERE T1.stock_code = T2.stock_code"

    try:
        if stock_code:
            sqlStr += " AND T1.stock_code = %s"
            valuesList.append(stock_code)
        if stock_name:
            sqlStr += " AND T2.stock_name = %s"
            valuesList.append(stock_name)
        if action:
            sqlStr += " AND action = %s"
            valuesList.append(action)
        if readFlag:
            sqlStr += " AND readFlag = %s"
            valuesList.append(readFlag)
        if adjust:
            sqlStr += " AND adjust = %s"
            valuesList.append(adjust)
        if period:
            sqlStr += " AND period = %s"
            valuesList.append(period)
        if indicator:
            sqlStr += " AND indicator = %s"
            valuesList.append(indicator)       

        if date:
            sqlStr += " AND date = %s"
            valuesList.append(date)

        if start_date:
            sqlStr += " AND date >= %s"
            valuesList.append(start_date)
        if end_date:
            sqlStr += " AND date < %s"
            valuesList.append(end_date)

        if sortFlag:
            sqlStr += f" ORDER BY date {sortFlag}"

        if limitNum > 0:
            sqlStr += f" LIMIT {limitNum}"

        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            dataList = dataFormatConvert(dataList)
            result = list(dataList)

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result

#technical_signal end 


# 查询表数据中date的最大值和最小值(第一个值和最后一个值)
def query_min_max_data(tableName,column="date",stock_code="",industry_code="",period="",adjust="",indicator=""):
    result = {}
    valuesList = []
    sqlStr = f"SELECT min({column}) as min_data, max({column}) as max_data FROM {tableName}"

    try:
        if stock_code:
            sqlStr += " WHERE stock_code = %s"
            valuesList.append(stock_code)
        if industry_code:
            if valuesList:
                sqlStr += " AND industry_code = %s"
            else:
                sqlStr += " WHERE industry_code = %s"
            valuesList.append(industry_code)
        if period:
            if valuesList:
                sqlStr += " AND period = %s"
            else:
                sqlStr += " WHERE period = %s"
            valuesList.append(period)
        if adjust:
            if valuesList:
                sqlStr += " AND adjust = %s"
            else:
                sqlStr += " WHERE adjust = %s"
            valuesList.append(adjust)
        if indicator:
            if valuesList:
                sqlStr += " AND indicator = %s"
            else:
                sqlStr += " WHERE indicator = %s"
            valuesList.append(indicator)
        
        rtn = mysqlDB.executeRead(sqlStr, tuple(valuesList))
        if rtn > 0:
            dataList = mysqlDB.fetchAll()
            min_data = dataList[0]["min_data"]
            if not min_data:
                min_data = ""
            max_data = dataList[0]["max_data"]
            if not max_data:
                max_data = ""
            result["min_data"] = min_data
            result["max_data"] = max_data
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        # if _DEBUG:
            # _LOG.error(f"{errMsg}")

    return result
 

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

    tableName = tablename_convertor_industry_info()
    if chkTableExist(tableName) == False:
        rtn = create_industry_info(tableName)

    tableName = tablename_convertor_stock_info()
    if chkTableExist(tableName) == False:
        rtn = create_stock_info(tableName)

    periods = ["day","week","month"]
    adjusts = ["","hfq","qfq"]
    for period in periods:
        for adjust in adjusts:
            #stock history data
            tableName = tablename_convertor_stock_history_data(period,adjust)
            if chkTableExist(tableName) == False:
                rtn = create_stock_history_data(tableName)
            tableName = tablename_convertor_technical_indicators(period,adjust)
            #stock technical indicators
            if chkTableExist(tableName) == False:
                rtn = create_technical_indicators(tableName)

    tableName = tablename_convertor_stock_dividend_data()
    if chkTableExist(tableName) == False:
        rtn = create_stock_dividend_data(tableName)

    periods = ["day","week","month"]
    for period in periods:
        #industry history data
        tableName = tablename_convertor_industry_history_data(period)
        if chkTableExist(tableName) == False:
            rtn = create_industry_history_data(tableName)
        #industry technical indicators
        tableName = tablename_convertor_technical_indicators(period,industry=True)
        if chkTableExist(tableName) == False:
            rtn = create_technical_indicators(tableName)

    tableName = tablename_convertor_balance_sheets()
    if chkTableExist(tableName) == False:
        rtn = create_balance_sheets(tableName)

    tableName = tablename_convertor_income_statements()
    if chkTableExist(tableName) == False:
        rtn = create_income_statements(tableName)

    tableName = tablename_convertor_cash_flow_statements()
    if chkTableExist(tableName) == False:
        rtn = create_cash_flow_statements(tableName)

    tableName = tablename_convertor_indicator_medians()
    if chkTableExist(tableName) == False:
        rtn = create_indicator_medians(tableName)

    tableName = tablename_convertor_user_stock_list()
    if chkTableExist(tableName) == False:
        rtn = create_user_stock_list(tableName)

    tableName = tablename_convertor_data_check_log()
    if chkTableExist(tableName) == False:
        rtn = create_data_check_log(tableName)

    tableName = tablename_convertor_trade_day_record()
    if chkTableExist(tableName) == False:
        rtn = create_trade_day_record(tableName)

    tableName = tablename_convertor_technical_signal()
    if chkTableExist(tableName) == False:
        rtn = create_technical_signal(tableName)


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

    tableName = tablename_convertor_industry_info()
    if chkTableExist(tableName):
        rtn = drop_industry_info(tableName)

    tableName = tablename_convertor_stock_info()
    if chkTableExist(tableName):
        rtn = drop_stock_info(tableName)

    periods = ["day","week","month"]
    adjusts = ["","hfq","qfq"]
    for period in periods:
        for adjust in adjusts:
            tableName = tablename_convertor_stock_history_data(period,adjust)
            if chkTableExist(tableName):
                rtn = drop_stock_history_data(tableName)
            tableName = tablename_convertor_technical_indicators(period,adjust)
            if chkTableExist(tableName):
                rtn = drop_technical_indicators(tableName)

    tableName = tablename_convertor_stock_dividend_data()
    if chkTableExist(tableName):
        rtn = drop_stock_dividend_data(tableName)

    periods = ["day","week","month"]
    for period in periods:
        #industry history data
        tableName = tablename_convertor_industry_history_data(period)
        if chkTableExist(tableName):
            rtn = drop_industry_history_data(tableName)
            #industry technical indicators
            tableName = tablename_convertor_technical_indicators(period,industry=True)
            if chkTableExist(tableName):
                rtn = drop_technical_indicators(tableName)

    tableName = tablename_convertor_balance_sheets()
    if chkTableExist(tableName):
        rtn = drop_balance_sheets(tableName)

    tableName = tablename_convertor_income_statements()
    if chkTableExist(tableName):
        rtn = drop_income_statements(tableName)

    tableName = tablename_convertor_cash_flow_statements()
    if chkTableExist(tableName):
        rtn = drop_cash_flow_statements(tableName)

    tableName = tablename_convertor_indicator_medians()
    if chkTableExist(tableName):
        rtn = drop_indicator_medians(tableName)

    tableName = tablename_convertor_user_stock_list()
    if chkTableExist(tableName):
        rtn = drop_user_stock_list(tableName)

    tableName = tablename_convertor_data_check_log()
    if chkTableExist(tableName):
        rtn = drop_data_check_log(tableName)

    tableName = tablename_convertor_trade_day_record()
    if chkTableExist(tableName):
        rtn = drop_trade_day_record(tableName)

    tableName = tablename_convertor_technical_signal()
    if chkTableExist(tableName):
        rtn = drop_technical_signal(tableName)


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

