#! /usr/bin/env python3
#encoding: utf-8


#Filename: redisCommon.py  
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com  
#Date: 2019-08-03
#Description:   redis 处理代码

_VERSION="20251101"

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import time
import hashlib
import copy

#global defintion/common var etc.
from common import globalDefinition as comGD

#code/decode functions
#from common import codingDecoding as comCD

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#setting files
from config import redisSettings as settings

if "dbMainW" not in dir() or not dbMainW:
    dbMainW = settings.dbMainW
    dbMainR = settings.dbMainR
    redisMainDB = settings.redisMainDB
    redisPipe = settings.pipeMainHandle


#generate the redis db key
def genDBKey(level1, level2, level3 = "", level4 = "", level5 = ""):
    result = ""
    if level3 == "":
        result = f"{str(level1)}.{str(level2)}"
        # result = "%s.%s" % (str(level1), str(level2))
    elif level4 == "":
        result = f"{str(level1)}.{str(level2)}.{str(level3)}"
        # result = "%s.%s.%s" % (str(level1), str(level2), str(level3))
    elif level5 == "":
        result = f"{str(level1)}.{str(level2)}.{str(level3)}.{str(level4)}"
        # result="%s.%s.%s.%s" % (str(level1), str(level2), str(level3), str(level4))
    else:
        result = f"{str(level1)}.{str(level2)}.{str(level3)}.{str(level4)}.{str(level5)}"
        # result="%s.%s.%s.%s.%s" % (str(level1), str(level2), str(level3), str(level4), str(level5))
    return result


#数据存盘
def save():
    result = redisMainDB.save()
    return result


def scanKeys(dbKey):
    result = []
    cursor = 0
    cursor,  result = redisMainDB.scan(cursor, dbKey)
    while cursor > 0:
        cursor,  keysList = redisMainDB.scan(cursor, dbKey)
        aList = []
        for key in keysList:
            if isinstance(key, bytes):
                key = key.decode()
            aList.append(key)
        result += aList
            
    return result 


def scanLimitKeys(dbKey,limit):
    result = []
    cursor = 0
    cursor,  result = redisMainDB.scan(cursor, dbKey)
    total = 0
    while cursor > 0:
        cursor,  keysList = redisMainDB.scan(cursor, dbKey)
        aList = []
        currNum = 0
        for key in keysList:
            if isinstance(key, bytes):
                key = key.decode()
            aList.append(key)
            currNum += 1
        result += aList
        total += currNum
        if (total >= limit):
            break
            
    return cursor,result 

#系统初始配置信息
def getSysConfig():
    dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_CONFIG)
    result = dbMainR.hgetall(dbKey)
    if result == None:
        result = {}
    return result
    
    
def chkSysConfigExist():
    dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_CONFIG)
    rtn = dbMainR.exists(dbKey)
    if rtn == 0:
        result = False
    else:
        result = True
    return result


def setSysConfig(mapping):
    dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_CONFIG)
    mapping["modifyYMDHMS"] = misc.getTime()
    result = dbMainW.hmset(dbKey,mapping)
    return result

    
#系统消息计数
def resetMsgSeqNum():
    dbKey = genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_MSG_SEQ_NUM)
    result = dbMainW.set(dbKey, 0)
    return result


def getMsgSeqNum():
    dbKey = genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_MSG_SEQ_NUM)
    result = dbMainR.get(dbKey)
    return result


def incrMsgSeqNum():
    dbKey = genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_MSG_SEQ_NUM)
    result = dbMainW.incr(dbKey)
    return result


def resetSysHumanTime():
    dbKey = genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_KICKOFF_HUMANTIME)
    timeString = misc.getTime()
    result = dbMainW.set(dbKey, timeString)
    return result


def resetSysMicroSecond():
    dbKey = genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_KICKOFF_TIMESTAMP)
    result = int (time.time()*1000)
    dbMainW.set(dbKey, result)
    return result


def getSysMicroSecond():
    dbKey = genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_KICKOFF_TIMESTAMP)
    try:
        sysMicroseconds = dbMainR.get(dbKey)
        if sysMicroseconds == None:
            sysMicroseconds = resetSysMicroSecond()
        sysMicroseconds = int(sysMicroseconds)
    except:
        sysMicroseconds = resetSysMicroSecond()
    result = int (time.time()*1000) - sysMicroseconds
    return result    

    
#IP flood 
def chkIPCount(IP,expireTime=comGD._DEF_REDIS_DATA_IP_EXPIRE_TIME,checkFlag = True):
    result = True
    if checkFlag:
        dbKey = genDBKey(comGD._DEF_REDIS_STAT_LEVEL1, comGD._DEF_REDIS_DATA_IP_TIME, IP)
        saveTime = dbMainR.get(dbKey)
        if saveTime:
            saveTime = int(saveTime)
        else:
            saveTime = 0
        currTime = int(time.time())
        if currTime - saveTime > comGD._DEF_REDIS_DATA_IP_TIME_THRESOLD:
            dbKey = genDBKey(comGD._DEF_REDIS_STAT_LEVEL1, comGD._DEF_REDIS_DATA_IP_TIME, IP)
            redisPipe.set(dbKey, str(currTime))
            redisPipe.expire(dbKey, expireTime)
            dbKey = genDBKey(comGD._DEF_REDIS_STAT_LEVEL1, comGD._DEF_REDIS_DATA_IP_COUNT, IP)
            redisPipe.set(dbKey,"0")
            redisPipe.expire(dbKey, expireTime)
            redisPipe.execute()
        else:
            dbKey = genDBKey(comGD._DEF_REDIS_STAT_LEVEL1, comGD._DEF_REDIS_DATA_IP_COUNT, IP)
            visits = dbMainW.incr(dbKey)
            if visits > comGD._DEF_REDIS_DATA_IP_VISITS:
                result = False
    return result
    
def cleanIPCount(IP):
    dbKey = genDBKey(comGD._DEF_REDIS_STAT_LEVEL1, comGD._DEF_REDIS_DATA_IP_TIME, IP)
    dbMainW.delete(dbKey)
    dbKey = genDBKey(comGD._DEF_REDIS_STAT_LEVEL1, comGD._DEF_REDIS_DATA_IP_COUNT, IP)
    dbMainW.delete(dbKey)


#hwinfo 相关
def getAllHWInfo(dataSource=""):
    if dataSource:
        dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_HWINFO_LAST_STATUS,dataSource)
    else:
        dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_HWINFO_LAST_STATUS)
    result = {}
    dataSet = dbMainR.hgetall(dbKey)
    if dataSet != None:
        for k, v in dataSet.items():
            if isinstance(k, bytes):
                k = k.decode("UTF-8")
            if isinstance(v, bytes):
                v = v.decode("UTF-8")
            result[k] = v
    return result


def getHWInfo(key ,dataSource=""):
    result = ""
    if dataSource:
        dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_HWINFO_LAST_STATUS,dataSource)
    else:
        dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_HWINFO_LAST_STATUS)
    data = dbMainR.hget(dbKey, key)
    if data != None:
        if isinstance(data, bytes):
            data = data.decode("UTF-8")
        result = data
    return result
    
    
def chkHWInfoExist(key , dataSource=""):
    result = False
    if dataSource:
        dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_HWINFO_LAST_STATUS,dataSource)
    else:
        dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_HWINFO_LAST_STATUS)
    rtn = dbMainR.hexists(dbKey, key)
    if rtn == 0:
        result = False
    else:
        result = True
    return result


def setHWInfo(key, val ,dataSource=""):
    if dataSource:
        dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_HWINFO_LAST_STATUS,dataSource)
    else:
        dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_HWINFO_LAST_STATUS)
    result = dbMainW.hset(dbKey,key, val)
    return result


def delHWInfo(key ,dataSource=""):
    if dataSource:
        dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_HWINFO_LAST_STATUS,dataSource)
    else:
        dbKey=genDBKey(comGD._DEF_REDIS_SYS_LEVEL1, comGD._DEF_REDIS_SYS_HWINFO_LAST_STATUS)
    result = dbMainW.hdel(dbKey, key)
    return result


#buffer相关
def getBufferNameList(key=""):
    result = []
    maxNum = 10000
    cursor = 0
    count = 0
    if key:
        dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_DATA_NAME, key + "*")
    else:
        dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_DATA_NAME, "*")
    nLen = len(comGD._DEF_REDIS_BUFFER_LEVEL1 + comGD._DEF_BUFFER_DATA_NAME) + 2

    cursor,  allKeysList = dbMainR.scan(cursor, dbKey)
    count = len(allKeysList)
    if count < maxNum:
        while cursor > 0:
            cursor,  keysList = dbMainR.scan(cursor, dbKey)
            count += len(keysList)
            allKeysList += keysList
            if count >= maxNum:
                break
    for data in allKeysList:
        keyID = data[nLen:]
        if isinstance(keyID, bytes):
            keyID = keyID.decode()
        result.append(keyID)
    return result


def chkBufferExist(indexKey):
    result = False
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_DATA_NAME, indexKey)
    rtn = dbMainR.exists(dbKey)
    if rtn == 0:
        result = False
    else:
        result = True
    return result


def getBufferDataLen(indexKey):
    result = False
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_DATA_NAME, indexKey)
    result = dbMainR.llen(dbKey)
    return result


def delDataBuffer(indexKey):
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_DATA_NAME, indexKey)
    result = dbMainW.delete(dbKey)
    return result

    
def putAllDataBuffer(indexKey, dataList, expireTime = comGD._DEF_BUFFER_DATA_KEEP_TIME):
    result = 0
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_DATA_NAME, indexKey)
    rtn = dbMainW.delete(dbKey)
    for data in dataList:
        #dbMainW.rpush(dbKey, misc.jsonDumps(data)) #必须用rpush
        redisPipe.rpush(dbKey, misc.jsonDumps(data)) #必须用rpush
        result += 1
    redisPipe.execute()
    dbMainW.expire(dbKey, expireTime)
    return result        


def putDataBuffer(indexKey, data, expireTime = comGD._DEF_BUFFER_DATA_KEEP_TIME):
    result = 0
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_DATA_NAME, indexKey)
    result = dbMainW.rpush(dbKey, misc.jsonDumps(data)) #必须用rpush
    dbMainW.expire(dbKey, expireTime)
    return result        


def getAllDataBuffer(indexKey,  beginNum = 0, endNum = -1):
    result = getDataBuffer(indexKey)
    return result
    
    
def getDataBuffer(indexKey,  beginNum = 0, endNum = -1, expireTime = comGD._DEF_BUFFER_DATA_KEEP_TIME):
    result = []
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_DATA_NAME, indexKey)
    try:
        dataList =  dbMainR.lrange(dbKey, beginNum, endNum)
        for data in dataList:
            result.append(misc.jsonLoads(data))
        dbMainW.expire(dbKey, expireTime)
    except:
        pass
    return result  
    
    
def putStepBuffer(indexKey, beginNum=0, endNum = -1, expireTime = comGD._DEF_BUFFER_DATA_KEEP_TIME):
    result = 0
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_STEP_NAME, indexKey)
    data = {}
    data["beginNum"] = beginNum
    data["endNum"] = endNum
    data["step"] = endNum - beginNum
    result = dbMainW.set(dbKey, misc.jsonDumps(data))
    dbMainW.expire(dbKey, expireTime)
    return result        
    
    
def getStepBuffer(indexKey,expireTime = comGD._DEF_BUFFER_DATA_KEEP_TIME):
    result = {}
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_STEP_NAME, indexKey)
    data = dbMainR.get(dbKey)
    if data != "" and data:
        result = misc.jsonLoads(data)
        dbMainW.expire(dbKey, expireTime)
    else:
        result = {}
    return result        
    
        
#user info
def getUserAllInfo(userID):
    result = {}
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_USER_BASIC, userID)
    dataSet = dbMainR.hgetall(dbKey)
    for k, v in dataSet.items():
        if isinstance(k, bytes):
            k = k.decode("UTF-8")
        if isinstance(v, bytes):
            v = v.decode("UTF-8")
        if k in ["regPosition", "ruleInfo", "chiefVillageIDList"]:
            if v != "" and v:
                try:
                    v = misc.jsonLoads(v)
                except:
                    pass
        if k in ["roleName"]:
            if v == "" or v == None:
                v = "visitor" #默认是visitor
        result[k] = v
    return result


def scanUserAllInfo(fromPos = 0, key = "*"):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_USER_BASIC, level3=key)
    nextPos,  keysList = dbMainR.scan(fromPos, dbKey)
    return nextPos, keysList


def getAllUserIDList(maxNum = 2000, key = "*"):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_USER_BASIC, level3=key)
    cursor = 0
    count = 0
    cursor,  allKeysList = dbMainR.scan(cursor, dbKey)
    count = len(allKeysList)
    if count < maxNum:
        while cursor > 0:
            cursor,  keysList = dbMainR.scan(cursor, dbKey)
            count += len(keysList)
            allKeysList += keysList
            if count >= maxNum:
                break
    result = []
    nLen = len(comGD._DEF_REDIS_USER_LEVEL1 + comGD._DEF_REDIS_USER_DB_USER_BASIC) + 2
    for data in allKeysList:
        userID = data[nLen:]
        if isinstance(userID, bytes):
            userID = userID.decode()
        result.append(userID)
    return result
        

def getUserInfo(userID, itemsList):
    result = {}
    dbKey = genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_USER_BASIC, userID)
    aList = dbMainR.hmget(dbKey, itemsList)
    count = 0    
    for data in aList:
        if data == None:
            data = ""
        if isinstance(data, bytes):
            data = data.decode("UTF-8")
        if itemsList[count] in ["regPosition", "ruleInfo", "chiefVillageIDList"]:
            if data !="" and data:
                try:
                    data = misc.jsonLoads(data)
                except:
                    pass
        result[itemsList[count]] = data
        count += 1
    return result


def getUserRule(userID):
    result = {}
    dataSet = getUserInfo(userID, ["ruleInfo"])
    data = dataSet.get("ruleInfo", "")
    if isinstance(data,dict) == False:
        try:
            result = misc.jsonLoads(data)
        except:
            pass
    else:
        result = data
    return result


def getUserRoleName(userID):
    dataSet = getUserInfo(userID, ["roleName"])
    result = dataSet.get("roleName") #默认是visitor
    if result == "" or result == None:
        result = "visitor"
    return result    

    
def chkUserExist(userID):
    result = False
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_USER_BASIC, userID)
    rtn = dbMainR.exists(dbKey)
    if rtn == 0:
        result = False
    else:
        result = True
        aSet = getUserInfo(userID, [comGD._DEF_REDIS_USER_DB_DELETE_FLAG])
        try:
            if (aSet.get(comGD._DEF_REDIS_USER_DB_DELETE_FLAG)) == comGD._DEF_REDIS_USER_DB_DELETE_TRUE:
                result = False
        except:
            pass
    return result


def setUserInfo(userID, mapping):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_USER_BASIC, userID)
    mapping[comGD._DEF_REDIS_USER_DB_DELETE_FLAG] = comGD._DEF_REDIS_USER_DB_DELETE_FALSE
    mapping[comGD._DEF_REDIS_USER_DB_UPDATE_DATE] = misc.getTime()
    if "regPosition" in mapping:
        mapping["regPosition"] = misc.jsonDumps(mapping["regPosition"])
    if "ruleInfo" in mapping:
        mapping["ruleInfo"] = misc.jsonDumps(mapping["ruleInfo"])
    if "chiefVillageIDList" in mapping:
        mapping["chiefVillageIDList"] = misc.jsonDumps(mapping["chiefVillageIDList"])
    result = dbMainW.hmset(dbKey,mapping)
    return result


def delUserInfo(userID, openID = "", action = False):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_USER_BASIC, userID)
    if dbMainR.exists(dbKey) == 1:
        mapping = {}
        mapping[comGD._DEF_REDIS_USER_DB_DELETE_FLAG] = comGD._DEF_REDIS_USER_DB_DELETE_TRUE
        mapping[comGD._DEF_REDIS_USER_DB_DELETE_DATE] = misc.getTime()
        if openID != "":
            mapping["delOpenID"] = openID
        result = dbMainW.hmset(dbKey,mapping) #不要真的删除数据, 只标志删除
    if action:
        result = dbMainW.delete(dbKey)
    return result
    
    
#session list
def genUserSession(mapping, expireTime = comGD._DEF_USER_SESSION_EXPIRE_TIME):
    userID = mapping.get("loginID", "")
    key = (userID.encode("UTF-8") + misc.getTime().encode("UTF-8") + comGD._DEF_COMM_HASH_KEY_FOR_ALL.encode("UTF-8"))
    sessionID = hashlib.md5(key).hexdigest()
    setSessionInfo(sessionID, mapping,expireTime)
    result = sessionID
    return result


def saveUserSession(userID, sessionID, expireTime = comGD._DEF_USER_SESSION_EXPIRE_TIME):
    saveSet = {}
    saveSet["loginID"] = userID
    setSessionInfo(sessionID, saveSet)
    result = sessionID
    return result
    
    
def setSessionInfo(sessionID, mapping, expireTime = comGD._DEF_USER_SESSION_EXPIRE_TIME):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_SESSIONID_LIST, sessionID)
    result = dbMainW.hmset(dbKey,mapping)
    dbMainW.expire(dbKey, expireTime)
    return result
    
    
def getSessionInfo(sessionID, itemsList =[]):
    result = {}
    dbKey = genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_SESSIONID_LIST, sessionID)
    if itemsList == []:
        dataSet = dbMainR.hgetall(dbKey)
        if dataSet != None:
            for k, v in dataSet.items():
                if isinstance(k, bytes):
                    k = k.decode("UTF-8")
                if isinstance(v, bytes):
                    v = v.decode("UTF-8")
                result[k] = v
    else:
        aList = dbMainR.hmget(dbKey, itemsList)
        count = 0    
        for data in aList:
            if data == None:
                data = ""
            if isinstance(data, bytes):
                data = data.decode("UTF-8")
            result[itemsList[count]] = data
            count += 1
    return result

    
def chkUserSession(sessionID):
    dataSet = getSessionInfo(sessionID, ["loginID"])
    result = dataSet.get("loginID")
    return result
    
    
def extUserSession(sessionID, expireTime = comGD._DEF_USER_SESSION_EXPIRE_TIME):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_SESSIONID_LIST, sessionID)
    result = dbMainW.expire(dbKey, expireTime)
    return result


def delUserSession(sessionID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_SESSIONID_LIST, sessionID)
    result = dbMainW.delete(dbKey)
    return result    
    
    
#增加用户的访问的openID list 
def addUserOpenIDList(userID, openID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_OPENID, userID)
    if isinstance(openID, bytes):
        openID = openID.decode("UTF-8")
    result = dbMainW.sadd(dbKey, openID)
    return result


def scanUserOpenIDList(fromPos = 0, key = "*"):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_OPENID, level3=key)
    nextPos,  keysList = dbMainR.scan(fromPos, dbKey)
    return nextPos, keysList


def setUserOpenIDList(userID, openIDList):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_OPENID, userID)
    delUserOpenIDList(userID)
    for openID in openIDList:
        if isinstance(openID, bytes):
            openID = openID.decode("UTF-8")
        result = dbMainW.sadd(dbKey, openID)
    return result


def getUserOpenIDList(userID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_OPENID, userID)
    aList = dbMainR.smembers(dbKey)
    result = []
    for a in aList:
        if isinstance(a, bytes):
            a = a.decode("UTF-8")
        result.append(a)
    return result
    
    
def getUserOpenIDListNum(userID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_OPENID, userID)
    result = dbMainR.scard(dbKey)
    return result


def removeUserOpenIDList(userID, openID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_OPENID, userID)
    if isinstance(openID, bytes):
        openID = openID.decode("UTF-8")
    result = dbMainW.srem(dbKey, openID)
    return result


def chkUserOpenIDList(userID, openID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_OPENID, userID)
    if isinstance(openID, bytes):
        openID = openID.decode("UTF-8")
    if dbMainW.sismember(dbKey, openID) == 1:
        result = True
    else:
        result = False
    return result


def delUserOpenIDList(userID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_OPENID, userID)
    result = dbMainW.delete(dbKey)
    return result


#微信小程序openid 对应的 loginID
def getLoginIDByOpenID(openID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_OPENID_LOGINID, openID)
    data = dbMainR.get(dbKey)
    if data != None:
        if isinstance(data, bytes):
            data = data.decode("UTF-8")
    else:
        data = ""
    result = data
    return result


def addLoginIDByOpenID(loginID, openID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_OPENID_LOGINID, openID)
    if isinstance(loginID, bytes):
        loginID = loginID.decode("UTF-8")
    result = dbMainW.set(dbKey, loginID)
    return result
    
    
def delLoginIDByOpenID(openID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_OPENID_LOGINID, openID)
    result = dbMainW.delete(dbKey)
    return result

    
#微信小程序code保存
def setMiniProgramCode(code,  dataSet, expireTime = comGD._DEF_REDIS_USER_WECHAT_CODE_KEEP_TIME):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_CODE, code)
    data = misc.jsonDumps(dataSet)
#    if isinstance(data, bytes):
#        data = data.decode("UTF-8")
    result = dbMainW.set(dbKey, data)
    dbMainW.expire(dbKey, expireTime)
    return result


def getMiniProgramCode(code):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_CODE, code)
    dataStr = dbMainR.get(dbKey)
    try:
        result = misc.jsonLoads(dataStr)
    except:
        result = {}
    return result


def existMiniProgramCode(code):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_REDIS_USER_DB_WECHAT_CODE, code)
    if dbMainR.exists(dbKey) == 1:
        result = True
    else:
        result = False
    return result

    
#验证码code保存
def setVerifyCode(userID, code,  expireTime = comGD._DEF_DATA_SMS_CODE_KEEP_TIME):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_DATA_SMS_NAME, userID)
    result = dbMainW.set(dbKey, code)
    dbMainW.expire(dbKey, expireTime)
    return result


def getVerifyCode(userID):
    dbKey=genDBKey(comGD._DEF_REDIS_USER_LEVEL1, comGD._DEF_DATA_SMS_NAME, userID)
    data = dbMainR.get(dbKey)
    if data == None:
        result = ""
    else:
        if isinstance(data, bytes):
            data = data.decode()
        result = data
    return result


#file information handler
#给客户端提供的在线存储, G1A0,G2A0, userID = loginID or openID
def getAllSavedData(userID):
    dbKey=genDBKey(comGD._DEF_REDIS_DATA_LEVEL1, comGD._DEF_REDIS_USER_DATA_SAVE_NAME, userID)
    result = {}
    dataSet = dbMainR.hgetall(dbKey)
    if dataSet != None:
        for k, v in dataSet.items():
            if isinstance(k, bytes):
                k = k.decode("UTF-8")
            if isinstance(v, bytes):
                v = v.decode("UTF-8")
            result[k] = v
    return result


def getSavedData(userID, key):
    result = ""
    dbKey=genDBKey(comGD._DEF_REDIS_DATA_LEVEL1, comGD._DEF_REDIS_USER_DATA_SAVE_NAME, userID)
    data = dbMainR.hget(dbKey, key)
    if data != None:
        if isinstance(data, bytes):
            data = data.decode("UTF-8")
        result = data
    return result
    
    
def chkSavedDataExist(userID, key):
    result = False
    dbKey=genDBKey(comGD._DEF_REDIS_DATA_LEVEL1, comGD._DEF_REDIS_USER_DATA_SAVE_NAME, userID)
    rtn = dbMainR.hexists(dbKey, key)
    if rtn == 0:
        result = False
    else:
        result = True
    return result


def setSavedData(userID, key,  val):
    dbKey=genDBKey(comGD._DEF_REDIS_DATA_LEVEL1, comGD._DEF_REDIS_USER_DATA_SAVE_NAME, userID)
    result = dbMainW.hset(dbKey,key, val)
    return result


def delSavedData(userID, key):
    dbKey=genDBKey(comGD._DEF_REDIS_DATA_LEVEL1, comGD._DEF_REDIS_USER_DATA_SAVE_NAME, userID)
    result = dbMainW.hdel(dbKey, key)
    return result


#file information handler
#把文件ID保存在数据库内, 等客户端上传之后,删除对应ID, 剩余的定时清理,都是没有使用的. 
def getAllFilesInfo():
    dbKey=genDBKey(comGD._DEF_REDIS_FILE_LEVEL1, comGD._DEF_FILE_INDEX_NAME, comGD._DEF_FILE_INDEX_FILEID)
    result = {}
    dataSet = dbMainR.hgetall(dbKey)
    if dataSet != None:
        for k, v in dataSet.items():
            if isinstance(k, bytes):
                k = k.decode("UTF-8")
            if isinstance(v, bytes):
                v = v.decode("UTF-8")
            aSet = misc.jsonLoads(v)
            result[k] = aSet
    return result


def getFileInfo(fileID):
    result = {}
    dbKey=genDBKey(comGD._DEF_REDIS_FILE_LEVEL1, comGD._DEF_FILE_INDEX_NAME, comGD._DEF_FILE_INDEX_FILEID)
    data = dbMainR.hget(dbKey, fileID)
    if data != None:
        if isinstance(data, bytes):
            data = data.decode("UTF-8")
        result = misc.jsonLoads(data)
    return result
    
    
def chkFileExist(fileID):
    result = False
    dbKey=genDBKey(comGD._DEF_REDIS_FILE_LEVEL1, comGD._DEF_FILE_INDEX_NAME, comGD._DEF_FILE_INDEX_FILEID)
    rtn = dbMainR.hexists(dbKey, fileID)
    if rtn == 0:
        result = False
    else:
        result = True
    return result


def setFileInfo(fileID, dataSet):
    dbKey=genDBKey(comGD._DEF_REDIS_FILE_LEVEL1, comGD._DEF_FILE_INDEX_NAME, comGD._DEF_FILE_INDEX_FILEID)
    data = misc.jsonDumps(dataSet)
#    if isinstance(data, bytes):
#        data = data.decode("UTF-8")
    result = dbMainW.hset(dbKey,fileID, data)
    return result


def delFileInfo(fileID):
    dbKey=genDBKey(comGD._DEF_REDIS_FILE_LEVEL1, comGD._DEF_FILE_INDEX_NAME, comGD._DEF_FILE_INDEX_FILEID)    
    result = dbMainW.hdel(dbKey, fileID)
    return result


#把数据转到相应的列表
def putMsg2Queue(key,  data,  limit = 0):
    dbKey = genDBKey(comGD._DEF_REDIS_DATA_LEVEL1, comGD._DEF_REDIS_DATA_TYPE_QUEUE, level3=key)
    result = dbMainW.lpush(dbKey, misc.jsonDumps(data))
    if limit > 0:
        dbMainW.ltrim(dbKey, 0, limit)
    return result

#把数据转到相应的列表
def putMsg2QueuePipe(key,  data,  limit = 0):
    dbKey = genDBKey(comGD._DEF_REDIS_DATA_LEVEL1, comGD._DEF_REDIS_DATA_TYPE_QUEUE, level3=key)
    result = redisPipe.lpush(dbKey, misc.jsonDumps(data))
    if limit > 0:
        dbMainW.ltrim(dbKey, 0, limit)
    return result


def getMsg2Queue(key,timeout=10):
    dbKey = genDBKey(comGD._DEF_REDIS_DATA_LEVEL1, comGD._DEF_REDIS_DATA_TYPE_QUEUE, level3=key)
    try:
        #result = misc.jsonLoads(dbMainW.brpop(dbKey, 10)[1].decode("UTF-8"))
        result = misc.jsonLoads(dbMainW.brpop(dbKey, timeout)[1])
    except:
        result = {}
    return result


def getMsgList2Queue(key, beginNum, endNum):
    result = []
    dbKey = genDBKey(comGD._DEF_REDIS_DATA_LEVEL1, comGD._DEF_REDIS_DATA_TYPE_QUEUE, level3=key)
    try:
        dataList =  dbMainR.lrange(dbKey, beginNum, endNum)
        for data in dataList:
            result.append(misc.jsonLoads(data))
    except:
        pass
    return result


def cleanMsg2Queue(key = "*"):
    dbKey = genDBKey(comGD._DEF_REDIS_DATA_LEVEL1, comGD._DEF_REDIS_DATA_TYPE_QUEUE, level3=key)
    result = 0
    cursor = 0
    cursor,  keysList = dbMainR.scan(cursor, dbKey)
    result += len(keysList)
    for keys in keysList:
        dbMainW.delete(keys)
    while cursor > 0:
        cursor,  keysList = dbMainR.scan(cursor, dbKey)
        result += len(keysList)
        for keys in keysList:
            dbMainW.delete(keys)
    return result


def trans2MysqlList(dataType, dataSet):
    transSet = {}
    transSet["dataType"]  = dataType
    transSet["payload"] = dataSet
    putMsg2Queue(comGD._DEF_NTN_MYSQL_TITLE, transSet)


def getMysqlList():
    transSet = getMsg2Queue(comGD._DEF_NTN_MYSQL_TITLE)
    result = transSet
    return result
    
    
def trans2FileList(dataType, dataSet):
    transSet = {}
    transSet["dataType"]  = dataType
    transSet["payload"] = dataSet
    putMsg2Queue(comGD._DEF_NTN_FILE_LIST, transSet)


def getFileList():
    transSet = getMsg2Queue(comGD._DEF_NTN_FILE_LIST)
    result = transSet
    return result
    

def statSaveDataGeneral(dataType,  dataSet):
    dbKey = genDBKey(comGD._DEF_REDIS_STAT_LEVEL1,comGD._DEF_XJY_STAT_BEFORE_NAME,  dataType)
    data = misc.jsonDumps(dataSet)
    dbMainW.set(dbKey, data)
    return True


def statGetDataGeneral(dataType):
    result = {}
    dbKey = genDBKey(comGD._DEF_REDIS_STAT_LEVEL1,comGD._DEF_XJY_STAT_BEFORE_NAME,  dataType)
    data = dbMainW.get(dbKey)
    if data:
        result = misc.jsonLoads(data)
    return result

    
#channel operation part begin
def publish2Channel(channelType, CMD , dataSet):
    dbKey=genDBKey(comGD._DEF_REDIS_CHANNEL_LEVEL1,channelType, CMD)
    try:
        msg = misc.jsonDumps(dataSet)
    except:
        msg = str(dataSet)
        
    result = dbMainW.publish(dbKey, msg)
    
    return result

#channel operation part end


#检查某类key是否存在的相关redis函数 begin 
def setkeyTypeData(key, field,val):
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_KEY_TYPE_NAME, key)
    result = dbMainW.hset(dbKey,field,val)
    return result

def scankeyTypeData(fromPos = 0, key = "*"):
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_KEY_TYPE_NAME, level3=key)
    nextPos,  keysList = dbMainR.scan(fromPos, dbKey)
    return nextPos, keysList

def getkeyTypeData(key,field):
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_KEY_TYPE_NAME, key)
    val = dbMainW.hget(dbKey,field)
    if isinstance(val, bytes):
        val = val.decode("UTF-8")
    return val    

def delkeyTypeData(key):
    dbKey=genDBKey(comGD._DEF_REDIS_BUFFER_LEVEL1, comGD._DEF_BUFFER_KEY_TYPE_NAME, key)
    result = dbMainW.delete(dbKey)
    return result
#检查某类key是否存在的相关redis函数 end 


if __name__ == "__main__":
    # import pdb
    # pdb.set_trace()
    print ("_SYS",settings._SYS)
