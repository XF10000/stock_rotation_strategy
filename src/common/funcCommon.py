#!/usr/bin/env python3
#encoding: utf-8

#Filename: funcCommon.py  
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com  
#Date: 2019-08-02
#Description:   这个应用的通用函数

_VERSION="20260203"


import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

#import time
import hashlib
import uuid
import random
import pathlib

import re

import requests

#import numpy as np

#global defintion/common var etc.
from common import globalDefinition as comGD

#code/decode functions
#from common import codingDecoding as comCD

#common functions(log,time,string, json etc)
from common import miscCommon as misc

# from common.id_validator import validator as PIDValidator
# from id_validator import validator as PIDValidator
# import id_validator as PIDValidator

#setting files
from config import basicSettings as settings
# from config import selfFileSettings as selfSettings

_processorPID = os.getpid()

#common function:
#如果执行失败或者异常返回消息定义
CONST_ERROR_wordList = {
    "default":{
        "EN":{
            "OK" :'success',
            "ERR_UPDATE" :'exception. check the updated value.',
            "ERR_FIELD" :'exception. the field %s not found or null.',
            "ERR_TOKEN" :'exception. token or format error.',
            "ERR_SESSION" :'exception. session or format error.',
            
            "ERR_DEVID" :"%s - it's a invalid device ID.",
            "ERR_LOGIN" :"login fail, no user or password wrong",
            "ERR_NO_REG" :"%s - it should be registered firstly.",
            "ERR_REPEAT_REG" :"%s - it has beed registered.",
            "ERR_INVALID":"exception. data in valid", 
            "ERR_WARN":"warning. cells status is not correct, pls check... ", 
            "ERR_GENERAL":"exception. unknow error. %s", 
            "ERR_NOCMD":"data format ERROR, no CMD", 
            "ERR_IPFLOOD":"too many visits", 

            "ERR_PID":"personal ID is invalid, format error", 
            "ERR_TEL":"telephone number is too short, format error", 
            "ERR_NAME":"name is too short, format error", 
            "ERR_HOTELNAME":"hotel name is too short, format error", 
            "ERR_ADDR":"address is too short, format error", 
            "ERR_EMAIL":"email format error", 
            
            "WARN_FACE":"face might not match with ID", 
            
            "B0":"success.",
            "B1":"loginID is invalid.",
            "B2":"loginID is too short.",
            "B3":"passwd is invalid.",
            "B4":"loginID is already exist.",
            "B5":"loginID or passwd is invalid.",
            "B6":"new passwd is invalid.",
            "B7":"new loginID is invalid.",
            "B8":"exception. sessionID or format error.",
            "B9":"alreay exist with same name and address, conflict.",
            "BA":"data is invalid %s.",
            "BE":"user need to be registered.",
            "BF":"mini program code error.",
            "BG":"access denied.",
            "BH":"Due fee, please recharge.",
            "BI":"recID is invalid.",
            "BK":"search option format error.",
            "BL":"file upload error. %s",
            "BM":"verify code is invalid or password is wrong, pls try to reset password",
            "BN":"verify code send error %s",
            "BO":"key or val error",
            "BP":"superiorID is invalid",
            "BQ":"category is invalid", 
            "BS":"this app don's support resistration",        
            "BT":"you dont' have right to access",       
            "BU":"wechat pay process error",    
            "BV":"face might not match with ID",    
            "BW":"mobile numer is wrong, or could not find",    
            "BX":"no code or wrong code, or no openID is wrong",    
            "BY":"no openID or wrong openID",    
            "BZ":"exception. token or format error.",    
            "CA":"repeat record,pls check",    
            "CB":"no record,pls check",    
            "CC":"expired, re-query",    
            "CD":"only register user could apply",    
            "CE":"invalid visitYMD",    
            "CF":"mobile, name might not match with ID",    
            "CG":"insert record error",    
            "CH":"templateID error",    
            "CU":"update record error",    
            "CV":"delete record error",    
            "DA":"device is not online!!!",    
            "DB":"send cmd to device fail,fail count:[%s]",    
            "EA":"pls select projectID",    
            "EL":"The uploaded data content or format is incorrect, please check!",    
        }, 
        "CN":{
            "OK" :'成功',
            "ERR_UPDATE" :'exception. check the updated value.',
            "ERR_FIELD" :'exception. the field %s not found or null.',
            "ERR_TOKEN" :'exception. token or format error.',
            "ERR_SESSION" :'exception. session or format error.',
            
            "ERR_DEVID" :"%s - it's a invalid device ID.",
            "ERR_LOGIN" :"login fail, no user or password wrong",
            "ERR_NO_REG" :"%s - it should be registered firstly.",
            "ERR_REPEAT_REG" :"%s - it has beed registered.",
            "ERR_INVALID":"exception. data in valid", 
            "ERR_WARN":"warning. cells status is not correct, pls check... ", 
            "ERR_GENERAL":"exception. unknow error. %s", 
            "ERR_NOCMD":"权限错误，请联系管理员", 
            "ERR_IPFLOOD":"短时期太多访问", 
            
            "ERR_PID":"身份证号码是无效的", 
            "ERR_TEL":"电话号码太短,少于8位,请查看", 
            "ERR_NAME":"姓名太短,少于2个汉字,请查看", 
            "ERR_HOTELNAME":"酒店名称,少于3个汉字,请查看", 
            "ERR_ADDR":"地址信息太短,少于5个汉字,请查看", 
            "ERR_EMAIL":"email地址不对,请查看", 

            "WARN_FACE":"人脸和身份证可能不匹配,请查验", 

            "B0":"成功",
            "B1":"登录账号不存在",
            "B2":"登录账号太短,不少于6个字符",
            "B3":"密码错",
            "B4":"登录账号重复",
            "B5":"登录账号或者密码错",
            "B6":"新密码错",
            "B7":"新登录账号无效",
            "B8":"sessionID 无效",
            "B9":"已经存在,不能新增",
            "BA":"数据格式有问题, %s.",
            "BE":"用户没有注册,或者限制注册",   
            "BF":"小程序 code error.",
            "BG":"您的权限不足",
            "BH":"您已经欠费,请充值",
            "BI":"recID 无效.",
            "BK":"搜索逻辑格式错误",
            "BL":"文件上传失败. %s",
            "BM":"校验码无效,或者是用户密码错误,建议尝试密码找回",
            "BN":"验证码发送失败, %s",
            "BO":"key 或者 val 无效",
            "BP":"superiorID 错误",
            "BQ":"category 错误",
            "BR":"你没有登录此小程序的权限",        
            "BS":"不支持注册功能",        
            "BT":"无权访问", 
            "BU":"微信支付出现问题",    
            "BV":"人脸和身份证可能不匹配,请查验",    
            "BW":"手机号码错误, 或者找不到",    
            "BX":"没有小程序code,或者是code错误,导致openID不正确",    
            "BY":"没有小程序openID,或者是生成的openID错误",      
            "BZ":"token错误",    
            "CA":"重复的记录,请检查",    
            "CB":"无此的记录,请检查",    
            "CC":"数据过期,请重新查询",    
            "CD":"只有注册用户可以申请,请先注册",    
            "CF":"手机号,身份证, 姓名不匹配",    
            "CG":"记录添加失败",    
            "CU":"记录更新失败",    
            "CV":"记录删除失败",    
            "DA":"设备不在线!!!",    
            "DB":"发送命令失败,失败次数:[%s]",    
            "EA":"请选择项目",    
            "EL":"上传的数据内容或者格式错误,请检查!",    
        }
    },
    "account":{
        "EN":{
            "OK" :'success',
            "ERR_UPDATE" :'exception. check the updated value.',
            "ERR_FIELD" :'exception. the field %s not found or null.',
            "ERR_TOKEN" :'exception. token or format error.',
            "ERR_SESSION" :'exception. session or format error.',
            
            "ERR_DEVID" :"%s - it's a invalid device ID.",
            "ERR_LOGIN" :"login fail, no user or password wrong",
            "ERR_NO_REG" :"%s - it should be registered firstly.",
            "ERR_REPEAT_REG" :"%s - it has beed registered.",
            "ERR_INVALID":"exception. data in valid", 
            "ERR_WARN":"warning. cells status is not correct, pls check... ", 
            "ERR_GENERAL":"exception. unknow error. %s", 
            "ERR_NOCMD":"data format ERROR, no CMD", 
            "ERR_IPFLOOD":"too many visits", 

            "ERR_PID":"personal ID is invalid, format error", 
            "ERR_TEL":"telephone number is too short, format error", 
            "ERR_NAME":"name is too short, format error", 
            "ERR_HOTELNAME":"hotel name is too short, format error", 
            "ERR_ADDR":"address is too short, format error", 
            "ERR_EMAIL":"email format error", 
            
            "WARN_FACE":"face might not match with ID", 
            
            "B0":"success.",
            "B1":"loginID is invalid.",
            "B2":"loginID is too short.",
            "B3":"passwd is invalid.",
            "B4":"loginID is already exist.",
            "B5":"loginID or passwd is invalid.",
            "B6":"new passwd is invalid.",
            "B7":"new loginID is invalid.",
            "B8":"exception. sessionID or format error.",
            "B9":"alreay exist with same name and address, conflict.",
            "BA":"data is invalid %s.",
            "BE":"user need to be registered.",
            "BF":"mini program code error.",
            "BG":"access denied.",
            "BH":"Due fee, please recharge.",
            "BI":"recID is invalid.",
            "BK":"search option format error.",
            "BL":"file upload error. %s",
            "BM":"verify code is invalid",
            "BN":"verify code send error %s",
            "BO":"key or val error",
            "BP":"superiorID is invalid",
            "BQ":"category is invalid", 
            "BS":"this app don's support resistration",        
            "BT":"you dont' have right to access",       
            "BU":"wechat pay process error",    
            "BV":"face might not match with ID",    
            "BW":"mobile numer is wrong, or could not find",    
            "BX":"no code or wrong code, or no openID is wrong",    
            "BY":"no openID or wrong openID",    
            "BZ":"exception. token or format error.",    
            "CA":"repeat record,pls check",    
            "CB":"no record,pls check",    
            "CC":"expired, re-query",    
            "CD":"only register user could apply",    
            "CE":"invalid visitYMD",    
            "CF":"mobile, name might not match with ID",    
            "CG":"insert record error",    
            "CH":"templateID error",    
            "CI":"backbone service is offline",    
            "CU":"update record error",    
            "CV":"delete record error",    
            "CS":"The message has been sent too many times in a short period, please try again later.",     
        }, 
        "CN":{
            "OK" :'成功',
            "ERR_UPDATE" :'exception. check the updated value.',
            "ERR_FIELD" :'exception. the field %s not found or null.',
            "ERR_TOKEN" :'错误, token,内部错误或者是格式错误.',
            "ERR_SESSION" :'错误, session,内部错误或者是格式错误.',
            
            "ERR_DEVID" :"%s - it's a invalid device ID.",
            "ERR_LOGIN" :"login fail, no user or password wrong",
            "ERR_NO_REG" :"%s - it should be registered firstly.",
            "ERR_REPEAT_REG" :"%s - it has beed registered.",
            "ERR_INVALID":"exception. data in valid", 
            "ERR_WARN":"warning. cells status is not correct, pls check... ", 
            "ERR_GENERAL":"错误, 严重错误. %s", 
            "ERR_NOCMD":"权限错误，请联系管理员", 
            "ERR_IPFLOOD":"短时期太多访问", 
            
            "ERR_PID":"身份证号码是无效的", 
            "ERR_TEL":"电话号码太短,少于8位,请查看", 
            "ERR_NAME":"姓名太短,少于2个汉字,请查看", 
            "ERR_HOTELNAME":"酒店名称,少于3个汉字,请查看", 
            "ERR_ADDR":"地址信息太短,少于5个汉字,请查看", 
            "ERR_EMAIL":"email地址不对,请查看", 

            "WARN_FACE":"人脸和身份证可能不匹配,请查验", 

            "B0":"成功",
            "B1":"登录账号不存在",
            "B2":"登录账号太短,不少于6个字符",
            "B3":"密码错",
            "B4":"登录账号重复",
            "B5":"登录账号或者密码错",
            "B6":"新密码错",
            "B7":"新登录账号无效",
            "B8":"sessionID 无效",
            "B9":"已经存在,不能新增",
            "BA":"数据格式有问题, %s.",
            "BE":"用户没有注册,或者限制注册",   
            "BF":"小程序 code error.",
            "BG":"您的权限不足",
            "BH":"您已经欠费,请充值",
            "BI":"recID 无效.",
            "BK":"搜索逻辑格式错误",
            "BL":"文件上传失败. %s",
            "BM":"校验码无效",
            "BN":"验证码发送失败, %s",
            "BO":"key 或者 val 无效",
            "BP":"superiorID 错误",
            "BQ":"category 错误",
            "BR":"你没有登录此小程序的权限",        
            "BS":"不支持注册功能",        
            "BT":"无权访问", 
            "BU":"微信支付出现问题",    
            "BV":"人脸和身份证可能不匹配,请查验",    
            "BW":"手机号码错误, 或者找不到",    
            "BX":"没有小程序code,或者是code错误,导致openID不正确",    
            "BY":"没有小程序openID,或者是生成的openID错误",      
            "BZ":"token错误",    
            "CA":"重复的记录,请检查",    
            "CB":"无此的记录,请检查",    
            "CC":"数据过期,请重新查询",    
            "CD":"只有注册用户可以申请,请先注册",    
            "CF":"手机号,身份证, 姓名不匹配",    
            "CG":"记录添加失败",    
            "CH":"templateID 错误",    
            "CI":"基础服务请求失败",    
            "CU":"记录更新失败",    
            "CV":"记录删除失败",    
            "CS":"短信短时间发送次数太多, 请稍后再发",     
        }
    },
    "hotel":{
        "EN":{
            "OK" :'success',
            "ERR_UPDATE" :'exception. check the updated value.',
            "ERR_FIELD" :'exception. the field %s not found or null.',
            "ERR_TOKEN" :'exception. token or format error.',
            "ERR_SESSION" :'exception. session or format error.',
            
            "ERR_DEVID" :"%s - it's a invalid device ID.",
            "ERR_LOGIN" :"login fail, no user or password wrong",
            "ERR_NO_REG" :"%s - it should be registered firstly.",
            "ERR_REPEAT_REG" :"%s - it has beed registered.",
            "ERR_INVALID":"exception. data in valid", 
            "ERR_WARN":"warning. cells status is not correct, pls check... ", 
            "ERR_GENERAL":"exception. unknow error. %s", 
            "ERR_NOCMD":"data format ERROR, no CMD", 
            "ERR_IPFLOOD":"too many visits", 

            "ERR_PID":"personal ID is invalid, format error", 
            "ERR_TEL":"telephone number is too short, format error", 
            "ERR_NAME":"name is too short, format error", 
            "ERR_HOTELNAME":"hotel name is too short, format error", 
            "ERR_ADDR":"address is too short, format error", 
            "ERR_EMAIL":"email format error", 
            
            "WARN_FACE":"face might not match with ID", 
            
            "B0":"success.",
            "B1":"loginID is invalid.",
            "B2":"loginID is too short.",
            "B3":"passwd is invalid.",
            "B4":"loginID is already exist.",
            "B5":"loginID or passwd is invalid.",
            "B6":"new passwd is invalid.",
            "B7":"new loginID is invalid.",
            "B8":"exception. sessionID or format error.",
            "B9":"alreay exist with same name and address, conflict.",
            "BA":"data is invalid %s.",
            "BE":"user need to be registered.",
            "BF":"mini program code error.",
            "BG":"access denied.",
            "BH":"Due fee, please recharge.",
            "BI":"recID is invalid.",
            "BK":"search option format error.",
            "BL":"file upload error. %s",
            "BM":"verify code is invalid",
            "BN":"verify code send error %s",
            "BO":"key or val error",
            "BP":"superiorID is invalid",
            "BQ":"category is invalid", 
            "BS":"this app don's support resistration",        
            "BT":"you dont' have right to access",       
            "BU":"wechat pay process error",    
            "BV":"face might not match with ID",    
            "BW":"mobile numer is wrong, or could not find",    
            "BX":"no code or wrong code, or no openID is wrong",    
            "BY":"no openID or wrong openID",    
            "BZ":"exception. token or format error.",    
            "CA":"repeat record,pls check",    
            "CB":"no record,pls check",    
            "CC":"expired, re-query",    
            "CD":"only register user could apply",    
            "CE":"invalid visitYMD",    
            "CF":"mobile, name might not match with ID",    
            "CG":"insert record error",    
            "CH":"templateID error",    
            "CU":"update record error",    
            "CV":"delete record error",    
        }, 
        "CN":{
            "OK" :'成功',
            "ERR_UPDATE" :'exception. check the updated value.',
            "ERR_FIELD" :'exception. the field %s not found or null.',
            "ERR_TOKEN" :'exception. token or format error.',
            "ERR_SESSION" :'exception. session or format error.',
            
            "ERR_DEVID" :"%s - it's a invalid device ID.",
            "ERR_LOGIN" :"login fail, no user or password wrong",
            "ERR_NO_REG" :"%s - it should be registered firstly.",
            "ERR_REPEAT_REG" :"%s - it has beed registered.",
            "ERR_INVALID":"exception. data in valid", 
            "ERR_WARN":"warning. cells status is not correct, pls check... ", 
            "ERR_GENERAL":"exception. unknow error. %s", 
            "ERR_NOCMD":"data format ERROR, no CMD", 
            "ERR_IPFLOOD":"短时期太多访问", 
            
            "ERR_PID":"身份证号码是无效的", 
            "ERR_TEL":"电话号码太短,少于8位,请查看", 
            "ERR_NAME":"姓名太短,少于2个汉字,请查看", 
            "ERR_HOTELNAME":"酒店名称,少于3个汉字,请查看", 
            "ERR_ADDR":"地址信息太短,少于5个汉字,请查看", 
            "ERR_EMAIL":"email地址不对,请查看", 

            "WARN_FACE":"人脸和身份证可能不匹配,请查验", 

            "B0":"成功",
            "B1":"登录账号不存在",
            "B2":"登录账号太短,不少于6个字符",
            "B3":"密码错",
            "B4":"登录账号重复",
            "B5":"登录账号或者密码错",
            "B6":"新密码错",
            "B7":"新登录账号无效",
            "B8":"sessionID 无效",
            "B9":"已经存在,不能新增",
            "BA":"数据格式有问题, %s.",
            "BE":"用户没有注册,或者限制注册",   
            "BF":"小程序 code error.",
            "BG":"您的权限不足",
            "BH":"您已经欠费,请充值",
            "BI":"recID 无效.",
            "BK":"搜索逻辑格式错误",
            "BL":"文件上传失败. %s",
            "BM":"校验码无效",
            "BN":"验证码发送失败, %s",
            "BO":"key 或者 val 无效",
            "BP":"superiorID 错误",
            "BQ":"category 错误",
            "BR":"你没有登录此小程序的权限",        
            "BS":"不支持注册功能",        
            "BT":"无权访问", 
            "BU":"微信支付出现问题",    
            "BV":"人脸和身份证可能不匹配,请查验",    
            "BW":"手机号码错误, 或者找不到",    
            "BX":"没有小程序code,或者是code错误,导致openID不正确",    
            "BY":"没有小程序openID,或者是生成的openID错误",      
            "BZ":"token错误",    
            "CA":"重复的记录,请检查",    
            "CB":"无此的记录,请检查",    
            "CC":"数据过期,请重新查询",    
            "CD":"只有注册用户可以申请,请先注册",    
            "CF":"手机号,身份证, 姓名不匹配",    
            "CG":"记录添加失败",    
            "CU":"记录更新失败",    
            "CV":"记录删除失败",    
        }
    },
}

    
def rtnMSG(errCode,field = '', lang = "CN", msgKey="default"):
    result = {}

    if errCode == "ERROR": #修正一个错误
        errCode = field

    if msgKey not in CONST_ERROR_wordList:
        msgKey = "default"
    if lang not in CONST_ERROR_wordList[msgKey]:
        lang = "EN"
    wordList = CONST_ERROR_wordList[msgKey][lang]

    if errCode in wordList:
        word = wordList[errCode]
        if len(word.split('%s')) > 1:
            word = word % (field)
    else:
        word = f'exception. unknow error ID,errCode:{errCode},field:{field}'
    
    result["MSG"] = {"errCode":str(errCode), "content":word}
    result["msgKey"] = msgKey
    
    return result
    
    
def getErrMsg(errCode, field ="", lang = "CN"):
    result = ""
    if lang not in CONST_ERROR_wordList:
        lang = "EN"
    wordList = CONST_ERROR_wordList[lang]
    if errCode in wordList:
        word = wordList[errCode]
        if len(word.split('%s')) > 1:
            word = word % (field)
    else:
        word = f'exception. unknow error ID,errCode:{errCode},field:{field}'
    result = word

    return result


#其他系统的消息翻译
TRANS_OTHER_MSG = {
    "EN-CN":{
    "the number of sms messages sent from a single mobile number every day exceeds the upper limit":"向对应号码发送的短信数量超过当日最大数量", 
    }, 
    "CN-EN":{
    },  
    }

def transOtherMsg(msg,  lang):
    result = msg
    if lang == "CN":
        transTag = "EN-CN"
    else:
        transTag = "CN-EN"
    newMsg = TRANS_OTHER_MSG[transTag].get(msg)
    if newMsg:
        result = newMsg
    return result

    
def calTableYMD(beginYMDHM, endYMDHM):
    result = []
    if endYMDHM > beginYMDHM:
        beginY = int(beginYMDHM[0:4])
        beginM = int(beginYMDHM[4:6])
        endY = int(endYMDHM[0:4])
        endM = int(endYMDHM[4:6])

        for Y in range(beginY,  endY+1):
            if beginY == endY:
                endMonth = endM+1
                for M in range(beginM, endMonth):
                    tableYM = "%04d%02d" % (beginY, M) 
                    result.append(tableYM)
            else:
                if Y == endY:
                    beginMonth = 1
                    endMonth = endM+1
                elif Y == beginY:
                    beginMonth = beginM
                    endMonth = 13
                else:
                    beginMonth = 1
                    endMonth = 13
                for M in range(beginMonth, endMonth):
                    tableYM = "%04d%02d" % (Y, M) 
                    result.append(tableYM)
    return result


def calTableYear(beginYMDHM, endYMDHM):
    result = []
    if endYMDHM > beginYMDHM:
        beginY = int(beginYMDHM[0:4])
        endY = int(endYMDHM[0:4])
 
        for Y in range(beginY,  endY+1):
            if beginY == endY:
                tableY = "%04d" % (beginY) 
                result.append(tableY)
    return result

        
#把相近的两个地理位置数据做个简单迁移,避免显示重叠
def shiftPosition(lat1, lng1, lat2, lng2):
    dataType = misc.isStr(lat1)
    vectorScale = 10000
    newLat1 = float(lat1) * vectorScale
    newLng1 = float(lng1) * vectorScale
    newLat2 = float(lat2) * vectorScale
    newLng2 = float(lng2) * vectorScale
    if (int(newLat1) == int(newLat2)) and (int(newLng1) == int(newLng2)):
        shiftPosition = random.randint(0, 10) 
        newLat2 += shiftPosition
        shiftPosition = random.randint(0, 10) 
        newLng2 += shiftPosition
    if dataType:
        result = str(newLat1/vectorScale), str(newLng1/vectorScale), str(newLat2/vectorScale), str(newLng2/vectorScale)
    else:
        result = (newLat1/vectorScale), (newLng1/vectorScale), (newLat2/vectorScale), (newLng2/vectorScale)
    return result
    
    
#把list数据缩减到要求的长度以内,会根据要求的长度和实际长度,挑选一些值,抛弃一些值, 适合比较平滑的数据
def compressList(dataList, requestLen):
    result = []
    actualLen = len(dataList)
    compressRate = int((actualLen + requestLen -1)/requestLen)
    if (compressRate) >= 2:
        #两倍长度以上,直接压缩
        for i in range(0, actualLen, compressRate):
            result.append(dataList[i])
    else:
        result = dataList
    return result


def genDigest(d1,  d2 = "",  d3 = "",  d4 = "", d5 =""):
    if sys.version_info.major <= 2:
        if isinstance(d1, str) or isinstance(d1, unicode):
            d1 = d1.encode("UTF-8")
        if isinstance(d2, str) or isinstance(d2, unicode):
            d2 = d2.encode("UTF-8")
        if isinstance(d3, str) or isinstance(d3, unicode):
            d3 = d3.encode("UTF-8")
        if isinstance(d4, str) or isinstance(d4, unicode):
            d4 = d4.encode("UTF-8")
        if isinstance(d5, str) or isinstance(d5, unicode):
            d5 = d5.encode("UTF-8")       
    else:
        if isinstance(d1, str) or isinstance(d1, bytes):
            d1 = d1.encode("UTF-8")
        if isinstance(d2, str) or isinstance(d2, bytes):
            d2 = d2.encode("UTF-8")
        if isinstance(d3, str) or isinstance(d3, bytes):
            d3 = d3.encode("UTF-8")
        if isinstance(d4, str) or isinstance(d4, bytes):
            d4 = d4.encode("UTF-8")
        if isinstance(d5, str) or isinstance(d5, bytes):
            d5 = d5.encode("UTF-8")       
  
    tempStr = d1+d2+d3+d4+d5+comGD._DEF_COMM_HASH_KEY_FOR_ALL.encode("UTF-8")
    result = hashlib.md5(tempStr).hexdigest()
    if sys.version_info.major <= 2:
        if isinstance(result, unicode):
            result = result.encode("UTF-8")

    else:
        if isinstance(result, bytes):
            result = result.decode("UTF-8")
    return result 
    
    
#check 身份证信息
def chkPersonalID(data, lang):
    result = {"rtn":True, "msg":"", "data":{}}
    try:
        rtnData = PIDValidator.get_info(data)
        if rtnData == False:
            result["rtn"] = False
            result["msg"] = getErrMsg("ERR_PID", lang = lang)
        else:            
            result["data"]["birthday"] = rtnData.get("birthday_code", "")
            result["data"]["address"] = rtnData.get("address", "")
            result["data"]["length"] = str(rtnData.get("length", 18))
            if rtnData.get("sex") == 1:
                result["data"]["sex"] = "M"
            else:
                result["data"]["sex"] = "F"

    except:
        pass
    return result


#check 姓名
def chkPeronalName(data, lang):
    result = {"rtn":True, "msg":"", "data":{}}
    if len(data) < comGD._DEF_PERSONAL_NAME_MIN_LENGTH:
        result["rtn"] = False
        result["msg"] = getErrMsg("ERR_NAME", lang = lang)        
    return result


#check TEl
def chkTelNo(data, lang):
    result = {"rtn":True, "msg":"", "data":{}}
    if len(data) < comGD._DEF_TEL_NO_MIN_LENGTH:
        result["rtn"] = False
        result["msg"] = getErrMsg("ERR_TEL", lang = lang)        
    return result


#check China mobile phone number
chinaMobilePhoneNoRule = re.compile(r'\(?1\d{2,3}[)-]?\d{7,8}')
def chkChinaMobileNo(data, lang = "EN"):
    result = {"rtn":True, "msg":"", "data":{}}
    m = chinaMobilePhoneNoRule.match(data)
    if m:
        result["rtn"] = True   
    else:
        result["rtn"] = False
        result["msg"] = getErrMsg("ERR_TEL", lang = lang)        
    return result


#check 城市信息
def chkCityName(data, lang):
    result = {"rtn":True, "msg":"", "data":{}}
    return result


#check 区县信息
def chkAreaName(data, lang):
    result = {"rtn":True, "msg":"", "data":{}}
    return result


#check 地址信息
def chkAddrName(data, lang):
    result = {"rtn":True, "msg":"", "data":{}}
    if len(data) < comGD._DEF_ADDR_MIN_LENGTH:
        result["rtn"] = False
        result["msg"] = getErrMsg("ERR_ADDR", lang = lang)        
    return result


#check 完整地址信息
def chkWholeAddr(data, lang):
    result = {"rtn":True, "msg":"", "data":{}}
    return result


#check 邮箱信息
def chkEmailAddr(data, lang):
    result = {"rtn":True, "msg":"", "data":{}}
    #ruleStr=r'^[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+){0,4}@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+){0,4}$'
    ruleStr = '[^@]+@[^@]+\.[^@]+'
    if re.match(ruleStr,data):
        result["rtn"] = True
    else:
        result["rtn"] = False
        result["msg"] = getErrMsg("ERR_EMAIL", lang = lang)             
    return result
    

CMDMap = {
    #用户注册
    comGD._DEF_PID_LABEL:chkPersonalID, 
    comGD._DEF_PERSONAL_NAME_LABEL:chkPeronalName, 
    comGD._DEF_TEL_LABEL:chkTelNo, 
    comGD._DEF_CITY_NAME_LABEL:chkCityName, 
    comGD._DEF_AREA_NAME_LABEL:chkAreaName, 
    comGD._DEF_WHOLE_ADDR_LABEL:chkWholeAddr, 
    comGD._DEF_EMAIL_LABEL:chkEmailAddr, 
    }

#check 地址,姓名,身份证号等的有消息
def chkDataValidataion(data, dataType, lang = "CN"):
    result = {"rtn":True, "msg":"", "data":{}}
    if dataType in CMDMap:
        result = CMDMap[dataType](data, lang)
    
    return result

def chkLoginIDPasswd(loginID, passwd):
    result = "B0"
    if loginID == "":
        result = "B1"
    elif len(loginID) <= comGD._DEF_REDIS_USER_ID_LENGTH:
        result = "B2"
    elif passwd == "":
        result = "B3"
    return result


#模仿前端生成passwd方式
# 前端的passwd计算方法
# passwd(用户输入的)+loginID 然后再md5
def genLoginIDPasswd(loginID, passwd):
    result = ""
    
    newPasswd = passwd + loginID
    result = hashlib.md5(newPasswd.encode("utf-8")).hexdigest()

    return result

    
def getProvince(cityName):
    result = ""
    return result


#身份证脱敏处理
def PIDConvertor(personID):
    result = personID
    nLen = len(personID)
    if nLen >=comGD._DEF_PID_ID_MIN_LENGTH and nLen <= comGD._DEF_PID_ID_MAX_LENGTH:
        result = personID[0:12]+"XXXX"+personID[17:]
    return result


#处理搜索问题, 把keyword转换为 handleKeyword 所需要的格式
#空格分隔的是 or, + 分隔的是 and
def keyword2option(keyword):
    result = {}
    logic = comGD._DEF_GE_LOGIC_OR
    keyList = []
    if "+" in keyword:
        logic = comGD._DEF_GE_LOGIC_AND
        aList = keyword.split("+")
    else:
        logic = comGD._DEF_GE_LOGIC_OR
        aList = keyword.split(" ")
    for key in aList:
        key = key.strip()
        if key:
            keyList.append(key)

    result["logic"] = logic
    result["keyList"] = keyList
    return result


#处理搜索问题, 支持and, or, not 和全局搜索
#ruleSet 格式
#{"logic":"AND","keyList":["\u5c71\u4e1c\u7701","\u6dc4\u535a\u5e02"]}
#allowlList: ["projectName","rules"]
def handleKeyword(ruleSet,allowList,dataList):
    result = {"rtn":"B0", "data":[]} 
    try:
        rtnData = []
        logic = (ruleSet.get("logic", "")).upper()
        keyList = ruleSet.get("keyList",[])
        if logic == comGD._DEF_GE_LOGIC_AND:
            ruleNum = len(keyList)
            if ruleNum > 0:
                for data in dataList:
                    aList = []
                    for dictKey in allowList:
                        val = data.get(dictKey)
                        try:
                            val = misc.jsonLoads(val)
                        except:
                            pass
                        if isinstance(val,list):
                            val = ",".join(val)
                        if isinstance(val,bytes):
                            val = val.decode()
                        aList.append(val)
                    strT = " ".join(aList) 
                    ruleCount = 0
                    for key in keyList:
                        if key in strT:
                            ruleCount += 1
                        else:
                            break
                    if ruleCount == ruleNum:
                        rtnData.append(data)
                result["data"] = rtnData
            else:
                result["rtn"] = "BK"
        elif logic == comGD._DEF_GE_LOGIC_OR:
            for data in dataList:
                aList = []
                for dictKey in allowList:
                    val = data.get(dictKey)
                    try:
                        val = misc.jsonLoads(val)
                    except:
                        pass
                    if isinstance(val,list):
                        val = ",".join(val)
                    if isinstance(val,bytes):
                        val = val.decode()
                    aList.append(val)
                strT = " ".join(aList) 
                for key in keyList:
                    if key in strT:
                        rtnData.append(data)
            result["data"] = rtnData
        else:
            pass
    except:
        result["rtn"] = "BK"
    return result


#处理搜索问题, 支持and, or, not 和全局搜索
#ruleSet 格式
#"searchOption":{"logic":"AND","optionList":[{"province":"\u5c71\u4e1c\u7701"},{"city":"\u6dc4\u535a\u5e02"},{"area":"\u6dc4\u5ddd\u533a"}]}
def handleSearchOption(ruleSet, allowList, dataList):
    result = {"rtn":"B0", "data":[]}
    hitKeyFlag = False
    try:
        rtnData = []
        logic = (ruleSet.get("logic", "")).upper()
        if logic == comGD._DEF_GE_LOGIC_AND:
            optionList = ruleSet.get("optionList", [])
            ruleNum = len(optionList)
            if ruleNum > 0:
                for data in dataList:
                    ruleCount = 0
                    for rule in optionList:
                        for ruleKey, ruleVal in rule.items():
                            if ruleKey in allowList:
                                hitKeyFlag = True
                                dataVal = data.get(ruleKey)
                                if ruleVal in dataVal:
                                    ruleCount += 1
                    if ruleCount == ruleNum:
                        rtnData.append(data)
                result["data"]  = rtnData
            else:
                result["rtn"] = "BK"
        elif logic == comGD._DEF_GE_LOGIC_OR:
            optionList = ruleSet.get("optionList", [])
            ruleNum = len(optionList)
            if ruleNum > 0:
                for data in dataList:
                    ruleCount = 0
                    for rule in optionList:
                        for ruleKey, ruleVal in rule.items():
                            if ruleKey in allowList:
                                hitKeyFlag = True
                                dataVal = data.get(ruleKey)
                                if ruleVal in dataVal:
                                    ruleCount = ruleNum
                                    break
                        if ruleCount == ruleNum:
                            break
                    if ruleCount == ruleNum:
                        rtnData.append(data)
                result["data"]  = rtnData
            else:
                result["rtn"] = "BK"
        elif logic == comGD._DEF_GE_LOGIC_NOT:
            optionList = ruleSet.get("optionList", [])
            ruleNum = len(optionList)
            if ruleNum > 0:
                for data in dataList:
                    ruleCount = 0
                    for rule in optionList:
                        for ruleKey, ruleVal in rule.items():
                            if ruleKey in allowList:
                                hitKeyFlag = True
                                dataVal = data.get(ruleKey)
                                if ruleVal not in dataVal:
                                    ruleCount += 1
                    if ruleCount == ruleNum:
                        rtnData.append(data)
                result["data"]  = rtnData
            else:
                result["rtn"] = "BK"
        elif logic == comGD._DEF_GE_LOGIC_ALL:
            option = ruleSet.get("option")
            for data in dataList:
                if option:
                    for dataKey,  dataVal in data.items():
                        if dataKey in allowList:
                            hitKeyFlag = True
                            if option in dataVal:
                                rtnData.append(data)
                                break
                else:
                    rtnData.append(data)
                    #result["rtn"] = "BK"
            result["data"]  = rtnData
            
        if hitKeyFlag == False:
            #条件不在allowList
            result["data"]  = dataList
    except:
        result["rtn"] = "BK"
    return result
    
  
#比较权限大小, 如果roleName权限比targetRoleName权限小, 返回 False, 否则返回True
def chkSetRoleRight(roleName, targetRoleName):
    result = False
    roleValueSet = settings.ROLE_RIGHT_SET
    roleVal = roleValueSet.get(roleName, 1000)
    targetRoleVal = roleValueSet.get(targetRoleName)
    if targetRoleVal:
        if roleName in comGD._DEF_CQDATA_OPERATOR_ROLE_LIST:
            if roleVal <= targetRoleVal:
                result = True
            if targetRoleName == "administrator":
                result = False
        else:
            if targetRoleName not in comGD._DEF_CQDATA_OPERATOR_ROLE_LIST:
                if roleVal <= targetRoleVal:
                    result = True            
    return result
    
    
def chkIsRegisterUser(roleName):
    result = False
    if roleName in ["customer","operator", "manager", "administrator"]:
        result = True
    return result

def chkIsCustomer(roleName):
    result = False
    if roleName in ["customer"]:
        result = True
    return result

def chkIsOperator(roleName):
    result = False
    if roleName in ["administrator", "manager", "operator"]:
        result = True
    return result

def chkIsOperatorOnly(roleName):
    result = False
    if roleName in ["operator"]:
        result = True
    return result

def chkIsManager(roleName):
    result = False
    if roleName in ["administrator", "manager"]:
        result = True
    return result
    

def getExpireTime(roleName):
    result = comGD._DEF_OPERATOR_SESSION_EXPIRE_TIME
    if roleName in ["owner"]:
        result = comGD._DEF_USER_SESSION_EXPIRE_TIME
    return result
        

#文件系统请求方式        
def fileServerRequest(serverName, dataSet):
    result = {}
    try:
        url = settings.FILE_SERVER_URL.get(serverName)
        header = {"content-type":"application/json"}
        payload = misc.jsonDumps(dataSet)
        r = requests.post(url, data=payload, headers = header) 
        if r.status_code == requests.codes.ok:
            result =misc.jsonLoads(r.text)
    except:
        pass
        
    return result
    

def getRandomList(listNum,  maxNum):
    result = []
    selectList = []
    count = 0
    num = 0 
    if maxNum > 1:
        while True:
            nT1 = random.randint(0, maxNum-1)
            if nT1 not in selectList:
                selectList.append(nT1) #随机内容
                num += 1
            count += 1
            if (num >= listNum) or (count > (listNum *10)):
                break
    elif maxNum == 1:
        selectList = [0]
    result = selectList
    return result
    

def convertRandomList(dataList):
    result = []
    nLen = len(dataList)
    selectList = getRandomList(nLen, nLen)
    for i in selectList:
        result.append(dataList[i])
    return result 
    
    
def getRandomDrop(molecules, denominators):
    result = True
    nT1 = random.randint(0, denominators)
    if nT1 <= molecules: #随机抛弃1/4内容
        result = True
    else:
        result = False
    return result
    
  
#根据linkType找出相应的parentID 和 childID的名称
#需要修改
def transParentIDChildIDKey(linkType):
    result = ()
    if linkType == comGD._DEF_XJY_LINKTYPE_VILLAGE_CHIEF:
        parentKey = "chiefID"
        childKey = "villageID"
    elif linkType == comGD._DEF_XJY_LINKTYPE_HOUSE_VILLAGE:
        parentKey = "villageID"
        childKey = "houseID"            
    elif linkType == comGD._DEF_XJY_LINKTYPE_HOUSE_HOUSEHOLDER:
        parentKey = "houseHolderID"
        childKey = "houseID"            
    elif linkType == comGD._DEF_XJY_LINKTYPE_CONTENT_VILLAGE:
        parentKey = "villageID"
        childKey = "contentID"            
    elif linkType == comGD._DEF_XJY_LINKTYPE_CONTENT_HOUSE:
        parentKey = "houseID"
        childKey = "contentID"            
    elif linkType == comGD._DEF_XJY_LINKTYPE_CONTENT_USER:
        parentKey = "loginID"
        childKey = "contentID"            
    elif linkType == comGD._DEF_XJY_LINKTYPE_CONTENT_TOPIC:
        parentKey = "topicID"
        childKey = "contentID"            
    elif linkType == comGD._DEF_XJY_LINKTYPE_CONTENT_CONTENT:
        parentKey = "parentContentID"
        childKey = "contentID"            
    elif linkType == comGD._DEF_XJY_LINKTYPE_USER_SUB_VILLAGE:
        parentKey = "villageID"
        childKey = "loginID"            
    elif linkType == comGD._DEF_XJY_LINKTYPE_USER_SUB_HOUSE:
        parentKey = "houseID"
        childKey = "loginID"            
    elif linkType == comGD._DEF_XJY_LINKTYPE_USER_SUB_CHIEF:
        parentKey = "chiefID"
        childKey = "loginID"            
    elif linkType == comGD._DEF_XJY_LINKTYPE_USER_SUB_HOUSEHOLDER:
        parentKey = "houseHolderID"
        childKey = "loginID"            
    result = (parentKey, childKey)
    return result 


#递归遍历某个目录的所有文件
def getFilePathList(dirPath):
    """
    递归遍历目录，返回所有路径的简单列表
    """
    result = []
    
    for item in os.listdir(dirPath):
        itemPath = os.path.join(dirPath, item)
        result.append(itemPath)  # 添加当前项
        
        if os.path.isdir(itemPath):
            # 递归遍历子目录并合并结果
            result.extend(getFilePathList(itemPath))
    
    return result


def getStructuredDirInfo(dirPath):
    """
    递归遍历目录，返回结构化的结果
    
    Returns:
        dict: 包含文件和目录信息的字典
    """
    result = {
        'files': [],
        'directories': []
    }
    
    for item in os.listdir(dirPath):
        itemPath = os.path.join(dirPath, item)
        
        if os.path.isdir(itemPath):
            # 添加目录信息
            dirInfo = {
                'path': itemPath,
                'name': item,
                'type': 'directory'
            }
            result['directories'].append(dirInfo)
            
            # 递归遍历子目录并合并结果
            sub_result = getStructuredDirInfo(itemPath)
            result['files'].extend(sub_result['files'])
            result['directories'].extend(sub_result['directories'])
        else:
            # 添加文件信息
            fileInfo = {
                'path': itemPath,
                'name': item,
                'type': 'file',
                'size': os.path.getsize(itemPath) if os.path.exists(itemPath) else 0
            }
            result['files'].append(fileInfo)
    
    return result


#获取某个目录下的文件名
def getFileNameList(dirName, extName=None,extNameList=[]):
    result = []
    if extName:
        extNameList.append(extName)
    # 遍历所有文件和目录（包括子目录）
    dirInfo = getStructuredDirInfo(dirName)
    fileInfoList = dirInfo.get("files")
    for fileInfo in fileInfoList:
        filePath = fileInfo.get("path")
        validFlag = False
        if extNameList:
            for extName in extNameList:
                if filePath.endswith(extName):
                    validFlag = True
                    break
        else:
            validFlag = True
        if validFlag:
            result.append(filePath)
    return result


#获取某个目录下的子目录名称
def getDirNameList(dirName):
    result = []
    for root,  dirs,  files in os.walk(dirName):
        for dir in dirs:
            currDirName = os.path.join(root, dir)
            currDirName = pathlib.Path(currDirName).as_posix()
            result.append(currDirName)
    return result


#建立目录
def createDir(dirName):
    if not os.path.exists(dirName):
        os.makedirs(dirName)


#生成临时文件名
def genTempFileName(extName = "",prefix="",tail="",body=""):
    result = ""
    tempDir = os.path.join(settings.LOCAL_FILE_SERVER_BASE,  "output")
    createDir(tempDir)

    if body:
        fileBodyName = prefix + "_" + body + "_" + tail 
    else:
        fileBodyName = str(uuid.uuid4()).replace("-","") 
    fileName = fileBodyName

    while True:
        tempFileName = fileName + extName
        tempFileName = os.path.join(tempDir, tempFileName)
        tempFileName = pathlib.Path(tempFileName).as_posix()
        if os.path.exists(tempFileName) == False:
            result = tempFileName
            break
        fileName = fileBodyName + "_" + str(random.randint(1000,9999))
    return result


#生成相应的url
def genTempFileUrl(filePath):
    result = ""
    baseUrl = settings.LOCAL_FILE_SERVER_PATH +"output/"
    basedir,fileName = os.path.split(filePath)
    result = baseUrl + fileName
    return result
    
    
def downloadFile(url,  fileName):
    result = 0
    r = requests.get(url)
    if r.status_code == 200:
        try:
            with open(fileName, "wb") as hFile:
                hFile.write(r.content)
            result = os.path.getsize(fileName)
        except:
            pass
    return result
    

#向ylwz文件服务器上传文件,返回的是一个含url的字典
def sendFile(filePath, description = "file"):
    result = {}
    
    url = settings.FILE_UPLOAD_URL
    try:
        #fileName = os.path.basename(filePath)

        multipart_form_data = {
            'file': (filePath, open(filePath, 'rb')),
            'description': ('', str(description)),
        }
        
        r = requests.post(url, files=multipart_form_data)
        if r.status_code == requests.codes.ok:
            rtnData = misc.jsonLoads(r.text)
        
            result = rtnData
        
    except Exception as e:
        pass

    return result


#copy file to private network, and change the url, new version
def save2newLocation(fileID,  objectName=None, requestType = "", prefix = "", 
                     privateFlag = False, compressFlag = comGD._CONST_NO):
    result = fileID
    try:

        fileInfoData = {}

        fileInfoData["CMD"] = "F0A0"
        fileInfoData["serverName"] = settings._SYS_SERVER_NAME

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

        fileInfoData["token"] = genDigest(settings.GEN_DIGIST_KEY, fileInfoData["CMD"], fileInfoData["YMDHMS"])

        rtnData = fileServerRequest(settings._SYS_SERVER_NAME, fileInfoData)
        if rtnData:
            if rtnData.get("errCode") == "B0":
                fileUrl = rtnData.get("fileUrl")
                if fileUrl:
                    result = fileUrl
                pass

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def currencyConvert(currency):
    result = {}
    if currency not in comGD._DEF_CURRENCY_UNITS:
        currency = comGD._DEF_CURRENCY_DEFAULT
    result["unit"] = comGD._DEF_CURRENCY_UNITS[currency]["unit"]
    result["rate"] = comGD._DEF_CURRENCY_UNITS[currency]["rate"]
    return result


def writeData2csv(dataList):
    result = ("","")
    filePath = genTempFileName(".csv")
    fileUrl = genTempFileUrl(filePath)
    dataLen = len(dataList)
    if dataLen > 0:
        #generate title
        data = dataList[0]
        keys = data.keys()
        strT = ",".join(keys)
        #write to file
        with open(filePath,"w",encoding = "utf-8") as hFile:
            hFile.write(strT)
            hFile.write("\n")
            for data in dataList:
                aList = []
                for key in keys:
                    aList.append(data[key])
                strT = ",".join(aList)
                hFile.write(strT)
                hFile.write("\n")
        result = (filePath,fileUrl)
    return result


def date2YMD(dateString):
    result = dateString
    aList = dateString.split("-")
    YMDList = []
    for val in aList:
        val = int(val)
        YMDList.append(val)
    if len(YMDList) == 3:
        result = f"{YMDList[0]:04d}{YMDList[1]:02d}{YMDList[2]:02d}"
    return result


def YMD2Date(YMD):
    result = YMD
    if len(YMD) == 8:
        result = YMD[0:4] + "-" + YMD[4:6] + "-" + YMD[6:8]
    return result


'''
1mb = 1 month before
1ma = 1 month after
'''
# 1mb=1 month before
def decodeRequireDate(requireDate):
    beforeAfterFlag = None
    YMD = None
    requireDate = requireDate.lower()
    try:
        if len(requireDate) >= 3:
            beforeAfterChar = requireDate[-1]
            if beforeAfterChar == "b":
                beforeAfterFlag = ">="
            else:
                beforeAfterFlag = "<="
            unitChar = requireDate[-2]
            if unitChar == "m":
                days = 30
            elif unitChar == "y":
                days = 365
            else:
                days = 1
            val = int(requireDate[0:-2])
            actualDays = val * days               
            YMD = misc.getPassday(actualDays)
    except:
        pass
    return beforeAfterFlag, YMD


'''
文件大小单位换算
'''
def fileSize2text(size):
    result = 0
    try:
        size = float(size)
        units = ["B","KB","MB","GB","TB","PB"]
        unitSize = 1024
        for i in range(len(units)):
            if (size/unitSize) < 1:
                result = "%.1f%s"% (size,units[i])
                break
            size = size/unitSize
    except:
        pass
    return result


#系统菜单参数
def loadMenuPararmeters():
    result = settings.menuParametersDefault
    try:
        fileName = settings.menuParametersFileName
        if os.path.exists(fileName):
            rtnData = misc.loadJsonData(fileName,"dict")
            if rtnData:
                result = rtnData
        else:
            #保存文件
            misc.saveJsonData(fileName,settings.menuParametersDefault,indent=2)
            
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def saveMenuPararmeters(sysMenuParameters):
    try:
        fileName = settings.menuParametersFileName
        misc.saveJsonData(fileName,sysMenuParameters,indent=2)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")


def calcQuarterByDate(dateType,beginDate,endDate):
    result = {}
    result["EN"] = []
    result["CN"] = []
    result["quarterNum"] = 0
    try:
        if dateType == "YMD":
            beginYM = beginDate[0:6]
            endYM = endDate[0:6]
        else:
            beginYM = beginDate
            endYM = endDate
        beginY = int(beginYM[0:4])
        beginM = int(beginYM[4:6])
        endY = int(endYM[0:4])
        endM = int(endYM[4:6])
        currY = beginY
        currM = beginM
        quarterNum = 0
        while True:
            if currM >= 1 and currM <= 3:
                currQ = 1
                currCQ = "一"
            elif currM >= 4 and currM <= 6:
                currQ = 2
                currCQ = "二"
            elif currM >= 7 and currM <= 9:
                currQ = 3
                currCQ = "三"
            else:
                currQ = 4
                currCQ = "四"
            currYQ = str(currY) + str(currQ)
            if currYQ not in result["EN"]:
                quarterNum += 1
                currCYQ = f"{currY}年度第{currCQ}季度"
                result["EN"].append(currYQ)
                result["CN"].append(currCYQ)
            if currY == endY and currM == endM:
                break
            currM += 1
            if currM > 12:
                currM = 1
                currY += 1
        result["quarterNum"] = quarterNum
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#根据模板和{}占位符生成字符串
def dynamicFormat(template, dataList):
    result = ""
    # 统计占位符数量（注意：需确保模板无转义字符）
    try:
        placeHoldersNum = template.count('{}')
        if len(dataList) == placeHoldersNum:
            result = template.format(*dataList)
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#根据keyList 检查data中是否包含keyList中的所有key, 而且符合key 的顺序
def ifMatchKeys(data, keyList):
    result = False
    try:
        currString = data
        count = len(keyList)
        for key in keyList:
            pos = currString.find(key)
            if pos >= 0:
                pos = currString.find(":")
                if pos >=0:
                    pos += 1
                    currString = currString[pos:]
                count -= 1
            else:
                break
        if count == 0:
            result = True
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


#检查数据是否来源于可信domain
def chkTrustDomain(url):
    result = False
    try:
        url = str(url)
        if url.startswith("http://") or url.startswith("https://"):
            for trustDomain in settings.TRUST_DOMAIN_LIST:
                if url.startswith(trustDomain):
                    result = True
                    break
        else:
            result = True
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


urlPattern = re.compile(r'(?i)(<script|<iframe|<object|<embed|javascript:|vbscript:|data:|active\w+)', re.IGNORECASE)
def isSafeUrl(url):
    global urlPattern
    result = False
    try:
        # 定义一个正则表达式，用于匹配可能的注入模式
        # 这个正则表达式可能需要根据你的具体需求进行调整
        
        # 使用正则表达式检查URL
        if not urlPattern.search(url):
            result = True
              # URL不包含潜在的注入模式，安全
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
    return result  # URL看起来是安全的


#上传内容格式检查, 主要是是否含html和其他url等
def uploadContentCheck(content):
    result = True
    try:
        if isinstance(content,str):
            if content:
                result = isSafeUrl(content)

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        # _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


def list2dict(keyList,valList):
    result = {}
    try:
        keyLen = len(keyList)
        valLen = len(valList)
        if keyLen == valLen:
            for i in range(keyLen):
                key = keyList[i]
                val = valList[i]
                result[key] = val
    except:
        pass
    return result


def genDictFileName(baseName):
    result = ""
    try:
        fileName = baseName + "_" + comGD._DEF_KEYNAME_DICT_FILE_NAME_SUFFIX
        templateDir = settings._INST_TEMPLATE_CONFIG_NAME_DIR
        filePath = os.path.join(templateDir,fileName)
        result = pathlib.Path(filePath).as_posix()
    except:
        pass

    return result


#用于取出多重字典中的所有键，如果遇到列表则忽略
def extractDictKeys(data):
    """
    递归提取字典中的所有键，遇到列表则忽略
    参数:
        data: 输入数据（字典或包含字典的结构）
    返回:
        set: 包含所有键的集合
    """
    result = []
    try:
        keys = set()
        if isinstance(data, dict):
            for key, value in data.items():
                keys.add(key)
                # 递归处理字典值
                keys.update(extractDictKeys(value))
        elif isinstance(data, list):
            # 忽略列表，不进行处理
            pass
        # 其他类型（如字符串、数字等）不需要处理
        result = list(keys)
    except:
        pass
    
    return result


#获取标准的城市名称,没有空格, 全部小写
def getStandardCityName(cityName):
    result = cityName
    try:
        tempList = cityName.split()
        tempName = "_".join(tempList)
        tempName = tempName.lower()
        result = tempName
    except:
        pass
    return result


#获取标准的首字母大写名称,用空格分隔
def getTitleName(name):
    result = name
    try:
        tempList = name.split()
        tempName = " ".join(tempList)
        tempName = tempName.title()
        result = tempName
    except:
        pass
    return result


#把欧洲格式的小数点","转换为标准的浮点数
def euroFloat(val):
    result = 0.0
    try:
        val.replace(",",".")
        result = float(val)
    except:
        pass
    return result


def genHotelUniqueID(cityName,address,countryCode2=""):
    result = ""
    try:
        cityName = getStandardCityName(cityName)
        address = getTitleName(address)
        tempStr = cityName + address
        tempStr = tempStr.encode()
        hashVal = hashlib.md5(tempStr).hexdigest()
        if countryCode2:
            result = countryCode2 + "_" + hashVal
        else:
            result = "CITY_" + hashVal
    except Exception as e:
        pass
    return result


#判断某天是否公共假期
'''
本地保存一个文件, 这样可以, 一天清理一次, 主要是处理三天前的数据
首先判断本地是否保存了数据, 然后考虑是否需要从网络更新
'''
_localIsPublucHolidayFileName = "publicholiday.json"
def isPublicHoliday(YMD):
    result = False
    currYMDHMS = misc.getTime()
    saveData = misc.loadJsonData(_localIsPublucHolidayFileName,"dict")
    if saveData:
        saveFlag = False
        saveYMDHMS = saveData.get("YMDHMS")
        holidayInfoData = saveData.get("data")
        if saveYMDHMS[0:8] != currYMDHMS[0:8]:
            #日期不同需要清理数据
            passYMDHMS = misc.getPassday(3) + "000000"
            currKeys = list(holidayInfoData.keys())
            for key in currKeys:
                if key < passYMDHMS:
                    del holidayInfoData[key]
                    saveFlag = True
                    saveData["YMDHMS"] = currYMDHMS
        if YMD in holidayInfoData:
            result = holidayInfoData[YMD]["isHoliday"]
        else:
            #调用外部接口
            holidayInfo = getPublicHolidayInfo(YMD)
            if holidayInfo:
                saveData["data"][YMD] = holidayInfo
                saveFlag = True

        if saveFlag:
            misc.saveJsonData(_localIsPublucHolidayFileName, saveData,indent = 2)
    else:
        #调用外部接口
        holidayInfo = getPublicHolidayInfo(YMD)
        if holidayInfo:
            saveData = {}
            saveData["data"] = {}
            saveData["YMDHMS"] = currYMDHMS
            result = holidayInfo.get("isHoliday")
            saveData["data"][YMD] = holidayInfo
            misc.saveJsonData(_localIsPublucHolidayFileName, saveData,indent = 2)

    return result


#利用外部接口获取公共假日信息
'''
利用外部的接口
https://github.com/Haoshenqi0123/holiday
https://api.haoshenqi.top/holiday?date=2023-10-01
    {
        "date": "2019-05-01",
        "year": 2019,
        "month": 5,
        "day": 1,
        "status": 3
    }
status: 0普通工作日1周末双休日2需要补班的工作日3法定节假日
'''
def getPublicHolidayInfo(YMD):
    result = {}
    try:
        url = "http://api.haoshenqi.top/holiday"
        currDate = YMD[0:4] + "-" + YMD[4:6] + "-" + YMD[6:8]
        header = {"content-type":"application/json"}
        url = url + "?date=" + currDate
        r = requests.get(url,headers = header) 
        if r.status_code == requests.codes.ok:
            dataList = misc.jsonLoads(r.text)
            if dataList:
                currDataSet = dataList[0]
                status = currDataSet.get("status")
                result["status"] = status
                result["date"] = currDataSet.get("date")
                if status == 0:
                    result["isHoliday"] = False
                else:
                    result["isHoliday"] = True
    except:
        pass

    return result


def test():
    # 示例
    filePath = "restore_file.sh"
    rtnData = sendFile(filePath)

    pass

if __name__ == "__main__":
    if len(sys.argv) > 1:
        pass
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
    
    test()
