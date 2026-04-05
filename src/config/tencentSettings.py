#! /usr/bin/env python3
#encoding: utf-8

#Filename: tencentSettings.py  
#Author: Steven Lian's team
#E-mail:  / /steven.lian@gmail.com  
#Date: 2020-04-16
# https://cloud.tencent.com/document/product/436/12269
# pip install -U cos-python-sdk-v5
#Description:   腾讯云通用的配置管理,

_VERSION="20260405"


import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

from config import local_settings as local_settings


#生产环境 rss | 测试环境 dss
_SYS = local_settings._SYS
#_SYS = "server_01"
#_SYS = "local"

_SYS_SERVER_NAME = local_settings._SYS_SERVER_NAME

_DEBUG = True  #预设trace开关，禁止修改

#SMS Service 
TECENT_SMS_SERVICE = {
"local":{
    "SecretId":"YOUR secretId", 
    "SecretKey":"YOUR_CODE", 
    "reqTimeout":30, 
    "endpoint":"sms.tencentcloudapi.com", 
    "RegionId":"ap-chengdu", 
    "SmsSdkAppid":"your_appid", 
    "Sign":"your_sign", 
    "TemplateID":"581449", #验证码
    "appChiefID":"604273", #村长受理通知
    "notificationChiefID":"604274", #村长审核结果通知
    "appVillageID":"604193", #村庄受理通知
    "notificationVillageID":"604271", #村庄审核结果通知
    "appVisitor":"609593", #看房申请
    "visitorAlarm":"609623", #看房通知客服
    "villageIDAlarm":"609624", #村庄申请客服通知
    "chiefIDAlarm":"609625", #村长申请客服通知
    "TemplateParamSet":None, 
    }, 
"server_01":{
    "SecretId":"YOUR secretId", 
    "SecretKey":"YOUR_CODE", 
    "reqTimeout":30, 
    "endpoint":"sms.tencentcloudapi.com", 
    "RegionId":"ap-chengdu", 
    "SmsSdkAppid":"your_appid", 
    "Sign":"your_sign", 
    "TemplateID":"581449", #验证码
    "appChiefID":"604273", #村长受理通知
    "notificationChiefID":"604274", #村长审核结果通知
    "appVillageID":"604193", #村庄受理通知
    "notificationVillageID":"604271", #村庄审核结果通知
    "appVisitor":"609593", #看房申请
    "visitorAlarm":"609623", #看房通知客服
    "villageIDAlarm":"609624", #村庄申请客服通知
    "chiefIDAlarm":"609625", #村长申请客服通知
    "TemplateParamSet":None, 
    }, 
"server_02":{
    "SecretId":"YOUR secretId", 
    "SecretKey":"YOUR_CODE", 
    "reqTimeout":30, 
    "endpoint":"sms.tencentcloudapi.com", 
    "RegionId":"ap-chengdu", 
    "SmsSdkAppid":"your_appid", 
    "Sign":"your_sign", 
    "TemplateID":"581449", #验证码
    "appChiefID":"604273", #村长受理通知
    "notificationChiefID":"604274", #村长审核结果通知
    "appVillageID":"604193", #村庄受理通知
    "notificationVillageID":"604271", #村庄审核结果通知
    "appVisitor":"609593", #看房申请
    "visitorAlarm":"609623", #看房通知客服
    "villageIDAlarm":"609624", #村庄申请客服通知
    "chiefIDAlarm":"609625", #村长申请客服通知
    "TemplateParamSet":None, 
    }, 
"home":{
    "SecretId":"YOUR secretId", 
    "SecretKey":"YOUR_CODE", 
    "reqTimeout":30, 
    "endpoint":"sms.tencentcloudapi.com", 
    "SmsSdkAppid":"your_appid", 
    "Sign":"your_sign", 
    "TemplateID":"581449", 
    "appChiefID":"604273", #村长受理通知
    "notificationChiefID":"604274", #村长审核结果通知
    "appVillageID":"604193", #村庄受理通知
    "notificationVillageID":"604271", #村庄审核结果通知
    "appVisitor":"609593", #看房申请
    "visitorAlarm":"609623", #看房通知客服
    "villageIDAlarm":"609624", #村庄申请客服通知
    "chiefIDAlarm":"609625", #村长申请客服通知
    "TemplateParamSet":None, 
    }, 
}[_SYS]


#COS Service 
TECENT_COS_SERVICE = {
"local":{
    "secretId":"YOUR secretId", 
    "secretKey":"YOUR_CODE", 
    "domainBase":"myqcloud.com", 
    "regionId":"ap-chengdu", 
    "bucketName":"xjy-data-home", 
    "url":"https://xjy-data-home.cos.ap-chengdu.myqcloud.com", 
    "privateBucketName":"xjy-private-home", 
    "privateUrl":"https://xjy-private-home.cos.ap-chengdu.myqcloud.com", 
    "connTimeOut":60, 
    "dispTimeOut":1800, 
    }, 
"server_01":{
    "secretId":"YOUR secretId", 
    "secretKey":"YOUR_CODE", 
    "domainBase":"myqcloud.com", 
    "regionId":"ap-chengdu", 
    "bucketName":"xjy-test-home", 
    "url":"https://xjy-test-home.cos.ap-chengdu.myqcloud.com", 
    "privateBucketName":"xjy-private-home", 
    "privateUrl":"https://xjy-private-home.cos.ap-chengdu.myqcloud.com", 
    "connTimeOut":60, 
    "dispTimeOut":1800, 
    }, 
"server_02":{
    "secretId":"YOUR secretId", 
    "secretKey":"YOUR_CODE", 
    "domainBase":"myqcloud.com", 
    "regionId":"ap-chengdu", 
    "bucketName":"xjy-test-home", 
    "url":"https://xjy-test-home.cos.ap-chengdu.myqcloud.com", 
    "privateBucketName":"xjy-private-home", 
    "privateUrl":"https://xjy-private-home.cos.ap-chengdu.myqcloud.com", 
    "connTimeOut":60, 
    "dispTimeOut":1800, 
    }, 
"home":{
    "secretId":"YOUR secretId", 
    "secretKey":"YOUR_CODE", 
    "domainBase":"myqcloud.com", 
    "regionId":"ap-chengdu", 
    "bucketName":"xjy-data-home", 
    "url":"https://xjy-data-home.cos.ap-chengdu.myqcloud.com", 
    "privateBucketName":"xjy-private-home", 
    "privateUrl":"https://xjy-private-home.cos.ap-chengdu.myqcloud.com", 
    "connTimeOut":60, 
    "dispTimeOut":1800, 
    }
    
}[_SYS]


#OCR service 
TECENT_OCR_SERVICE = {
"local":{
    "url":"http://dm-51.data.aliyun.com/rest/160601/ocr/ocr_idcard.json", 
    "AppKey":"YOUR KEY", 
    "AppSecret":"YOUR_CODE", 
    "AppCode":"YOUR_CODE", 
    }, 
"server_01":{
    "url":"http://dm-51.data.aliyun.com/rest/160601/ocr/ocr_idcard.json", 
    "AppKey":"YOUR KEY", 
    "AppSecret":"YOUR_CODE", 
    "AppCode":"YOUR_CODE", 
    }, 
"server_02":{
    "url":"http://dm-51.data.aliyun.com/rest/160601/ocr/ocr_idcard.json", 
    "AppKey":"YOUR KEY", 
    "AppSecret":"YOUR_CODE", 
    "AppCode":"YOUR_CODE", 
    }, 
"home":{
    "url":"http://dm-51.data.aliyun.com/rest/160601/ocr/ocr_idcard.json", 
    "AppKey":"YOUR KEY", 
    "AppSecret":"YOUR_CODE", 
    "AppCode":"YOUR_CODE", 
    }
}[_SYS]


#AI 365 服务, 人脸识别, 身份认证
TECENT_AI365_FACE_SERVICE = {
"local":{
    "url":"https://service-q3toequb-1301232119.bj.apigw.tencentcs.com/release/idfaceIdentity", 
    "SecretID":"YOUR SECRET", 
    "SecretKey":"YOUR_CODE", 
    }, 
"server_01":{
    "url":"https://service-q3toequb-1301232119.bj.apigw.tencentcs.com/release/idfaceIdentity", 
    "SecretID":"YOUR SECRET", 
    "SecretKey":"YOUR_CODE", 
    }, 
"server_02":{
    "url":"https://service-q3toequb-1301232119.bj.apigw.tencentcs.com/release/idfaceIdentity", 
    "SecretID":"YOUR SECRET", 
    "SecretKey":"YOUR_CODE", 
    }, 
"home":{
    "url":"https://service-q3toequb-1301232119.bj.apigw.tencentcs.com/release/idfaceIdentity", 
    "SecretID":"YOUR_CODE", 
    "SecretKey":"YOUR_CODE", 
    }
}[_SYS]


#AI 365  运营商实名认证接口三要素手机验证
TECENT_AI365_OPERATOR_SERVICE = {
"local":{
    "url":"http://service-4epp7bin.ap-beijing.apigateway.myqcloud.com/release/efficient/cellphone", 
    "SecretID":"YOUR SECRET", 
    "SecretKey":"YOUR_CODE ", 
    }, 
"server_01":{
    "url":"http://service-4epp7bin.ap-beijing.apigateway.myqcloud.com/release/efficient/cellphone", 
    "SecretID":"YOUR SECRET", 
    "SecretKey":"YOUR_CODE ", 
    }, 
"server_02":{
    "url":"http://service-4epp7bin.ap-beijing.apigateway.myqcloud.com/release/efficient/cellphone", 
    "SecretID":"YOUR SECRET", 
    "SecretKey":"YOUR_CODE ", 
    }, 
"home":{
    "url":"http://service-4epp7bin.ap-beijing.apigateway.myqcloud.com/release/efficient/cellphone", 
    "AppSecret":"YOUR SECRET", 
    "AppCode":"YOUR_CODE ", 
    }
}[_SYS]


if __name__ == "__main__":
    pass
    # import pdb
    # pdb.set_trace()
    print ("_SYS",_SYS)
    print ("_SYS_SERVER_NAME",_SYS_SERVER_NAME)
    print ("TECENT_SMS_SERVICE",TECENT_SMS_SERVICE)
    print ("TECENT_COS_SERVICE",TECENT_COS_SERVICE)
    print ("TECENT_OCR_SERVICE",TECENT_OCR_SERVICE)
    print ("TECENT_AI365_FACE_SERVICE",TECENT_AI365_FACE_SERVICE)
