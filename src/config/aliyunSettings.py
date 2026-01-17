#! /usr/bin/env python3
#encoding: utf-8

#Filename: AliyunSettings.py  
#Author: Steven Lian
#E-mail:  / /steven.lian@gmail.com  
#Date: 2019-11-30
#Description:   阿里云通用的配置管理
# policy Editor, http://gosspublic.alicdn.com/ram-policy-editor/index.html

_VERSION="20251221"


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
#_SYS = "project_01_1"
#_SYS = "local"

_SYS_SERVER_NAME = local_settings._SYS_SERVER_NAME

_DEBUG = True  #预设trace开关，禁止修改

#SMS Service 
ALIYUN_SMS_SERVICE = {
"local":{
    "AccessKeyId":"LTAI5tC6fqyJCdyVrsAKejEQ", 
    "AccessKeySecret":"YOUR_CODE", 
    "Domain":"cn-zhangjiakou", 
    "RegionId":"cn-zhangjiakou", 
    "code":"code",
    "SignName":"深圳湔端", 
    "TemplateCode":"SMS_249250399", 
    "infoCode":"phone", 
    "infoSignName":"深圳湔端通知", 
    "infoTemplateCode":"SMS_253000069", 
    "roleChangeProjectName":"projectName", 
    "roleChangeRoleName":"roleName", 
    "roleChangeSignName":"深圳湔端通知", 
    "roleChangeTemplateCode":"SMS_254130635", 
    }, 
"project_01":{
    "AccessKeyId":"LTAI5tC6fqyJCdyVrsAKejEQ", 
    "AccessKeySecret":"YOUR_CODE", 
    "Domain":"cn-zhangjiakou", 
    "RegionId":"cn-zhangjiakou", 
    "code":"code",
    "SignName":"深圳湔端", 
    "TemplateCode":"SMS_249250399", 
    "infoCode":"phone", 
    "infoSignName":"深圳湔端通知", 
    "infoTemplateCode":"SMS_253000069", 
    "roleChangeProjectName":"projectName", 
    "roleChangeRoleName":"roleName", 
    "roleChangeSignName":"深圳湔端通知", 
    "roleChangeTemplateCode":"SMS_254130635", 
    }, 
"homeServer":{
    "AccessKeyId":"LTAI5tC6fqyJCdyVrsAKejEQ", 
    "AccessKeySecret":"YOUR_CODE", 
    "Domain":"cn-zhangjiakou", 
    "RegionId":"cn-zhangjiakou", 
    "code":"code",
    "SignName":"深圳湔端", 
    "TemplateCode":"SMS_249250399", 
    "infoCode":"phone", 
    "infoSignName":"深圳湔端通知", 
    "infoTemplateCode":"SMS_253000069", 
    "roleChangeProjectName":"projectName", 
    "roleChangeRoleName":"roleName", 
    "roleChangeSignName":"深圳湔端通知", 
    "roleChangeTemplateCode":"SMS_254130635", 
    }, 
"iottest-01":{
    "AccessKeyId":"LTAI5tC6fqyJCdyVrsAKejEQ", 
    "AccessKeySecret":"YOUR_CODE", 
    "Domain":"cn-zhangjiakou", 
    "RegionId":"cn-zhangjiakou", 
    "code":"code",
    "SignName":"深圳湔端", 
    "TemplateCode":"SMS_249250399", 
    "infoCode":"phone", 
    "infoSignName":"深圳湔端通知", 
    "infoTemplateCode":"SMS_253000069", 
    "roleChangeProjectName":"projectName", 
    "roleChangeRoleName":"roleName", 
    "roleChangeSignName":"深圳湔端通知", 
    "roleChangeTemplateCode":"SMS_254130635", 
    }, 
"vc-voice":{
    "AccessKeyId":"LTAI5tC6fqyJCdyVrsAKejEQ", 
    "AccessKeySecret":"YOUR_CODE", 
    "Domain":"cn-zhangjiakou", 
    "RegionId":"cn-zhangjiakou", 
    "code":"code",
    "SignName":"深圳湔端", 
    "TemplateCode":"SMS_249250399", 
    "infoCode":"phone", 
    "infoSignName":"深圳湔端通知", 
    "infoTemplateCode":"SMS_253000069", 
    "roleChangeProjectName":"projectName", 
    "roleChangeRoleName":"roleName", 
    "roleChangeSignName":"深圳湔端通知", 
    "roleChangeTemplateCode":"SMS_254130635", 
    }, 
"home":{
    "AccessKeyId":"LTAI5tC6fqyJCdyVrsAKejEQ", 
    "AccessKeySecret":"YOUR_CODE", 
    "Domain":"cn-zhangjiakou", 
    "RegionId":"cn-zhangjiakou", 
    "code":"code",
    "SignName":"深圳湔端", 
    "TemplateCode":"SMS_249250399", 
    "infoCode":"phone", 
    "infoSignName":"深圳湔端通知", 
    "infoTemplateCode":"SMS_253000069", 
    "roleChangeProjectName":"projectName", 
    "roleChangeRoleName":"roleName", 
    "roleChangeSignName":"深圳湔端通知", 
    "roleChangeTemplateCode":"SMS_254130635", 
    }, 
}[_SYS]


#OSS Service 
ALIYUN_OSS_SERVICE = {
"local":{
    "RegionId":"cn-zhangjiakou", 
    "roleArn":"acs:ram::1392433711034021:role/aliyunosstokengeneratorrole",
    "AccessKeyId":"LTAI5t5jMGja38rpxSNjL9th", 
    "AccessKeySecret":"YOUR_CODE", 
    "readOnlyAccessKeyId":"LTAI5t8S6nUMBYPySaUVtCaA", 
    "readOnlyAccessKeySecret":"YOUR_CODE", 
    "stsRegionId":"oss-cn-zhangjiakou", 
    "stsAccessKeyId":"LTAI5tPpkp526RfrRJB5LGnY", 
    "stsAccessKeySecret":"YOUR_CODE", 
    "Endpoint":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointExternal":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointInternal":"https://oss-cn-zhangjiakou-internal.aliyuncs.com", 
    "BucketName":"caict-private-20220822", 
    "ConnTimeOut":60, 
    "DispTimeOut":1800, 
    "stsPolicyData":{
        "Version": "1",
        "Statement": 
        [{
            "Effect": "Allow",
            "Action": ["oss:Put*"],
            "Resource": ["acs:oss:*:*:caict-private-20220822","acs:oss:*:*:caict-private-20220822/*"],
            "Condition": {}
            }]
        },
    },
    "readOnlyAccessKeyId":{
    "RegionId":"cn-zhangjiakou", 
    "roleArn":"acs:ram::1392433711034021:role/aliyunosstokengeneratorrole",
    "AccessKeyId":"LTAI5t5jMGja38rpxSNjL9th", 
    "AccessKeySecret":"YOUR_CODE", 
    "readOnlyAccessKeyId":"LTAI5t8S6nUMBYPySaUVtCaA", 
    "readOnlyAccessKeySecret":"YOUR_CODE", 
    "stsRegionId":"oss-cn-zhangjiakou", 
    "stsAccessKeyId":"LTAI5tPpkp526RfrRJB5LGnY", 
    "stsAccessKeySecret":"YOUR_CODE", 
    "Endpoint":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointExternal":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointInternal":"https://oss-cn-zhangjiakou-internal.aliyuncs.com", 
    "BucketName":"caict-private-20220822", 
    "ConnTimeOut":60, 
    "DispTimeOut":1800, 
    "stsPolicyData":{
        "Version": "1",
        "Statement": 
        [{
            "Effect": "Allow",
            "Action": ["oss:Put*"],
            "Resource": ["acs:oss:*:*:caict-private-20220822","acs:oss:*:*:caict-private-20220822/*"],
            "Condition": {}
            }]
        },
   }, 
"project_01":{
    "RegionId":"cn-zhangjiakou", 
    "roleArn":"acs:ram::1392433711034021:role/aliyunosstokengeneratorrole",
    "AccessKeyId":"LTAI5t5jMGja38rpxSNjL9th", 
    "AccessKeySecret":"YOUR_CODE", 
    "readOnlyAccessKeyId":"LTAI5t8S6nUMBYPySaUVtCaA", 
    "readOnlyAccessKeySecret":"YOUR_CODE", 
    "stsRegionId":"oss-cn-zhangjiakou", 
    "stsAccessKeyId":"LTAI5tPpkp526RfrRJB5LGnY", 
    "stsAccessKeySecret":"YOUR_CODE", 
    "Endpoint":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointExternal":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointInternal":"https://oss-cn-zhangjiakou-internal.aliyuncs.com", 
    "BucketName":"caict-private-20220822", 
    "ConnTimeOut":60, 
    "DispTimeOut":1800, 
    "stsPolicyData":{
        "Version": "1",
        "Statement": 
        [{
            "Effect": "Allow",
            "Action": ["oss:Put*"],
            "Resource": ["acs:oss:*:*:caict-private-20220822","acs:oss:*:*:caict-private-20220822/*"],
            "Condition": {}
            }]
        },
    }, 
"homeServer":{
    "RegionId":"cn-zhangjiakou", 
    "roleArn":"acs:ram::1392433711034021:role/aliyunosstokengeneratorrole",
    "AccessKeyId":"LTAI5t5jMGja38rpxSNjL9th", 
    "AccessKeySecret":"YOUR_CODE", 
    "readOnlyAccessKeyId":"LTAI5t8S6nUMBYPySaUVtCaA", 
    "readOnlyAccessKeySecret":"YOUR_CODE", 
    "stsRegionId":"oss-cn-zhangjiakou", 
    "stsAccessKeyId":"LTAI5tPpkp526RfrRJB5LGnY", 
    "stsAccessKeySecret":"YOUR_CODE", 
    "Endpoint":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointExternal":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointInternal":"https://oss-cn-zhangjiakou-internal.aliyuncs.com", 
    "BucketName":"caict-private-20220822", 
    "ConnTimeOut":60, 
    "DispTimeOut":1800, 
    "stsPolicyData":{
        "Version": "1",
        "Statement": 
        [{
            "Effect": "Allow",
            "Action": ["oss:Put*"],
            "Resource": ["acs:oss:*:*:caict-private-20220822","acs:oss:*:*:caict-private-20220822/*"],
            "Condition": {}
            }]
        },
    }, 
"iottest-01":{
    "RegionId":"cn-zhangjiakou", 
    "roleArn":"acs:ram::1392433711034021:role/aliyunosstokengeneratorrole",
    "AccessKeyId":"LTAI5t5jMGja38rpxSNjL9th", 
    "AccessKeySecret":"YOUR_CODE", 
    "readOnlyAccessKeyId":"LTAI5t8S6nUMBYPySaUVtCaA", 
    "readOnlyAccessKeySecret":"YOUR_CODE", 
    "stsRegionId":"oss-cn-zhangjiakou", 
    "stsAccessKeyId":"LTAI5tPpkp526RfrRJB5LGnY", 
    "stsAccessKeySecret":"YOUR_CODE", 
    "Endpoint":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointExternal":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointInternal":"https://oss-cn-zhangjiakou-internal.aliyuncs.com", 
    "BucketName":"caict-private-20220822", 
    "ConnTimeOut":60, 
    "DispTimeOut":1800, 
    "stsPolicyData":{
        "Version": "1",
        "Statement": 
        [{
            "Effect": "Allow",
            "Action": ["oss:Put*"],
            "Resource": ["acs:oss:*:*:caict-private-20220822","acs:oss:*:*:caict-private-20220822/*"],
            "Condition": {}
            }]
        },
    }, 
"vc-voice":{
    "RegionId":"cn-zhangjiakou", 
    "roleArn":"acs:ram::1392433711034021:role/aliyunosstokengeneratorrole",
    "AccessKeyId":"LTAI5t5jMGja38rpxSNjL9th", 
    "AccessKeySecret":"YOUR_CODE", 
    "readOnlyAccessKeyId":"LTAI5t8S6nUMBYPySaUVtCaA", 
    "readOnlyAccessKeySecret":"YOUR_CODE", 
    "stsRegionId":"oss-cn-zhangjiakou", 
    "stsAccessKeyId":"LTAI5tPpkp526RfrRJB5LGnY", 
    "stsAccessKeySecret":"YOUR_CODE", 
    "Endpoint":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointExternal":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointInternal":"https://oss-cn-zhangjiakou-internal.aliyuncs.com", 
    "BucketName":"caict-private-20220822", 
    "ConnTimeOut":60, 
    "DispTimeOut":1800, 
    "stsPolicyData":{
        "Version": "1",
        "Statement": 
        [{
            "Effect": "Allow",
            "Action": ["oss:Put*"],
            "Resource": ["acs:oss:*:*:caict-private-20220822","acs:oss:*:*:caict-private-20220822/*"],
            "Condition": {}
            }]
        },
    }, 
"home":{
    "RegionId":"cn-zhangjiakou", 
    "roleArn":"acs:ram::1392433711034021:role/aliyunosstokengeneratorrole",
    "AccessKeyId":"LTAI5t5jMGja38rpxSNjL9th", 
    "AccessKeySecret":"YOUR_CODE", 
    "readOnlyAccessKeyId":"LTAI5t8S6nUMBYPySaUVtCaA", 
    "readOnlyAccessKeySecret":"YOUR_CODE", 
    "stsRegionId":"oss-cn-zhangjiakou", 
    "stsAccessKeyId":"LTAI5tPpkp526RfrRJB5LGnY", 
    "stsAccessKeySecret":"YOUR_CODE", 
    "Endpoint":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointExternal":"https://oss-cn-zhangjiakou.aliyuncs.com", 
    "EndpointInternal":"https://oss-cn-zhangjiakou-internal.aliyuncs.com", 
    "BucketName":"test-20220830", 
    "ConnTimeOut":60, 
    "DispTimeOut":1800, 
    "stsPolicyData":{
        "Version": "1",
        "Statement": 
        [{
            "Effect": "Allow",
            "Action": ["oss:Put*"],
            "Resource": ["acs:oss:*:*:test-20220830","acs:oss:*:*:test-20220830/*"],
            "Condition": {}
            }]
        },
    }
}[_SYS]


if __name__ == "__main__":
    pass
    # import pdb
    # pdb.set_trace()
    print ("_SYS",_SYS)
    print ("_SYS_SERVER_NAME",_SYS_SERVER_NAME)
    print ("ALIYUN_SMS_SERVICE",ALIYUN_SMS_SERVICE)
    print ("ALIYUN_OSS_SERVICE",ALIYUN_OSS_SERVICE)