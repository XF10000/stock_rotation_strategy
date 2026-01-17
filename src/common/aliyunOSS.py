#! /usr/bin/env python
#encoding: utf-8

#Filename: aliyunOSS.py
#Author: Steven Lian
#E-mail:  steven.lian@gmail.com  
#Date: 2019-09-28
#Description:   阿里云OSS功能
# https://github.com/aliyun/aliyun-oss-python-sdk
# pip3 install oss2

_VERSION="20230410"

_DEBUG=True

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#setting files
from config import aliyunSettings as settings

import oss2 as oss
from aliyunsdkcore import client
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

# _DEBUG =  settings._DEBUG
_DEBUG = False
# command part

authWrite = oss.Auth(settings.ALIYUN_OSS_SERVICE["AccessKeyId"],settings.ALIYUN_OSS_SERVICE["AccessKeySecret"])
authRead = oss.Auth(settings.ALIYUN_OSS_SERVICE["readOnlyAccessKeyId"],settings.ALIYUN_OSS_SERVICE["readOnlyAccessKeySecret"])
endpoint = settings.ALIYUN_OSS_SERVICE["Endpoint"]
endpointExternal = settings.ALIYUN_OSS_SERVICE["EndpointExternal"]
endpointInternal = settings.ALIYUN_OSS_SERVICE["EndpointInternal"]
bucketWrite = oss.Bucket(authWrite, endpoint, settings.ALIYUN_OSS_SERVICE["BucketName"], connect_timeout = settings.ALIYUN_OSS_SERVICE["ConnTimeOut"])
bucketWriteExternal = oss.Bucket(authWrite, endpointExternal, settings.ALIYUN_OSS_SERVICE["BucketName"], connect_timeout = settings.ALIYUN_OSS_SERVICE["ConnTimeOut"])
bucketRead = oss.Bucket(authRead, endpoint, settings.ALIYUN_OSS_SERVICE["BucketName"], connect_timeout = settings.ALIYUN_OSS_SERVICE["ConnTimeOut"])
bucketReadExternal = oss.Bucket(authRead, endpointExternal, settings.ALIYUN_OSS_SERVICE["BucketName"], connect_timeout = settings.ALIYUN_OSS_SERVICE["ConnTimeOut"])

# <yourLocalFile>由本地文件路径加文件名包括后缀组成，例如/users/local/myfile.txt
if _DEBUG:
    _LOG = misc.setLogNew("ALIOSS", "aliosslog")

#把本地文件上传
def uploadFile(objName, fileName,downloadName = ""):
    result = False
    try:
        if downloadName:
            contentDisposition = f"attachment;filename={downloadName}"
            headers={"Content-Disposition":contentDisposition}
            response = bucketWrite.put_object_from_file(objName, fileName,headers=headers)
        else:
            response = bucketWrite.put_object_from_file(objName, fileName)
        if response.status == 200:
            if existFile(objName):
                result = True
        if not result:
            if _DEBUG:
                _LOG.warning(f"response.status:{response.status}")
        
    except Exception as e:
        errMsg = f"errMsg:{str(e)}"
        if _DEBUG:
            _LOG.error(f"errMsg:{errMsg}")
        
    return result


#下载文件到本地 
def downloadFile(objName, fileName):
    result = False
    try:
        response = bucketRead.get_object_to_file(objName, fileName)
        if response.status == 200:
            result = True
        
    except Exception as e:
        errMsg = f"errMsg:{str(e)}"
        if _DEBUG:
            _LOG.error(f"errMsg:{errMsg}")
        
    return result


#判断文件是否存在
def existFile(objName):
    result = False
    try:
        response = bucketRead.object_exists(objName)
        result = response
        
    except Exception as e:
        errMsg = f"errMsg:{str(e)}"
        if _DEBUG:
            _LOG.error(f"errMsg:{errMsg}")
        
    return result


#删除文件
def deleteFile(objName):
    result = False
    try:
        response = bucketWrite.delete_object(objName)
        result = response
        
    except Exception as e:
        errMsg = f"errMsg:{str(e)}"
        if _DEBUG:
            _LOG.error(f"errMsg:{errMsg}")
        
    return result


#设置文件存取权限
def setFileAccess(objName, accessRight):
    result = False
    try:
        response = bucketWrite.put_object_acl(objName, accessRight)
        result = response
        
    except Exception as e:
        errMsg = f"errMsg:{str(e)}"
        if _DEBUG:
            _LOG.error(f"errMsg:{errMsg}")
        
    return result


#获取文件信息
def getFileInfo(objName):
    result = {}
    try:
        response = bucketRead.get_object_meta(objName)
        fileSize = response.headers["Content-Length"]
        if fileSize:
            try:
                fileSize = int(fileSize)
            except:
                fileSize = 0
        result["fileSize"] = fileSize
        try:
            result["ETag"] = misc.jsonLoads(response.headers["ETag"])
        except:            
            result["ETag"] = response.headers["ETag"]
        GMT_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
        misc.datetime.datetime.utcnow().strftime(GMT_FORMAT)
        dtVal = misc.datetime.datetime.strptime(response.headers["Last-Modified"], GMT_FORMAT)
        YMDHMS_FORMAT = "%Y%m%d%H%M%S"
        YMDHMS = dtVal.strftime(YMDHMS_FORMAT)
        result["modifyYMDHMS"] = YMDHMS
        
    except Exception as e:
        errMsg = f"errMsg:{str(e)}"
        if _DEBUG:
            _LOG.error(f"errMsg:{errMsg}")
        
    return result


#生成文件的临时下载url
def genFileTempUrl(objName, timeOut = settings.ALIYUN_OSS_SERVICE["DispTimeOut"]):
    result = ""
    try:
        #response = bucketRead.sign_url("GET", objName, timeOut)
        response = bucketReadExternal.sign_url("GET", objName, timeOut)
        result = response
        
    except Exception as e:
        errMsg = f"errMsg:{str(e)}"
        if _DEBUG:
            _LOG.error(f"errMsg:{errMsg}")
        
    return result


#生成文件的临时上传url
def genFileUploadUrl(objName, timeOut = settings.ALIYUN_OSS_SERVICE["DispTimeOut"]):
    result = ""
    try:
        #response = bucketWrite.sign_url("PUT", objName, timeOut,slash_safe=True)
        response = bucketWriteExternal.sign_url("PUT", objName, timeOut,slash_safe=True)
        result = response
        
    except Exception as e:
        errMsg = f"errMsg:{str(e)}"
        if _DEBUG:
            _LOG.error(f"errMsg:{errMsg}")
        
    return result


# 通过阿里云STS为前端直接上传提供所需参数
def genSTSToken(objName,sessionName = "session_default"):
    global endpoint
    result = {}
    try:
    
        # yourEndpoint填写Bucket所在地域对应的Endpoint。以华东1（杭州）为例，Endpoint填写为https://oss-cn-hangzhou.aliyuncs.com。
        # endpoint = "https://oss-cn-zhangjiakou.aliyuncs.com"
        # 阿里云账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM用户进行API访问或日常运维，请登录RAM控制台创建RAM用户。
        access_key_id = settings.ALIYUN_OSS_SERVICE["stsAccessKeyId"]
        access_key_secret = settings.ALIYUN_OSS_SERVICE["stsAccessKeySecret"]
        region_id = settings.ALIYUN_OSS_SERVICE["RegionId"]
        stsRegionId = settings.ALIYUN_OSS_SERVICE["stsRegionId"]
        # 填写Bucket名称，例如examplebucket。
        bucket_name = settings.ALIYUN_OSS_SERVICE["BucketName"]
        # 填写Object完整路径，例如exampledir/exampleobject.txt。Object完整路径中不能包含Bucket名称。
        # object_name = 'exampledir/exampleobject.txt'
        # 您可以登录RAM控制台，在RAM角色管理页面，搜索创建的RAM角色后，单击RAM角色名称，在RAM角色详情界面查看和复制角色的ARN信息。
        # 填写角色的ARN信息。格式为acs:ram::$accountID:role/$roleName。
        # $accountID为阿里云账号ID。您可以通过登录阿里云控制台，将鼠标悬停在右上角头像的位置，直接查看和复制账号ID，或者单击基本资料查看账号ID。
        # $roleName为RAM角色名称。您可以通过登录RAM控制台，单击左侧导航栏的RAM角色管理，在RAM角色名称列表下进行查看。
        role_arn = settings.ALIYUN_OSS_SERVICE["roleArn"]

        # 创建权限策略。
        # 只允许对名称为examplebucket的Bucket下的所有资源执行GetObject操作。
        # policy_text = '{"Version": "1", "Statement": [{"Action": ["oss:GetObject"], "Effect": "Allow", "Resource": ["acs:oss:*:*:test-20220830/*"]}]}'
        policy_text = misc.jsonDumps(settings.ALIYUN_OSS_SERVICE["stsPolicyData"])

        clt = client.AcsClient(access_key_id, access_key_secret, region_id)
        req = AssumeRoleRequest.AssumeRoleRequest()

        # 设置返回值格式为JSON。
        req.set_accept_format('json')
        req.set_RoleArn(role_arn)
        # 自定义角色会话名称，用来区分不同的令牌，例如可填写为session-test。
        req.set_RoleSessionName(sessionName)
        req.set_Policy(policy_text)
        body = clt.do_action_with_exception(req)

        # 使用RAM用户的AccessKeyId和AccessKeySecret向STS申请临时访问凭证。
        token = misc.jsonLoads(oss.to_unicode(body))

        result["bucketName"] = bucket_name
        result["regionID"] = stsRegionId
        # result["uploadUrl"] = genFileUploadUrl(objName)
        result["securityToken"] = token['Credentials']['SecurityToken']
        result["accessKeyID"] = token['Credentials']['AccessKeyId']
        result["accessKeySecret"] = token['Credentials']['AccessKeySecret']
        result["expiration"] = token['Credentials']['Expiration']

    except Exception as e:
        errMsg = f"errMsg:{str(e)}"
        if _DEBUG:
            _LOG.error(f"errMsg:{errMsg}")

    return result


def testStsUpload():
    import requests
    objName = "aliyunOSS.py"
    rtnData = genSTSToken(objName)
    putUrl = genFileUploadUrl(objName)
    print(putUrl)
    # 通过签名URL上传文件，以requests为例说明。
    # 填写本地文件路径，例如D:\\exampledir\\examplefile.txt。
    headers = {}
    headers['Content-Type'] = 'text/txt'
    r = requests.put(putUrl, data=open(objName, 'rb').read(), headers=headers)  
    if r.status_code == 200:  # OK.  Everything worked as expected.  :)  :-)  :-)  :-)  :-)
        print ("Put to bucket successful!")  # This is what we wanted.  It worked.  :-)  :-)  :-)  :-)
    else:
        print (r.status_code)


def main():
    testStsUpload()
    objName = "aliyunOSS.py"
    # rtnData = genSTSToken(objName)
    # putUrl = genFileUploadUrl(objName)
    # print(putUrl)
    # objName = "hotelPhoto-acab4a88-b7a4-46f9-abea-ca12df861cde"
    fileName = r"aliyunOSS.py"
    uploadFile(objName, fileName,fileName)
    # if existFile(objName):
    #     fileName = r"D:\data\AI\hotel\hotel-0.jpg"
    #     downloadFile(objName, fileName)
    # objName = "hotelPhoto-acab4a88-b7a4-46f9-abea-ca12df861cde"
    if existFile(objName):
        genFileTempUrl(objName)
    

if __name__ == "__main__":
    main()



