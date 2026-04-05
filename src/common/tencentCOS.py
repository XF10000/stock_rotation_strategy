#! /usr/bin/env python
#encoding: utf-8

#Filename: tecentCOS.py
#Author: Steven Lian
#E-mail:  steven.lian@gmail.com  
#Date: 2020-04-16
# https://cloud.tencent.com/document/product/436/12269
# pip install -U cos-python-sdk-v5
#Description:   腾讯云COS功能, 类似 阿里云OSS功能

_VERSION="20200606"

_DEBUG=True

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

import traceback

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#setting files
from config import tencentSettings as settings

#tencent service
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client

if "_TESTLOG" not in dir() or not _TESTLOG:
    _TESTLOG = misc.setLogNew("TENCENT", "tencentCOSlog")
    systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
    _TESTLOG.info("python version:[%s], code version:[%s]" %(systemVersion, _VERSION))

_DEBUG =  settings._DEBUG
_DEBUG =  True
# command part

secretID = settings.TECENT_COS_SERVICE["secretId"]      # 替换为用户的 secretId
secretKey = settings.TECENT_COS_SERVICE["secretKey"]       # 替换为用户的 secretKey
regionID = settings.TECENT_COS_SERVICE["regionId"]      # 替换为用户的 Region https://cloud.tencent.com/document/product/436/6224
token = None                # 使用临时密钥需要传入 Token，默认为空，可不填
scheme = 'https'            # 指定使用 http/https 协议来访问 COS，默认为 https，可不填
bucketName = settings.TECENT_COS_SERVICE["bucketName"] 
privateBucketName =  settings.TECENT_COS_SERVICE["privateBucketName"] 

txCOSConfig = CosConfig(Region=regionID, SecretId=secretID, SecretKey=secretKey, Token=token, Scheme=scheme)

bucketWrite = CosS3Client(txCOSConfig)
bucketRead = CosS3Client(txCOSConfig)

# <yourLocalFile>由本地文件路径加文件名包括后缀组成，例如/users/local/myfile.txt

def uploadFile(keyName, fileName, privateFlag = False):
    #_TESTLOG.info("%s: %s %s" % ("uploadFile", keyName, fileName, ))
    result = False
    if privateFlag:
        localBucketName = privateBucketName
    else:
        localBucketName = bucketName
        
    try:
        response = bucketWrite.upload_file(
            Bucket = localBucketName, 
            LocalFilePath = fileName, 
            Key = keyName, 
            PartSize = 1, 
            MAXThread = 10, 
            EnableMD5 = False
            )

        #_TESTLOG.info("%s: %s %s %s" % ("uploadFile", keyName, fileName, str(response)))

        if isinstance(response, dict):
            ETag = response.get("ETag")
            if ETag:
                result = True
    except Exception as e:
        f = sys._getframe().f_back
        errMsg = '%s' % (keyName)
        #_TESTLOG.error( 'getTempLocation %s, %s' %(errMsg, traceback.format_exc()))

        pass
        
    return result

    
def downloadFile(keyName, fileName, privateFlag = False):
    result = False
    if privateFlag:
        localBucketName = privateBucketName
    else:
        localBucketName = bucketName

    try:
        response = bucketRead.get_object(
            Bucket = localBucketName, 
            Key = keyName
            )
        if response != {}:
            result = True
            response['Body'].get_stream_to_file(fileName)
            
    except:
        pass
        
    return result


def existFile(keyName, privateFlag = False):
    result = False
    if privateFlag:
        localBucketName = privateBucketName
    else:
        localBucketName = bucketName
        
    try:
        response = bucketRead.list_objects(
            Bucket = localBucketName, 
            Prefix = keyName
            )
        if response != {}:
            result = True
    except:
        pass
        
    return result


def getFileInfo(keyName, privateFlag = False):
    result = {}
    if privateFlag:
        localBucketName = privateBucketName
    else:
        localBucketName = bucketName
        
    try:
        response = bucketRead.list_objects(
            Bucket = localBucketName, 
            Prefix = keyName
            )
        if response != {}:
            result = response
    except:
        pass
        
    return result
    

def deleteFile(keyName, privateFlag = False):
    result = False
    if privateFlag:
        localBucketName = privateBucketName
    else:
        localBucketName = bucketName
        
    try:
        response = bucketRead.delete_object(
            Bucket = localBucketName, 
            Key = keyName
            )
        if response != {}:
            result = True
    except:
        pass
        
    return result


def setFileAccess(keyName, accessRight, privateFlag = False):
    result = False
    if privateFlag:
        localBucketName = privateBucketName
    else:
        localBucketName = bucketName
        
    try:
        response = bucketWrite.put_object_acl(
            Bucket = localBucketName, 
            Key = keyName, 
            ALC = accessRight
            )
        if response != {}:
            result = True
    except:
        pass
    return result


def genFileTempUrl(keyName, privateFlag = False):
    result = ""
    if privateFlag:
        localBucketName = privateBucketName
    else:
        localBucketName = bucketName
        
    try:
        response = bucketWrite.get_presigned_download_url(
            #Method = "PUT", 
            Bucket = localBucketName, 
            Key = keyName, 
            Expired=3600 #过期时间是1个小时
            )
        result = response
    except:
        pass
        
    return result
    

def genFilePublicUrl(keyName, privateFlag = False):
    result = ""
    if privateFlag:
        localUrl = settings.TECENT_COS_SERVICE["privateUrl"]
    else:
        localUrl = settings.TECENT_COS_SERVICE["url"]
        
    try:
        baseUrl = localUrl
        result = baseUrl + "/" + keyName
    except:
        pass
        
    return result
    
    
def listFiles(maxNum  = 1000,  privateFlag = False):
    result = []
    total = 0
    nextMarker = None
    
    if maxNum >= 1000:
        maxKeys = 1000
    else:
        maxKeys = maxNum

    if privateFlag:
        localBucketName = privateBucketName
    else:
        localBucketName = bucketName
        
    try:
        while True:
            if nextMarker == None:
                response = bucketRead.list_objects(
                        Bucket = localBucketName, 
                        MaxKeys = maxKeys, 
                        )
            else:
                response = bucketRead.list_objects(
                        Bucket = localBucketName, 
                        MaxKeys = maxKeys, 
                        Marker = nextMarker,                     
                        )
            if response != {}:
                partList = response.get("Contents", [])
                result += partList
                total += len(partList)
                nextMarker = response.get("NextMarker")
                isTruncated = response.get("IsTruncated")
                if isTruncated == "false" or total >= maxNum :
                    break 
            else:
                break
    except:
        pass
        
    return result
    
    
def main():
    if len(sys.argv) > 1:
        pass
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
    
    dataList = listFiles(500)
        
    keyName = "12345678901"
    keyName = "12345678903"
    fileName = r"D:\data\AI\hotel\hotel-1.jpg"
#    uploadFile(keyName, fileName, privateFlag = True)
#    fileName = r"D:\data\AI\hotel\hotel-0.jpg"
#    if getFileInfo(keyName):
#        downloadFile(keyName, fileName)
#    deleteFile(keyName)
#    objName = "hotelPhotoacab4a88-b7a4-46f9-abea-ca12df861cde"
    if existFile(keyName, privateFlag = True):
        genFileTempUrl(keyName, privateFlag = True)
    

if __name__ == "__main__":
    main()



