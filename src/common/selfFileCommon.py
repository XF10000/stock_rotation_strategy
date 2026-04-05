#! /usr/bin/env python
#encoding: utf-8

#Filename: selfFileCommon.py
#Author: Steven Lian
#E-mail:  steven.lian@gmail.com  
#Date: 2020-04-16
#Description:   本地文件系统, 类似 阿里云OSS功能

_VERSION="20250922"

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
import pathlib 
import random
import uuid
import shutil
import subprocess

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#setting files
from config import selfFileSettings as settings


if "_LOG" not in dir() or not _LOG:
    _LOG = misc.setLogNew("SELF", "selffilelog")
    systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
    _LOG.info("python version:[%s], code version:[%s]" %(systemVersion, _VERSION))

_DEBUG =  settings._DEBUG

_processorPID = os.getpid()

_SYS_SERVER_NAME = settings._SYS_SERVER_NAME

# FILE_SYSTEM_MODE = settings.FILE_SYSTEM_MODE
# FASTDFS_SERVER_PATH = settings.FASTDFS_SERVER_PATH
LOCAL_FILE_SERVER_DIR_NAME = settings.LOCAL_FILE_SERVER_DIR_NAME
LOCAL_FILE_SERVER_PATH = settings.LOCAL_FILE_SERVER_PATH
LOCAL_FILE_SERVER_BASE = settings.LOCAL_FILE_SERVER_BASE
LOCAL_FILE_TEMP_WEB_DIR = settings.LOCAL_FILE_TEMP_WEB_DIR

SELF_FILE_SERVER_STORAGE_IF_REMOTE = settings.SELF_FILE_SERVER_STORAGE_IF_REMOTE
SELF_FILE_SERVER_STORAGE_ADDR = settings.SELF_FILE_SERVER_STORAGE_ADDR
SELF_FILE_SERVER_STORAGE_DIR = settings.SELF_FILE_SERVER_STORAGE_DIR
LOCAL_FILE_STORAGE_DIR_MAX_NUM = settings.LOCAL_FILE_STORAGE_DIR_MAX_NUM
LOCAL_FILE_STORAGE_DIR_LEN = settings.LOCAL_FILE_STORAGE_DIR_LEN

# command part begin
# 获取文件唯一ID
def getFileUUID():
    fileID = str(uuid.uuid4())
    fileID = fileID.replace("-","")
    return fileID


# 创建目录, 如果目录不存在,就创建
def create_dir(dirName):
    if not os.path.exists(dirName):
        os.mkdir(dirName)
    #如果文件存储在其他服务器, 先建立本地目录, 然后复制到远端:
    if SELF_FILE_SERVER_STORAGE_IF_REMOTE:
        cmdLine = f"scp -r {dirName} {SELF_FILE_SERVER_STORAGE_ADDR}:{dirName}" 
        p = subprocess.Popen(cmdLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) 
        lines = p.stdout.readlines()        


# 获取唯一文件名
def unique_file_name(filePath):
    result = filePath
    count = 10
    while count > 0:
        if not SELF_FILE_SERVER_STORAGE_IF_REMOTE:
            count -= 1
            if not os.path.exists(result):
                break
            else:
                dirName, fileExt = os.path.splitext(result)
                result = dirName + "_" + str(random.randint(0,1000000)) + fileExt
        else:
            #远程暂不判断, 重复概率很低
            pass

    return result


#生成本地存储目录,如果目录不存在,就创建
def genSelfFileStorageDir():
    targetDir = SELF_FILE_SERVER_STORAGE_DIR
    subDirNum = random.randint(0,LOCAL_FILE_STORAGE_DIR_MAX_NUM-1)
    subDirName = str(subDirNum).zfill(LOCAL_FILE_STORAGE_DIR_LEN)
    fullDirPath = os.path.join(targetDir,subDirName)
    create_dir(fullDirPath)
    return fullDirPath,subDirName


#生成本地存储文件唯一ID
def genSelfFileFileID():
    dirName,subDirName = genSelfFileStorageDir()
    fileID = "file" + getFileUUID()
    fullPath = os.path.join(dirName,fileID)
    fullPath = pathlib.Path(fullPath).as_posix()
    fileID = os.path.join(subDirName,fileID)
    fileID = pathlib.Path(fileID).as_posix()
    return fullPath,fileID

# command part end

#把本地临时文件存储到SELFFILE的存储区域
#这里先选择生成一个随机目录, 然后生成两个文件
#一个数据, 一个是文件原来的主要信息
def uploadFile(fileInfo, privateFlag = False):
    result = ""
    try:
        fileName = fileInfo.get("fileName")
        #首先, 生成一个文件ID,包括完全路径和filID
        fullPath,fileID = genSelfFileFileID()
        
        #两个文件一个保存文件,一个保存相关信息
        targetFilePath = fullPath + ".data"
        targetInfoPath = fullPath + ".info"

        #存储文件到文件目的地址,copy
        shutil.copyfile(fileName, targetFilePath)

        #存储文件信息到信息目的地址
        misc.saveJsonData(targetInfoPath,fileInfo,indent=2)

        result = fileID

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result

    
def downloadFile(fileID, targetFileName, privateFlag = False,
                 overWriteFlag = True,saveFileCopy = True):
    result = ""
    try:
        sourceDir = SELF_FILE_SERVER_STORAGE_DIR
    
        #两个文件一个保存文件,一个保存相关信息
        fileName = fileID + ".data"
        infoName = fileID + ".info"

        filePath = os.path.join(sourceDir,fileName)
        filePath = pathlib.Path(filePath).as_posix()

        infoPath = os.path.join(sourceDir,infoName)
        infoPath = pathlib.Path(infoPath).as_posix()

        #临时文件输出位置
        midDigital = random.randint(0,9)
        midDir = str(midDigital) + '/'

        outfileDir = LOCAL_FILE_SERVER_BASE + LOCAL_FILE_TEMP_WEB_DIR + midDir 

        #获取本地文件信息
        fileInfo = misc.loadJsonData(infoPath,"dict")
        if fileInfo:
            objectName = fileInfo.get("objectName")
            #复制文件到临时目录
            if targetFileName:
                tempLocalFilePath = os.path.join(outfileDir,targetFileName)
                tempLocalFilePath = pathlib.Path(tempLocalFilePath).as_posix()
            elif objectName:
                #使用老的文件名
                tempLocalFilePath = os.path.join(outfileDir,objectName)
                tempLocalFilePath = pathlib.Path(tempLocalFilePath).as_posix()
            else:
                tempFileName = "file_" + getFileUUID()
                tempLocalFilePath = os.path.join(outfileDir,tempFileName)

            #保证唯一
            if not overWriteFlag:
                tempLocalFilePath = unique_file_name(tempLocalFilePath)

            #存储文件到文件目的地址,copy
            if saveFileCopy and os.path.isfile(tempLocalFilePath):
                if SELF_FILE_SERVER_STORAGE_IF_REMOTE:
                    #如果文件存储在其他服务器, 会用scp复制
                    cmdLine = f"scp  {filePath} {SELF_FILE_SERVER_STORAGE_ADDR}:{tempLocalFilePath}" 
                    p = subprocess.Popen(cmdLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) 
                    lines = p.stdout.readlines()        
                    if _DEBUG:
                        _LOG.info(f"saveFileCopy:{tempLocalFilePath},same size")
                else:
                    if os.path.getsize(filePath) != os.path.getsize(tempLocalFilePath):
                        shutil.copyfile(filePath, tempLocalFilePath)
                    else:
                        if _DEBUG:
                            _LOG.info(f"saveFileCopy:{tempLocalFilePath},same size")
            else:
                if SELF_FILE_SERVER_STORAGE_IF_REMOTE:
                    shutil.copyfile(filePath, tempLocalFilePath)
                else:
                    shutil.copyfile(filePath, tempLocalFilePath)

            result = tempLocalFilePath

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def existFile(fileID, privateFlag = False):
    result = False
    result = {}
    try:
        if not SELF_FILE_SERVER_STORAGE_IF_REMOTE:
            sourceDir = SELF_FILE_SERVER_STORAGE_DIR

            #两个文件一个保存文件,一个保存相关信息
            fileName = fileID + ".data"
            infoName = fileID + ".info"

            filePath = os.path.join(sourceDir,fileName)
            filePath = pathlib.Path(filePath).as_posix()

            infoPath = os.path.join(sourceDir,infoName)
            infoPath = pathlib.Path(infoPath).as_posix()

            if os.path.isfile(filePath) and os.path.isfile(infoPath):
                result = True
        else:
            result = True

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        
    return result


def getFileInfo(fileID, privateFlag = False):
    result = {}
    try:
        infoName = fileID + ".info"
        if not SELF_FILE_SERVER_STORAGE_IF_REMOTE:
            sourceDir = SELF_FILE_SERVER_STORAGE_DIR

            #两个文件一个保存文件,一个保存相关信息
            # fileName = fileID + ".data"

            # filePath = os.path.join(sourceDir,fileName)
            # filePath = pathlib.Path(filePath).as_posix()

            infoPath = os.path.join(sourceDir,infoName)
            infoPath = pathlib.Path(infoPath).as_posix()

            # if os.path.isfile(filePath):
            #     os.remove(filePath)
            #     result = True

            if os.path.isfile(infoPath):
                result = misc.loadJsonData(infoPath,"dict")
        else:
            #文件在远程服务器, 需要先复制到本地
            #先复制文件到临时目录
            tempDirName = LOCAL_FILE_SERVER_BASE + "output/"
            tempFileName = os.path.join(tempDirName,infoName)
            tempFileName = pathlib.Path(tempFileName).as_posix()
            #如果文件存储在其他服务器, 会用scp复制
            cmdLine = f"scp {SELF_FILE_SERVER_STORAGE_ADDR}:{infoName}  {tempFileName} " 
            p = subprocess.Popen(cmdLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) 
            lines = p.stdout.readlines()        
            if os.path.isfile(tempFileName):
                result = misc.loadJsonData(tempFileName,"dict")
            pass

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        
    return result
    

def deleteFile(fileID, privateFlag = False):
    result = False
    try:
        sourceDir = SELF_FILE_SERVER_STORAGE_DIR

        #两个文件一个保存文件,一个保存相关信息
        fileName = fileID + ".data"
        infoName = fileID + ".info"

        filePath = os.path.join(sourceDir,fileName)
        filePath = pathlib.Path(filePath).as_posix()

        infoPath = os.path.join(sourceDir,infoName)
        infoPath = pathlib.Path(infoPath).as_posix()

        if not SELF_FILE_SERVER_STORAGE_IF_REMOTE:
            if os.path.isfile(filePath):
                os.remove(filePath)
                result = True

            if os.path.isfile(infoPath):
                os.remove(infoPath)
                result = True
        else:
            #文件在远程服务器, 需要用ssh方式
            cmdLine = f'ssh {SELF_FILE_SERVER_STORAGE_ADDR} "rm -f {filePath}" ' 
            p = subprocess.Popen(cmdLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) 
            lines = p.stdout.readlines()        
            cmdLine = f'ssh {SELF_FILE_SERVER_STORAGE_ADDR} "rm -f {infoPath}" ' 
            p = subprocess.Popen(cmdLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) 
            lines = p.stdout.readlines()        
            pass

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


#根据fileID把文件挪到一个临时可以访问的位置
#默认使用原来的文件名, 如果重复自动+1
def genFileTempUrl(fileID, targetFileName="",privateFlag = False,localAddress = False,
                 overWriteFlag = True,saveFileCopy = True,sourceServerAddr=""):
    result = ""
    try:
        sourceDir = SELF_FILE_SERVER_STORAGE_DIR
    
        #两个文件一个保存文件,一个保存相关信息
        fileName = fileID + ".data"
        infoName = fileID + ".info"

        filePath = os.path.join(sourceDir,fileName)
        filePath = pathlib.Path(filePath).as_posix()

        infoPath = os.path.join(sourceDir,infoName)
        infoPath = pathlib.Path(infoPath).as_posix()

        #临时文件输出位置
        midDigital = random.randint(0,9)
        midDir = str(midDigital) + '/'

        outfileDir = LOCAL_FILE_SERVER_BASE + LOCAL_FILE_TEMP_WEB_DIR + midDir 

        if not SELF_FILE_SERVER_STORAGE_IF_REMOTE:
            #获取本地文件信息
            fileInfo = misc.loadJsonData(infoPath,"dict")
            if fileInfo:
                objectName = fileInfo.get("objectName")
                #复制文件到临时目录
                if targetFileName:
                    tempLocalFilePath = os.path.join(outfileDir,targetFileName)
                    tempLocalFilePath = pathlib.Path(tempLocalFilePath).as_posix()
                elif objectName:
                    #使用老的文件名
                    tempLocalFilePath = os.path.join(outfileDir,objectName)
                    tempLocalFilePath = pathlib.Path(tempLocalFilePath).as_posix()
                else:
                    tempFileName = "file_" + getFileUUID()
                    tempLocalFilePath = os.path.join(outfileDir,tempFileName)

                #保证唯一
                if not overWriteFlag:
                    tempLocalFilePath = unique_file_name(tempLocalFilePath)

                #存储文件到文件目的地址,copy
                if saveFileCopy and os.path.isfile(tempLocalFilePath):
                    if os.path.getsize(filePath) != os.path.getsize(tempLocalFilePath):
                        shutil.copyfile(filePath, tempLocalFilePath)
                    else:
                        if _DEBUG:
                            _LOG.info(f"saveFileCopy:{tempLocalFilePath},same size")
                else:
                    shutil.copyfile(filePath, tempLocalFilePath)

                if localAddress:
                    result = tempLocalFilePath
                else:
                    #生成本地可以访问的url
                    baseLen = len(LOCAL_FILE_SERVER_BASE)
                    fileName = tempLocalFilePath[baseLen:]
                    if sourceServerAddr:
                        result = sourceServerAddr + "/" + LOCAL_FILE_SERVER_DIR_NAME + "/" + fileName
                    else:
                        result = LOCAL_FILE_SERVER_PATH + fileName
        else:
            #文件存储在远端服务器
            fileInfo = getFileInfo(fileID, privateFlag = privateFlag)
            if fileInfo:
                objectName = fileInfo.get("objectName")

                #复制文件到临时目录
                if targetFileName:
                    tempLocalFilePath = os.path.join(outfileDir,targetFileName)
                    tempLocalFilePath = pathlib.Path(tempLocalFilePath).as_posix()
                elif objectName:
                    #使用老的文件名
                    tempLocalFilePath = os.path.join(outfileDir,objectName)
                    tempLocalFilePath = pathlib.Path(tempLocalFilePath).as_posix()
                else:
                    tempFileName = "file_" + getFileUUID()
                    tempLocalFilePath = os.path.join(outfileDir,tempFileName)

                #存储文件到文件目的地址,copy
                cmdLine = f"scp  {SELF_FILE_SERVER_STORAGE_ADDR}:{filePath} {tempLocalFilePath}" 
                p = subprocess.Popen(cmdLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) 
                lines = p.stdout.readlines()        

                if localAddress:
                    result = tempLocalFilePath
                else:
                    #生成本地可以访问的url
                    baseLen = len(LOCAL_FILE_SERVER_BASE)
                    fileName = tempLocalFilePath[baseLen:]
                    if sourceServerAddr:
                        result = sourceServerAddr + "/" + LOCAL_FILE_SERVER_DIR_NAME + "/" + fileName
                    else:
                        result = LOCAL_FILE_SERVER_PATH + fileName
            pass

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result
    

def listFiles(maxNum  = 1000,  privateFlag = False):
    result = []
    total = 0

    dirName = SELF_FILE_SERVER_STORAGE_DIR
    dirLen = len(dirName)

    try:
        for home, dirs, files in os.walk(dirName):
            for filename in files:
                # 文件名列表, 去除dirName
                fullFilePath = os.path.join(home, filename)
                fullFilePath = pathlib.Path(fullFilePath).as_posix()
                if fullFilePath[-4:] == "data":
                    fileID = fullFilePath[dirLen:-5]           
                    total += 1
                    result.append(fileID)
                    if total >= maxNum:
                        break

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        
    return result
    
    
def main():
    # print ("_SYS",_SYS)
    print ("_SYS_SERVER_NAME",_SYS_SERVER_NAME)

    print ("LOCAL_FILE_SERVER_BASE", LOCAL_FILE_SERVER_BASE)
    print ("LOCAL_FILE_SERVER_PATH", LOCAL_FILE_SERVER_PATH)
    if len(sys.argv) > 1:
        pass
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
    
    # dataList = listFiles(500)
        
    # keyName = "12345678901"
    # keyName = "12345678903"
    # fileName = r"D:\data\AI\hotel\hotel-1.jpg"
#    uploadFile(keyName, fileName, privateFlag = True)
#    fileName = r"D:\data\AI\hotel\hotel-0.jpg"
#    if getFileInfo(keyName):
#        downloadFile(keyName, fileName)
#    deleteFile(keyName)
#    objName = "hotelPhotoacab4a88-b7a4-46f9-abea-ca12df861cde"
    # if existFile(keyName, privateFlag = True):
    #     genFileTempUrl(keyName, privateFlag = True)
    

if __name__ == "__main__":
    main()



