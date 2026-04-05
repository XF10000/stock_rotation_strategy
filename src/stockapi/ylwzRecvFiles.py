#! /usr/bin/env python3
#encoding: utf-8

#Filename: ylwzRecvFiles.py
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com  
#Date: 2019-08-01
#Description:   文件接收功能/增加腾讯COS支持
'''
增加 SELFFILE 的支持, 这个版本支持在本地存储数据, 并根据需要移动文件到可访问区域
这个和NGINX的区别是 NGINX 存储的位置是在/data/webserver内, 理论上外面可以访问
SELFFILE的位置是 /data/
'''


_VERSION="20250922"


from flask import Flask, request

application = Flask(__name__)

import traceback

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

import uuid
import subprocess
import shutil 
import random
import pathlib

from PIL import Image,ImageDraw,ImageFont #图片
# import ffmpy #视频文件

#global defintion/common var etc.
from common import globalDefinition as comGD

#common functions(log,time,string, json etc)
from common import miscCommon as misc

#common functions(database operation)
from common import redisCommon as comDB

#common functions(funct operation)
from common import funcCommon as comFC

#from common import aiDogCat as aiDogCat
from common import aliyunOSS as OSS
from common import tencentCOS as COS
from common import selfFileCommon as comSelf

#setting files
from config import basicSettings as settings
# from config import redisSettings as redisSettings


_processorPID = os.getpid()

if "_LOG" not in dir() or not _LOG:
    _LOG = misc.setLogNew("RECVFILE", "recvfileslog")
systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
_LOG.info(f"PID:{_processorPID}, python version:{systemVersion}, code version:{_VERSION}")

_DEBUG =  settings._DEBUG

_SYS_SERVER_NAME = settings._SYS_SERVER_NAME

FILE_SYSTEM_MODE = settings.FILE_SYSTEM_MODE
FASTDFS_SERVER_PATH = settings.FASTDFS_SERVER_PATH
LOCAL_FILE_SERVER_DIR_NAME =settings.LOCAL_FILE_SERVER_DIR_NAME
LOCAL_FILE_SERVER_PATH = settings.LOCAL_FILE_SERVER_PATH
LOCAL_FILE_SERVER_BASE = settings.LOCAL_FILE_SERVER_BASE
LOCAL_FILE_TEMP_WEB_DIR = settings.LOCAL_FILE_TEMP_WEB_DIR

SELF_FILE_SERVER_STORAGE_IF_REMOTE = comSelf.settings.SELF_FILE_SERVER_STORAGE_IF_REMOTE
SELF_FILE_SERVER_STORAGE_ADDR = comSelf.settings.SELF_FILE_SERVER_STORAGE_ADDR
SELF_FILE_SERVER_STORAGE_DIR = comSelf.settings.SELF_FILE_SERVER_STORAGE_DIR
LOCAL_FILE_STORAGE_DIR_MAX_NUM = comSelf.settings.LOCAL_FILE_STORAGE_DIR_MAX_NUM
LOCAL_FILE_STORAGE_DIR_LEN = comSelf.settings.LOCAL_FILE_STORAGE_DIR_LEN

thumbnailSize = settings.THUMBNAIL_SIZE
maxPicSize = settings.MAX_PIC_SIZE

maxPicEnable = True
if maxPicSize == ():
    maxPicEnable = False

# command part

#CONST_PHOTO_FILE_MAX_SIZE_LIMIT = 100000 #100K
CONST_PHOTO_FILE_MAX_SIZE_LIMIT = 1000000 #1M

#图片和视频文件扩展名清单
photoFileExtList = [".png", ".jpg", ".jpeg", ".bmp", ".gif", ".jfif"]
movFileExtList = [".avi", ".mov", ".mp4", ".mpeg", ".mpg"]
officeExtList = [".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".csv"]
textExtList = [".txt", ".htm", ".html", ".ini"]


def getDefaultFileType(filePath):
    tempDir, tempExt = os.path.splitext(filePath)
    if tempExt in photoFileExtList:
        result = "pic"
    elif tempExt in movFileExtList:
        result = "video"
    elif tempExt in officeExtList:
        result = "office"
    elif tempExt in textExtList:
        result = "text"
    else:
        result = "file" 
    return result
    

def getFileUUID():
    fileID = str(uuid.uuid4())
    fileID = fileID.replace("-","")
    return fileID
    
    
def modifyFileName(fileName):
    try:
        newFileName = getFileUUID()
        dirName,baseName = os.path.split(fileName)
        newFileName = os.path.join(dirName, newFileName)
        if _DEBUG:
            _LOG.info(f"DEBUG: modifyFileName, fileName:{fileName}, newFileName:{newFileName}")
        os.rename(fileName, newFileName)
        if os.path.isfile(newFileName):
            result = newFileName
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, delTempFile"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return result


def delTempFile(fileName):
    result = False
    try:
        if os.path.exists(fileName):
            os.remove(fileName)
            result = True
                            
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, delTempFile"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        
    return result


def create_dir(dirName):
    if not os.path.exists(dirName):
        os.mkdir(dirName)


def unique_file_name(filePath):
    result = filePath
    count = 10
    while count > 0:
        count -= 1
        if not os.path.exists(result):
            break
        else:
            dirName, fileExt = os.path.splitext(result)
            result = dirName + "_" + str(random.randint(0,1000000)) + fileExt
    return result


#upload file to fastdfs system
#upload file fail, error no: 13, error info: Permission denied
#group1/M00/00/00/rBpay118VmmALvO7AAK9qX4Hokg5056897
def upload2fdfs(fileName):
    result = ""
    try:
        cmdLine = settings.FASTDFS_CMD_PATH + "/fdfs_upload_file "+settings.FASTDFS_CLIENT_CONF_PATH+" "+fileName
        p = subprocess.Popen(cmdLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)  
        outputString=p.stdout.readlines()
        text = outputString[0]
        if sys.version_info.major <= 2:
            if isinstance(text, unicode):
                text = text.encode("UTF-8")       
        else:
            if isinstance(text, bytes):
                text = text.decode()
        if (text.find("group") == 0):
            result = text.strip("\n")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, upload2fdfs"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")

    return result


def genFdfsURL(fileName):
    result = FASTDFS_SERVER_PATH + fileName
    return result


def genLocalURL(fileName):
    baseLen = len(LOCAL_FILE_SERVER_BASE)
    fileName = fileName[baseLen:]
    result = LOCAL_FILE_SERVER_PATH + fileName
    if _DEBUG:
        _LOG.info(f"DEBUG: genLocalURL, fileName:{fileName},baseLen:{baseLen}, result:{result}")
    return result


#把文件ID转成临时url
def getTempLocation(fileID, privateFlag = True,localAccess = False,localAddress = False,targetFileName="",sourceServerAddr=""):
    result = fileID
    if fileID:
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
                    if targetFileName:
                        outfilePath = os.path.join(outfileDir,  targetFileName)
                    else:
                        outfilePath = os.path.join(outfileDir,  fileID)
                    outfilePath = pathlib.Path(outfilePath).as_posix()
                    if os.path.exists(outfilePath) == False:
                        try:
                            #download file from oss
                            rtn = OSS.downloadFile(fileID, outfilePath)
                            if rtn == False:
                                _LOG.warning(f"DEBUG: getTempLocation, FILE_SYSTEM_MODE:[{FILE_SYSTEM_MODE}] fileID:[{fileID}] result:[{rtn}]")                        
                        except Exception as e:
                            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, {outfilePath}"
                            _LOG.error(f"getTempLocation,{errMsg}, {traceback.format_exc()}")
                    if localAddress:
                        result = outfilePath
                    else:
                        if sourceServerAddr:
                            result = sourceServerAddr + "/" + LOCAL_FILE_SERVER_DIR_NAME + "/" + LOCAL_FILE_TEMP_WEB_DIR + midDir  + fileID
                        else:
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
                    if targetFileName:
                        outfilePath = os.path.join(outfileDir,  targetFileName)
                    else:
                        outfilePath = os.path.join(outfileDir,  fileID)
                    outfilePath = pathlib.Path(outfilePath).as_posix()
                    if os.path.exists(outfilePath) == False:
                        try:
                        #download file from oss
                            rtn = COS.downloadFile(fileID, outfilePath, privateFlag=privateFlag)
                            if rtn == False:
                                _LOG.warning("DEBUG: {0} FILE_SYSTEM_MODE:[{1}] fileID:[{2}] result:[{3}]".format("getTempLocation", FILE_SYSTEM_MODE, fileID, rtn))                        
                        except Exception as e:
                            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, {outfilePath}"
                            _LOG.error(f"getTempLocation,{errMsg}, {traceback.format_exc()}")

                    if localAddress:
                        result = outfilePath
                    else:
                        if sourceServerAddr:
                            result = sourceServerAddr + "/" + LOCAL_FILE_SERVER_DIR_NAME + "/" + LOCAL_FILE_TEMP_WEB_DIR + midDir  + fileID
                        else:
                            result = LOCAL_FILE_SERVER_PATH + LOCAL_FILE_TEMP_WEB_DIR + midDir  + fileID

        elif FILE_SYSTEM_MODE == "SELFFILE":
            if fileID[0:4] != "http":
                # result = selfFileMove2TempLocation(fileID)
                result = comSelf.genFileTempUrl(fileID,localAddress=localAddress,targetFileName=targetFileName,sourceServerAddr=sourceServerAddr)
        else:
            pass

    if _DEBUG:
        pass
        # _LOG.info(f"DEBUG: getTempLocation, FILE_SYSTEM_MODE:[{FILE_SYSTEM_MODE}] fileID:[{fileID}] result:[{result}]")
    
    return result


#文件特殊处理 begin    
def analysisPhoto(fileType, fileName, fileSize):
    result = {}
    result["fileName"] = fileName
    result["oldFileName"] = fileName
    #fileUrl = genLocalURL(fileName)
    
    fileType = fileType.lower()
    
    if fileType in ["photoidfront"]: 
        pass
    elif fileType in ["photoidback"]: 
        pass
    else:
        pass    
    return result


#生成resizePicFile缩略图
def resizePicFile(inputFileName,  outputFileName, fileExtName,  picSize = thumbnailSize):
    result = True
    try:
        #不生成了, ffmpeg比较麻烦
        if False:
            #生成mov文件第一帧缩略图
            if fileExtName in movFileExtList:
                tempFileName = inputFileName + "_temp.jpg"
                ff = ffmpy.FFmpeg(
                    inputs = {inputFileName:None}, 
                    #outputs = {tempFileName:['-ss', '00:00:00.000', '-vframes', '1',  '-y']}
                    outputs = {tempFileName:['-ss', '00:00:00.000', '-vframes', '1', '-y']}
                )
                oldExtName = fileExtName
                try:
                    ff.run()

                    if _DEBUG:
                        _LOG.info(f"DEBUG resizePicFile: {inputFileName}, {tempFileName}, {outputFileName}")

                    #变成图片文件
                    inputFileName = tempFileName
                    fileExtName = ".jpg"
                except Exception as e:
                    fileExtName = oldExtName
                    if _DEBUG:
                        _LOG.warning(f"DEBUG resizePicFile: {e},{inputFileName}, {tempFileName}, {outputFileName}")

        #压缩图片文件
        if fileExtName in photoFileExtList:
            im = Image.open(inputFileName)
            im.thumbnail(picSize)
            im = im.convert("RGB")
            im.save(outputFileName, "JPEG")
            im.close()
            
        else:
            #其他生成一个带扩展名的图片
            #shutil .copyfile(inputFileName, outputFileName)
            fontPath = "CrimsonText-Bold-2.ttf"
            width,  height  = 128, 128
            im = Image.new('RGB', (width, height), "white")
            draw = ImageDraw.Draw(im)
            #按宽度比例显示文字
            maxChars = 5
            text = fileExtName[0:maxChars]
            textLen = len(text)
            fontSize = 48
            leftShiftLen = (maxChars-textLen) * 10
            leftPointLeft = 8 + leftShiftLen
            leftPointTop = 24
            leftPoint = (leftPointLeft, leftPointTop)
            font = ImageFont.truetype(fontPath, fontSize)
            # 浅色字体
            draw.text(leftPoint, text, (128, 128, 128),font=font)
            im.save(outputFileName, "JPEG")
            im.close()
            
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, resizePicFile"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        result = False
    
    return result

#文件特殊处理 end

    
#把多个图片文件合并为一个单一文件
def multiImageMerge(fileNameList,  outputFileName):
    result = ""
    try:
        spacePixels = 6

        imCount = 0
        #读取图片, 并计算大小
        maxWidth = 0
        maxHeight = 0
        for fileName in fileNameList:
            imCount += 1
            im = Image.open(fileName)
            imSize = im.size
            if maxWidth < imSize[0]:
                maxWidth = imSize[0]
            if maxHeight < imSize[1]:
                maxHeight = imSize[1]
            im.close()
        
        maxWidth += spacePixels
        maxHeight += spacePixels * imCount
        
        #建立一个新的图片文件
        newImg = Image.new("RGB",  (maxWidth,  maxHeight * imCount))
        imCount = 0
        for fileName in fileNameList:
            im = Image.open(fileName)
            newImg.paste(im,  (int(spacePixels/2),  (maxHeight + int (spacePixels/2)) * imCount ))
            imCount += 1
        newImg.save(outputFileName)
        newImg.close()
        result = outputFileName
        
    except:
        pass
    return result


#nginx upload module 回调处理
def fileHandler(dataSet):
    result = {}
    errCode = "B0"
    rtnCMD = "UP"+errCode
    rtnField = ""
    lang = dataSet.get("lang", "CN")
    msgKey = "default"

    debugStep = 0
    if _DEBUG:
        _LOG.info(f"DEBUG:PID:{_processorPID}, {debugStep}, fileHandler: {dataSet}")
        
    try:
        filePath = ""
        fileSize = 0
        fileType = dataSet.get("description", [""])[0].strip()
        if "image.content_type" in dataSet:
            contentType = dataSet["image.content_type"][0]
        if "image.name" in dataSet:
            fileName = dataSet["image.name"][0]
        if "image.path" in dataSet:
            filePath = dataSet["image.path"][0]
        
            if _DEBUG:
                imgPath = dataSet["image.path"]
                debugStep = 1
                _LOG.info(f"DEBUG:PID:{_processorPID}, {debugStep}, image.path: {imgPath},{filePath}")
            
        if "image.md5" in dataSet:
            fileMd5 = dataSet["image.md5"][0]
        if "image.size" in dataSet:
            try:
                fileSize = int(dataSet["image.size"][0])
            except:
                fileSize = 0

        if "file.content_type" in dataSet:
            contentType = dataSet["file.content_type"][0]
        if "file.name" in dataSet:
            fileName = dataSet["file.name"][0]
        if "file.path" in dataSet:
            filePath = dataSet["file.path"][0]
        if "file.md5" in dataSet:
            fileMd5 = dataSet["file.md5"][0]
        if "file.size" in dataSet:
            try:
                fileSize = int(dataSet["file.size"][0])
            except:
                fileSize = 0
        
        #如果用户不是用file,image,例如用avatar, 会出现 avatar.size,avatar.filePath 
        if fileSize <= 0 or filePath == "":
            for k, v in dataSet.items():
                pos = k.find(".")
                if pos >= 0:
                    aList = k.split(".")
                    if len(aList) >= 1:
                        fieldName = aList[1]
                        if fieldName == "path":
                            filePath = v[0]
                        elif fieldName == "content_type":
                            contentType = v[0]
                        elif fieldName == "name":
                            fileName = v[0]
                        elif fieldName == "md5":
                            fileMd5 = v[0]
                        elif fieldName == "size":
                            fileSize = v[0]
        try:
            fileSize = int(fileSize)
        except:
            fileSize = 0

        baseName,extName = os.path.splitext(fileName)
        if _DEBUG:
            debugStep = 2
            _LOG.info(f"DEBUG:PID:{_processorPID}, {debugStep}, {fileType},{fileName},{extName},{filePath},{fileMd5},{fileSize}")

        #过滤文件类型
        if settings.ALLOW_FILE_TYPE_LIST:
            if extName not in settings.ALLOW_FILE_TYPE_LIST:
                _LOG.warning(f"DEBUG: file format error:{fileName}")
                errCode = "BL"
                if lang == "CN":
                    rtnField = "文件格式/类型错误, 不许上传"
                else:
                    rtnField = "File format/type error, upload not allowed."
        
        if fileSize != 0 and filePath != "" and errCode == "B0":
            #满足需要的文件, 去处理相应的数据. 
#            indexID = getFileUUID()
            if fileType == "":
                fileType = getDefaultFileType(fileName)
                
            fileSet = analysisPhoto(fileType, filePath,  fileSize)
            newFileName = fileSet.get("fileName")
            newFileSize = os.path.getsize(newFileName)

            #修改文件名字
            newFileName = modifyFileName(newFileName)
            
            if FILE_SYSTEM_MODE == "FASTDFS":
                localFileID = upload2fdfs(newFileName)
                if localFileID != "":
                    matchDir = LOCAL_FILE_SERVER_BASE
                    if filePath.find(matchDir) == 0:
                        delTempFile(filePath) #删除临时文件
                    if newFileName.find(matchDir) == 0:
                        delTempFile(newFileName) #删除临时文件
                    fileUrl = genFdfsURL(localFileID) #生成文件URL
            elif FILE_SYSTEM_MODE == "ALIOSS":
                localFileID = newFileName
                fileUrl = genLocalURL(newFileName)
            elif FILE_SYSTEM_MODE == "TENCENT":
                localFileID = newFileName
                fileUrl = genLocalURL(newFileName)
            elif FILE_SYSTEM_MODE == "NGINX":
                localFileID = newFileName
                fileUrl = genLocalURL(newFileName)
            elif FILE_SYSTEM_MODE == "SELFFILE":
                localFileID = newFileName
                fileUrl = genLocalURL(newFileName)
            else:
                localFileID = newFileName
                fileUrl = genLocalURL(newFileName)
                if _DEBUG:
                    debugStep = 3
                    _LOG.info(f"DEBUG:PID:{_processorPID}, {debugStep}, fileUrl:{fileUrl}  localFileID:{localFileID}")
                
            if localFileID != "":
                #保存文件
                saveSet = {}                       
                saveSet["serverName"] = _SYS_SERVER_NAME
                saveSet["fileSystem"] = FILE_SYSTEM_MODE
                saveSet["description"] = fileType
                saveSet["fileName"] = newFileName
                saveSet["oldFileName"] = fileName
                saveSet["fileUrl"] = fileUrl
                saveSet["fileSize"] = newFileSize

                saveSet["uploadYMDHMS"] = misc.getTime()
                if _DEBUG:
                    debugStep = 4
                    _LOG.info(f"DEBUG:PID:{_processorPID}, {debugStep}, fileUrl:{fileUrl}, saveSet:{saveSet}")
                
                rtn = comDB.setFileInfo(fileUrl, saveSet) #把文件ID保存在数据库内, 等客户端上传之后,删除对应ID, 剩余的定时清理,都是没有使用的. 

                if _DEBUG:
                    debugStep = 5
                    _LOG.info(f"DEBUG:PID:{_processorPID}, {debugStep},  rtn:{rtn}")

                result = saveSet
                result.pop("serverName")
                result.pop("fileSystem")
                
            else:
                errCode = "BL"
            
        else:
            #unnecessary file, delete it
            errCode = "BL"
    
        rtnCMD = "UP" + errCode
        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        result["errCode"] = errCode
        result["MSG"] = rtnSet["MSG"]

        if errCode != "B0":
            result["fileUrl"] = ""

        if _DEBUG:
            debugStep = 6
            _LOG.info(f"DEBUG:{_processorPID}, {debugStep},  result:{result}")

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, fileHander"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        errCode = "BL"
        
    return result


'''
上传到永久存储
把文件从临时存储转到永久存储

  *阿里云/腾讯云是上传 OSS/COS 对象存储, 
  *NGINX/SELFFILE是本地处理
  *FASTDFS是本地云处理
  *缩略图类型不删除原始数据

正常接收到的请求数据:

{"serverName":"amom_aliyun_01","fileSystem":"ALIOSS","description":"pic",
"fileName":"/data/webserver/temp/0/0055395770","oldFileName":"2022-2.png",
"fileUrl":"https://www.amom.org.cn/temp/0/0055395770","fileSize":299208,
"uploadYMDHMS":"20230627163451","CMD":"F0A0",
"objectName":"FILE_20230627163727_2022-2.png",
"requestType":"","prefix":"","delFlag":"N",
"privateFlag":true,"compressFlag":"Y","YMDHMS":"20230627163808",
"token":"3be9b907a8c9f4e56356e285c358a327"}

返回数据:

{'fileUrl': 'FILE_20230627163727_2022-2.png', 'CMD': 'F0B0', 
'errCode': 'B0', 'MSG': {'errCode': 'B0', 'content': 'success.'}, 'YMDHMS': '20230627164039'}

'''
def cmdF0A0(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD[0:2]+errCode
    rtnField = ""
    rtnData = {}
    lang = dataSet.get("lang", "EN")
    msgKey = "default"
    
    try:
        
        token = dataSet.get("token")
        YMDHMS = dataSet.get("YMDHMS")
        genToken = comFC.genDigest(settings.GEN_DIGIST_KEY, CMD, YMDHMS)   
        fileID = dataSet.get("fileID")

        if genToken == token and fileID: #权限判断
            #首先获取文件信息
            fileInfoData = comDB.getFileInfo(fileID)

            fileSystem = fileInfoData.get("fileSystem", "")
            if not fileSystem:
                fileSystem = FILE_SYSTEM_MODE

            delFlag = fileInfoData.get("delFlag", comGD._CONST_NO)

            requestType = dataSet.get("requestType", "")
            prefix = dataSet.get("prefix", "")
            if prefix != "":
                prefix += "_" #文件前缀
            privateFlag = dataSet.get("privateFlag", False) #私有文件标志, 默认是非
            # compressFlag = dataSet.get("compressFlag", comGD._CONST_YES) #压缩文件标志, 默认是"Y"
            compressFlag = dataSet.get("compressFlag", comGD._CONST_NO) #压缩文件标志, 默认是"N"
            
            if fileSystem == "ALIOSS":
                fileName = fileInfoData.get("fileName", "") #完整路径
                fileRoot,  fileExt = os.path.splitext(fileName) #分离扩展名
                oldFileName = fileInfoData.get("oldFileName", "")
                tempDir, tempExt = os.path.splitext(oldFileName)
                tempExt = tempExt.lower()
                if fileExt == "":
                    fileExt = tempExt #如果nginx存储的文件没有扩展名, 用原来名字的扩展名

                if fileName:
                    try:
                        objectName = dataSet.get("objectName")
                        fileSize = fileInfoData.get("fileSize")
                        picSize = (0,0)
                        description = fileInfoData.get("description", "")
                        if requestType == comGD._DEF_FILE_REQUEST_TYPE_THUMBNAIL:
                            matchDir = LOCAL_FILE_SERVER_BASE
                            thumbnailFileName = fileRoot + "_" + requestType
                            picSize = thumbnailSize
                            if resizePicFile(fileName, thumbnailFileName, fileExt, picSize):
                                fileName = thumbnailFileName
                                #获取文件信息, 例如大小
                                fileStatInfo = os.stat(fileName)
                                fileSize = fileStatInfo.st_size
                                fileExt = ".jpg"
                            objectName = dataSet.get("objectName")
                            if objectName:
                                objRoot,  objExt = os.path.splitext(objectName) #分离扩展名
                                objectName = objRoot + "_" + requestType + fileExt
                            else:
                               objectName = prefix + description + "_" + misc.getTime() + "_" + str(uuid.uuid4()).replace("-","") +"_" + requestType + fileExt
                        else:                            
                            if maxPicEnable and compressFlag == comGD._CONST_YES and fileExt in photoFileExtList:
                                newFileName = fileRoot + "_" + requestType
                                picSize = maxPicSize
                                if resizePicFile(fileName, newFileName, fileExt, picSize):
                                    fileName = newFileName
                                    #获取文件信息, 例如大小
                                    fileStatInfo = os.stat(fileName)
                                    fileSize = fileStatInfo.st_size
                                    fileExt = ".jpg"
                            objectName = dataSet.get("objectName")
                            if objectName == None:
                                objectName = prefix + description + "_" + misc.getTime() + "_" + str(uuid.uuid4()).replace("-","") + fileExt
                            matchDir = LOCAL_FILE_SERVER_BASE
                                    
                        if OSS.uploadFile(objectName, fileName):
                            matchDir = LOCAL_FILE_SERVER_BASE
                            if fileName.find(matchDir) == 0:
                                if delFlag == comGD._CONST_YES:
                                    delTempFile(fileName)
                            rtnData["fileUrl"] = objectName
                        else:
                            _LOG.warning(f"cmdF0A0: fileSystem:{fileSystem},requestType:{requestType},picSize:{picSize},description:{description},prefix:{prefix},fileName:{fileName},objectName:{objectName}")
                                                        
                        if _DEBUG:
                            _LOG.info(f"cmdF0A0: {fileSystem},{requestType},{picSize},{description},{prefix},{fileName},{objectName}")
                    except:
                        pass
                pass
                
            elif fileSystem == "TENCENT":
                fileName = fileInfoData.get("fileName", "") #完整路径
                fileRoot,  fileExt = os.path.splitext(fileName) #分离扩展名
                oldFileName = fileInfoData.get("oldFileName", "")
                tempDir, tempExt = os.path.splitext(oldFileName)
                tempExt = tempExt.lower()
                if fileExt == "":
                    fileExt = tempExt #如果nginx存储的文件没有扩展名, 用原来名字的扩展名

                if fileName:
                    try:
                        objectName = dataSet.get("objectName")
                        fileSize = fileInfoData.get("fileSize")
                        picSize = (0,0)
                        description = fileInfoData.get("description", "")
                        if requestType == comGD._DEF_FILE_REQUEST_TYPE_THUMBNAIL:
                            matchDir = LOCAL_FILE_SERVER_BASE
                            thumbnailFileName = fileRoot + "_" + requestType
                            picSize = thumbnailSize
                            if resizePicFile(fileName, thumbnailFileName, fileExt, picSize):
                                fileName = thumbnailFileName
                                #获取文件信息, 例如大小
                                fileStatInfo = os.stat(fileName)
                                fileSize = fileStatInfo.st_size
                                fileExt = ".jpg"
                            objectName = dataSet.get("objectName")
                            if objectName:
                                objRoot,  objExt = os.path.splitext(objectName) #分离扩展名
                                objectName = objRoot + "_" + requestType + fileExt
                            else:
                                objectName = prefix + description + "_" + misc.getTime() + "_" + str(uuid.uuid4()).replace("-","") +"_" + requestType + fileExt
                        else:
                            if maxPicEnable and compressFlag == comGD._CONST_YES and fileExt in photoFileExtList:
                                newFileName = fileRoot + "_" + requestType
                                picSize = maxPicSize
                                if resizePicFile(fileName, newFileName, fileExt, picSize):
                                    fileName = newFileName
                                    #获取文件信息, 例如大小
                                    fileStatInfo = os.stat(fileName)
                                    fileSize = fileStatInfo.st_size
                                    fileExt = ".jpg"           
                            objectName = dataSet.get("objectName")
                            if objectName == None:
                                objectName = prefix + description + "_" + misc.getTime() + "_" + str(uuid.uuid4()).replace("-","") + fileExt
                            matchDir = LOCAL_FILE_SERVER_BASE

                        if COS.uploadFile(objectName, fileName, privateFlag = privateFlag):
                            matchDir = LOCAL_FILE_SERVER_BASE
                            if fileName.find(matchDir) == 0:
                                if delFlag == comGD._CONST_YES:
                                    delTempFile(fileName)
                            rtnData["fileUrl"] = objectName
                        else:
                            _LOG.warning(f"cmdF0A0: {fileSystem},{requestType},{picSize},{description},{prefix},{fileName},{objectName}")
                                                        
                        if _DEBUG:
                            _LOG.info(f"cmdF0A0: {fileSystem},{requestType},{picSize},{description},{prefix},{fileName},{objectName}")
                    except:
                        pass
                pass

            elif fileSystem == "SELFFILE":
                #本地存储的文件, 不需要处理文件名, 只需要处理是否是缩略图
                fileName = fileInfoData.get("fileName", "") ##完整路径
                fileRoot,  fileExt = os.path.splitext(fileName) #分离扩展名
                oldFileName = fileInfoData.get("oldFileName", "")
                tempDir, tempExt = os.path.splitext(oldFileName)
                tempExt = tempExt.lower()
                if fileExt == "":
                    fileExt = tempExt #如果nginx存储的文件没有扩展名, 用原来名字的扩展名

                if fileName:
                    try:
                        objectName = dataSet.get("objectName")
                        fileSize = fileInfoData.get("fileSize")
                        picSize = (0,0)
                        description = fileInfoData.get("description", "")
                        if requestType == comGD._DEF_FILE_REQUEST_TYPE_THUMBNAIL:
                            matchDir = LOCAL_FILE_SERVER_BASE
                            thumbnailFileName = fileRoot + "_" + requestType
                            picSize = thumbnailSize
                            if resizePicFile(fileName, thumbnailFileName, fileExt, picSize):
                                fileName = thumbnailFileName
                                #获取文件信息, 例如大小
                                fileStatInfo = os.stat(fileName)
                                fileSize = fileStatInfo.st_size
                            fileExt = ".jpg"
                            # objectName = dataSet.get("objectName")
                            if objectName:
                                objRoot,  objExt = os.path.splitext(objectName) #分离扩展名
                                objectName = objRoot + "_" + requestType + fileExt
                            else:
                               objectName = prefix + description + "_" + misc.getTime() + "_" + str(uuid.uuid4()).replace("-","") +"_" + requestType + fileExt
                        else:                            
                            if maxPicEnable and compressFlag == comGD._CONST_YES and fileExt in photoFileExtList:
                                newFileName = fileRoot + "_" + requestType
                                picSize = maxPicSize
                                if resizePicFile(fileName, newFileName, fileExt, picSize):
                                    fileName = newFileName
                                fileExt = ".jpg"
                            if not objectName:
                                if oldFileName:
                                    objectName = oldFileName
                                else:
                                    objectName = prefix + description + "_" + misc.getTime() + "_" + str(uuid.uuid4()).replace("-","") + fileExt
                            # matchDir = LOCAL_FILE_SERVER_BASE
                        
                        #构建本地文件的主要信息
                        fileInfo = {}
                        fileInfo["serverName"] = dataSet.get("serverName")
                        fileInfo["fileSystem"] = fileInfoData.get("fileSystem")
                        fileInfo["description"] = fileInfoData.get("description")
                        fileInfo["fileName"] = fileName
                        fileInfo["oldFileName"] = oldFileName
                        fileInfo["objectName"] = objectName
                        fileInfo["fileExtName"] = fileExt
                        fileInfo["fileSize"] = fileSize
                        fileInfo["fileUrl"] = fileInfoData.get("fileUrl")
                        fileInfo["uploadYMDHMS"] = fileInfoData.get("uploadYMDHMS")
                        fileInfo["requestType"] = dataSet.get("requestType")
                        fileInfo["prefix"] = dataSet.get("prefix")
                        fileInfo["compressFlag"] = dataSet.get("compressFlag")

                        #保存文件到本地
                        # fileID = upload2selfFile(fileInfo)
                        fileID = comSelf.uploadFile(fileInfo)
                        if fileID:
                            delFlag = fileInfoData.get("delFlag")
                            if delFlag == comGD._CONST_YES:
                                delTempFile(fileName)
                            rtnData["fileUrl"] = fileID
                        else:
                            _LOG.warning(f"cmdF0A0: fileSystem:{fileSystem},requestType:{requestType},picSize:{picSize},description:{description},prefix:{prefix},fileName:{fileName},objectName:{objectName}")
                                                        
                        if _DEBUG:
                            _LOG.info(f"cmdF0A0: {fileSystem},{requestType},{picSize},{description},{prefix},{fileName},{objectName}")
                    except:
                        pass
                pass
                
            elif fileSystem == "NGINX":
                fileName = fileInfoData.get("fileName", "")
                fileRoot,  fileExt = os.path.splitext(fileName)
                oldFileName = fileInfoData.get("oldFileName", "")
                tempDir, tempExt = os.path.splitext(oldFileName)
                tempExt = tempExt.lower()
                if fileExt == "":
                    fileExt = tempExt #如果nginx存储的文件没有扩展名, 用原来名字的扩展名

                if fileName:
                    try:
                        picSize = (0,0)
                        description = fileInfoData.get("description", "")
                        if requestType == comGD._DEF_FILE_REQUEST_TYPE_THUMBNAIL:
                            thumbnailFileName = fileRoot + "_" + requestType
                            picSize = thumbnailSize
                            if resizePicFile(fileName, thumbnailFileName, fileExt, picSize):
                                fileUrl = genLocalURL(thumbnailFileName)
                                fileExt = ".jpg"
                                rtnData["fileUrl"] = fileUrl
                            else:
                                rtnData["fileUrl"]  = fileInfoData["fileUrl"]
                        else:
                            if maxPicEnable and compressFlag == comGD._CONST_YES and fileExt in photoFileExtList:
                                newFileName = fileRoot + "_" + requestType
                                if resizePicFile(fileName, newFileName, fileExt, maxPicSize):
                                    fileUrl = genLocalURL(newFileName)
                                    rtnData["fileUrl"] = fileUrl
                                    fileExt = ".jpg"
                                else:
                                    rtnData["fileUrl"]  = fileInfoData["fileUrl"]
                                                        
                        if _DEBUG:
                            _LOG.info(f"DEBUG cmdF0A0: {fileSystem},{requestType},{picSize},{description},{fileName},fileUrl:{objectName}")
                    except:
                        pass
                pass
                   
            else:
                rtnData["fileUrl"]  = fileInfoData["fileUrl"]
             
            result = rtnData 
        
        else:
            errCode = "ERR_TOKEN"
        
        rtnCMD = CMD[0:2]+errCode
        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        result["CMD"] = rtnCMD
        result["errCode"] = errCode
        result["MSG"] = rtnSet["MSG"]
        
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, {CMD}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL", "")
        result = rtnSet
       
    return result    
    
    
#删除临时文件
def cmdF1A0(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD[0:2]+errCode
    rtnField = ""
    rtnData = {}
    lang = dataSet.get("lang", "EN")
    msgKey = "default"

    try:
        
        token = dataSet.get("token")
        YMDHMS = dataSet.get("YMDHMS")
        genToken = comFC.genDigest(settings.GEN_DIGIST_KEY, CMD, YMDHMS)   
        fileID = dataSet.get("fileID")

        if genToken == token and fileID: #权限判断
            #首先获取文件信息
            fileInfoData = comDB.getFileInfo(fileID)

            fileSystem = fileInfoData.get("fileSystem", "")
            #privateFlag = dataSet.get("privateFlag", False) #私有文件标志, 默认是非
            
            if fileSystem in ["ALIOSS","TENCENT","SELFFILE"]:
                fileName = fileInfoData.get("fileName", "")
                if fileName:
                    try:
                        matchDir = LOCAL_FILE_SERVER_BASE
                        if fileName.find(matchDir) == 0:
                            delTempFile(fileName)
                                                        
                        if _DEBUG:
                            _LOG.info(f"DEBUG cmdF1A0: {CMD}, {fileName}")
                    except:
                        pass
                pass
            else:
                #old system
                fileUrl = fileInfoData["fileUrl"]
                matchStr = 'http://app.iottest.online:8080/group1/M00/'
                matchLen = len(matchStr)
                if fileUrl.find(matchStr) >= 0:
                    fastdfsdir = "/data/fastdfs/data/"
                    fileName = fastdfsdir + fileUrl[matchLen:]
                    if os.path.exists(fileName):
                        try:
                            os.remove(fileName)
                        except Exception as e:
                            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, {CMD}"
                            _LOG.error(f"{errMsg}, {traceback.format_exc()}")
                            data = str(CMD)
                            
                        rtnData["fileName"] = fileName
             
            result = rtnData 
            
        else:
            errCode = "ERR_TOKEN"
            
        rtnCMD = CMD[0:2]+errCode
        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        result["CMD"] = rtnCMD
        result["errCode"] = errCode
        result["MSG"] = rtnSet["MSG"]

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, {CMD}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL", "")
        result = rtnSet
       
    return result    
    

#删除长期文件
def cmdF2A0(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD[0:2]+errCode
    rtnField = ""
    rtnData = {}
    lang = dataSet.get("lang", "EN")
    msgKey = "default"

    try:
        
        token = dataSet.get("token")
        YMDHMS = dataSet.get("YMDHMS")
        genToken = comFC.genDigest(settings.GEN_DIGIST_KEY, CMD, YMDHMS)   
        fileID = dataSet.get("fileID")

        if genToken == token and fileID: #权限判断
 
            fileSystem = dataSet.get("fileSystem", "")
            privateFlag = dataSet.get("privateFlag", False) #私有文件标志, 默认是非
            
            if fileSystem == "ALIOSS":
                rtn = OSS.deleteFile(fileID)
                if rtn == False:
                    _LOG.warning(f"cmdF2A0: {fileID}")
                else: 
                    if _DEBUG:
                        _LOG.info(f"cmdF2A0: {fileID}")

            elif fileSystem == "TENCENT":
                rtn = COS.deleteFile(fileID, privateFlag = privateFlag)
                if rtn == False:
                    _LOG.warning(f"cmdF2A0: {fileID}")
                else: 
                    if _DEBUG:
                        _LOG.info(f"cmdF2A0: {fileID}")

            elif fileSystem == "SELFFILE":
                # rtn = selfFileDeleteFile(fileID)
                rtn = comSelf.deleteFile(fileID)
                if rtn == False:
                    _LOG.warning(f"cmdF2A0: {fileID}")
                else: 
                    if _DEBUG:
                        _LOG.info(f"cmdF2A0: {fileID}")
            else:
                pass
             
            result = rtnData 
        
        else:
            errCode = "ERR_TOKEN"
        
        rtnCMD = CMD[0:2]+errCode
        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        result["CMD"] = rtnCMD
        result["errCode"] = errCode
        result["MSG"] = rtnSet["MSG"]

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, {CMD}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL", "")
        result = rtnSet
       
    return result    


#合并多个图片文件
def cmdF4A0(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD[0:2]+errCode
    rtnField = ""
    rtnData = {}
    lang = dataSet.get("lang", "EN")
    msgKey = "default"

    try:
        
        token = dataSet.get("token")
        YMDHMS = dataSet.get("YMDHMS")
        genToken = comFC.genDigest(settings.GEN_DIGIST_KEY, CMD, YMDHMS)   

        if genToken == token: #权限判断
            fileInfoList = dataSet.get("fileInfoList", [])
            if len(fileInfoList ) > 0:
                if _DEBUG:
                    _LOG.info(f"DEBUG cmdF4A0: fileInfoList:{fileInfoList}")
                fileNameList = []
                for fileInfo in fileInfoList:
                    fileName = fileInfo.get("fileName")
                    fileNameList.append(fileName)
                if _DEBUG:
                    _LOG.info(f"DEBUGcmdF4A0: fileNameList:{fileNameList}")
                tempLocalFileName = comFC.genTempFileName(".jpg")
                localFileName = multiImageMerge(fileNameList,  tempLocalFileName)
                if localFileName == tempLocalFileName:
                    fileUrl = genLocalURL(localFileName)
                    fileSize = os.path.getsize(localFileName)
                    #保存文件
                    saveSet = {}                       
                    saveSet["serverName"] = _SYS_SERVER_NAME
                    saveSet["fileSystem"] = FILE_SYSTEM_MODE
                    saveSet["description"] = "mergerImage"
                    saveSet["fileName"] = localFileName
                    saveSet["oldFileName"] = fileName
                    saveSet["fileUrl"] = fileUrl
                    saveSet["fileSize"] = fileSize

                    saveSet["uploadYMDHMS"] = misc.getTime()
                    if _DEBUG:
                        _LOG.info(f"DEBUG F4A0,fileUrl:{fileUrl}, saveSet:{saveSet}")
                    
                    #
                    comDB.setFileInfo(fileUrl, saveSet) #把文件ID保存在数据库内, 等客户端上传之后,删除对应ID, 剩余的定时清理,都是没有使用的. 
                
                    rtnData = saveSet
                    rtnData.pop("serverName")
                    rtnData.pop("fileSystem")
                    
                else:
                    _LOG.warning("cmdF4A0: localFileName:{localFileName}")
                    
            result = rtnData 
        
        else:
            errCode = "ERR_TOKEN"
        
        rtnCMD = CMD[0:2]+errCode
        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        result["CMD"] = rtnCMD
        result["errCode"] = errCode
        result["MSG"] = rtnSet["MSG"]

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, {CMD}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL", "")
        result = rtnSet
       
    return result    


#获取临时文件信息
def cmdF6A0(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD[0:2]+errCode
    rtnField = ""
    rtnData = {}
    lang = dataSet.get("lang", "EN")
    msgKey = "default"

    try:
        
        token = dataSet.get("token")
        YMDHMS = dataSet.get("YMDHMS")
        genToken = comFC.genDigest(settings.GEN_DIGIST_KEY, CMD, YMDHMS)   
        fileID = dataSet.get("fileID")

        if genToken == token and fileID: #权限判断
            #首先获取文件信息
            fileInfoData = comDB.getFileInfo(fileID)
            if fileInfoData:
                oldFileName = fileInfoData.get("oldFileName")
                dirName, extName = os.path.splitext(oldFileName)
                if extName:
                    fileInfoData["fileExtName"] = extName
                rtnData = fileInfoData
                # rtnData["data"] = fileInfoData
            else:
                _LOG.warning("cmdF6A0: localFileName:{localFileName}")
                    
            result = rtnData 
        
        else:
            errCode = "ERR_TOKEN"
        
        rtnCMD = CMD[0:2]+errCode
        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        result["CMD"] = rtnCMD
        result["errCode"] = errCode
        result["MSG"] = rtnSet["MSG"]

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, {CMD}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL", "")
        result = rtnSet
       
    return result    


#把文件转存到本地临时目录, 并生成Url
def cmdF7A0(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD[0:2]+errCode
    rtnField = ""
    rtnData = {}
    lang = dataSet.get("lang", "EN")
    msgKey = "default"

    try:
        
        token = dataSet.get("token")
        YMDHMS = dataSet.get("YMDHMS")
        genToken = comFC.genDigest(settings.GEN_DIGIST_KEY, CMD, YMDHMS)   

        if genToken == token: #权限判断
            fileID = dataSet.get("fileID")
            if "privateFlag" not in dataSet:
                privateFlag = True
            else:
                privateFlag = dataSet.get("privateFlag")
            if "localAccess" not in dataSet:
                localAccess = True
            else:
                localAccess = dataSet.get("localAccess")
            if "localAddress" not in dataSet:
                localAddress = False
            else:
                localAddress = dataSet.get("localAddress")
            targetFileName = dataSet.get("targetFileName")
            sourceServerAddr = dataSet.get("sourceServerAddr")
            if fileID:
                fileUrl = getTempLocation(fileID, privateFlag = privateFlag,localAccess = localAccess,
                                          localAddress = localAddress, targetFileName=targetFileName,sourceServerAddr=sourceServerAddr)
                #判断扩展名
                if settings.ALLOW_FILE_TYPE_LIST:
                    aList = fileUrl.split(".")
                    extName = aList[-1]
                    extName = extName.lower()
                    extName = "." + extName
                    if extName not in settings.ALLOW_FILE_TYPE_LIST:
                        _LOG.warning(f"DEBUG: file format error:{fileUrl}")
                        errCode = "BL"
                        if lang == "CN":
                            rtnField = "文件格式/类型错误, 不能下载"
                        else:
                            rtnField = "File format/type error, download not allowed."
                        fileUrl = ""
                rtnData["fileUrl"] = fileUrl
                pass
                    
            result = rtnData 
        
        else:
            errCode = "ERR_TOKEN"
        
        rtnCMD = CMD[0:2]+errCode
        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        result["CMD"] = rtnCMD
        result["errCode"] = errCode
        result["MSG"] = rtnSet["MSG"]

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, {CMD}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL", "")
        result = rtnSet
       
    return result    


#获取永久文件信息
def cmdF8A0(CMD, dataSet, sessionIDSet):
    result = {}
    errCode = "B0"
    rtnCMD = CMD[0:2]+errCode
    rtnField = ""
    rtnData = {}
    lang = dataSet.get("lang", "EN")
    msgKey = "default"

    try:
        
        token = dataSet.get("token")
        YMDHMS = dataSet.get("YMDHMS")
        genToken = comFC.genDigest(settings.GEN_DIGIST_KEY, CMD, YMDHMS)   
        fileID = dataSet.get("fileID")

        if genToken == token and fileID: #权限判断
            fileSystem = dataSet.get("fileSystem", "")
            privateFlag = dataSet.get("privateFlag", False) #私有文件标志, 默认是非

            if fileSystem == "ALIOSS":
                if OSS.existFile(fileID):
                    fileInfo = OSS.getFileInfo(fileID)
                    rtnData = fileInfo
                    if _DEBUG:
                        _LOG.info(f"cmdF2A0: {fileInfo}")
                else:
                    _LOG.warning(f"cmdF8A0: {fileID}")

            elif fileSystem == "TENCENT":
                if COS.existFile(fileID):
                    fileInfo = OSS.getFileInfo(fileID)
                    rtnData = fileInfo
                    if _DEBUG:
                        _LOG.info(f"cmdF2A0: {fileInfo}")

            elif fileSystem == "SELFFILE":
                rtn = comSelf.getFileInfo(fileID)
                if rtn == False:
                    _LOG.warning(f"cmdF2A0: {fileID}")
                else: 
                    if _DEBUG:
                        _LOG.info(f"cmdF2A0: {fileID}")
            else:
                pass

            fileInfoData = comDB.getFileInfo(fileID)
            if fileInfoData:
                oldFileName = fileInfoData.get("oldFileName")
                dirName, extName = os.path.splitext(oldFileName)
                if extName:
                    fileInfoData["fileExtName"] = extName
                rtnData = fileInfoData
                # rtnData["data"] = fileInfoData
            else:
                _LOG.warning("cmdF6A0: localFileName:{localFileName}")
                
            result = rtnData 
        
        else:
            errCode = "ERR_TOKEN"
        
        rtnCMD = CMD[0:2]+errCode
        rtnSet = comFC.rtnMSG(errCode, rtnField, lang,msgKey)
        result["CMD"] = rtnCMD
        result["errCode"] = errCode
        result["MSG"] = rtnSet["MSG"]

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, {CMD}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        rtnSet = comFC.rtnMSG("ERROR", "ERR_GENERAL", "")
        result = rtnSet
       
    return result    


urlPathMap = {

    #把文件从临时存储转到永久存储
    "F0A0":cmdF0A0, 
    #删除临时文件
    "F1A0":cmdF1A0, 
    #删除长期文件
    "F2A0":cmdF2A0, 
    # #
    # "F3A0":cmdF3A0, 
    #合并多个图片文件
    "F4A0":cmdF4A0, 
    # #人脸识别
    # "F5A0":cmdF5A0, 
    #获取临时文件信息
    "F6A0":cmdF6A0, 
    #把文件转存到本地临时目录, 并生成Url
    "F7A0":cmdF7A0, 
    #获取永久文件信息
    "F8A0":cmdF8A0, 
}


def fileRequestServices(dataSet):
    CMD = "CAT"
    errCode = "OK"
    rtnField = ""
    sessionIDSet = dataSet.get("sessionIDSet", {})
    
    CMD = dataSet.get("CMD", "")
    
    rtnSet = {}
    if CMD in urlPathMap:
        rtnSet = urlPathMap[CMD](CMD, dataSet, sessionIDSet)
    else:
        rtnSet = comFC.rtnMSG("ERROR", "ERR_NOCMD", "")        
    rtnSet["YMDHMS"]  = misc.getTime()
    
    return rtnSet


@application.route("/hfile", methods=['POST'])
def flaskHFile():
    rtnSet = {}
    
    try:
        dataSet = {}
        if request.mimetype == "multipart/form-data":
            dataSet = request.form.to_dict(flat=False)
            if _DEBUG:
                _LOG.info(f"R: {request.mimetype},{misc.jsonDumps(dataSet)}")
                
            rtnSet = fileHandler(dataSet)
            
        elif  request.mimetype == "application/json":
            
            dataSet = request.json
            
            if _DEBUG:
                _LOG.info(f"R: {request.mimetype},{misc.jsonDumps(dataSet)}")

            rtnSet = fileRequestServices(dataSet) 
            
            # if _DEBUG:
            #     _LOG.info(f"S: {request.mimetype},{misc.jsonLoads(rtnSet)}")
                
        else:
            if _DEBUG:
                _LOG.info(f"R: {request.mimetype}")
            
        
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}, flaskHFile"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    
    result = misc.jsonDumps(rtnSet)
    if _DEBUG:
        _LOG.info(f"S: {result}")
    return result


def testResizePicFile():

    inputFileName = r"/data/webserver/temp/0/0000000140"
    outputFileName = r"2.jpg"
    fileExtName = ".mp4"
    resizePicFile(inputFileName,  outputFileName, fileExtName)
    
    inputFileName = r"d:\share\LoRa.zip"
    extList = [".1", ".ai", ".doc", ".docx", ".data", ".xls"]
    for fileExtName in extList:
        outputFileName = "D:\\data\\temp\\"+fileExtName [1:]+ ".jpg"
        resizePicFile(inputFileName,  outputFileName, fileExtName)


if __name__ == "__main__":    
    if _DEBUG:
        _LOG.info("main function is ready!!!")
        
    if len(sys.argv) > 1:
        pass
        import platform
        if platform.system()=='Linux':
            import pdb
            pdb.set_trace()
            msg = sys.argv[1]
            dataSet = misc.jsonLoads(msg)
            fileRequestServices(dataSet)
            exit(0)
            
#        testResizePicFile()

    data = {"description":["cat"],"file.path":[r"D:\cats\dogs-vs-cats\small\train\cats\cat.0.jpg"],"file.md5":["cc55351e3a43447c7f7d79a28b0ba3a3"],"file.name":["owner-3.jpg"],"file.size":["90582"]}
    msg = '{"avatarfile.name": ["afile.jpg"], "avatarfile.content_type": ["image/jpeg"], "avatarfile.path": ["/data/webserver/temp/2/0000000322"], "avatarfile.md5": ["5e801c1df5aab84dc4e6ba9e9e807a1f"], "avatarfile.size": ["170995"]}'
    data = misc.jsonLoads(msg)
    fileHandler(data)

    from werkzeug.contrib.fixers import ProxyFix
    application.wsgi_app = ProxyFix(application.wsgi_app)
    application.run(host='0.0.0.0')



