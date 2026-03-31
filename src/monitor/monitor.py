#! /usr/bin/env python3
#encoding: utf-8

#Filename: monitor.py  
# @Author: sevncz
# @Date:   2016-03-04 15:49:00
# @Last modified by:   sevncz
# @Last modified time: 2016-04-13T10:29:29+08:00
# modified by SL 2020/12/15

# */1 * * * * python /xxx/monitor.py >> /xxx/logs/monitor.log 2>&1  &
#*/1 * * * * python /data/airLink/monitor/monitor.py >> /data/airLink/monitor/monitor.log 2>&1  &

_VERSION = "20251128"


#add src directory
import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import getopt
import time
import subprocess
import socket
import signal
import json
import requests

import traceback

import monitorConfig as comMC

_DISPLAY_IN_MAIN = False

_processorPID = os.getpid()

#datetime function
def getTime():
    return time.strftime("%Y%m%d%H%M%S", time.localtime())

def getHumanTimeStamp():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

def time2HumandTimeStamp(t):
    x = time.localtime(t)
    return time.strftime('%Y-%m-%d %H:%M:%S',x)

# JSON/string function
# the function of JSON format converter, SL:2014/10/3
def jsonDumps(data, ensure_ascii=True, indent = 0):
    if indent > 0:
        return json.dumps(data,separators=(',', ':'), ensure_ascii=ensure_ascii, indent = indent)
    else:
        return json.dumps(data,separators=(',', ':'), ensure_ascii=ensure_ascii)

   
def jsonLoads(data):
    return json.loads(data)  

    
def loadJsonData(fileName, typeStyle="list"):
    result = None
    try:
      with open (fileName, "r") as hFile:
            if typeStyle == "list":
                lines=hFile.readlines()
                result=[]
                for a in lines:
                    set1=json.loads(a)
                    result.append(set1)
            else:
                result = json.load(hFile)
    except:
        pass

    return result


def saveJsonData(fileName, dataList, ensure_ascii=True,  indent = 0):
    if dataList!=None:
        typeStyle=type(dataList)
#        print (dataList)
        with open(fileName, "w", encoding='utf-8') as hFile:
            if typeStyle==dict:
                strT=jsonDumps(dataList, ensure_ascii, indent)+"\n"
                hFile.write(strT)
            else:
                for a in dataList:
                    strT=jsonDumps(a, ensure_ascii)+"\n"
                    hFile.write(strT)
    pass


def this_abs_path(script_name):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), script_name))


def readProcessData(fileName):
    result = []
    try:
        filePath = os.path.join(comMC.monitorWorkDir, fileName)
        result = loadJsonData(filePath, typeStyle = "dict")
    except:
        pass
    return result


def getRequest(url,paramsData,headers={"content-type": "application/json"},timeout=10):
    result = {}
    
    rtnData = {}

    try:
        r = requests.get(url, params = paramsData, headers = headers,timeout=timeout)
        if r.status_code == 200:
            try:
                rtnData["data"] = jsonLoads(r.content)
            except:
                rtnData["data"] = r.content
        else:
            pass

        rtnData["status"] = r.status_code
        result = rtnData

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e),traceback.format_exc()}"

    return result   


def postRequest(url,requestData,paramsData={},headers={"content-type": "application/json"},timeout=10):
    result = {}

    rtnData = {}
    try:
        payload = jsonDumps(requestData)
        if paramsData:
            r = requests.post(url, data = payload, params=paramsData,headers = headers,timeout=timeout)
        else:
            r = requests.post(url, data = payload, headers = headers,timeout=timeout)

        if r.status_code == 200:
            try:
                rtnData["data"] = jsonLoads(r.content)
            except:
                rtnData["data"] = r.content
        else:
            pass

        rtnData["status"] = r.status_code
        result = rtnData

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e),traceback.format_exc()}"

    return result   


#进程检测处理 begin

def monitor_process(key_word, cmd, extra=None):
    p1 = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(['grep', key_word], stdin=p1.stdout, stdout=subprocess.PIPE)
    p3 = subprocess.Popen(['grep', '-v', 'grep'], stdin=p2.stdout, stdout=subprocess.PIPE)
    if extra:
        if not isinstance(extra, str):
            extra = str(extra)
        p3 = subprocess.Popen(['grep', extra], stdin=p3.stdout, stdout=subprocess.PIPE)

    lines = p3.stdout.readlines()
    if len(lines) > 0:
        result = "success"
    else:
        print (lines)
        if not cmd:
            result = "fail"
        else:
            sys.stderr.write('process[%s] is lost, run [%s]\n' % (key_word, cmd))
            result = subprocess.call(cmd, shell=True)
    return result


def chkProcessExist(key, chkPID = True):
    result = False
    """
    检查某个进程是否存在
    """
    if not isinstance(key, bytes):
        bKey = key.encode()
    else:
        bKey = key
        
    p1 = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(['grep', key], stdin=p1.stdout, stdout=subprocess.PIPE)
    p3 = subprocess.Popen(['grep', '-v', 'grep'], stdin=p2.stdout, stdout=subprocess.PIPE)
    lines = p3.stdout.readlines()
    for line in lines:
        if chkPID:
            aList = line.split()
            if len(aList) > 1:
                pid = int(aList[1])
                if pid == key:
                    result = True
        else:
            if line.find(bKey) >= 0:
                result = True
                
    return result
    

def monitorKill(key):
    result = 0
    """
    监控进程，并kill掉
    """
    if not key:
        return result

    p1 = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(['grep', key], stdin=p1.stdout, stdout=subprocess.PIPE)
    p3 = subprocess.Popen(['grep', '-v', 'grep'], stdin=p2.stdout, stdout=subprocess.PIPE)

    lines = p3.stdout.readlines()
    for line in lines:
        aList = line.split()
        if len(aList) > 1:
            pid = int(aList[1])
            os.kill(pid, signal.SIGKILL)
            tryTimes = 3
            while tryTimes > 0:
                tryTimes -= 1
                if chkProcessExist(str(pid)) == False:
                    result += 1
                    break
                time.sleep(0.1)
                    
    return result


#监控端口
def monitor_port(protocol, port, cmd):
    address = ('127.0.0.1', port)
    socket_type = socket.SOCK_STREAM if protocol == 'tcp' else socket.SOCK_DGRAM
    client = socket.socket(socket.AF_INET, socket_type)

    try:
        client.bind(address)
    except Exception as e:
        pass
    else:
        sys.stderr.write('port[%s-%s] is lost, run [%s]\n' % (protocol, port, cmd))
        subprocess.call(cmd, shell=True)
    finally:
        client.close()


def monitorProcess(key, cmd,  param = None,num = 0):
    """
    监控进程,及其参数，可以处理参数不同进程，并重启
    """
    global _DISPLAY_IN_MAIN
    result = 0

    findCount = 0
    findPIDList = []

    if key and cmd:
        nLen = len(key)
        grepString = f"grep {key}"
        cmdLine = f"ps -ef |{grepString}"
        p = subprocess.Popen(cmdLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) 
        lines = p.stdout.readlines()

        if not isinstance(grepString, bytes):
            bGrepString = grepString.encode()
        else:
            bGrepString = grepString

        if not isinstance(key, bytes):
            bKey = key.encode()
        else:
            bKey = key

        if not isinstance(param, bytes):
            bParam = param.encode()
        else:
            bParam = param

        for line in lines:
            pos = line.find(bGrepString)
            if pos >= 0:
                continue
            pos = line.find(bKey)
            # print(pos,line)
            if pos > 0:
                aList = line.split()
                if len(aList) > 1:
                    pid = int(aList[1])
                if param:
                    if line[pos+nLen:].find(bParam) >=0:
                        findCount += 1
                        findPIDList.append(pid)
                        #break
                else:
                    findCount += 1
                    findPIDList.append(pid)
                    #break

        # if _DISPLAY_IN_MAIN:
        #     print(findCount)

        if num:
            if findCount != num:
                #kill all
                for pid in findPIDList:
                    os.kill(pid, signal.SIGKILL)
                    tryTimes = 3
                    while tryTimes > 0:
                        tryTimes -= 1
                        if chkProcessExist(str(pid)) == False:
                            findCount -= 1
                            break
                        time.sleep(0.1)

        if findCount <= 0:
            tempStr = f"process[{key}], parm:[{param}]  is lost, run [{cmd}]\n"
            if _DISPLAY_IN_MAIN:
                sys.stderr.write(tempStr)

            if not isinstance(cmd,bytes):
                bCmd = cmd.encode()
            else:
                bCmd = cmd

            try:
                p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) 
                #lines = p.stdout.readlines()
            except:
                pass

            tryTimes = 3
            while tryTimes > 0:
                tryTimes -= 1                
                if (chkProcessExist(key, False)):
                    result += 1
                    tryTimes = 0
                    break 
                time.sleep(0.5)

        else:                    
            result = findCount              

    return result
    

def funcKillProcess():
    result = 0
    #kill exist process
    try:
        totalKilled = 0
        for data in comMC.existProcessKeys:
            try:
                totalKilled += monitorKill(data)
                time.sleep(1)
            except:
                pass 
        result = totalKilled
    except:
        pass 
    return result


def funcMonitorProcess(processData):
    global _DISPLAY_IN_MAIN
    result = 0

    if _DISPLAY_IN_MAIN:
        print()
        print (f"funcMonitorProcess : {getHumanTimeStamp()}")

    try:
        dataList = processData
        for data in dataList:
            key = data["key"]       
            cmd = data["cmd"]
            param = data.get("param", None)
            num = data.get("num")

            try:
                if num:
                    result = monitorProcess(key, cmd, param,num) 
                else:
                    result = monitorProcess(key, cmd, param) 
            except Exception as e:
                errMsg = 'fail:{},{}'.format(str(e), traceback.format_exc())
                result = errMsg
                    
            if _DISPLAY_IN_MAIN:
                print (key, param,  result)
    except:
        pass 
    
    return result

#进程检测处理 end


#文件变化检测处理 begin

existFileInfoData = {}
currFileInfoData = {}

def readExistFileInfo():
    global existFileInfoData
    monitorWorkDir = comMC.monitorWorkDir
    saveFileName = os.path.join(monitorWorkDir,comMC.monitorFileStatusFileName)
    currData = loadJsonData(saveFileName,typeStyle = "dict")
    if currData:
        existFileInfoData = currData


def saveCurrFileInfo():
    global currFileInfoData
    monitorWorkDir = comMC.monitorWorkDir
    saveFileName = os.path.join(monitorWorkDir,comMC.monitorFileStatusFileName)

    saveJsonData(saveFileName,currFileInfoData,indent=2)


def chkFileChanges(fileName):
    global existFileInfoData
    global currFileInfoData
    result = False
    fileChangedFlag = False
    if os.path.exists(fileName):
        #
        currModifyTime = os.path.getmtime(fileName)
        currCreateTime = os.path.getctime(fileName)
        currSize = os.path.getsize(fileName)

        aSet = {}
        if isinstance(fileName,str):
            aSet["fileName"] = fileName
        else:
            aSet["fileName"] = fileName.decode()
        aSet["modifyTime"] = currModifyTime
        aSet["createTime"] = currCreateTime
        aSet["fileSize"] = currSize

        currFileInfoData[fileName] = aSet

        if fileName in existFileInfoData:
            existDataSet = existFileInfoData.get(fileName)
            existModifyTime = existDataSet.get("modifyTime")
            existFileSize = existDataSet.get("fileSize")
            currModifyHumanTime = time2HumandTimeStamp(currModifyTime)
            existModifyHumanTime = time2HumandTimeStamp(existModifyTime)
            if _DISPLAY_IN_MAIN:
                print (f"fileName:{fileName}: modifyTime:{currModifyHumanTime}<->{existModifyHumanTime},size:{currSize}<->{currSize} ")
            if currModifyTime != existModifyTime or currSize != existFileSize:
                fileChangedFlag = True
        else:
            fileChangedFlag = True

    result = fileChangedFlag

    return result


#检测文件变化, 如果变化了, 就重启
def funcMonitorFileChanges(processData):
    global _DISPLAY_IN_MAIN

    if _DISPLAY_IN_MAIN:
        print()
        print (f"funcMonitorFileChanges : {getHumanTimeStamp()}")

    result = 0
    try:
        #读取数据
        readExistFileInfo()

        dataList = processData
        for data in dataList:
            key = data["key"]      
            cmd = data["cmd"]

            fileName = data.get("file", None)
            fileNameList = data.get("fileList",[])
            if not fileNameList:
                fileNameList = []
            fileNameList.append(fileName)

            changedFlag = False

            for fileName in fileNameList:
                if fileName:
                    # currChangedFlag = chkFileChanges(key, fileName) 
                    currChangedFlag = chkFileChanges(fileName) 
                    if currChangedFlag:
                        changedFlag = True
                    if _DISPLAY_IN_MAIN:
                        print (key, fileName,currChangedFlag)

            if changedFlag:
                #kill process
                if _DISPLAY_IN_MAIN:
                    print (f"{key}:{fileNameList} changed, kill !!!")
                try:
                    monitorKill(key)
                    time.sleep(1)
                except Exception as e:
                    errMsg = f'fail:{str(e)},{traceback.format_exc()}'
                    result = errMsg

        #保存数据
        saveCurrFileInfo()

    except:
        pass 
    
    return result

#文件变化检测处理 end

def funcMonitorService():
    global _DISPLAY_IN_MAIN

    if _DISPLAY_IN_MAIN:
        print()
        print (f"funcMonitorService : {getHumanTimeStamp()}")

    result = 0
    try:
        #读取数据
        serviceMonitorData = comMC.serviceMonitorData

        for data in serviceMonitorData:
            method = data["method"]
            description = data["description"]
            host = data["host"]
            port = data["port"]
            dictFlag = data["dictFlag"]
            headers = data["headers"]
            urlPath = data["urlPath"]
            requestData = data["requests"]
            paramsData = data["params"]
            expectResult = data["expectResult"]
            key = data["key"]      
            cmd = data["cmd"]

            url = "http://" + host + "/" + urlPath

            if _DISPLAY_IN_MAIN:
                print (f"{description}:{host}:{port}:{urlPath}:{requestData}:{paramsData}:{expectResult}")
            
            if method == "post":
                rtnData = postRequest(url,requestData,paramsData)
            else:
                rtnData = getRequest(url,paramsData)  
            
            statusCode = rtnData.get("status")
            exceptStatusCode = expectResult.get("status")
            if statusCode != exceptStatusCode:
                #service is done, kill application
                if _DISPLAY_IN_MAIN:
                    print (f"{key}:is out of service, {statusCode},{exceptStatusCode}, kill !!!")
                try:
                    monitorKill(key)
                    time.sleep(1)
                    result += 1
                except Exception as e:
                    errMsg = f'fail:{str(e)},{traceback.format_exc()}'
                    result = errMsg
            elif statusCode == 200:
                if _DISPLAY_IN_MAIN:
                    print (f"{key}:is in service, {statusCode},{exceptStatusCode}")

    except Exception as e:
        pass 
    
    return result


def main():
    global _DISPLAY_IN_MAIN
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hki:d", ["help", "kill","input=","debug"])
    except getopt.GetoptError:
        sys.exit() 
    
    debugFlag = False
    killExistFlag = False
    inputFileName = ""
    
    for name , value in opts:
        if name in ("-h", "--help"):
            #打印帮助信息
            print ("-k kill -i input -d debug")
            sys.exit()
            
        elif name in ("-k", "--kill"):
            killExistFlag = True
        elif name in ("-i", "--input"):
            inputFileName = value
        elif name in ("-d", "--debug"):
            debugFlag = True
        else:
            pass

    if debugFlag:
        import pdb 
        pdb.set_trace()
        
    _DISPLAY_IN_MAIN = True
        
    systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
    print (f"\nmonitor process: python version {systemVersion}, code version {_VERSION}  : {getHumanTimeStamp()} ")
    
    if inputFileName:
        processData = readProcessData(inputFileName)
        print (f"processData from :{inputFileName}")
    else:
        processData = comMC.processData

    if killExistFlag:
        funcKillProcess()
        time.sleep(2)

    #检测服务是否正常
    funcMonitorService()

    #检测文件变化, 如果变化了, 就重启
    funcMonitorFileChanges(processData)

    #then 监控进程是否顺利启动
    funcMonitorProcess(processData)

    print()
            

if __name__ == '__main__':
    main()

