#! /usr/bin/env python3
#encoding: utf-8

#import flup.server.fcgi as flups
from flask import Flask, request

application = Flask(__name__)

_VERSION="20260115"

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

import traceback

#global defintion/common var etc.
from common import globalDefinition as comGD  #modify here

from common import miscCommon as misc

import stockWebAPIPost as userApp #modify here

_processorPID = os.getpid()

appName = "stockWebAPI" #modify here
routeAddr = "/stockapi/<urlPath>" #modify here
httpMethod = ["GET","POST"] #modify here
appType = ""

_DEBUG = True #modify here
# _DEBUG = False #modify here

if "_LOG" not in dir() or not _LOG:
    _LOG = misc.setLogNew(comGD._DEF_LOG_STOCK_WEBAPI_TITLE, comGD._DEF_LOG_STOCK_WEB_API_NAME) #modify here

userApp._LOG = _LOG

systemVersion = str(sys.version_info.major) + "." + str(sys.version_info.minor ) + "." + str(sys.version_info.micro )
_LOG.info(f"PID:{_processorPID}, python version:{systemVersion}, start code version:{_VERSION}, main code version:{userApp._VERSION}")
    
_LOG.info(f"C: routeAddr:{routeAddr},methods:{httpMethod},appType:{appType}")

@application.route(routeAddr, methods = httpMethod ) 
def main(urlPath):
    rtnSet = {}

    try:
        #IP = "0.0.0.0"
        IP =  request.remote_addr
        try:
            x_forward_for = request.headers.get('X-Forwarded-For', '')

            aList = x_forward_for.split(",")
            for a in aList:
                if len(a) > 3:
                    lastIP = a
                    IP = lastIP
            if  _DEBUG:
                pass
                _LOG.info(f"DEBUG:{request.mimetype},{urlPath},{len(lastIP)},{lastIP}")

        except:
            pass
            
        x_server_addr = request.headers.get('X-Server-Addr', '')
        x_server_port = request.headers.get('X-Server-Port', '')
        x_protocol_used = request.headers.get('X-Protocol-Used', '')

        environSet = {}
        environSet["REQUEST_METHOD"] = request.method
        environSet["CONTENT_LENGTH"] = request.content_length
        environSet["CONTENT_TYPE"] = request.content_type

        environSet["_x_server_addr"] = x_server_addr
        environSet["_x_server_port"] = x_server_port
        environSet["_x_protocol_used"] = x_protocol_used

        # if _DEBUG:
        #     headers = dict(request.headers)
        #     _LOG.info(f"DEBUG: environSet:{environSet},headers:{headers}")

        dataSet = {}
        if request.mimetype == "multipart/form-data":
            dataSet = request.form.to_dict(flat=False)

            # if _DEBUG:
            #     pass
            #     _LOG.info(f"MR:form-data {request.mimetype},{misc.jsonDumps(dataSet)},{type(dataSet)}")
            
            rtnSet = userApp.post(urlPath, dataSet, IP, environSet, appType)

            # if _DEBUG:
            #     _LOG.info(f"MS:form-data {request.mimetype},{misc.jsonDumps(rtnSet)}")

        elif  request.mimetype == "application/json":
            
            dataSet = request.json
            
            # if _DEBUG:
            #     _LOG.info(f"MR:form-data {request.mimetype},{misc.jsonDumps(dataSet)},{type(dataSet)}")

            rtnSet = userApp.post(urlPath, dataSet, IP, environSet, appType)
            
            # if _DEBUG:
            #     _LOG.info(f"MS:form-data {request.mimetype},{misc.jsonDumps(rtnSet)}")
                
        else:
            if _DEBUG:
                _LOG.info(f"MR: {request.mimetype},{request.data}")

            try:
                dataSet = misc.jsonLoads(request.data)
                rtnSet = userApp.post(urlPath, dataSet, IP, environSet, appType)
                
                if _DEBUG:
                    _LOG.info(f"MS:form-data {request.mimetype},{misc.jsonDumps(rtnSet)}")
                    
            except Exception as e:
                # errMsg = f"PID: {_processorPID},request.data:{request.data},errMsg:{str(e)}"
                errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
                _LOG.error(f"{errMsg}, {traceback.format_exc()}")
            
    except Exception as e:
        # errMsg = f"PID: {_processorPID},request.data:{request.data},errMsg:{str(e)}"
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    
    result = misc.jsonDumps(rtnSet)
    
    if _DEBUG:
        pass
#        MAX_PRINT_LEN = 10000
#        if len(result) > MAX_PRINT_LEN:
#            _LOG.info("S: MAX {0},{1},{2}".format(IP,appType, result[0:MAX_PRINT_LEN]))
#        else:
#            _LOG.info("S: {0},{1},{2}".format(IP,appType, result))
            
    return result
    
    
if __name__ == "__main__":    
    
    from werkzeug.contrib.fixers import ProxyFix
    application.wsgi_app = ProxyFix(application.wsgi_app)
    application.run(host='0.0.0.0')
