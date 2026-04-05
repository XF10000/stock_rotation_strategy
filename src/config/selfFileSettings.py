#! /usr/bin/env python3
#encoding: utf-8

#Filename: selfFileSettings.py  
#Author: Steven Lian's team
#E-mail:  / /steven.lian@gmail.com  
#Date: 2022-08-23
#Description:   本地文件系统的配置管理等

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


_DEBUG = True

#生产环境 rss | 测试环境 dss
_SYS = local_settings._SYS
#_SYS = "server_01"
#_SYS = "local"

_SYS_SERVER_NAME = local_settings._SYS_SERVER_NAME

_HOME_DIR = {
    "local":r"/data/stockapp",
    "server_01":r"/data/stockapp", 
    "server_02":r"/data/stockapp", 
    "home":r"../../",  
    }[_SYS]


LOCAL_FILE_SERVER_DIR_NAME = "temp"

#local server path 
LOCAL_FILE_SERVER_PATH ={
    # "local":"http://telemed.caict.ac.cn/temp/",
    "local":f"http://192.168.100.100/{LOCAL_FILE_SERVER_DIR_NAME}/",
    "server_01":f"http://192.168.3.253/{LOCAL_FILE_SERVER_DIR_NAME}/", 
    "server_02":f"http://192.168.100.100/{LOCAL_FILE_SERVER_DIR_NAME}/", 
    "home":f"http://192.168.100.100/{LOCAL_FILE_SERVER_DIR_NAME}/", 
}[_SYS]

#local server path 
LOCAL_FILE_SERVER_BASE ={
    "local":r"/data/webserver/temp/",
    "server_01":r"/data/webserver/temp/", 
    "server_02":r"/data/webserver/temp/", 
    "home":r"/data/webserver/temp/", 
}[_SYS]

LOCAL_FILE_TEMP_WEB_DIR = 'web/'

#local server file storage
# SELF_FILE_SERVER_STORAGE_IF_REMOTE = True #是否远程存储
SELF_FILE_SERVER_STORAGE_IF_REMOTE = False #是否远程存储

SELF_FILE_SERVER_STORAGE_ADDR = {
    "local":"127.0.0.1",
    "server_01":"127.0.0.1",
    "server_02":"127.0.0.1",
    "home":"127.0.0.1",
}

SELF_FILE_SERVER_STORAGE_DIR ={
    "local":r"/data/filestorage/",
    "server_01":r"/data/filestorage/", 
    "server_02":r"/data/filestorage/", 
    "home":r"/data/filestorage/", 
}[_SYS]

#本地目录下面最多有1000个目录,随机存储
LOCAL_FILE_STORAGE_DIR_MAX_NUM = 1000 
LOCAL_FILE_STORAGE_DIR_LEN = 3 #1000个是3位从000-999 


if __name__ == "__main__":
    pass
    # import pdb
    # pdb.set_trace()
    print ("_SYS",_SYS)

    print ("_SYS_SERVER_NAME", _SYS_SERVER_NAME)
    print ("_HOME_DIR", _HOME_DIR)
    print ("LOCAL_FILE_SERVER_PATH", LOCAL_FILE_SERVER_PATH)
    print ("LOCAL_FILE_SERVER_BASE", LOCAL_FILE_SERVER_BASE)
    print ("SELF_FILE_SERVER_STORAGE_DIR", SELF_FILE_SERVER_STORAGE_DIR)
    
