#! /usr/bin/env python3
#encoding: utf-8

#Filename: basicSettings.py  
#Author: Steven Lian's team
#E-mail:  / /steven.lian@gmail.com  
#Date: 2022-08-23
#Description:   通用的配置管理,网络地址等

_VERSION="20260113"


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
#_SYS = "local"

_SYS_SERVER_NAME = local_settings._SYS_SERVER_NAME


_HOME_DIR = {
    "local":r"/data/stock_rotation_strategy",
    "server_01":r"/data/stock_rotation_strategy", 
    "server_02":r"/data/stock_rotation_strategy", 
    # "home":r"../..",  
    "home":r"..",  
    }[_SYS]

_DATA_DIR = {
    "local":f"{_HOME_DIR}/data",
    "server_01":f"{_HOME_DIR}/data",
    "server_02":f"{_HOME_DIR}/data",
    "home":f"{_HOME_DIR}/data",
    }[_SYS]

_DATA_CONFIG_DIR = {
    "local":f"{_DATA_DIR}/config",
    "server_01":f"{_DATA_DIR}/config",
    "server_02":f"{_DATA_DIR}/config",
    "home":f"{_DATA_DIR}/config",
    }[_SYS]


ACCOUNT_SERVICE_URL ={
    "local":"http://127.0.0.1:8160/acis",
    "server_01":"http://127.0.0.1:8160/acis", 
    "server_02":"http://dfs.iottest.online:8160/acis", 
    "home":"http://127.0.0.1:8160/acis", 
}[_SYS]


#服务器地址等信息
FILE_SYSTEM_MODE = {
    # "local":"ALIOSS",
    "local":"SELFFILE",
    "server_01":"SELFFILE", 
    "server_02":"SELFFILE", 
    "home":"SELFFILE", 
}[_SYS]


#fastdfs  cmd path (upload,delete, etc.)
FASTDFS_CMD_PATH ={
    "local":r"/usr/bin/",
    "server_01":r"/usr/bin/", 
    "server_02":r"/usr/bin/", 
    "home":r"/usr/bin/", 
}[_SYS]

#fastdfs client conf path
FASTDFS_CLIENT_CONF_PATH ={
    "local":r"/etc/fdfs/client.conf",
    "server_01":r"/etc/fdfs/client.conf", 
    "server_02":r"/etc/fdfs/client.conf", 
    "home":r"/etc/fdfs/client.conf", 
}[_SYS]

#fastdfs server path 
FASTDFS_SERVER_PATH ={
    "local":"http://127.0.0.1:8080/",
    "server_01":"http://www.aifortest.tech:8080/", 
    "server_02":"http://www.aifortest.tech:8080/", 
    "home":"http://192.168.100.100:8080/", 
}[_SYS]

#local server path 
LOCAL_FILE_SERVER_PATH ={
    # "local":"http://www.aifortest.tech:9000/temp/",
    "local":"http://www.aifortest.tech:9000/temp/",
    "server_01":"http://www.aifortest.tech:9000/temp/", 
    "server_02":"http://www.aifortest.tech:9000/temp/", 
    "home":"http://stevenlian.asuscomm.com:9000/temp/", 
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
LOCAL_FILE_SERVER_STORAGE_DIR ={
    "local":r"/data/filestorage/",
    "server_01":r"/data/filestorage/", 
    "server_02":r"/data/filestorage/", 
    "home":r"/data/filestorage/", 
}[_SYS]

#本地目录下面最多有1000个目录,随机存储
LOCAL_FILE_STORAGE_DIR_MAX_NUM = 1000 
LOCAL_FILE_STORAGE_DIR_LEN = 3 #1000个是3位从000-999 

_STAT_DATA_FILE_NAME = "statDataFile.json"

#默认系统自动loginID
SYS_DEFAULT_AUTO_LOGINID ={
    "local":"10010001000", 
    "server_01":"10010001000", 
    "server_02":"10010001000", 
    "home":"10010001000", 
}[_SYS]

#genDigistKey
GEN_DIGIST_KEY ={
    "local":"ylwzlylc", 
    "server_01":"ylwzlylc", 
    "server_02":"ylwzlylc", 
    "home":"ylwzlylc", 
}[_SYS]

#file server upload url dataSet, 注意这个是一个字典
FILE_UPLOAD_URL ={
    "local":"http://www.aifortest.tech:9000/upload",
    "server_01":"http://www.aifortest.tech:9000/upload", 
    "server_02":"http://www.aifortest.tech:9000/upload", 
    "home":"http://192.168.100.100:9000/upload", 
}[_SYS]

#file server url dataSet, 注意这个是一个字典
FILE_SERVER_URL ={
    "local":"http://www.aifortest.tech:9000/hfile",
    "server_01":"http://www.aifortest.tech:9000/hfile", 
    "server_02":"http://app.iottest.online/hfile", 
    "home":"http://192.168.100.100/hfile", 
}


#图片文件最大大小 (宽,高) (width, height)
MAX_PIC_SIZE = {
    "local":(1920, 1920),
    "server_01":(1920, 1920),
    "server_02":(1920, 1920),
    "home":(1920, 1920),
}[_SYS]


#thumbnail 缩略图文件大小 (宽,高) (width, height)
THUMBNAIL_SIZE = {
    "local":(720, 720),
    "server_01":(720, 720),
    "server_02":(720, 720),
    "home":(360, 360),
}[_SYS]


#role 角色权限
ROLE_RIGHT_SET ={
    "administrator":0, 
    "manager":10, 
    "operator":20, 
    "expert":30, 
    "customer":60, 
    "visitor":70, 
}

#account service roleName 
# administrator,manager,operator,chief,customer,visitor
accountServiceDefaultLoginID = "gluser"
accountServiceDefaultRoleName = "customer"

#role 角色分配的功能清单

ROLE_EN_CN_NAME_DATA = {
    "administrator":"系统管理员",
    "manager":"全域管理员",
    "operator":"区域管理员",
    "customer":"普通用户",
    "visitor":"访客",
}

#本系统到 account service 的role转换表
# accout service:(adminstractor,manager,operator,chief,customer,visitor)
ROLE_ACCOUNT_ROLE = {
    "administrator":"administrator",
    "manager":"manager",
    "operator":"operator",
    "customer":"customer",
    "visitor":"visitor",
}


#不需要sessionID的命令入口清单
NO_SESSIONID_CMD_LIST = {
    #业务标签
    "generalnext", 
    "login", "registration","smsrequest","smsverify","resetpasswd","chkuserexist",
    #获取主要参数, 例如菜单输入项目等
    "getmenuparameters",
    }

#role 角色分配的功能清单


ROLE_CMD_LIST =\
{
"administrator":[
    #user related
    "registration", "logout", "useradd", "userdel", "usermodify", "getuserinfo", "usersearch", "userinfoqry",
    "usersavedata", "usergetdata",
    #保存/查询默认主页消息
    "savehomepagemsg","qryhomepagemsg",
    #获取当前用户的默认主页数据
    "gethomepagedata",
    ], 
"manager":    [
    #user related
    "registration", "logout", "useradd", "userdel", "usermodify", "getuserinfo", "usersearch", "userinfoqry",
    "usersavedata", "usergetdata",
    #保存/查询默认主页消息
    "savehomepagemsg","qryhomepagemsg",
    #获取当前用户的默认主页数据
    "gethomepagedata",
    ], 
"operator":[
    #user related
    "registration", "logout",  "userdel", "usermodify", "getuserinfo", "usersearch", "userinfoqry",
    "usersavedata", "usergetdata",
    #获取当前用户的默认主页
    "gethomepagedata",
    ], 
"customer":[
    #user related
    "registration", "logout",  "userdel", "usermodify", "getuserinfo", "usersearch", "userinfoqry",
    "usersavedata", "usergetdata",
    #获取当前用户的默认主页
    "gethomepagedata",
    ], 
}


FUNCTION_CMD_CNNAME_DATA = {
    "chkuserexist":"用户是否存在",
    "generalnext":"获取下一批数据",
    "getmenuparameters":"获取菜单参数",
    "getuserinfo":"用户信息获取",
    "login":"用户登录",
    "logout":"用户注销/登出",
    "registration":"用户注册",
    "resetpasswd":"用户重置密码",
    "smsrequest":"短信验证请求",
    "smsverify":"短信验证反馈",
    "statprojectfiles":"获取项目文件统计信息",
    "uploadbigfile":"上传超大文件",
    "uploaddatafile":"上传文件",
    "useradd":"用户增加",
    "userdel":"用户删除",
    "usergetdata":"获取用户存储数据",
    "userinfoqry":"用户信息查询",
    "usermodify":"用户修改",
    "usersavedata":"用户存储数据",
    "usersearch":"用户查询",
}

menuParameters = {
}


#注册行为短信通知用户清单
REGISTRATION_NOTIFICATION_USER_LIST = [
    "13910710766",
]


_LOG = None #预设日志对象，禁止修改
_DEBUG = True  #预设trace开关，禁止修改


#stock data config begin

STOCK_DATA_DIR_NAME = {
    "local":f"{_DATA_DIR}/stock_data", 
    "server_01":f"{_DATA_DIR}/stock_data", 
    "server_02":f"{_DATA_DIR}/stock_data",
    "home":f"{_DATA_DIR}/stock_data",
}[_SYS]

STOCK_CONFIG_DIR_NAME = {
    "local":f"{_DATA_DIR}/config",
    "server_01":f"{_DATA_DIR}/config",
    "server_02":f"{_DATA_DIR}/config",
    "home":f"{_DATA_DIR}/config",
}[_SYS]

STOCK_DATA_CACHE_DIR_NAME = {
    "local":f"{_DATA_DIR}/cache",
    "server_01":f"{_DATA_DIR}/cache",
    "server_02":f"{_DATA_DIR}/cache",
    "home":f"{_DATA_DIR}/cache",
}[_SYS]

STOCK_DATA_SAVE_DIR_NAME = {
    "local":f"{STOCK_DATA_CACHE_DIR_NAME}/stock_data",
    "server_01":f"{STOCK_DATA_CACHE_DIR_NAME}/stock_data",
    "server_02":f"{STOCK_DATA_CACHE_DIR_NAME}/stock_data",
    "home":f"{STOCK_DATA_CACHE_DIR_NAME}/stock_data",
}[_SYS]

STOCK_PORTFOLIO_CONFIG_FILE = "portfolio_config.csv"
STOCK_PORTFOLIO_CONFIG_JSON_FILE = "stock_portfolio_config.json"

STOCK_SW_STOCK_INDUSTRY_MAP_FILE = "stock_to_industry_map.json" #股票到申银万国行业的映射文件,存储在 STOCK_CONFIG_DIR_NAME 下
STOCK_BASIC_INFO_FILE = "stock_basic_info.json" #股票基本信息文件,存储在 STOCK_CONFIG_DIR_NAME 下

#stock data config end


if __name__ == "__main__":
    pass
    # import pdb
    # pdb.set_trace()
    print ("_SYS",_SYS)
    print ("_SYS_SERVER_NAME",_SYS_SERVER_NAME)

    print ("FILE_SYSTEM_MODE", FILE_SYSTEM_MODE)
    print ("THUMBNAIL_SIZE", THUMBNAIL_SIZE)
    print ("ROLE_CMD_LIST", ROLE_CMD_LIST)
    print ("SYS_DEFAULT_AUTO_LOGINID", SYS_DEFAULT_AUTO_LOGINID)

    print ("LOCAL_FILE_SERVER_BASE", LOCAL_FILE_SERVER_BASE)
    print ("LOCAL_FILE_SERVER_PATH", LOCAL_FILE_SERVER_PATH)

    print ("DATA_RECV_SERVICE_URL", DATA_RECV_SERVICE_URL)

