#! /usr/bin/env python3
#encoding: utf-8

#Filename: basicSettings.py  
#Author: Steven Lian's team
#E-mail:  / /steven.lian@gmail.com  
#Date: 2022-08-23
#Description:   通用的配置管理,网络地址等

_VERSION="20260118"


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

#根据 industry index 计算的rsi 数据存放地
INDEX_DATA_SAVE_DIR_NAME = {
    "local":f"{STOCK_DATA_CACHE_DIR_NAME}/rsi_data",
    "server_01":f"{STOCK_DATA_CACHE_DIR_NAME}/rsi_data",
    "server_02":f"{STOCK_DATA_CACHE_DIR_NAME}/rsi_data",
    "home":f"{STOCK_DATA_CACHE_DIR_NAME}/rsi_data",
}[_SYS]

STOCK_PORTFOLIO_CONFIG_FILE = "portfolio_config.csv"
STOCK_PORTFOLIO_CONFIG_JSON_FILE = "stock_portfolio_config.json"

STOCK_SW_STOCK_INDUSTRY_MAP_FILE = "stock_to_industry_map.json" #股票到申银万国行业的映射文件,存储在 STOCK_CONFIG_DIR_NAME 下
STOCK_BASIC_INFO_FILE = "stock_basic_info.json" #股票基本信息文件,存储在 STOCK_CONFIG_DIR_NAME 下


"""
申万二级行业RSI阈值计算配置文件
用户可以通过修改此文件来调整计算参数
"""

# RSI阈值分位数配置
RSI_THRESHOLDS = {
    # 普通阈值（适用于所有行业的基础阈值）
    "普通超卖": 3,  # 15%分位数
    "普通超买": 97,  # 85%分位数
    
    # 极端阈值（根据波动率分层设置）
    "极端阈值": {
        "高波动": {
            "超卖": 1,   # 5%分位数 - 高波动行业更容易触发极端信号
            "超买": 99   # 95%分位数
        },
        "中波动": {
            "超卖": 1,   # 8%分位数 - 中等波动行业
            "超买": 99   # 92%分位数
        },
        "低波动": {
            "超卖": 1,  # 10%分位数 - 低波动行业较难触发极端信号
            "超买": 99   # 90%分位数
        }
    }
}

# 极端阈值系数配置
# 新的RSI极端阈值 = 配置的分位数对应的RSI值 × 系数
STOCK_RSI_EXTREME_THRESHOLD_COEFFICIENTS = {
    "高波动": {
        "超卖系数": 0.95,  # 高波动行业极端超卖阈值稍微收紧
        "超买系数": 1.05   # 高波动行业极端超买阈值稍微放宽
    },
    "中波动": {
        "超卖系数": 0.95,  # 中波动行业极端超卖阈值轻微收紧
        "超买系数": 1.05   # 中波动行业极端超买阈值轻微放宽
    },
    "低波动": {
        "超卖系数": 0.95,  # 低波动行业保持原分位数值
        "超买系数": 1.05   # 低波动行业保持原分位数值
    }
}

# 计算周期配置
STOCK_RSI_CALCULATION_PERIODS = {
    # 历史数据回看周数（用于计算RSI分位数）
    "lookback_weeks": 104,  # 104周 ≈ 2年
    
    # RSI计算周期
    "rsi_period": 14,  # 14周RSI
    
    # 波动率分层的分位数
    "volatility_quantiles": {
        "q1": 25,  # 25%分位数 - 低波动与中波动的分界线
        "q3": 75   # 75%分位数 - 中波动与高波动的分界线
    }
}

# 数据质量控制
STOCK_RSI_DATA_QUALITY = {
    # 最少需要的数据点数
    "min_data_points": 50,  # 至少50周数据
    "min_rsi_points": 20,   # 至少20个有效RSI数据点
    
    # API重试配置
    "retry_times": 3,       # 重试次数
    "retry_delay": 2        # 重试间隔（秒）
}

# 输出配置
STOCK_RSI_OUTPUT_CONFIG = {
    # 输出文件名
    "output_filename": "sw2_rsi_threshold.csv",
    
    # 数值精度
    "float_precision": 2,   # 保留2位小数
    
    # 是否包含调试信息
    "include_debug_info": True
}

# 使用说明
"""
配置参数说明：

1. RSI_THRESHOLDS - RSI阈值分位数配置
   - 普通超卖/超买：适用于所有行业的基础阈值
   - 极端阈值：根据行业波动率分层设置不同的极端阈值
   
2. CALCULATION_PERIODS - 计算周期配置
   - lookback_weeks：历史数据回看周数，建议52-156周（1-3年）
   - rsi_period：RSI计算周期，通常为14
   - volatility_quantiles：波动率分层的分位数设置
   
3. 调整建议：
   - 如果希望更敏感的信号：降低超卖阈值，提高超买阈值
   - 如果希望更保守的信号：提高超卖阈值，降低超买阈值
   - 历史数据周数越长，阈值越稳定但对近期变化反应越慢
   
4. 常用配置组合：
   - 激进策略：普通(10,90)，极端高波动(3,97)，中波动(5,95)，低波动(8,92)
   - 保守策略：普通(20,80)，极端高波动(8,92)，中波动(12,88)，低波动(15,85)
   - 平衡策略：当前默认配置
"""

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

