#! /usr/bin/env python3
#encoding: utf-8

#Filename: globalDefinition.py  
#Author: Steven Lian's team/xie_frank@163.com
#E-mail:  steven.lian@gmail.com  
#Date: 2019-08-01
#Description:   定义全局常量

_VERSION="20260124"

#导入具体应用的全局变量

#common const definiation
_CONST_YES = "Y"
_CONST_NO = "N"

#language
_DEF_DEFAULT_LANGUAGE = "CN"

#common msg key words
_DEF_COMM_CODINGTYPE_NAME = "codingType"
_DEF_COMM_DATA_PACKAGE_NAME = "data"

_DEF_COMM_CODING_TYPE_JSON = "JSON" #
_DEF_COMM_CODING_TYPE_BCD = "BCD"
_DEF_COMM_CODING_TYPE_B6400 = "B64.00" #standard base64
_DEF_COMM_CODING_TYPE_B6401 = "B64.01" #加密的base64,simpleEncode
_DEF_COMM_CODING_TYPE_B6402 = "B64.02"
_DEF_COMM_CODING_TYPE_B64C1 = "B64.C1"

_DEF_COMM_CODING_TYPE_JSON_NUM = "0" #上面类型的对应数字表示
_DEF_COMM_CODING_TYPE_BCD_NUM = "1"
_DEF_COMM_CODING_TYPE_B6400_NUM = "2"
_DEF_COMM_CODING_TYPE_B6401_NUM = "3"
_DEF_COMM_CODING_TYPE_B6402_NUM = "4"
_DEF_COMM_CODING_TYPE_B64C1_NUM = "5"

_DEF_COMM_HASH_KEY_FOR_ALL = "THISISSTEVENCODE"

#redis key prefix
_DEF_REDIS_SYS_LEVEL1 = "SYS" #系统相关
_DEF_REDIS_USER_LEVEL1 = "USER" #用户相关
_DEF_REDIS_DEVICE_LEVEL1 = "DEV" #设备相关
_DEF_REDIS_TIMER_LEVEL1 = "TIMER" #定时相关
_DEF_REDIS_DATA_LEVEL1 = "DATA" #数据相关
_DEF_REDIS_FILE_LEVEL1 = "FILE" #文件相关
_DEF_REDIS_STAT_LEVEL1 = "STAT" #统计数据
_DEF_REDIS_CHANNEL_LEVEL1 = "CHANNEL" #频道相关
_DEF_REDIS_FORM_LEVEL1 = "FORM" #表单相关
_DEF_REDIS_FORM_HISTORY = "HISTORY" #历史数据相关
_DEF_REDIS_BUFFER_LEVEL1 = "BUFFER" #数据临时缓冲区

 
_DEF_REDIS_SYS_CONN_INTERVAL = "connInterval"
_DEF_REDIS_SYS_CONFIG = "CONFIG"

_DEF_REDIS_SYS_CONN_DEFAULT="default"
_DEF_REDIS_SYS_CONN_HIGH="high"
_DEF_REDIS_SYS_CONN_LOW="low"
_DEF_REDIS_SYS_CONN_RATE="rate"

_DEF_DATA_SMS_NAME = "SMS"
_DEF_DATA_SMS_CODE_KEEP_TIME = (10*60) #保存10分钟

_DEF_REDIS_USER_USER_ID = "userID"

#ip flood 
_DEF_REDIS_DATA_IP_COUNT = "IPCOUNT"
_DEF_REDIS_DATA_IP_TIME = "IPTIME"
_DEF_REDIS_DATA_IP_TIME_THRESOLD = 300 #五分钟
_DEF_REDIS_DATA_IP_VISITS = (_DEF_REDIS_DATA_IP_TIME_THRESOLD * 2) #五分钟
_DEF_REDIS_DATA_IP_EXPIRE_TIME = (30*24*60*60) #30天

#给客户端提供的在线存储
_DEF_REDIS_USER_DATA_SAVE_NAME = "USER_SAVE"

_DEF_REDIS_DEV_DEVICE_ID = "devID"

_DEF_REDIS_DEV_MAID="MAID"
_DEF_REDIS_DEV_MAID_PIDLIST="PIDList"

_DEF_REDIS_DEV_MAID_PID="MAID_PID"
_DEF_REDIS_DEV_MAID_PID_CONN_INTERVAL="connInterval"


_DEF_REDIS_DEV_TYPE_ID="typeID"
_DEF_REDIS_DEV_TYPE_ID_INFO="info"
_DEF_REDIS_DEV_TYPE_ID_CONFIG="config"
_DEF_REDIS_DEV_TYPE_ID_INST_SET="instSet"

_DEF_REDIS_DEV_CMD_SET="cmdSet"

# CMD channel 
_DEF_REDIS_CHANNEL_MAIN = "MAIN"

#二级系统参数 _DEF_REDIS_SYS_LEVEL1对应的
_DEF_REDIS_SYS_STATUS = "STATUS"
_DEF_REDIS_SYS_MSG_SEQ_NUM = "MSG_SEQ_NUM"
_DEF_REDIS_SYS_KICKOFF_TIMESTAMP = "KICKOFF_TIMESTAMP"
_DEF_REDIS_SYS_KICKOFF_HUMANTIME = "KICKOFF_HUMANTIME"

#二级系统参数 _DEF_REDIS_DATA_LEVEL1对应的
_DEF_REDIS_DATA_TYPE_QUEUE = "QUEUE"
_DEF_REDIS_DATA_TYPE_STAT = "STAT"
_DEF_REDIS_DATA_TYPE_MSG_RECV = "MSG_RECV"

#redis init log
_DEF_REDIS_LOG_TITLE = "INIT_V1"
_DEF_REDIS_LOG_NAME = "initlog"

#redis user database key
_DEF_REDIS_USER_DB_USER_BASIC = "BASIC"
_DEF_REDIS_USER_DB_USER_WARN = "WARN" #Warning list
_DEF_REDIS_USER_DB_USER_NAME = "userName"
_DEF_REDIS_USER_DB_PASSWORD = "password"
_DEF_REDIS_USER_DB_ASSETS_ID = "assetsID" #资产ID
_DEF_REDIS_USER_DB_WECHAT_OPENID = "weChatOpenID" #某个用户对应的微信小程序openID
_DEF_REDIS_USER_DB_WECHAT_CODE = "weChatCode" #微信小程序 code对应的openid 和sessionkey
_DEF_REDIS_USER_DB_OPENID_LOGINID = "openID_loginID" #微信小程序openid 对应的 loginID
_DEF_REDIS_USER_WECHAT_CODE_KEEP_TIME = (60*10) #微信小程序 code 保存时间
_DEF_REDIS_USER_DB_DELETE_FLAG = "delFlag"
_DEF_REDIS_USER_DB_DELETE_TRUE = "1"
_DEF_REDIS_USER_DB_DELETE_FALSE = "0"
_DEF_REDIS_USER_DB_DELETE_DATE = "delYMDHMS"
_DEF_REDIS_USER_DB_UPDATE_DATE = "updateYMDHMS"
#_DEF_REDIS_USER_DB_USER_SALT = "userSalt"
_DEF_REDIS_USER_DB_ACTIVE = "active"
_DEF_REDIS_USER_DB_DEV_LIST = "devList"
_DEF_REDIS_USER_DB_FRIEND_LIST = "friendList"
_DEF_REDIS_USER_DB_CHAT_LIST = "chatList"
_DEF_REDIS_USER_DB_CMD_LIST = "cmdList"
_DEF_REDIS_USER_DB_POSITION = "position"
_DEF_REDIS_USER_DB_SESSIONID_LIST = "sessionIDList"
_DEF_REDIS_USER_DB_REG_TIME = "regTime"
_DEF_REDIS_USER_DB_LAST_CONN_TIME = "lastConnTime"
_DEF_REDIS_USER_DB_SCHEDULE_TASK = "scheduleTask"
_DEF_REDIS_USER_DB_DAYTIME_TASK = "dayTimeTask"

#default user
_DEF_REDIS_USER_DEFAULT_USER_NAME = "000000"
_DEF_REDIS_USER_DEFAULT_PASSWORD = "000000"

_DEF_REDIS_USER_ID_LENGTH = 5

_DEF_USER_SESSION_EXPIRE_TIME = (30*60) #用户进程过期时间默认是30分钟

#是否允许用户的其他数据保存
_DEF_REDIS_USER_SAVE_NON_PROTECT_KEYS = True
#用户信息中受包含的关键词, 全部小写保护,
_DEF_REDIS_USER_PROTECT_KEYS_LIST =set ([
  "username", "userid", "loginid","user", "name","realname","openid", "regopenid", "modifyopenid", "regid", "modifyid",  "updateid", "updateopenid", "master", "masterid", 
  "passwd", "password", 
  "rolename", "ruleinfo", 
  "avatarid", "mobilephoneno", "province", "city", "area", "address", "addr", 
  "chiefvillageidlist", 
  "email", 
  "pid", 
  "photoid", "photoidfront", "photoidback", "photo", "faceid", 
  "regymdhms", "ymdhms", "modifyymdhms",  "updateymdhms", "lastloginymdhms", "passwordymdhms", 
  "delflag", 
])

#用户信息中不主动给用户的数据
_DEF_REDIS_USER_NOSHOW_KEYS_LIST =set ([
  "realname","modifyid",  "updateid", "updateopenid", "master", "masterid", 
  "passwd", "password", 
  "rolename", "ruleinfo", 
  "pid", 
  "photoid", "photoidfront", "photoidback", "photo", 
  "regymdhms",  "modifyymdhms",  "updateymdhms", "lastloginymdhms", "passwordymdhms", 
  "delflag", 
])


# FILE

_DEF_FILE_INDEX_NAME = "INDEX"
_DEF_FILE_INDEX_FILEID = "FILEID"

_DEF_FILE_REQUEST_TYPE_THUMBNAIL = "thumbnail" #缩略图请求类型

_CONST_MAXSALT_SIMPLELEN="128"

#buffer二级
_DEF_BUFFER_DATA_NAME = "DATA" #数据存储区域
_DEF_BUFFER_STEP_NAME = "STEP" #数据存取的位置区域
_DEF_BUFFER_KEY_TYPE_NAME = "KEYTYPE" #关键词存储区域

_DEF_BUFFER_DATA_KEEP_TIME = 900 #查询缓冲区保存时间, 默认是60*15 15分钟, 900秒
_DEF_BUFFER_DATA_BEGIN_NUM = 0 # 默认一次提供500个数据
_DEF_BUFFER_DATA_END_NUM = 499 #默认一次提供500个数据


#支付相关
#currency 币种,最小单位换算关系
_DEF_CURRENCY_AUD = "AUD" #澳大利亚元
_DEF_CURRENCY_CAD = "CAD" #加拿大元 
_DEF_CURRENCY_CHF = "CHF" #瑞士法郎
_DEF_CURRENCY_CNY = "CNY" #人民币
_DEF_CURRENCY_EUR = "EUR" #欧元
_DEF_CURRENCY_GBP = "GBP" #英镑
_DEF_CURRENCY_HKD = "HKD" #港币
_DEF_CURRENCY_IDR = "IDR" #印尼盾
_DEF_CURRENCY_JPY = "JPY" #日元
_DEF_CURRENCY_KRW = "KRW" #韩国元 
_DEF_CURRENCY_MYR = "MYR" #马来西亚林吉特 
_DEF_CURRENCY_NZD = "NZD" #新西兰元
_DEF_CURRENCY_PHP = "PHP" #菲律宾比索 
_DEF_CURRENCY_SGD = "SGD" #新加坡元 
_DEF_CURRENCY_SUR = "SUR" #俄罗斯卢布
_DEF_CURRENCY_THB = "THB" #泰铢 
_DEF_CURRENCY_USD = "USD" #美元

_DEF_CURRENCY_DEFAULT = _DEF_CURRENCY_CNY #默认币种

_DEF_CURRENCY_UNITS = {
    "CN":{
    _DEF_CURRENCY_AUD:{"unit":"澳元","rate":100},
    _DEF_CURRENCY_CAD:{"unit":"加元","rate":100},
    _DEF_CURRENCY_CHF:{"unit":"法郎","rate":100},
    _DEF_CURRENCY_CNY:{"unit":"元","rate":100},
    _DEF_CURRENCY_EUR:{"unit":"欧元","rate":100},
    _DEF_CURRENCY_GBP:{"unit":"英镑","rate":100},
    _DEF_CURRENCY_HKD:{"unit":"元","rate":100},
    _DEF_CURRENCY_IDR:{"unit":"盾","rate":1},
    _DEF_CURRENCY_JPY:{"unit":"日元","rate":1},
    _DEF_CURRENCY_KRW:{"unit":"韩元","rate":1},
    _DEF_CURRENCY_MYR:{"unit":"吉特","rate":1},
    _DEF_CURRENCY_NZD:{"unit":"新西兰元","rate":100},
    _DEF_CURRENCY_PHP:{"unit":"比索","rate":100},
    _DEF_CURRENCY_SGD:{"unit":"元","rate":100},
    _DEF_CURRENCY_SUR:{"unit":"卢布","rate":100},
    _DEF_CURRENCY_THB:{"unit":"铢","rate":100},
    _DEF_CURRENCY_USD:{"unit":"美元","rate":100},
  },
}["CN"]


#"payType CHAR(16) NOT NULL,",  #支付形式, 现金, 零钱, 银行卡, .信用卡.. ... 
_DEF_PAYTYPE_CASH = "CASH"
_DEF_PAYTYPE_POCKET = "POCKET"
_DEF_PAYTYPE_DEBIT = "DEBIT"
_DEF_PAYTYPE_CREDIT = "CREDIT"

#"payPlatform CHAR(16) NOT NULL,",  #支付平台, 微信, 支付宝, 工商银行等 
_DEF_PAYTYPE_WEXIN = "WEIXIN"
_DEF_PAYTYPE_ALIPAY = "ALIPAY"
_DEF_PAYTYPE_BANK_ICBC = "BANK_ICBC"
_DEF_PAYTYPE_BANK_BOC = "BANK_BOC"
_DEF_PAYTYPE_BANK_COB = "BANK_COB"

#订单类型
#"orderType CHAR(16) NOT NULL,",  #订单类型, 定金, 订金, 余款
_DEF_ORDER_TYPE_DOWNPAYMENT = "DOWNPAYMENT"
_DEF_ORDER_TYPE_DEPOSITE  = "DEPOSITE" #定金
_DEF_ORDER_TYPE_BALANCE  = "BALANCE" #余款
_DEF_ORDER_TYPE_REFUND  = "REFUND" #退款

#订单状态
_DEF_ODRER_STATUS_OPEN = "OPEN" #新发起
_DEF_ORDER_STATUS_CLOSE = "CLOSE" #关闭
_DEF_ORDER_STATUS_INPROGRSS = "INPROGRSS" #过程中
_DEF_ORDER_STATUS_CANCEL = "CANCEL" #删除 
_DEF_ORDER_STATUS_ERROR = "ERROR" #错误 

#general definitions 
_DEF_GE_LOGIC_AND = "AND"
_DEF_GE_LOGIC_OR = "OR"
_DEF_GE_LOGIC_NOT = "NOT"
_DEF_GE_LOGIC_ALL = "ALL"


_DEF_WECHAT_PAY_STATYS_CREATE = "C"  #建立 #weixin 
_DEF_WECHAT_PAY_STATYS_SUCCESS = "S" #成功
_DEF_WECHAT_PAY_STATYS_RECALL = "R" #回退
_DEF_WECHAT_PAY_STATYS_REFUND = "U" #退款
_DEF_WECHAT_PAY_STATYS_CALLBACK_RECEIVED = "B" #数据收到
_DEF_WECHAT_PAY_STATYS_CALLBACK_CREATE = "A" #call back 建立

_DEF_MAX_QUERY_LIMIT_NUM = 5000 #默认最多允许搜索数据个数
_DEF_MIN_QUERY_LIMIT_NUM = 1000 #默认最少允许搜索数据个数
_DEF_MAX_DISP_LIMIT_NUM = 1000 #默认最多显示数据个数
_DEF_BATCH_QUERY_LIMIT_NUM = 5000 #默认每次查询数据个数


_DEF_NICKNAME_PREFIX = "user"
_DEF_NICKNAME_LEN = 12


_DEF_PID_LABEL ="PID"
_DEF_PID_ID_MIN_LENGTH = 18 #身份证长度
_DEF_PID_ID_MAX_LENGTH = 18 #身份证长度

_DEF_TEL_NO_MIN_LENGTH = 8 #电话号码长度
_DEF_TEL_NO_MAX_LENGTH = 20 #电话号码长度

_DEF_PERSONAL_NAME_LABEL = "PNAME"
_DEF_PERSONAL_NAME_MIN_LENGTH = 2 #姓名最小2个汉字
_DEF_PERSONAL_NAME_MAX_LENGTH = 20 #姓名最长20个汉字

_DEF_TEL_LABEL = "TEL"

_DEF_CITY_NAME_LABEL ="CNAME"
_DEF_CITY_NAME_MIN_LENGTH = 2 #城市名称最少2个汉字
_DEF_CITY_NAME_MAX_LENGTH = 15 #城市最长15个汉字

_DEF_AREA_NAME_LABEL ="ANAME"
_DEF_AREA_NAME_MIN_LENGTH = 2 #区县名称最少2个汉字
_DEF_AREA_NAME_MAX_LENGTH = 15 #区县最长15个汉字

_DEF_EMAIL_LABEL ="EMAIL"

_DEF_WHOLE_ADDR_LABEL ="WADDR"


#stock info begin
_DEF_STOCK_MYSQL_TITLE = "STOCK_MYSQL" #transmysql redis key

_DEF_STOCK_PORTFOLIO_CASH_NAME = "CASH" #现金

_DEF_STOCK_KEEP_HISTORY_DATA_DAYS = (365*5) #默认保留5年历史数据
_DEF_STOCK_INDUSTRY_MAPING_DAYS = 90 #默认股票行业映射文件更新间隔90天
_DEF_STOCK_DIVIDEND_DATA_DAYS = 90 #默认股票分红数据文件更新间隔90天
_DEF_STOCK_INDUSTRY_UPDATE_DAYS = 7 #默认股票行业更新间隔7天

_DEF_STOCK_BACKTEST_DAYS = (365*2) #默认回测2年数据

#价值比过滤器判断
_DEF_STOCK_VALUE_SCREEN_SELL = "sell" #价值比大于80%
_DEF_STOCK_VALUE_SCREEN_BUY = "buy" #价值比小于70%
_DEF_STOCK_VALUE_SCREEN_HOLD = "hold" #价值比在70%到80%之间

#EMA趋势过滤器判断
_DEF_STOCK_EMA_TREND_DOWN = "down" #20周EMA趋势过滤器, 趋势向下
_DEF_STOCK_EMA_TREND_UP = "up" #20周EMA趋势过滤器, 趋势向上
_DEF_STOCK_EMA_TREND_HOLD = "hold" #20周EMA趋势过滤器, 趋势无明显变化

#log name
_DEF_GENRAL_MYSQL_LOG_NAME = "mysqllog"

_DEF_LOG_STOCK_WEBAPI_TITLE = "STOCK"
_DEF_LOG_STOCK_WEB_API_NAME = "stockwebapilog"

_DEF_LOG_STOCK_TEST_NAME = "stocktestlog"

_DEF_LOG_STOCK_FETCH_DATA_NAME = "stockfetchdatalog"
_DEF_LOG_STOCK_UPLOAD_DATA_NAME = "stockuploaddatalog"
_DEF_LOG_STOCK_REGULAR_UPDATE_NAME = "stockregularupdatelog"

_DEF_LOG_STOCK_MAIN_LOG_NAME = "stockmainlog"


#stock info end 


