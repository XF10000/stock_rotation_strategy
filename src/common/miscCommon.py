#! /usr/bin/env python
#encoding: utf-8

#Filename: miscCommon.py
#Author: Steven Lian's team
#E-mail: steven.lian@gmail.com
#Date: 2015-12-05
#Description:   通用杂项函数,例如时间,字符串处理等

_VERSION = "20260117"

#common function:
import time, datetime

import sys
if sys.getdefaultencoding() != 'utf-8':
    pass
    #reload(sys)
    #sys.setdefaultencoding('utf-8')

import json
import logging
import bcrypt
import os
import inspect
import struct
#import arrow
import re
import csv
import pickle


#datetime function
def getTime():
    return time.strftime("%Y%m%d%H%M%S", time.localtime())


def getHour():
  return time.strftime("%Y%m%d%H", time.localtime())


def humanTime(strTime):
  t = time.strptime(strTime,'%Y%m%d%H%M%S')
  # return '%02d日%02d时%02d分' % (t[2], t[3], t[4])
  return '%04d-%02d-%02d %02d:%02d:%02d' % (t[0], t[1], t[2], t[3], t[4], t[5])


def getTimeStamp():
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%Y%m%d%H%M%S", local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = "%s.%03d" % (data_head, data_secs)
    return time_stamp


def getHumanTimeStamp():
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = "%s%03d" % (data_head, data_secs)
    return time_stamp


#add the time interval
def diffTime(t1, t2):
    result=0
    try:
        t1 = time.strptime(t1,'%Y%m%d%H%M%S')
        t1 = datetime.datetime(* t1[:6])
        t2 = time.strptime(t2,'%Y%m%d%H%M%S')
        t2 = datetime.datetime(* t2[:6])

        t=t2-t1
        result=t.days*86400+t.seconds # 60*60*24=86400
    except:
        pass
    return result

def diffDays(t1, t2):
    result=0
    try:
        t1 = time.strptime(t1,'%Y%m%d%H%M%S')
        t1 = datetime.datetime(* t1[:6])
        t2 = time.strptime(t2,'%Y%m%d%H%M%S')
        t2 = datetime.datetime(* t2[:6])

        t=t2-t1
        result=t.days
    except:
        pass
    return result

def humanTime2YMDHMS(a):
    if a != "":
        result = a[0:4]+a[5:7]+a[8:10]+a[11:13]+a[14:16]+a[17:19]
    else:
        result = ""
    return result

def str2Time(a):
    a = time.strptime(a,'%Y%m%d%H%M%S')
    a = datetime.datetime(* a[:6])
    return a

def date2YMD(a):
    return datetime.datetime.strftime(a, "%Y%m%d")

#a = "2022-04-06 09:08:32"
def humanTime2Time(a):
    a = time.strptime(a,'%Y-%m-%d %H:%M:%S')
    a = datetime.datetime(* a[:6])
    return a

def time2YMDHMS(a):
    timeArray = time.localtime(a)
    result = time.strftime("%Y%m%d%H%M%S", timeArray)
    return result

def YMDHMS2time(YMDHMS):
    timeArray = time.strptime(YMDHMS,'%Y%m%d%H%M%S')
    result = int(time.mktime(timeArray))
    return result

def addTime(t,interval):
    t = time.strptime(t,'%Y%m%d%H%M%S')
    t = datetime.datetime(* t[:6])
    i = int(interval)
    t = t + datetime.timedelta(seconds=i)
    return t.strftime('%Y%m%d%H%M%S')


def formatTime(a) :
    if a != "":
        result = a[0:4]+'-'+a[4:6]+'-'+a[6:8]+' '+a[8:10]+':'+a[10:12]+':'+a[12:14]
    else:
        result = ""
    return result


def getPassday(nums,YMD=""):
    if YMD:
        t = datetime.datetime.strptime(YMD, "%Y%m%d").date()
    else:
        t = datetime.date.today()
    t = t + datetime.timedelta(-nums)
    t = str(t)
    day = t[0:4]+t[5:7]+t[8:10]
    return day


#获取上一天
def getYesterday():
  t = datetime.date.today()
  t = t + datetime.timedelta(-1)
  t = str(t)
  day = t[0:4]+t[5:7]+t[8:10]
  return day


#获取上一周
def getLastWeek():
  t = datetime.date.today()
  t = t + datetime.timedelta(-7)
  t = str(t)
  day = t[0:4]+t[5:7]+t[8:10]
  return day


#获取上个月
def getLastMonth():
  t = datetime.date.today()
  t = t + datetime.timedelta(-30)
  t = str(t)
  day = t[0:4]+t[5:7]+t[8:10]
  return day


class weekDay:
    __weekParam={
    "range":(0, 1, 2, 3, 4, 5, 6),
    "weekName":{0:"MON", 1:"TUE", 2:"WED", 3:"THU", 4:"FRI", 5:"SAT",6:"SUN", },
    "weekFullName":{0:"Monday", 1:"Tuesday", 2:"Wendesday", 3:"Thursday", 4:"Friday", 5:"Saturday",6:"Sunday", },
    "weekCNName":{0:"星期一", 1:"星期二", 2:"星期三", 3:"星期四", 4:"星期五", 5:"星期六",6:"星期日", },
    "xls":(2, 3, 4, 5, 6, 7, 1)
    }
    def __init__(self, YMDHMS=""):
        if YMDHMS=="":
            YMDHMS=time.strftime("%Y%m%d%H%M%S", time.localtime())
        self.YMDHMS=YMDHMS
        t = time.strptime(YMDHMS,'%Y%m%d%H%M%S')
        self.wday=t[6]
        self.yday=t[7]
        self.year=t[0]
        self.month=t[1]
        self.day=t[2]
        self.hour=t[3]
        self.min=t[4]
        self.wdayName=self.__weekParam["weekName"][self.wday]
        self.wdayFullName=self.__weekParam["weekFullName"][self.wday]
        self.wdayCNName=self.__weekParam["weekCNName"][self.wday]
        self.xlsWDay=self.__weekParam["xls"][self.wday]

    def getCNName(self, wday):
        result=self.__weekParam["weekCNName"][wday]
        return result

    def getName(self, wday):
        result=self.__weekParam["weekName"][wday]
        return result

    def getFullName(self, wday):
        result=self.__weekParam["weekFullName"][wday]
        return result

    def time(self):
        return self.YMDHMS

    def easyFormat(self):
        t=self.YMDHMS[0:4]+"-"+self.YMDHMS[4:6]+"-"+self.YMDHMS[6:8]+ " "+self.YMDHMS[8:10]+":"+self.YMDHMS[10:12]
        return t


class getNow:
    def __init__(self):
        self.now=datetime.datetime.now()
        self.YMDHMS=time.strftime("%Y%m%d%H%M%S", time.localtime())
    def diff(self, t1):
        t=self.now-t1
        result=t.days*86400+t.seconds # 60*60*24=86400
        return result


#log hanlding function
def setlog(title,logfile) :
    logger = logging.getLogger(title)
    #将来日志需要分片，比如按天，按小时分片，需要用到下面这个语句
#    file_handler = TimedRotatingFileHandler(logfile,when="D", interval=1, backupCount=0) #when="D" when="midnight"
#    file_handler.suffix = "%Y%m%d"
    file_handler = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s')
    file_handler.setFormatter(formatter)
    #stream_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(file_handler)
    #logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)
    logger.logFileName = logfile
    return logger


def setLogNew(title, filebasename, homeDir=r"../../log"):
    #logfile = r"../../log/%s" % (filebasename)
    logfile=os.path.join(homeDir, filebasename)
    logger = logging.getLogger(title)
    file_handler = logging.FileHandler(logfile)
    formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    logger.logFileName = logfile
    return logger


# JSON/string function
# the function of JSON format converter, SL:2014/10/3
def jsonDumps(data, ensure_ascii=False, indent = 0):
    if indent > 0:
        return json.dumps(data,separators=(',', ':'), ensure_ascii=ensure_ascii, indent = indent)
    else:
        return json.dumps(data,separators=(',', ':'), ensure_ascii=ensure_ascii)


def jsonLoads(data):
    return json.loads(data)


def str2utf8(str):
  return str.decode("utf8")


def jsonShiftReturn(data):
    result = data.replace("\n","<br/>")
    return result


def jsonResumeReturn(data):
    result = data.replace("<br/>", "\n")
    return result


# HEX CONVERT
# input dec:2-->hex:2, dec 15-->hex f
def hex1(n):
    s=hex(n)
    return s[2::].upper()

# input dec: 2--> hex: 02; dec:1234-->04d2
def bhex(n): #Big-endian
    s=hex(n)
    l=len(s)
    if l % 2==1:
        return "0"+s[2::].upper()
    else:
        return s[2::].upper()


# input dec: 2--> hex: 0002; dec:1234-->04d2
def bhex4(n):#Big-endian for 4 digital
    if n<256:
        return "00"+bhex(n).upper()
    else:
        return bhex(n).upper()

# input dec: 2--> hex: 02; dec:1234-->d204
def lhex(n): #little-endian
    s=bhex(n)
    l=len(s)
    #r=""
    for i in range (0,l,4):
        r=s[2+i:4+i]+s[0+i:2+i]
    return r.upper()


# input dec: 2--> hex: 0200; dec:1234-->d204
def lhex4(n):#little-endian for 4 digital
    if n<256:
        return lhex(n)+"00".upper()
    else:
        return lhex(n).upper()


# input hex: 0200-->2; hex:d204-->1234(dec)
def lhex4toint(s):#little-endian for 4 digital
    nT1=s[2:4]+s[0:2]
    return int(nT1, 16)

# input dec: 2--> hex: 02; dec<256
int2hex256=("00","01","02","03","04","05","06","07","08","09","0A","0B","0C","0D","0E","0F","10","11","12","13","14","15","16","17","18","19","1A","1B","1C","1D","1E","1F","20","21","22","23","24","25","26","27","28","29","2A","2B","2C","2D","2E","2F","30","31","32","33","34","35","36","37","38","39","3A","3B","3C","3D","3E","3F","40","41","42","43","44","45","46","47","48","49","4A","4B","4C","4D","4E","4F","50","51","52","53","54","55","56","57","58","59","5A","5B","5C","5D","5E","5F","60","61","62","63","64","65","66","67","68","69","6A","6B","6C","6D","6E","6F","70","71","72","73","74","75","76","77","78","79","7A","7B","7C","7D","7E","7F","80","81","82","83","84","85","86","87","88","89","8A","8B","8C","8D","8E","8F","90","91","92","93","94","95","96","97","98","99","9A","9B","9C","9D","9E","9F","A0","A1","A2","A3","A4","A5","A6","A7","A8","A9","AA","AB","AC","AD","AE","AF","B0","B1","B2","B3","B4","B5","B6","B7","B8","B9","BA","BB","BC","BD","BE","BF","C0","C1","C2","C3","C4","C5","C6","C7","C8","C9","CA","CB","CC","CD","CE","CF","D0","D1","D2","D3","D4","D5","D6","D7","D8","D9","DA","DB","DC","DD","DE","DF","E0","E1","E2","E3","E4","E5","E6","E7","E8","E9","EA","EB","EC","ED","EE","EF","F0","F1","F2","F3","F4","F5","F6","F7","F8","F9","FA","FB","FC","FD","FE","FF")
def int2hex(n):
    result=None
    if n<256 and n>=0:
        result= int2hex256[n]
    return result

# input dec: 2--> hex: 40; dec<256
int2lhex256=("00","80","40","C0","20","A0","60","E0","10","90","50","D0","30","B0","70","F0","08","88","48","C8","28","A8","68","E8","18","98","58","D8","38","B8","78","F8","04","84","44","C4","24","A4","64","E4","14","94","54","D4","34","B4","74","F4","0C","8C","4C","CC","2C","AC","6C","EC","1C","9C","5C","DC","3C","BC","7C","FC","02","82","42","C2","22","A2","62","E2","12","92","52","D2","32","B2","72","F2","0A","8A","4A","CA","2A","AA","6A","EA","1A","9A","5A","DA","3A","BA","7A","FA","06","86","46","C6","26","A6","66","E6","16","96","56","D6","36","B6","76","F6","0E","8E","4E","CE","2E","AE","6E","EE","1E","9E","5E","DE","3E","BE","7E","FE","01","81","41","C1","21","A1","61","E1","11","91","51","D1","31","B1","71","F1","09","89","49","C9","29","A9","69","E9","19","99","59","D9","39","B9","79","F9","05","85","45","C5","25","A5","65","E5","15","95","55","D5","35","B5","75","F5","0D","8D","4D","CD","2D","AD","6D","ED","1D","9D","5D","DD","3D","BD","7D","FD","03","83","43","C3","23","A3","63","E3","13","93","53","D3","33","B3","73","F3","0B","8B","4B","CB","2B","AB","6B","EB","1B","9B","5B","DB","3B","BB","7B","FB","07","87","47","C7","27","A7","67","E7","17","97","57","D7","37","B7","77","F7","0F","8F","4F","CF","2F","AF","6F","EF","1F","9F","5F","DF","3F","BF","7F","FF")
def int2lhex(n):
    result=None
    if n<256 and n>=0:
        result=int2lhex256[n]
    return result


def int2lhex4(n):
    result=None
    if n<65536 and n>0:
        n1=n/256
        n2=n%256
        result=int2hex(n2)+int2hex(n1)
    return result


def strReverse (s):
    return s[::-1]


int2bin256=("00000000","00000001","00000010","00000011","00000100","00000101","00000110","00000111","00001000","00001001","00001010","00001011","00001100","00001101","00001110","00001111","00010000","00010001","00010010","00010011","00010100","00010101","00010110","00010111","00011000","00011001","00011010","00011011","00011100","00011101","00011110","00011111","00100000","00100001","00100010","00100011","00100100","00100101","00100110","00100111","00101000","00101001","00101010","00101011","00101100","00101101","00101110","00101111","00110000","00110001","00110010","00110011","00110100","00110101","00110110","00110111","00111000","00111001","00111010","00111011","00111100","00111101","00111110","00111111","01000000","01000001","01000010","01000011","01000100","01000101","01000110","01000111","01001000","01001001","01001010","01001011","01001100","01001101","01001110","01001111","01010000","01010001","01010010","01010011","01010100","01010101","01010110","01010111","01011000","01011001","01011010","01011011","01011100","01011101","01011110","01011111","01100000","01100001","01100010","01100011","01100100","01100101","01100110","01100111","01101000","01101001","01101010","01101011","01101100","01101101","01101110","01101111","01110000","01110001","01110010","01110011","01110100","01110101","01110110","01110111","01111000","01111001","01111010","01111011","01111100","01111101","01111110","01111111","10000000","10000001","10000010","10000011","10000100","10000101","10000110","10000111","10001000","10001001","10001010","10001011","10001100","10001101","10001110","10001111","10010000","10010001","10010010","10010011","10010100","10010101","10010110","10010111","10011000","10011001","10011010","10011011","10011100","10011101","10011110","10011111","10100000","10100001","10100010","10100011","10100100","10100101","10100110","10100111","10101000","10101001","10101010","10101011","10101100","10101101","10101110","10101111","10110000","10110001","10110010","10110011","10110100","10110101","10110110","10110111","10111000","10111001","10111010","10111011","10111100","10111101","10111110","10111111","11000000","11000001","11000010","11000011","11000100","11000101","11000110","11000111","11001000","11001001","11001010","11001011","11001100","11001101","11001110","11001111","11010000","11010001","11010010","11010011","11010100","11010101","11010110","11010111","11011000","11011001","11011010","11011011","11011100","11011101","11011110","11011111","11100000","11100001","11100010","11100011","11100100","11100101","11100110","11100111","11101000","11101001","11101010","11101011","11101100","11101101","11101110","11101111","11110000","11110001","11110010","11110011","11110100","11110101","11110110","11110111","11111000","11111001","11111010","11111011","11111100","11111101","11111110","11111111",)
def int2bin(n):
    result=None
    if n<256 and n>=0:
        result=int2bin256[n]
    return result


#计算1在一个字节中出现的次数
count1in256=(0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8,)
def parity(n):
    result=None
    if n<256 and n>=0:
        result= count1in256[n]
    return result


#to determine if a char is a hex
def isHex(s):
    result=False
    list=["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "A", "B", "C", "D", "E", "F"]
    if s in list:
        result=True
    return result


#to determine if a char is a hex
def isDec(s):
    result=False
    list=["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    if s in list:
        result=True
    return result


def isStr(data):
    result = False
    dataType = isinstance(data, (str, str))
    if dataType == str:
        result = True
    return result


def hex2bcd(data):
    result = ""
    aList = []
    for a in data:
        aList.append(bhex(ord(a)))
    result = ''.join(aList)
    return result


#to read the data that powerPrepareData.py generated.
def loadJsonData(fileName, typeStyle="list", encoding = 'utf-8'):
    result = None
    try:
      with open (fileName, "r",encoding = encoding) as hFile:
            if typeStyle == "list":
                lines=hFile.readlines()
                result=[]
                for a in lines:
                    set1=json.loads(a)
                    result.append(set1)
            else:
                result = json.load(hFile)
    except Exception as e:
        pass

    return result


def saveJsonData(fileName, dataList, ensure_ascii=False, indent = 0):
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


def savePklData(fileName, data):
    with open(fileName, 'wb') as hFile:
       pickle.dump(data, hFile)
    pass


def loadPklData(fileName):
    result = 0
    if os.path.isfile(fileName):
        with open(fileName, 'rb') as hFile:
            result = pickle.load(hFile)
    return result


# security user password login /etc
def genSalt():
    return bcrypt.gensalt()


def genHashPasswd(passwd):
    salt=genSalt()
    #salt is in first half of hashed data
    if sys.version_info.major <= 2:
         passwd = passwd.encode()
    else:
        if isinstance(passwd, str):
            passwd = passwd.encode()
    hashed=bcrypt.hashpw(passwd, salt)
    if sys.version_info.major <= 2:
        if isinstance(hashed, unicode):
            hashed = hashed.encode("UTF-8")
    else:
        if isinstance(hashed, bytes):
            hashed = hashed.decode()
    return hashed


def checkPasswd(passwd, hashed):
    if sys.version_info.major <= 2:
        if  isinstance(passwd, unicode):
            passwd = passwd.encode("UTF-8")
        if isinstance(hashed, unicode):
            hashed = hashed.encode("UTF-8")
    else:
        if isinstance(passwd, str):
            passwd = passwd.encode()
        if isinstance(hashed, str):
            hashed = hashed.encode()
    result = bcrypt.checkpw(passwd, hashed)
    return result


def decodeU8(data):
    fmt = "<B"
    try:
        result = struct.unpack(fmt,data)
    except:
        result = data
    return result[0]


def decodeI8(data):
    fmt = "<b"
    try:
        result = struct.unpack(fmt,data)
    except:
        result = data
    return result[0]


def decodeU16(data):
    fmt = "<H"
    result = struct.unpack(fmt,data)
    return result[0]


def decodeI16(data):
    fmt = "<h"
    result = struct.unpack(fmt,data)
    return result[0]

def decodeU32(data):
    fmt = "<L"
    result = struct.unpack(fmt,data)
    return result[0]


def decodeI32(data):
    fmt = "<l"
    result = struct.unpack(fmt,data)
    return result[0]


def decodeU64(data):
    fmt = "<Q"
    result = struct.unpack(fmt,data)
    return result[0]


def decodeI64(data):
    fmt = "<q"
    result = struct.unpack(fmt,data)
    return result[0]


def encodeU8(data):
    fmt = "<B"
    result = struct.pack(fmt,data)
    return result


def encodeI8(data):
    fmt = "<b"
    result = struct.pack(fmt,data)
    return result


def encodeU16(data):
    fmt = "<H"
    result = struct.pack(fmt,data)
    return result


def encodeI16(data):
    fmt = "<h"
    result = struct.pack(fmt,data)
    return result


def encodeU32(data):
    fmt = "<L"
    result = struct.pack(fmt,data)
    return result


def encodeI32(data):
    fmt = "<l"
    result = struct.pack(fmt,data)
    return result


def encodeU64(data):
    fmt = "<Q"
    result = struct.pack(fmt,data)
    return result


def encodeI64(data):
    fmt = "<q"
    result = struct.pack(fmt,data)
    return result


def getOSPIDInfo():
    result = {}
    result["PID"] = str(os.getpid())
    result["startPath"] = inspect.stack()[1][1]
    result["YMDHMS"] = time.strftime("%Y%m%d%H%M%S", time.localtime())
    return result


BIT = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九']
UNIT = ['', '十', '百', '千']
SECOND_UNIT = ['', '万', '亿']
def digit2CN(num):
    result = ''
    strNum = str(num)
    unitStep = -1
    for i in range(-1, -len(strNum) - 1, -4):
        unitStep += 1
        digits = strNum[i : i - 4 : -1]
        if digits == '0000':continue
        digitStr = ''
        for j in range(0, len(digits)):
            d = int(digits[j])
            digitStr = BIT[d] + (d > 0 and UNIT[j] or '') + digitStr
        digitStr = re.sub('零{2,}', '零', digitStr)
        if digitStr[-1] == '零':
            digitStr = digitStr[ : -1]
        result = digitStr + SECOND_UNIT[unitStep] + result
    return result


def dictConvertFromBytes(dataSet):
    result = {}
    for k, v in dataSet.items():
        if isinstance(k, bytes):
            newKey = k.decode()
        if isinstance(v, bytes):
            newVal = v.decode()
        result[newKey] = newVal
    return result


def write2CSV(fileName, dataList, method="w"):
    with open(fileName, method, newline='') as csvFile:
        writer = csv.writer(csvFile)
        for row in dataList:
            writer.writerow(row)


def getFileList(baseDir):
    result = []

    for home, dirs, files in os.walk(baseDir):
        for filename in files:
            # 文件名列表，包含完整路径
            result.append(os.path.join(home, filename))
            # # 文件名列表，只包含文件名
            # result.append( filename)

    return result


def getFileCreatTime(filePath):
    ctime = os.path.getctime(filePath)
    timeStruct = time.localtime(ctime)
    result =  time.strftime('%Y%m%d%H%M%S',timeStruct)
    return result


#两个list 求交集，并集，差集
#求交集
def listIntersection(listA,listB):
    return list(set(listA).intersection(set(listB)))

#求并集
def listUnion(listA,listB):
    return list(set(listA).union(set(listB)))

#求差集，在A中但不在B中
def listDiff(listA,listB):
    return list(set(listA).difference(set(listB)))
