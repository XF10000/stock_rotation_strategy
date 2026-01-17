#! /usr/bin/env python
#coding=utf-8

#add src directory
# ver: 2020-05-26

_VERSION="20230716"

_DEBUG = True

import pymysql

if _DEBUG:
    import os
    import sys
    parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, parentdir)
    from common import miscCommon as misc

    if "_LOG" not in dir() or not _LOG:
        _LOG = misc.setLogNew("pymysql", "pymysqllog")
        _LOG.info("pymysql is running")
 
fetchManyBatchNumDefault = 2000  # fetchMany的默认值

def getMysqlDB(host,user,passwd,db,port = 3306,chatset="utf8mb4"):
    mysqlDB = pymysql.connect(host = host,port = port, 
      user = user,password = passwd,
      db = db,
      charset=chatset,
      cursorclass=pymysql.cursors.DictCursor)
    mysqlDB.autocommit(True)
    return mysqlDB


class mysqlHandle:
    lastErrMsg = ""
    lastRtnMsg = ""
    autoCommitFlag = True
    def __init__(self,dbW,dbR,autoCommitFlag = True):
        self.dbW = dbW
        self.dbR = dbR
        self.dbWCursor = self.dbW.cursor()
        self.dbRCursor = self.dbR.cursor()
        self.fetchManyBatchNum = fetchManyBatchNumDefault # fetchMany的值 
        self.autoCommitFlag = autoCommitFlag
    
    #connection 相关命令
    # 执行读的操作
    def executeRead(self, sqlstr, values =()):
        result = 0
        self.dbR.ping(True)
        cursor = self.dbRCursor
        try:
            if values == ():
                result = cursor.execute(sqlstr)
            else:
                result = cursor.execute(sqlstr, values)
            
        except Exception as e:
            result = -1
            # errMsg = f"executeRead:{e}"
            errMsg = f"executeRead:{e},sql:{sqlstr},{values}"
            self.lastErrMsg = errMsg
            if _DEBUG:
                _LOG.error(f"{errMsg}")
            
        return result

    # 执行写的操作
    def executeWrite(self, sqlstr, values =()):
        result = 0
        self.dbW.ping(True)
        cursor = self.dbWCursor
        try:
            if values == ():
                result = cursor.execute(sqlstr)
            else:
                result = cursor.execute(sqlstr, values)

            if self.autoCommitFlag:
                self.dbW.commit()                

        except Exception as e:
            result = -1
            # errMsg = f"executeWrite:{e}"
            errMsg = f"executeWrite:{e},sql:{sqlstr},{values}"
            self.lastErrMsg = errMsg
            if _DEBUG:
                _LOG.error(f"{errMsg}")

        return result

    # 执行读的系列操作
    def executeReadList(self, sqllist):
        result = []
        self.dbR.ping()
        cursor = self.dbRCursor
        try:
            for sqlstr, values in sqllist:
                if values == ():
                    ret = cursor.execute(sqlstr)
                else:
                    ret = cursor.execute(sqlstr, values)

                result.append(ret)

        except Exception as e:
            result = -1
            # errMsg = f"executeReadList:{e}"
            errMsg = f"executeReadList:{e},sql:{sqlstr},{values}"
            self.lastErrMsg = errMsg
            if _DEBUG:
                _LOG.error(f"{errMsg}")

        return result
            
    # 执行写的系列操作
    def executeWriteList(self, sqllist):
        result = []
        self.dbW.ping()
        cursor = self.dbWCursor
        try:
            for sqlstr, values in sqllist:
                if values == ():
                    ret = cursor.execute(sqlstr)
                else:
                    ret = cursor.execute(sqlstr, values)
                    
                result.append(ret)

            if self.autoCommitFlag:
                self.dbW.commit()                

        except Exception as e:
            result = -1
            # errMsg = f"executeWriteList:{e}"
            errMsg = f"executeWriteList:{e},sql:{sqlstr},{values}"
            self.lastErrMsg = errMsg
            if _DEBUG:
                _LOG.error(f"{errMsg}")

        return result


    def insertID(self):
        return self.dbWCursor.lastrowid
        

    def fetchAll(self):
#        self.dbR.ping()
        result = self.dbRCursor.fetchall()
        return result

    def fetchMany(self, num = fetchManyBatchNumDefault):
        if num:
            self.fetchManyBatchNum  = num
        self.dbR.ping()
        result = self.dbRCursor.fetchmany(num)
        return result

    def fetchOne(self):
#        self.dbR.ping()
        result = self.dbRCursor.fetchone()
        return result

    def scroll(self, rownum, mode = "relative"):
#        self.dbR.ping()
        result = self.dbRCursor.scroll(rownum, mode = mode)
        return result

    def rollbackRead(self, rownum, mode = "relative"):
#        self.dbR.ping()
        self.dbR.rollback()

    def rollbackWrite(self, rownum, mode = "relative"):
#        self.dbW.ping()
        self.dbW.rollback()
        
    # 关闭
    def close(self):
        self.dbW.close()
        self.dbR.close()

if __name__ == "__main__":
    #dbR = getMysqlDB("localhost","testuser","test123","testdb")
    #dbW = getMysqlDB("localhost","testuser","test123","testdb")
    dbW = getMysqlDB("192.168.50.100","testadmin","test123","test")
    dbR = getMysqlDB("192.168.50.100","testadmin","test123","test")
    mysqlDB = mysqlHandle(dbW=dbW,dbR=dbR)
    print (dbW)
    print (dbR)
    print (mysqlDB)
    sqlstr = """CREATE TABLE EMPLOYEE (
         FIRST_NAME  CHAR(20) NOT NULL,
         LAST_NAME  CHAR(20),
         AGE INT,  
         SEX CHAR(1),
         INCOME FLOAT )"""
    data = mysqlDB.executeWrite(sqlstr)
    sqlstr ="""INSERT INTO EMPLOYEE(FIRST_NAME,
         LAST_NAME, AGE, SEX, INCOME)
         VALUES ('Mac', 'Mohan', 20, 'M', 2000)"""
    data = mysqlDB.executeWrite(sqlstr)
    sqlstr = "select * from EMPLOYEE" 
    nums = mysqlDB.executeRead(sqlstr)
    data = mysqlDB.fetchAll()

