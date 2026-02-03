#! /usr/bin/env python
#coding=utf-8

#add src directory
# ver: 2020-05-26

_VERSION="20260130"

_DEBUG = True

import sqlite3

if _DEBUG:
    import os
    import sys
    parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, parentdir)
    from common import miscCommon as misc

    if "_LOG" not in dir() or not _LOG:
        _LOG = misc.setLogNew("SQLLiTE", "sqllitelog")
        _LOG.info("sqlliteHandle is running")
 
fetchManyBatchNumDefault = 2000  # fetchMany的默认值

def getSqlliteDB(dbFile):
    with sqlite3.connect(dbFile) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
    return conn


class sqliteHandle:
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
        cursor = self.dbRCursor
        try:
            if values == ():
                dbCursor = cursor.execute(sqlstr)
            else:
                #占位符替换, sqlite 用 ? 代替 %s
                sqlstr = sqlstr.replace("%s","?")
                dbCursor = cursor.execute(sqlstr, values)
            result = dbCursor.rowcount
            
        except Exception as e:
            result = -2
            # errMsg = f"executeRead:{e}"
            errMsg = f"executeRead:{e},sql:{sqlstr},{values}"
            self.lastErrMsg = errMsg
            if _DEBUG:
                _LOG.error(f"{errMsg}")
            
        return result


    # 执行写的操作
    def executeWrite(self, sqlstr, values =()):
        result = 0
        cursor = self.dbWCursor
        try:
            if values == ():
                dbCursor = cursor.execute(sqlstr)
            else:
                #占位符替换, sqlite 用 ? 代替 %s
                sqlstr = sqlstr.replace("%s","?")
                dbCursor = cursor.execute(sqlstr, values)

            if self.autoCommitFlag:
                self.dbW.commit()                
            result = dbCursor.rowcount

        except Exception as e:
            result = -2
            # errMsg = f"executeWrite:{e}"
            errMsg = f"executeWrite:{e},sql:{sqlstr},{values}"
            self.lastErrMsg = errMsg
            if _DEBUG:
                _LOG.error(f"{errMsg}")

        return result

    # 执行读的系列操作
    def executeReadList(self, sqllist):
        result = []
        cursor = self.dbRCursor
        try:
            for sqlstr, values in sqllist:
                if values == ():
                    dbCursor = cursor.execute(sqlstr)
                else:
                    #占位符替换, sqlite 用 ? 代替 %s
                    sqlstr = sqlstr.replace("%s","?")
                    dbCursor = cursor.execute(sqlstr, values)
                ret = dbCursor.rowcount

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
        cursor = self.dbWCursor
        try:
            for sqlstr, values in sqllist:
                if values == ():
                    dbCursor = cursor.execute(sqlstr)
                else:
                    #占位符替换, sqlite 用 ? 代替 %s
                    sqlstr = sqlstr.replace("%s","?")
                    dbCursor = cursor.execute(sqlstr, values)
                ret = dbCursor.rowcount
                    
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
        result = []
        rows = self.dbRCursor.fetchall()
        # 现在每行都是 Row 对象，可以像字典一样访问
        for row in rows:
            item = dict(row)
            result.append(item)
        return result

    def fetchMany(self, num = fetchManyBatchNumDefault):
        result = []
        # if num:
        #     self.fetchManyBatchNum  = num
        rows = self.dbRCursor.fetchmany(num)
        for row in rows:
            item = dict(row)
            result.append(item)
        return result

    def fetchOne(self):
        result = []
        row = self.dbRCursor.fetchone()
        if row:
            item = dict(row)
            result.append(item)
        return result

    def scroll(self, rownum, mode = "relative"):
        result = self.dbRCursor.scroll(rownum, mode = mode)
        return result

    def rollbackRead(self, rownum, mode = "relative"):
        self.dbR.rollback()

    def rollbackWrite(self, rownum, mode = "relative"):
        self.dbW.rollback()
        
    # 关闭
    def close(self):
        self.dbW.close()
        self.dbR.close()


if __name__ == "__main__":
    #dbR = getSqlliteDB("localhost","testuser","test123","testdb")
    #dbW = getSqlliteDB("localhost","testuser","test123","testdb")
    dbW = getSqlliteDB("test.db")
    dbR = getSqlliteDB("test.db")
    sqlliteDB = sqlliteHandle(dbW=dbW,dbR=dbR)
    print (dbW)
    print (dbR)
    print (sqlliteDB)
    # sqlstr = """CREATE TABLE EMPLOYEE (
    #      FIRST_NAME  CHAR(20) NOT NULL,
    #      LAST_NAME  CHAR(20),
    #      AGE INT,  
    #      SEX CHAR(1),
    #      INCOME FLOAT )"""
    # data = sqlliteDB.executeWrite(sqlstr)
    # sqlstr ="""INSERT INTO EMPLOYEE(FIRST_NAME,
    #      LAST_NAME, AGE, SEX, INCOME)
    #      VALUES ('Mac', 'Mohan', 20, 'M', 2000)"""
    # data = sqlliteDB.executeWrite(sqlstr)
    sqlstr = "select * from EMPLOYEE" 
    nums = sqlliteDB.executeRead(sqlstr)
    data = sqlliteDB.fetchMany(2)
    data = sqlliteDB.fetchOne()
    data = sqlliteDB.fetchAll()
    print(data)

