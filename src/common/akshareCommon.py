#! /usr/bin/env python
#encoding: utf-8

#Filename: akshareCommon.py
#Author: Steven Lian's team
#E-mail: steven.lian@gmail.com/xie_frank@163.com
#Date: 2025-02-05
#Description:   akshare common functions

# https://github.com/akfamily/akshare
# https://akshare.akfamily.xyz/data/stock/stock.html

#所有股票内容, symbol = 纯数字代码, 其他英文内容均采用小写,并用"_"连接

_VERSION = "20260125"

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import traceback

import akshare as ak
import pandas as pd

from common import miscCommon as misc


#common functions begin
'''
股票代码转换,数字到sina代码转换
'''
def symbol2symboleWithMarket(symbol):
    result = symbol
    try:
        if symbol.startswith("6") or symbol.startswith("9") or symbol.startswith("5") or symbol.startswith("1"):
            # 上海：6主板，9科创板，5基金，1债券
            result = "sh" + symbol
        elif symbol.startswith("0") or symbol.startswith("3") or symbol.startswith("2"):
            # 深圳：0主板，3创业板，2B股
            result = "sz" + symbol
        elif symbol.startswith("4") or symbol.startswith("8"):
            # 北交所：43、83、87、88开头
            result = "bj" + symbol
        else:
            # 其他情况，如B股（900开头是上海B股，200开头是深圳B股）
            # 但上面的代码已经覆盖了主要情况
            pass
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"

    return result


'''
股票代码转换,sina代码到数字转换
'''
def symbolWithMarket2symbole(symbol):
    result = symbol
    try:
        if symbol.startswith("sh"):
            #上海,基金
            result = symbol[2:] 
        elif symbol.startswith("sz"):
            #深圳
            result = symbol[2:] 
        elif symbol.startswith("bj"):
            #北交所
            result = symbol[2:] 
        # elif symbol[0:2] in ["83", "87", "88"]:
        #     result = "cu" + symbol[2:]
        # elif symbol[0:3] in ["920"]:
        #     result = "fn" + symbol[3:]
        else:
            pass
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


def getStockList():
    result = pd.DataFrame()
    try:
        result = ak.stock_zh_a_spot()
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


def getStockSymboleNameList():
    result = []
    try:
        stock_zh_ah_name_df = ak.stock_zh_ah_name()
        stock_list = stock_zh_ah_name_df.to_dict(orient='records')
        for stockInfo in stock_list:
            details = identifyMarket(stockInfo['代码'])
            details["stock_name"] = stockInfo['名称']
            result.append(details)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


def identifyMarket(symbol):
    """
    根据股票代码识别所属市场
    返回：市场名称字符串
    """
    result = {}
    try:
        code_str = str(symbol).strip()
        code_str = code_str.lower()
        
        # 处理包含市场后缀的代码（如 sh600000）
        if code_str.startswith(('sh', 'sz', 'bj')):
            prefix_map = {'sh': '沪市A股', 'sz': '深市A股', 'bj': '北交所A股','uk':'未知市场'}
            marketCode = code_str[2:]
            marketPrefix = prefix_map.get(code_str[:2], 'uk')
            marketName = prefix_map.get(marketPrefix, '未知市场')
            result['market_code'] = marketCode
            result['market_prefix'] = marketPrefix
            result['market_name'] = marketName
        
        # 判断纯数字代码
        if code_str.isdigit():
            # A股判断
            if len(code_str) == 6:
                if code_str.startswith('6'):  # 沪市主板/科创板
                    result['market_code'] = code_str
                    result['market_prefix'] = "sh"
                    if code_str.startswith('688'):
                        result['market_name'] = "沪市科创板"
                    else:
                        result['market_name'] = "沪市A股"

                elif code_str.startswith(('0', '3')):  # 深市
                    result['market_code'] = code_str
                    result['market_prefix'] = "sz"
                    if code_str.startswith('3'):
                        result['market_name'] = "深市创业板"
                    else:
                        result['market_name'] = "沪市科创板"
                elif code_str.startswith(('43', '83', '87', '88')):  # 新三板
                    result['market_code'] = code_str
                    result['market_prefix'] = "xsb"
                    result['market_name'] = "新三板"
                elif code_str.startswith('8'):  # 北交所
                    result['market_code'] = code_str
                    result['market_prefix'] = "bjs"
                    result['market_name'] = "北交所A股"
            # 港股判断（通常5位，也有4位）
            elif 4 <= len(code_str) <= 5:
                result['market_code'] = code_str
                result['market_prefix'] = "hk"
                result['market_name'] = "港股"
        
        # 判断美股（通常为纯字母，1-5个字符）
        elif code_str.isalpha() and 1 <= len(code_str) <= 5:
            result['market_code'] = code_str
            result['market_prefix'] = "usa"
            result['market_name'] = "美股"
        else:        
        # 其他情况
            result['market_code'] = code_str
            result['market_prefix'] = "uk"
            result['market_name'] = "未知市场"
        
        result["symbol"] = result['market_code']

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


def getStockIndividualInfo(symbol):
    result = pd.DataFrame()
    try:
        result = ak.stock_individual_info_em(symbol=symbol)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取申万三级行业数据
def swGetIndustryThirdList():
    result = []
    try:
        sw_third_list = ak.sw_index_third_info() # 返回包含行业代码和名称的DataFrame[citation:1]
        sw_third_list.rename(columns={'行业代码': 'industry_code', '行业名称': 'industry_name','上级行业':'parenet_industry',
        '成份个数':'num_of_constituents','静态市盈率':'static_PE_ratio','TTM(滚动)市盈率':'TTM_PE_ratio',
        '市净率':'PB_ratio','静态股息率':'static_divident_yield'}, inplace=True)
        sw_third_list = sw_third_list.to_dict(orient='records')
        for industryInfo in sw_third_list:
            industry_code = industryInfo['industry_code']
            aList = industry_code.split(".")
            if len(aList) == 2:
                industryInfo["industry_symbol"] = aList[0]
                industryInfo["market_prefix"] = aList[1]
            else:
                industryInfo["industry_symbol"] = ""
                industryInfo["market_prefix"] = ""
            result.append(industryInfo)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取申万二级行业数据
def swGetIndustryList():
    result = []
    try:
        # 获取申万二级行业信息
        sw_industry = ak.sw_index_second_info()
        sw_industry.rename(columns={'行业代码': 'industry_code', '行业名称': 'industry_name','上级行业':'parenet_industry',
        '成份个数':'num_of_constituents','静态市盈率':'static_PE_ratio','TTM(滚动)市盈率':'TTM_PE_ratio',
        '市净率':'PB_ratio','静态股息率':'static_divident_yield'}, inplace=True)
        sw_industry = sw_industry.to_dict(orient='records')
        for industryInfo in sw_industry:
            industry_code = industryInfo['industry_code']
            aList = industry_code.split(".")
            if len(aList) == 2:
                industryInfo["industry_symbol"] = aList[0]
                industryInfo["market_prefix"] = aList[1]
            else:
                industryInfo["industry_symbol"] = ""
                industryInfo["market_prefix"] = ""
            result.append(industryInfo)
        pass
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取申万行业包含的股票数据
def swGetIndustryConstituents(industry_symbol):
    try:
        stockList = ak.index_component_sw(symbol=industry_symbol)
        stockList.rename(columns={'序号': 'id', '证券代码': 'symbol', '证券名称': 'stock_name','最新权重':'last_weight',
        '计入日期':'date'}, inplace=True)
        result = stockList.to_dict(orient='records')
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取申万行业数据
def swGetStockInfoData():
    result = {}
    try:
        #获取申万行业数据
        industryList = swGetIndustryList()
        for industryInfo in industryList:
            industry_symbol = industryInfo["industry_symbol"]
            #获取行业包含的股票
            
            tryTimes = 3
            while tryTimes > 0:
                stockList = swGetIndustryConstituents(industry_symbol)
                if len(stockList) > 0:
                    break
                tryTimes -= 1
                misc.time.sleep(0.5)

            for stockInfo in stockList:
                item = {}
                symbol = stockInfo["symbol"]

                #股票信息
                item["symbol"] = symbol
                item["stock_name"] = stockInfo["stock_name"]
                item["last_weight"] = stockInfo["last_weight"]
                item["date"] = stockInfo["date"].strftime(f"%Y-%m-%d")

                #行业信息,以申万二级行业为准
                item["industry_name"] = industryInfo["industry_name"]
                item["industry_code"] = industry_symbol

                item["industry_name_sw"] = industryInfo["industry_name"]
                item["industry_code_sw"] = industry_symbol

                item["industry_type"] = "申银万国"               
                item["industry_type_sw"] = "申银万国"               

                #时间信息
                item["YMDMHS"] = misc.getTime()
                
                result[symbol] = item
                pass
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取申万指数历史行情
# period = "day", "week", "month" #日k线、周k线、月k线
def swGetIndexHistory(index_symbol,period="week",start_date="20200101"):
    result = []
    try:   
        indexHistory = ak.index_hist_sw(symbol=index_symbol,period=period)
        indexHistory.rename(columns={'日期': 'date', '代码': 'symbol','开盘':'open','收盘':'close','最高':'high','最低':'low',
        '成交量':'volume','成交额':'amount'}, inplace=True)
        indexHistory = indexHistory.to_dict(orient='records')
        for item in indexHistory:
            item["date"] = item["date"].strftime(f"%Y-%m-%d")
            YMD = item["date"].replace("-","")
            if YMD > start_date:
                result.append(item)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取东方财富行业数据
def emGetIndustryList():
    result = []
    try:
        industryList = ak.stock_board_industry_name_em() # 返回包含行业代码和名称的DataFrame[citation:1]
        industryList.rename(columns={'排名': 'id', '板块代码': 'industry_code', '板块名称': 'industry_name','最新价':'last_price',
        '涨跌额':'change','涨跌幅':'pct_change','总市值':'total_market_cap','领涨股票':'top_gainer',
        '换手率':'turnover_rate','上涨家数':'advancers','下跌家数':'decliners','领涨股票-涨跌幅':'TG_change'}, inplace=True)
        result = industryList.to_dict(orient='records')
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取行业包含的股票
def emGetIndustryConstituents(industry_name):
    result = []
    try:
        stockList = ak.stock_board_industry_cons_em(symbol = industry_name)
        stockList.rename(columns={'序号': 'id', '代码': 'symbol', '名称': 'stock_name','最新价':'last_price',
        '涨跌额':'change','涨跌幅':'pct_change','成交量':'volume','成交额':'amount', '今开':'open','振幅':'range', '最高':'high','最低':'low',
        '昨收':'prev_close','换手率':'turnover_rate','市盈率-动态':'forward_PE_ratio','市净率':'PB_ratio'}, inplace=True)
        result = stockList.to_dict(orient='records')
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


def emGetStockInfoData():
    result = {}
    try:
        #获取东方财富行业数据
        industryList = emGetIndustryList()
        for industryInfo in industryList:
            industry_name = industryInfo["industry_name"]
            #获取行业包含的股票
            stockList = emGetIndustryConstituents(industry_name)
            for stockInfo in stockList:
                item = {}
                symbol = stockInfo["symbol"]

                #股票信息
                item["symbol"] = symbol
                item["stock_name"] = stockInfo["stock_name"]
                item["last_price"] = stockInfo["last_price"]
                item["forward_PE_ratio"] = stockInfo["forward_PE_ratio"]
                item["PB_ratio"] = stockInfo["PB_ratio"]

                #行业信息
                # item["industry_name"] = industryInfo["industry_name"]
                # item["industry_code"] = industryInfo["industry_code"]
                item["industry_name_em"] = industryInfo["industry_name"]
                item["industry_code_em"] = industryInfo["industry_code"]
                item["industry_type_em"] = "东方财富"               

                #时间信息
                item["YMDMHS"] = misc.getTime()
                
                result[symbol] = item
                pass
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#common functions end

#realtime data begin
def getStockBidAskData(symbol):
    result = pd.DataFrame()
    try:
        result = ak.stock_bid_ask_em(symbol=symbol)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result

#realtime data end

#history data begin
'''
获取历史数据
symbol = "601088" #股票代码
startDate = "20240101" endDate = "20240102", YMD格式
period = "daily", "weekly", "monthly" #日k线、周k线、月k线
adjust = "qfq","hfq","" #前复权、后复权、不复权
'''
def gmGetHistroryData(symbol,startDate,endDate,period="daily",adjust=""):
    result = []
    try:
        stockDataList = ak.stock_zh_a_hist(symbol=symbol, period=period, start_date=startDate, end_date=endDate, adjust=adjust)
        stockDataList.rename(columns={'日期': 'date', '股票代码': 'symbol','开盘':'open','收盘':'close','最高':'high','最低':'low',
        '成交量':'volume','成交额':'amount','振幅':'range','涨跌幅':'pct_change','涨跌额':'change','换手率':'turnover_rate'}, inplace=True)
        stockDataList = stockDataList.to_dict(orient='records')
        for item in stockDataList:
            item["date"] = item["date"].strftime(f"%Y-%m-%d")
            result.append(item)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        result = None
    return result


#腾讯历史数据
def txGetHistroryData(symbol,startDate,endDate,period="daily",adjust=""):
    result = []
    try:
        #首先转换symbol为腾讯股票代码
        txSymbol = symbol2symboleWithMarket(symbol)       
        stockDataList = ak.stock_zh_a_hist_tx(symbol=txSymbol, start_date=startDate, end_date=endDate, adjust=adjust)
 
        #日数据到周数据转换
        if period == "weekly":
            if not isinstance(stockDataList.index, pd.DatetimeIndex):
                stockDataList["date"] = pd.to_datetime(stockDataList["date"])
                stockDataList.set_index("date", inplace=True)
                stockDataList = stockDataList.resample('W-FRI').agg({'open': 'last', 'high': 'max', 'low': 'min', 'close': 'last', 'amount': 'sum'})
                stockDataList["symbol"] = symbol 
                stockDataList["volume"] = stockDataList["amount"] / stockDataList["close"]
                stockDataList.reset_index(inplace=True)
                #指定顺序
                stockDataList = stockDataList[["date","open","high","low","close","volume","amount","symbol"]]           
        else:
            stockDataList["symbol"] = symbol 
            stockDataList["volume"] = stockDataList["amount"] / stockDataList["close"]
            #指定顺序
            stockDataList = stockDataList[["date","open","high","low","close","volume","amount","symbol"]]
        #转换为字典
        stockDataList = stockDataList.to_dict(orient='records')
        for item in stockDataList:
            item["date"] = item["date"].strftime(f"%Y-%m-%d")
            result.append(item)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        result = None
    return result


#新浪历史数据
def sinoGetHistroryData(symbol,startDate,endDate,period="daily",adjust=""):
    result = []
    try:
        #首先转换symbol为腾讯股票代码
        sinoSymbol = symbol2symboleWithMarket(symbol)       
        stockDataList = ak.stock_zh_a_daily(symbol=sinoSymbol, start_date=startDate, end_date=endDate, adjust=adjust)
 
        #日数据到周数据转换
        if period == "weekly":
            if not isinstance(stockDataList.index, pd.DatetimeIndex):
                stockDataList["date"] = pd.to_datetime(stockDataList["date"])
                stockDataList.set_index("date", inplace=True)
                stockDataList = stockDataList.resample('W-FRI').agg({'open': 'last', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', 'amount': 'sum'})
                stockDataList["symbol"] = symbol 
                stockDataList.reset_index(inplace=True)
                #指定顺序
                stockDataList = stockDataList[["date","open","high","low","close","volume","amount","symbol"]]           
        else:
            stockDataList["symbol"] = symbol 
            #指定顺序
            stockDataList = stockDataList[["date","open","high","low","close","volume","amount","symbol","outstanding_share"]]
        #转换为字典
        stockDataList = stockDataList.to_dict(orient='records')
        for item in stockDataList:
            item["date"] = item["date"].strftime(f"%Y-%m-%d")
            result.append(item)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        result = None
    return result


'''
获取分时数据
'''
#没有数据返回
def getHistoryMinData(symbol,startYMDHMS,endYMDHMS,period="1",adjust=""):
    result = pd.DataFrame()
    try:
        startDate = misc.humanTime(startYMDHMS)
        endDate = misc.humanTime(endYMDHMS)
        result  = ak.stock_zh_a_hist_min_em(symbol=symbol, period=period, start_date=startDate, end_date=endDate, adjust=adjust)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result

'''
日内分时数据-东财
'''
def getIntradayData(symbol):
    result = pd.DataFrame()
    try:
        result = ak.stock_intraday_em(symbol=symbol)
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#分红配股数据
# 代码 名称 上市日期 累计股息 年均股息 分红次数 融资总额 融资次数
def getDividendData():
    result = {}
    try:
        dividendList = ak.stock_history_dividend()
        dividendList.rename(columns={'代码': 'symbol', '名称': 'stock_name','上市日期':'ipo_date',
        '累计股息':'cumulative_dividend','年均股息':'annual_dividend',
        '分红次数':'dividend_count','融资总额':'total_financing','融资次数':'financing_count'}, inplace=True)
        dividendList = dividendList.to_dict(orient='records')
        for item in dividendList:
            symbol = item.get("symbol")
            item["ipo_date"] = item["ipo_date"].strftime(f"%Y-%m-%d")
            if symbol not in result:               
                result[symbol] = []
            result[symbol].append(item) 

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result
#history data end


#analysis tool begin

#analysis tool end



def test():
    pass
    # sinoGetHistroryData("601088","20240101","20260125",period="weekly")
    txGetHistroryData("601088","20240101","20260125",period="weekly")
    swGetIndexHistory("801030") 
    # swGetIndustryList()
    swGetStockInfoData()
    emGetStockInfoData()
    # getSWIndustryList()
    getStockSymboleNameList()
    # getStockList()
    symbol = "601088"
    getStockIndividualInfo(symbol)
    gmGetHistroryData(symbol,"20240101","20240102")
    getHistoryMinData(symbol,"20240103093000","20240103150000")
    getIntradayData(symbol)


if __name__ == '__main__':
    test()