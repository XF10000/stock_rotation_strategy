#! /usr/bin/env python
#encoding: utf-8

#Filename: tushareCommon.py
#Author: Steven Lian's team
#E-mail: steven.lian@gmail.com/xie_frank@163.com
#Date: 2025-02-05
#Description:   tushare common functions

# https://tushare.pro/
# https://waditu.com/

#所有股票内容, symbol = 纯数字代码, 其他英文内容均采用小写,并用"_"连接

_VERSION = "20260218"

import os
import sys

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import traceback

import tushare as ts
import pandas as pd

from common import miscCommon as misc

from config import basicSettings as settings

_processorPID = os.getpid()

# Tushare 初始化（需要 token）
def readTushareTokenFromEnv():
    token = ""
    try:
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            fileName = settings.STOCK_TUSHARE_TOKEN_FILE
            filePath = os.path.join(settings.STOCK_CONFIG_DIR_NAME, fileName)
            tokenSet = misc.loadJsonData(filePath,"dict")
            if tokenSet:
                token = tokenSet.get("token","")
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        _LOG.error(f"{errMsg}, {traceback.format_exc()}")
    return token


TOKEN = readTushareTokenFromEnv()  # 请替换为您的 Tushare token
ts.set_token(TOKEN)
tusharePro = ts.pro_api()

#common functions begin
'''
股票代码转换,数字到tushare代码转换
'''
def symbol2symboleWithMarket(symbol):
    result = symbol
    try:
        if symbol.startswith("6") or symbol.startswith("9") or symbol.startswith("5") or symbol.startswith("1"):
            # 上海：6主板，9科创板，5基金，1债券
            result = symbol + ".SH"
        elif symbol.startswith("0") or symbol.startswith("3") or symbol.startswith("2"):
            # 深圳：0主板，3创业板，2B股
            result = symbol + ".SZ"
        elif symbol.startswith("4") or symbol.startswith("8"):
            # 北交所：43、83、87、88开头
            result = symbol + ".BJ"
        else:
            # 其他情况
            pass
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"

    return result


'''
股票代码转换,tushare代码到数字转换
'''
def symbolWithMarket2symbole(symbol):
    result = symbol
    try:
        if symbol.endswith(".SH"):
            #上海
            result = symbol[:-3]
        elif symbol.endswith(".SZ"):
            #深圳
            result = symbol[:-3]
        elif symbol.endswith(".BJ"):
            #北交所
            result = symbol[:-3]
        else:
            pass
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#股票市场总体情况
def getStockMarketSummary(YMD=""):
    result = {}
    try:
        if not YMD:
            YMD = misc.getTime()[0:8]
        # Tushare 没有直接对应的接口，使用交易日历和指数数据替代
        # 获取交易日历
        trade_cal = tusharePro.trade_cal(exchange='', start_date=YMD, end_date=YMD)
        # 获取上证指数和深证成指数据
        sh_index = tusharePro.index_daily(ts_code='000001.SH', start_date=YMD, end_date=YMD)
        sz_index = tusharePro.index_daily(ts_code='399001.SZ', start_date=YMD, end_date=YMD)
        
        result = {
            'trade_date': YMD,
            'trade_cal': trade_cal.to_dict('records') if not trade_cal.empty else [],
            'sh_index': sh_index.to_dict('records') if not sh_index.empty else [],
            'sz_index': sz_index.to_dict('records') if not sz_index.empty else []
        }

    except Exception as e:
        errMsg = f"errMsg:{str(e)}"
        print(f"{errMsg}, {traceback.format_exc()}")
    return result


#获取所有股票列表
def getStockList():
    result = []
    try:
        # 获取A股股票列表
        df = tusharePro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,market')
        df.rename(columns={'ts_code': 'ts_code', 'symbol': 'symbol', 'name': 'stock_name',
                          'area': 'area', 'industry': 'industry', 'list_date': 'ipo_date',
                          'market': 'market'}, inplace=True)
        result = df.to_dict(orient='records')
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        result = None
    return result


def getStockSymboleNameList():
    result = []
    try:
        stock_list = tusharePro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
        stock_list = stock_list.to_dict(orient='records')
        for stockInfo in stock_list:
            details = identifyMarket(stockInfo['symbol'])
            details["stock_name"] = stockInfo['name']
            details["ts_code"] = stockInfo['ts_code']
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
        
        # 判断纯数字代码
        if code_str.isdigit():
            # A股判断
            if len(code_str) == 6:
                if code_str.startswith('6'):  # 沪市主板/科创板
                    result['market_code'] = code_str
                    result['market_prefix'] = "SH"
                    if code_str.startswith('688'):
                        result['market_name'] = "沪市科创板"
                    else:
                        result['market_name'] = "沪市A股"
                    result['ts_code'] = code_str + ".SH"

                elif code_str.startswith(('0', '3')):  # 深市
                    result['market_code'] = code_str
                    result['market_prefix'] = "SZ"
                    if code_str.startswith('3'):
                        result['market_name'] = "深市创业板"
                    else:
                        result['market_name'] = "深市A股"
                    result['ts_code'] = code_str + ".SZ"
                    
                elif code_str.startswith(('43', '83', '87', '88')):  # 北交所
                    result['market_code'] = code_str
                    result['market_prefix'] = "BJ"
                    result['market_name'] = "北交所A股"
                    result['ts_code'] = code_str + ".BJ"
                    
        # 港股判断（通常5位，也有4位）
        elif 4 <= len(code_str) <= 5 and code_str.isdigit():
            result['market_code'] = code_str
            result['market_prefix'] = "HK"
            result['market_name'] = "港股"
            result['ts_code'] = code_str + ".HK"
        
        else:
            # 其他情况
            result['market_code'] = code_str
            result['market_prefix'] = "unknown"
            result['market_name'] = "未知市场"
            result['ts_code'] = code_str
        
        result["symbol"] = result['market_code']

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


def getStockIndividualInfo(symbol):
    result = pd.DataFrame()
    try:
        # 转换为ts_code
        market_info = identifyMarket(symbol)
        ts_code = market_info.get('ts_code', symbol)
        
        # 获取股票基本信息
        df = tusharePro.stock_company(ts_code=ts_code)
        if not df.empty:
            result = df
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取申万行业数据（Tushare使用SW行业分类), 需要更高权限 
def swGetIndustryList():
    result = []
    try:
        # 获取申万行业分类
        sw_industry = tusharePro.index_classify(level='L2', src='SW')
        sw_industry.rename(columns={'index_code': 'industry_code', 'industry_name': 'industry_name',
                                  'level': 'level', 'src': 'src'}, inplace=True)
        
        for _, row in sw_industry.iterrows():
            industryInfo = row.to_dict()
            # 获取行业成分股数量
            try:
                cons = tusharePro.index_member(index_code=row['industry_code'])
                industryInfo['num_of_constituents'] = len(cons)
            except:
                industryInfo['num_of_constituents'] = 0
            
            result.append(industryInfo)
            
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取申万行业包含的股票数据
def swGetIndustryConstituents(industry_code):
    result = []
    try:
        cons_df = tusharePro.index_member(index_code=industry_code)
        if not cons_df.empty:
            cons_df.rename(columns={'index_code': 'industry_code', 'con_code': 'symbol',
                                  'con_name': 'stock_name', 'in_date': 'date',
                                  'out_date': 'out_date', 'is_new': 'is_new'}, inplace=True)
            result = cons_df.to_dict(orient='records')
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取申万行业股票信息数据
def swGetStockInfoData():
    result = {}
    try:
        # 获取申万二级行业数据
        industryList = swGetIndustryList()
        for industryInfo in industryList:
            industry_code = industryInfo["industry_code"]
            
            # 获取行业包含的股票
            tryTimes = 3
            stockList = []
            while tryTimes > 0:
                stockList = swGetIndustryConstituents(industry_code)
                if len(stockList) > 0:
                    break
                tryTimes -= 1
                misc.time.sleep(0.5)

            for stockInfo in stockList:
                item = {}
                symbol = stockInfo["symbol"]
                
                # 提取纯数字代码
                if '.' in symbol:
                    symbol_num = symbol.split('.')[0]
                else:
                    symbol_num = symbol

                # 股票信息
                item["symbol"] = symbol_num
                item["ts_code"] = symbol
                item["stock_name"] = stockInfo["stock_name"]
                item["date"] = stockInfo["date"]

                # 行业信息
                item["industry_name_sw"] = industryInfo["industry_name"]
                item["industry_code_sw"] = industry_code
                item["industry_type_sw"] = "申银万国"

                # 时间信息
                item["YMDMHS"] = misc.getTime()
                
                result[symbol_num] = item
                
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取申万指数历史行情
def swGetIndexHistory(index_symbol, period="week", start_date="20200101"):
    result = []
    try:
        # Tushare 使用不同的周期参数
        freq_map = {"day": "D", "week": "W", "month": "M"}
        freq = freq_map.get(period, "D")
        
        indexHistory = tusharePro.index_daily(ts_code=index_symbol, 
                                      start_date=start_date,
                                      end_date=misc.getTime()[:8])
        
        if not indexHistory.empty:
            indexHistory.rename(columns={'trade_date': 'date', 'ts_code': 'symbol',
                                       'open': 'open', 'close': 'close',
                                       'high': 'high', 'low': 'low',
                                       'vol': 'volume', 'amount': 'amount'}, inplace=True)
            
            # 转换为周线或月线
            if period in ["week", "month"]:
                indexHistory['date'] = pd.to_datetime(indexHistory['date'])
                indexHistory.set_index('date', inplace=True)
                
                if period == "week":
                    resampled = indexHistory.resample('W-FRI').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum',
                        'amount': 'sum',
                        'symbol': 'last'
                    })
                else:  # month
                    resampled = indexHistory.resample('M').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum',
                        'amount': 'sum',
                        'symbol': 'last'
                    })
                
                indexHistory = resampled.reset_index()
            
            indexHistory = indexHistory.to_dict(orient='records')
            
            for item in indexHistory:
                if isinstance(item['date'], pd.Timestamp):
                    item["date"] = item["date"].strftime("%Y-%m-%d")
                result.append(item)
                
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取东方财富行业数据（Tushare使用概念板块）
def emGetIndustryList():
    result = []
    try:
        # 获取概念板块列表
        concept_list = tusharePro.concept()
        concept_list.rename(columns={'code': 'industry_code', 'name': 'industry_name',
                                   'src': 'source'}, inplace=True)
        
        for _, row in concept_list.iterrows():
            industryInfo = row.to_dict()
            # 获取板块成分股数量
            try:
                cons = tusharePro.concept_detail(id=row['industry_code'])
                industryInfo['num_of_constituents'] = len(cons)
            except:
                industryInfo['num_of_constituents'] = 0
            
            result.append(industryInfo)
            
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取概念板块包含的股票
def emGetIndustryConstituents(industry_code):
    result = []
    try:
        cons_df = tusharePro.concept_detail(id=industry_code)
        if not cons_df.empty:
            cons_df.rename(columns={'id': 'industry_code', 'ts_code': 'symbol',
                                  'name': 'stock_name', 'concept_name': 'industry_name'}, inplace=True)
            result = cons_df.to_dict(orient='records')
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取概念板块股票信息数据
def emGetStockInfoData():
    result = {}
    try:
        # 获取概念板块数据
        industryList = emGetIndustryList()
        for industryInfo in industryList:
            industry_code = industryInfo["industry_code"]
            
            # 获取板块包含的股票
            stockList = emGetIndustryConstituents(industry_code)
            
            for stockInfo in stockList:
                item = {}
                symbol = stockInfo["symbol"]
                
                # 提取纯数字代码
                if '.' in symbol:
                    symbol_num = symbol.split('.')[0]
                else:
                    symbol_num = symbol

                # 股票信息
                item["symbol"] = symbol_num
                item["ts_code"] = symbol
                item["stock_name"] = stockInfo["stock_name"]

                # 行业信息
                item["industry_name_em"] = industryInfo["industry_name"]
                item["industry_code_em"] = industry_code
                item["industry_type_em"] = "概念板块"

                # 时间信息
                item["YMDMHS"] = misc.getTime()
                
                result[symbol_num] = item
                
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#common functions end

#realtime data begin
def getStockBidAskData(symbol):
    result = pd.DataFrame()
    try:
        # Tushare 实时数据需要专业版权限
        # 这里使用日线数据替代
        market_info = identifyMarket(symbol)
        ts_code = market_info.get('ts_code', symbol)
        result = tusharePro.daily(ts_code=ts_code, start_date=misc.getTime()[:8], end_date=misc.getTime()[:8])
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
def gmGetHistroryData(symbol, startDate, endDate, period="daily", adjust=""):
    result = []
    try:
        if len(startDate) == 10:
            startDate = startDate[:4] + startDate[5:7] + startDate[8:10]
        if len(endDate) == 10:
            endDate = endDate[:4] + endDate[5:7] + endDate[8:10]
        
        market_info = identifyMarket(symbol)
        ts_code = market_info.get('ts_code', symbol)
        
        # 获取日线数据
        df = tusharePro.daily(ts_code=ts_code, start_date=startDate, end_date=endDate)
        
        if not df.empty:
            df.rename(columns={'trade_date': 'date', 'ts_code': 'ts_code',
                             'open': 'open', 'close': 'close','pre_close':'prev_close',
                             'high': 'high', 'low': 'low',
                             'vol': 'volume', 'amount': 'amount',
                             'pct_chg': 'pct_change', 'change': 'change'}, inplace=True)
            
            # 转换为周线或月线
            if period in ["weekly", "monthly"]:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                
                if period == "weekly":
                    resampled = df.resample('W-FRI').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum',
                        'amount': 'sum',
                        'pct_change': 'last',
                        'change': 'last',
                        'symbol': 'last'
                    })
                else:  # monthly
                    resampled = df.resample('M').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum',
                        'amount': 'sum',
                        'pct_change': 'last',
                        'change': 'last',
                        'symbol': 'last'
                    })
                
                df = resampled.reset_index()
            # 转换日期格式为YYYY-MM-DD
            df['date'] = df['date'].str[:4] + '-' + df['date'].str[4:6] + '-' + df['date'].str[6:8]
            # 转换为symbol格式
            df['symbol'] = df['ts_code'].str.split('.').str[0]
            result = df.to_dict(orient='records')
            
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        result = None
    return result


#腾讯历史数据（使用Tushare替代）
def txGetHistroryData(symbol, startDate, endDate, period="daily", adjust=""):
    # 使用gmGetHistroryData替代
    return gmGetHistroryData(symbol, startDate, endDate, period, adjust)


#新浪历史数据（使用Tushare替代）
def sinoGetHistroryData(symbol, startDate, endDate, period="daily", adjust=""):
    # 使用gmGetHistroryData替代
    return gmGetHistroryData(symbol, startDate, endDate, period, adjust)


'''
获取分时数据
'''
def getHistoryMinData(symbol, startYMDHMS, endYMDHMS, period="1", adjust=""):
    result = pd.DataFrame()
    try:
        market_info = identifyMarket(symbol)
        ts_code = market_info.get('ts_code', symbol)
        
        # Tushare需要专业版才能获取分时数据
        # 这里返回空DataFrame
        print("Tushare分时数据需要专业版权限")
        
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


'''
日内分时数据
'''
def getIntradayData(symbol):
    result = pd.DataFrame()
    try:
        market_info = identifyMarket(symbol)
        ts_code = market_info.get('ts_code', symbol)
        
        # Tushare需要专业版才能获取日内分时数据
        # 这里返回空DataFrame
        print("Tushare日内分时数据需要专业版权限")
        
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#分红配股数据
def getDividendData():
    result = {}
    try:
        # 获取分红数据
        df = tusharePro.dividend()
        if not df.empty:
            df.rename(columns={'ts_code': 'symbol', 'end_date': 'ex_dividend_date',
                             'div_proc': 'dividend_process', 'stk_div': 'stock_dividend',
                             'stk_bo_rate': 'stock_bonus_rate', 'stk_co_rate': 'stock_conversion_rate',
                             'cash_div': 'cash_dividend', 'cash_div_tax': 'cash_dividend_tax',
                             'record_date': 'record_date', 'pay_date': 'pay_date',
                             'div_listdate': 'dividend_list_date', 'imp_ann_date': 'announcement_date'}, inplace=True)
            
            dividendList = df.to_dict(orient='records')
            for item in dividendList:
                symbol = item.get("symbol")
                # 提取纯数字代码
                if symbol and '.' in symbol:
                    symbol_num = symbol.split('.')[0]
                    item['symbol_num'] = symbol_num
                    result[symbol_num] = item

    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
    return result


#获取股票分红/配股详情数据
def getDividendDetails(symbol, indicator="", date=""):
    result = []
    try:
        market_info = identifyMarket(symbol)
        ts_code = market_info.get('ts_code', symbol)
        
        df = tusharePro.dividend(ts_code=ts_code)

        # 转换日期格式为YYYY-MM-DD
        df['end_date'] = df['end_date'].str[:4] + '-' + df['end_date'].str[4:6] + '-' + df['end_date'].str[6:8]
        df['ann_date'] = df['ann_date'].str[:4] + '-' + df['ann_date'].str[4:6] + '-' + df['ann_date'].str[6:8]
        df['record_date'] = df['record_date'].str[:4] + '-' + df['record_date'].str[4:6] + '-' + df['record_date'].str[6:8]
        df['ex_date'] = df['ex_date'].str[:4] + '-' + df['ex_date'].str[4:6] + '-' + df['ex_date'].str[6:8]
        df['pay_date'] = df['pay_date'].str[:4] + '-' + df['pay_date'].str[4:6] + '-' + df['pay_date'].str[6:8]
        df['div_listdate'] = df['div_listdate'].str[:4] + '-' + df['div_listdate'].str[4:6] + '-' + df['div_listdate'].str[6:8]
        df['imp_ann_date'] = df['imp_ann_date'].str[:4] + '-' + df['imp_ann_date'].str[4:6] + '-' + df['imp_ann_date'].str[6:8]
        
        result = df.to_dict(orient='records')
                
    except Exception as e:
        traceMsg = traceback.format_exc().strip("")
        errMsg = f"{e},{traceMsg}"
        result = None
    return result
#history data end


#analysis tool begin

#analysis tool end



def test():
    # 测试各个函数
    print("测试开始...")
    
    # 设置测试参数
    test_symbol = "601088"
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    
    # # 测试股票列表
    # print("1. 测试股票列表...")
    # stock_list = getStockList()
    # print(f"获取到 {len(stock_list)} 只股票")
    
    # 测试历史数据
    # print("2. 测试历史数据...")
    # hist_data = gmGetHistroryData(test_symbol, start_date, end_date, period="daily")
    # print(f"获取到 {len(hist_data)} 条历史数据")
    
    # 测试行业数据
    # print("3. 测试申万行业数据...")
    # sw_industry = swGetIndustryList()
    # print(f"获取到 {len(sw_industry)} 个申万行业")
    
    # 测试分红数据
    print("4. 测试分红数据...")
    dividend_data = getDividendDetails(test_symbol)
    print(f"获取到 {len(dividend_data)} 只股票的分红数据")
    
    print("测试完成！")


if __name__ == '__main__':
    test()