#! /usr/bin/env python3
#encoding: utf-8

#Filename: stockTechnicalSignal.py  
#Author: Steven Lian's team
#E-mail:  steven.lian@gmail.com  / xie_frank@163.com
#Date: 2019-08-01
#Description:   技术指标计算 - 计算主要技术指标代表的含义
# 1. macd的金叉/死叉,顶背离/底背离
# 2. bollinger band的金叉/死叉,顶背离/底背离
# 3. kdj的金叉/死叉,顶背离/底背离
# 4. rsi的金叉/死叉,顶背离/底背离
# 5. obv的金叉/死叉,顶背离/底背离(成交量指标)
# 6. cci的金叉/死叉,顶背离/底背离(价格指标)
# 7. frank xie 43指标的金叉/死叉,顶背离/底背离(综合指标)
# 8. rsi + macd 组合指标的金叉/死叉,顶背离/底背离(综合指标)
# 9. obv + ma + rsi 组合指标的金叉/死叉,顶背离/底背离(综合指标)

__VERSION="20260414"

import os
import sys
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import traceback

from numba.cuda import external_stream
import numpy as np
import pandas as pd

from common import miscCommon as misc


_processorPID = os.getpid()

_DEF_MARKET_TREND_UP = "up"
_DEF_MARKET_TREND_DOWN = "down"
_DEF_MARKET_TREND_RANGE = "range"
_DEF_MARKET_TREND_NEUTRAL = "neutral"
_DEF_MARKET_TREND_TREND = "trend" #up or down

_DEF_MACD_MARKET_TREND_CN = {
    _DEF_MARKET_TREND_NEUTRAL: "中性;建议策略:观望等待",
    _DEF_MARKET_TREND_UP: "短期动能开始强于长期，上涨动力正在形成，是潜在的买入时机。",
    _DEF_MARKET_TREND_DOWN: "短期动能弱于长期，下跌压力开始显现，是潜在的卖出或观望信号。",
    _DEF_MARKET_TREND_RANGE: "震荡行情;建议策略:观望等待",
    _DEF_MARKET_TREND_TREND: "趋势行情;建议策略:回踩中轨买入、突破上轨加仓",
}

_DEF_BOLL_MARKET_TREND_CN = {
    _DEF_MARKET_TREND_NEUTRAL: "中性;建议策略:观望等待",
    _DEF_MARKET_TREND_UP: "多头趋势;建议策略:回踩中轨买入、突破上轨加仓",
    _DEF_MARKET_TREND_DOWN: "空头趋势;建议策略:反弹中轨卖出、跌破下轨加空",
    _DEF_MARKET_TREND_RANGE: "震荡行情;建议策略:上下轨高抛低吸",
    _DEF_MARKET_TREND_TREND: "趋势行情;建议策略:回踩中轨买入、突破上轨加仓",
}

_DEF_SUGGESTION_BUY = "buy"
_DEF_SUGGESTION_SELL = "sell"
_DEF_SUGGESTION_HOLD = "hold"

_DEF_SUGGESTION_CN = {
    _DEF_SUGGESTION_BUY: "买入信号",
    _DEF_SUGGESTION_SELL: "卖出信号",
    _DEF_SUGGESTION_HOLD: "观望信号",
}

_DEF_SUBTYPE_LEN = 18
_DEF_DESCRIPTION_LEN = 240

# 获取指标名称
# @param signalType: 技术指标类型
# @return: 指标名称
indicatorNameDataSet = {
    # 基本指标
    "macdSignals": "macd",
    "bollSignals": "boll",
    "kdjSignals": "kdj",
    "rsiSignals": "rsi",
    "obvSignals": "obv",
    "cciSignals": "cci",
    #其他综合指标
    "fx43Signals": "fx43",
    "rsmaSignals": "rsma",
    "obmrSignals": "obmr",
    "brmaSignals": "brma",
}
def getIndicatorName(signalType):
    indicator = ""
    indicator = indicatorNameDataSet.get(signalType, "")
    return indicator

#stock technical signal -- class 
# 技术指标计算 - 计算主要技术指标代表的含义
# 分为几个类，每个类负责计算一个技术指标

# macd类,负责计算macd指标
# 数据格式: DataFrame
'''
MACD主要有三种经典的判断方法，从易到难分别是：
1. 交叉策略：寻找基础买卖点
这是MACD最直观的用法，通过观察快线和慢线的交叉来捕捉趋势的启动点。
* 黄金交叉（买入信号）：当DIF快线从下方向上穿越DEA慢线时，形成"金叉"。这通常意味着短期动能开始强于长期，上涨动力正在形成，是潜在的买入时机。在实际操作中，如果这个金叉发生在零轴上方，信号的可靠性和趋势强度会更高。
* 死亡交叉（卖出信号）：当DIF快线从上方向下穿越DEA慢线时，形成"死叉"。这表示短期动能弱于长期，下跌压力开始显现，是潜在的卖出或观望信号。同样，如果死叉发生在零轴下方，通常意味着下跌趋势可能延续。

2. 零轴判断：识别市场大方向
零轴是判断市场多空格局的关键指标。
* 零轴上方（多头市场）：当DIF线和DEA线运行在零轴之上，说明市场处于中长期的多头趋势。此时的操作策略应更倾向于"只做多，不做空"，即只在价格回调时寻找买入机会。
* 零轴下方（空头市场）：当两条线在零轴之下，则表明市场处于空头趋势。此时应保持谨慎，操作策略更倾向于"只做空，不做多"，或场外观望。

3. 背离策略：捕捉趋势反转的先机
背离是MACD最强大、最值得关注的功能，它能帮你提前预警趋势可能发生的反转。它的核心是价格与动能指标走势不一致。
* 顶背离（卖出预警）：当股价在上涨过程中创出新高，但MACD指标（通常看DIF线或柱状图的高点）却没能同步创出新高，反而走低。这就像一辆汽车虽然还在上坡，但油门（动能）已经松开了，预示着上涨动能衰竭，股价可能即将见顶回落。
* 底背离（买入预警）：当股价在下跌过程中创出新低，但MACD指标（通常看DIF线或柱状图的低点）却没同步创出新低，反而走高。这好比汽车下坡时开始轻点刹车，下跌动能减弱，预示着股价可能即将见底反弹。

进阶技巧与注意事项
除了以上核心用法，还有一些技巧能帮你更好地运用MACD：
*用柱状图预判：MACD柱状图是动能加速度的体现。当红色柱（正值）开始缩短时，即便DIF线和DEA线还未形成死叉，也意味着上涨动能正在减弱，你可以考虑提前部分止盈。同样，绿色柱缩短意味着下跌动能减弱。
* 警惕"风洞"信号：在盘整行情中，DIF线和DEA线可能会在很短的时间内（如5个交易日内）先死叉后金叉（多头风洞），或先金叉后死叉（空头风洞）。这通常是大资金在拉升前震仓或出货的信号，需要结合成交量等因素综合判断。
* 注意事项指标局限：MACD在趋势明显的行情中非常有效，但在横盘震荡市里，交叉信号会变得频繁且不准确，容易出现"假信号"。此外，它不适合用于判断超买超卖，这方面可以结合RSI或KDJ等指标。
'''

class MACDAnalyzer:
    """macd类 - 计算macd信号"""
    symbol = ""
    # macd_line = dif 
    # macd_signal = dea
    # macd_histogram = macd
    macdData = None
    indicator = "macd"
    macdSignals = {}
    macdKeyList = ["date","macd_line","macd_signal","macd_histogram","open","close","high","low","volume","amount","turnover_rate"]
    macdLongKeyList = ["date","macd_line_long","macd_signal_long","macd_histogram_long","open","close","high","low","volume","amount","turnover_rate"]
    macdDivergenceWindows = 20 #背离窗口大小,用于计算macd的背离
    macdSlopeWindows = 5 #斜率窗口大小,用于计算macd的斜率
    daysBeforeAfter = 2 #比较前后2个交易日的macd数据

    def __init__(self,symbol,df=None,isLong=False):
        self.symbol = symbol
        if df is not None:
            #取部分字段数据
            if isLong:
                self.macdData = df[self.macdLongKeyList]
                self.macdData.rename(columns={'macd_line_long':'dif','macd_signal_long':'dea','macd_histogram_long':'macd'},inplace=True)
            else:
                self.macdData = df[self.macdKeyList]
                self.macdData.rename(columns={'macd_line':'dif','macd_signal':'dea','macd_histogram':'macd'},inplace=True)

    def calc(self):
        result = {}
        """计算macd指标"""
        try:
            #发现金叉和死叉信号
            crossOverSignals = self.findCrossOver()
            #发现背离信号,含零轴判断的市场趋势信号
            divergenceSignals = self.findDivergence()
            #合并计算结果, 合并金叉和死叉信号, 以及背离信号
            #以金叉和死叉信号为基准, 合并背离信号
            intersectionDivergencs = divergenceSignals.get("intersectionDivergence",{})
            finalSignals = {}
            for YMD, item in crossOverSignals.items():
                date = item.get("date","")
                #获取前后2个交易日的macd数据
                YMDList = misc.getDaysBeforeAfter(self.daysBeforeAfter,YMD)
                crossSuggestion = item['suggestion']
                crossMarketTrend = item['marketTrend']

                #判断是否有对应的背离信号
                findDateFlag = False
                for YMD in YMDList:
                    if YMD in intersectionDivergencs:
                        findDateFlag = True
                        break               
                if not findDateFlag:
                    continue

                #获取背离信号
                intersectionDivergence = intersectionDivergencs[YMD]

                #有对应的背离信号, 合并结果
                finalSignals[YMD] = {}
                finalSignals[YMD]['suggestion'] = crossSuggestion
                finalSignals[YMD]['marketTrend'] = crossMarketTrend
                crossMarketTrendCN = _DEF_MACD_MARKET_TREND_CN.get(crossMarketTrend,_DEF_MARKET_TREND_NEUTRAL)

                #填充信号数据
                finalSignals[YMD]["symbol"] = self.symbol
                finalSignals[YMD]["indicator"] = self.indicator

                intersectionDivergenceDate = intersectionDivergence.get("date","")
                intersectionDivergenceMarketTrend = intersectionDivergence.get("marketTrend",_DEF_MARKET_TREND_NEUTRAL)
                intersectionDivergenceMarketTrendCN = _DEF_MACD_MARKET_TREND_CN.get(intersectionDivergenceMarketTrend,_DEF_MARKET_TREND_NEUTRAL)
                intersectionDivergenceSuggestion = intersectionDivergence.get("suggestion","")
                intersectionDivergenceSuggestionCN = _DEF_SUGGESTION_CN.get(intersectionDivergenceSuggestion,"")

                if crossSuggestion == _DEF_SUGGESTION_BUY:
                    finalSignals[YMD]["subtype"] = "macd黄金交叉（买入信号）"
                    description = f"{date},{self.symbol}:黄金交叉（买入信号）;市场趋势状态:{crossMarketTrendCN},背离状态:{intersectionDivergenceDate}:建议:{intersectionDivergenceSuggestionCN},"
                elif crossSuggestion == _DEF_SUGGESTION_SELL:
                    finalSignals[YMD]["subtype"] = "macd死叉交叉（卖出信号）"
                    description = f"{date},{self.symbol}:死叉交叉（卖出信号）;市场趋势状态:{crossMarketTrendCN},背离状态:{intersectionDivergenceDate}:建议:{intersectionDivergenceSuggestionCN},"
                else:
                    finalSignals[YMD]["subtype"] = "macd观望信号"
                    description = f"{date},{self.symbol}:观望信号;"
                    pass
                finalSignals[YMD]["suggestion"] = crossSuggestion
                finalSignals[YMD]["subtype"] = finalSignals[YMD]["subtype"][0:_DEF_SUBTYPE_LEN]
                description = description[:_DEF_DESCRIPTION_LEN]
                finalSignals[YMD]["description"] = description

                #填充详细信息, result_json 数据
                detail = {}
                detail['intersectionDivergence'] = intersectionDivergence
                detail['crossOverSignals'] = item
                finalSignals[YMD]["detail"] = detail

            self.macdSignals["finalSignals"] = finalSignals
            self.macdSignals["crossOverSignals"] = crossOverSignals
            self.macdSignals["divergenceSignals"] = divergenceSignals
            result = self.macdSignals
        
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"

        return result

    def findCrossOver(self):
        result = {}
        """识别金叉和死叉信号"""
        try:
             # 寻找金叉（DIF上穿DEA）
            # df = self.macdData.copy()
            df = self.macdData
            goldenCross = (df['dif'] > df['dea']) & (df['dif'].shift(1) <= df['dea'].shift(1))
             # 死叉（DIF下穿DEA）
            deathCross = (df['dif'] < df['dea']) & (df['dif'].shift(1) >= df['dea'].shift(1))
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                goldenCrossSignal = goldenCross[index]
                deathCrossSignal = deathCross[index]
                saveSet = {'date':currentDate,'goldenCross':False,'deathCross':False}
                if goldenCrossSignal:
                    saveSet['goldenCross'] = True
                    saveSet['suggestion'] = _DEF_SUGGESTION_BUY
                    saveSet['marketTrend'] = _DEF_MARKET_TREND_UP #"bullish"
                if deathCrossSignal:
                    saveSet['deathCross'] = True
                    saveSet['suggestion'] = _DEF_SUGGESTION_SELL
                    saveSet['marketTrend'] = _DEF_MARKET_TREND_DOWN #"bearish"
                if goldenCrossSignal or deathCrossSignal:
                    result[currentYMD] = saveSet
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"

        return result

    def calcSlope(self,series,window=None):
        """计算macd的斜率"""
        result = {}
        try:
            """
            计算斜率（线性回归）
            """
            if window is None:
                window = self.macdSlopeWindows
            
            slopes = []
            for i in range(len(series)):
                if i < window - 1:
                    slopes.append(0)
                    continue
                
                y = series.iloc[i-window+1:i+1].values
                x = np.arange(len(y))
                
                # 简单线性回归
                slope = np.polyfit(x, y, 1)[0]
                slopes.append(slope)
            
            result = pd.Series(slopes, index=series.index)
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    # 检查背离的持续性
    def checkPersistence(self,priceSlopes,difSlopes,minSlopeDiff):
        """检查背离的持续性"""
        result = True
        try:
            if len(priceSlopes) < 2:
                return result
            #计算有多少个周期满足背离条件
                    # 计算有多少个周期满足背离条件
            consistentCount = 0
            for i in range(len(priceSlopes)):
                if priceSlopes.iloc[i] > minSlopeDiff and difSlopes.iloc[i] < -minSlopeDiff:
                    consistentCount += 1
                elif priceSlopes.iloc[i] < -minSlopeDiff and difSlopes.iloc[i] > minSlopeDiff:
                    consistentCount += 1
        
            # 如果超过一半的周期满足条件，则认为信号持续
            result = consistentCount >= len(priceSlopes) / 2

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    def calcReliability(self,priceSlope,difSlope,isBearish):
        """
        计算背离的可靠性
        参数:
            priceSlope: 价格斜率
            difSlope: DIF斜率
            isBearish: 是否为顶背离
        
            返回:
            可靠性评级
        """
        result = ""
        try:
            if isBearish:
                # 顶背离：价格斜率越大，DIF斜率越小（负值越大），信号越强
                slopeRatio = abs(difSlope) / priceSlope if priceSlope != 0 else 0
            else:
                # 底背离：价格斜率越小（负值越大），DIF斜率越大，信号越强
                slopeRatio = abs(priceSlope) / difSlope if difSlope != 0 else 0
            
            if slopeRatio > 2:
                result = 'high'
            elif slopeRatio > 1:
                result = 'medium'
            else:
                result = 'low'

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    def detectSlopeDivergence(self,lookback=20,minSlopeDiff=0.001):
        """
            检测macd的斜率背离
            lookback 回溯周期窗口大小
            minSlopeDiff 最小斜率差异阈值,避免噪音干扰
        """
        result = {}
        try:
            df = self.macdData
            priceSlopes = self.calcSlope(df["close"])
            difSlopes = self.calcSlope(df["dif"])

            divergences = {}
            for i in range(lookback, len(df)):
                # 获取最近lookback期的数据
                currPriceSlope = priceSlopes.iloc[i]
                currDifSlope = difSlopes.iloc[i]

                # 跳过NaN值
                if pd.isna(currPriceSlope) or pd.isna(currDifSlope):
                    continue

                currDate = df['date'][i]
                currYMD = currDate.replace('-','')

                # 转换为浮点数并保留4位小数
                currPrice = float(df["close"][i])
                currPrice = round(currPrice,4)
                currDif = float(df["dif"][i])
                currDif = round(currDif,4)
                #零轴判断,判断市场趋势向上或向下
                if currDif > 0:
                    marketTrend = _DEF_MARKET_TREND_UP #"bullish" #多头市场
                    marketTrendCN = "多头市场"
                else:
                    marketTrend = _DEF_MARKET_TREND_DOWN #"bearish" #空头市场
                    marketTrendCN = "空头市场"

                currPriceSlope = float(currPriceSlope) 
                currDifSlope = float(currDifSlope) 
                currPriceSlope = round(currPriceSlope,4)
                currDifSlope = round(currDifSlope,4)

                # 检测斜率背离
                # 情况1：价格上涨趋势（正斜率）但DIF下降趋势（负斜率）-> 顶背离
                if currPriceSlope > minSlopeDiff and currDifSlope < -minSlopeDiff:
                    #验证背离的持续性,(检查前几个周期是否也符合条件)
                    persistence = self.checkPersistence(
                        priceSlopes.iloc[max(0, i-3):i+1],
                        difSlopes.iloc[max(0, i-3):i+1],
                        minSlopeDiff)
                    if persistence:
                        reliability = self.calcReliability(currPriceSlope,currDifSlope,True)
                        confidence = abs(currDifSlope) / abs(currPriceSlope) if currPriceSlope != 0 else 0  
                        confidence = float(round(confidence,4))
                        divergences[currYMD] = {
                            'type': 'Bearish Divergence (Slope)',
                            'typeCN': '顶背离（斜率）',
                            'suggestion':'sell',
                            'date': df['date'][i],
                            'price': currPrice,
                            'dif': currDif,
                            'marketTrend': marketTrend,
                            'marketTrendCN': marketTrendCN,
                            'lookback': lookback,
                            'priceSlope': currPriceSlope,
                            'priceChange': currPriceSlope*lookback,#近似价格变化
                            'difSlope': currDifSlope,
                            'difChange': currDifSlope*lookback,#近似DIF变化
                            'confidence': confidence,
                            'reliability': reliability,
                            'persistence': persistence
                        }
                # 情况2：价格下跌趋势（负斜率）但DIF上升趋势（正斜率）-> 底背离
                elif currPriceSlope < -minSlopeDiff and currDifSlope > minSlopeDiff:
                    persistence = self.checkPersistence(
                        priceSlopes.iloc[max(0, i-3):i+1],
                        difSlopes.iloc[max(0, i-3):i+1],
                        minSlopeDiff)
                    if persistence:
                        reliability = self.calcReliability(currPriceSlope,currDifSlope,True)
                        confidence = abs(currPriceSlope) / abs(currDifSlope) if currDifSlope != 0 else 0
                        confidence = float(round(confidence,4))
                        divergences[currYMD] = {
                            'type': 'Bullish Divergence (Slope)',
                            'typeCN': '底背离（斜率）',
                            'suggestion':'buy',
                            'date': df['date'][i],
                            'price': currPrice,
                            'dif': currDif,
                            'marketTrend': marketTrend,
                            'marketTrendCN': marketTrendCN,
                            'lookback': lookback,
                            'priceSlope': currPriceSlope,
                            'priceChange': currPriceSlope*lookback,#近似价格变化
                            'difSlope': currDifSlope,
                            'difChange': currDifSlope*lookback,#近似DIF变化
                            'confidence': confidence,
                            'reliability': reliability,
                            'persistence': persistence
                        }
            
            result = divergences
            
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    def simpleDivergence(self,window=20):
        """
        简单寻找顶背离和底背离  
        参数:
        window: 寻找背离的窗口大小
        """
        result = {}
        try:
            df = self.macdData
            
            df['priceHigh'] = df['high'].rolling(window=window, center=True).apply(lambda x: x.argmax() == window//2)
            df['priceLow'] = df['low'].rolling(window=window, center=True).apply(lambda x: x.argmin() == window//2)

            # 简单的背离识别逻辑（实际应用中需要更复杂的算法）
            df['bullishDiv'] = False  # 底背离
            df['bearishDiv'] = False  # 顶背离
            
            for i in range(window, len(df)-window):
                # 顶背离：价格创新高，DIF未创新高
                price_window = df['high'].iloc[i-window:i+window]
                dif_window = df['dif'].iloc[i-window:i+window]
                
                if df['priceHigh'].iloc[i] == price_window.max():
                    if df['dif'].iloc[i] < dif_window.max():
                        df.loc[df.index[i], 'bearishDiv'] = True
                
                # 底背离：价格创新低，DIF未创新低
                if df['priceLow'].iloc[i] == price_window.min():
                    if df['dif'].iloc[i] > dif_window.min():
                        df.loc[df.index[i], 'bullishDiv'] = True    

            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"

        return result

    def findDivergence(self):
        """
        寻找顶背离和底背离       
        参数:
        """
        result = {}
        try:
            #多周期背离识别
            lookbackList = [10,20,30] #10,20,30周期
            divergences = {}
            for lookback in lookbackList:
                divergences[lookback]= self.detectSlopeDivergence(lookback)
            #合并所有周期的背离信号
            intersectionDivergence = {}
            #以20周期为基准,合并其他周期的背离信号
            divergences20 = divergences[20] 
            for YMD,item in divergences20.items():
                item20suggestion = item['suggestion']
                if YMD not in divergences[10]:  
                    continue
                item10suggestion = divergences[10][YMD]['suggestion']
                if YMD not in divergences[30]:
                    continue
                item30suggestion = divergences[30][YMD]['suggestion']  
                if item20suggestion == item10suggestion and item20suggestion == item30suggestion:
                    intersectionDivergence[YMD] = item
            result["divergences"] = divergences
            result["intersectionDivergence"] = intersectionDivergence
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"

        return result


class KDJAnalyzer:
    """kdj类 - 计算kdj信号"""
    symbol = ""
    #kdj判断指标阈值
    _DEF_KDJ_OVERBOUGHT_THRESOLD = 80
    _DEF_KDJ_OVERSOLD_THRESHOLD = 20
    _DEF_KDJ_J_EXTREME_HIGH_THRESHOLD = 100
    _DEF_KDJ_J_EXTREME_LOW_THRESHOLD = 0
    #kdj数据
    indicator = "kdj"
    kdjData = None
    kdjSignals = {}
    kdjKeyList = ["date","ma_60","kdj_k","kdj_d","kdj_j","open","close","high","low","volume","amount","turnover_rate"]
    def __init__(self,symbol,df=None):
        self.symbol = symbol
        if df is not None:
            #取部分字段数据
            self.kdjData = df[self.kdjKeyList]

    def calc(self):
        result = {}
        """计算kdj指标"""
        try:
            kdjSignals = self.generateKDJSignal()
            #合并所有信号,暂时没有其他的指标信号
            finalSignals = {}
            for YMD,item in kdjSignals.items():
                date = item.get("date","")
                #填充信号数据
                finalSignals[YMD] = {}
                finalSignals[YMD]["symbol"] = self.symbol
                finalSignals[YMD]["date"] = date
                finalSignals[YMD]["indicator"] = self.indicator
                suggestion = item.get("suggestion","")
                oldDescription = item.get("description","")
                bullishTrend = item.get("bullishTrend","")
                if bullishTrend:
                    marketTrendCN = "多头趋势"
                else:
                    marketTrendCN = "空头趋势"
                overCN = ""
                overbought = item.get("overbought","")
                if overbought:
                    overCN = "超买"
                oversold = item.get("oversold","")
                if oversold:
                    overCN = "超卖"
                crossCN = ""
                goldCross = item.get("goldCross","")
                if goldCross:
                    crossCN = "金叉"
                deathCross = item.get("deathCross","")
                if deathCross:
                    crossCN = "死叉"
                jExtremeHigh = item.get("jExtremeHigh","")
                if jExtremeHigh:
                    jExtremeHighCN = "是"
                else:
                    jExtremeHighCN = "否"   

                jExtremeLow = item.get("jExtremeLow","")
                if jExtremeLow:
                    jExtremeLowCN = "是"
                else:
                    jExtremeLowCN = "否"
                
                jTurnUp = item.get("jTurnUp","")
                if jTurnUp:
                    jTurnUpCN = "是"
                else:
                    jTurnUpCN = "否"

                if suggestion == _DEF_SUGGESTION_BUY:
                    finalSignals[YMD]["subtype"] = "kdj买入信号"
                    description = f"{date},{self.symbol}-[短线指标]:买入信号,{oldDescription};市场趋势状态:{marketTrendCN},其他关键指标:超买/超卖:{overCN},金叉/死叉:{crossCN},J值极端高位:{jExtremeHighCN},J值极端低位:{jExtremeLowCN},J值上穿:{jTurnUpCN}"
                elif suggestion == _DEF_SUGGESTION_SELL:
                    finalSignals[YMD]["subtype"] = "kdj卖出信号"
                    description = f"{date},{self.symbol}-[短线指标]:死叉交叉（卖出信号）;市场趋势状态:{marketTrendCN},其他关键指标:超买/超卖:{overCN},金叉/死叉:{crossCN},J值极端高位:{jExtremeHighCN},J值极端低位:{jExtremeLowCN},J值上穿:{jTurnUpCN}"
                else:
                    finalSignals[YMD]["subtype"] = "kdj观望信号"
                    description = f"{date},{self.symbol}-[短线指标]:观望信号;"
                    pass
                finalSignals[YMD]["suggestion"] = suggestion
                finalSignals[YMD]["subtype"] = finalSignals[YMD]["subtype"][0:_DEF_SUBTYPE_LEN]
                description = description[:_DEF_DESCRIPTION_LEN]
                finalSignals[YMD]["description"] = description

                #填充详细信息, result_json 数据
                detail = item
                finalSignals[YMD]["detail"] = detail
                
            result["finalSignals"] = finalSignals
            self.kdjSignals = result
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"

        return result

    def generateKDJSignal(self):
        """ 根据KDJ数值生成买卖信号（日线级别，结合MA60过滤）
            参数:
            df: DataFrame, 需包含 k, d, j 列，以及 'close' 和 'ma60' 列（如需趋势过滤）
    
            返回:
            DataFrame, 添加信号列
        """   
        result = {}
        try:
            df = self.kdjData
            if 'ma_60' not in df.columns: 
                #计算60日均线
                df['ma_60'] = df['close'].rolling(window=60).mean()
            
            # 判断趋势：股价在MA60之上为多头趋势
            df['bullishTrend'] = df['close'] > df['ma_60']

            #计算金叉和死叉
            df['goldenCross'] = (df['kdj_k'] > df['kdj_d']) & (df['kdj_k'].shift(1) <= df['kdj_d'].shift(1)) 
            df['deathCross'] = (df['kdj_k'] < df['kdj_d']) & (df['kdj_k'].shift(1) >= df['kdj_d'].shift(1))

            #判断超买超卖区域
            df['overbought'] = (df['kdj_k'] > self._DEF_KDJ_OVERBOUGHT_THRESOLD)
            df['oversold'] = (df['kdj_k'] < self._DEF_KDJ_OVERSOLD_THRESHOLD) 

            #判断J值极端情况
            df['jExtremeHigh'] = df['kdj_j'] > self._DEF_KDJ_J_EXTREME_HIGH_THRESHOLD
            df['jExtremeLow'] = df['kdj_j'] < self._DEF_KDJ_J_EXTREME_LOW_THRESHOLD 

            # 生成买入信号
            # 条件1：多头趋势 + 超卖区金叉
            df["conditionBuy1"]  = (df['bullishTrend'] & df['oversold']) & (df['goldenCross'])
            # 条件2：J值极端低位拐头（需配合后续确认）
            df['jTurnUp'] = (df['kdj_j'] > df['kdj_j'].shift(1)) & (df['kdj_j'].shift(1) <= df['kdj_j'].shift(2))
            df["conditionBuy2"] = df['jExtremeLow'] & df['jTurnUp']

            # 生成卖出信号
            # 条件1：超买区死叉
            df["conditionSell1"] = (df['overbought']) & (df['deathCross'])
            # 条件2：顶背离（简化版，需要外部计算价格高点）
            df["conditionSell2"] = df['overbought'] & df['deathCross'] & df['jExtremeHigh']

            df['buySignal'] = df["conditionBuy1"] | df["conditionBuy2"]
            df['sellSignal'] = df["conditionSell1"] | df["conditionSell2"]
            
            #遍历所有行,生成信号
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                buySignal = row['buySignal']
                sellSignal = row['sellSignal']
                if buySignal or sellSignal:
                    saveSet = {}
                    saveSet["date"] = currentDate
                    aList = []
                    if buySignal:
                        saveSet["suggestion"] = _DEF_SUGGESTION_BUY
                        if row["conditionBuy1"]:
                            aList.append("多头趋势+超卖区金叉;")
                        if row["conditionBuy2"]:
                            aList.append("J值极端低位拐头;")
                    else:
                        saveSet["suggestion"] = _DEF_SUGGESTION_SELL
                        if row["conditionSell1"]:   
                            aList.append("超买区死叉;")
                        if row["conditionSell2"]:
                            aList.append("顶背离;")
                    description = ",".join(aList)
                    description = description[:_DEF_DESCRIPTION_LEN]
                    saveSet["description"] = description    
                    saveSet["price"] = row["close"]
                    saveSet["kdj_k"] = row["kdj_k"]
                    saveSet["kdj_d"] = row["kdj_d"]
                    saveSet["kdj_j"] = row["kdj_j"]
                    saveSet["bullishTrend"] = row["bullishTrend"] 
                    saveSet["goldenCross"] = row["goldenCross"]
                    saveSet["deathCross"] = row["deathCross"]
                    saveSet["overbought"] = row["overbought"]
                    saveSet["oversold"] = row["oversold"]
                    saveSet["jExtremeHigh"] = row["jExtremeHigh"]   
                    saveSet["jExtremeLow"] = row["jExtremeLow"]
                    saveSet["jTurnUp"] = row["jTurnUp"] 

                    result[currentYMD] = saveSet
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
            return False
        return result


# 布林带类,负责计算布林带指标
"""
1. 判断趋势（中轨法则）
中轨是趋势强弱的分水岭，也是动态的支撑阻力位。
强势多头：价格沿上轨运行，且始终位于中轨上方。此时应只做多，回踩中轨不破是理想的买点。
强势空头：价格沿下轨运行，且始终位于中轨下方。此时应只做空，反弹至中轨承压是理想的卖点。
震荡行情：价格反复穿越中轨，且三轨走平。此时中轨失效，应参考上下轨进行高抛低吸。

2. 判断买卖点（开口与收口）
利用布林带的带宽变化来捕捉行情起爆点或转折点。
开口（喇叭开花）：经过极度窄幅横盘（收口）后，价格放量突破上轨，且上下轨同步扩张。这代表新趋势启动，可顺势追入。
收口：价格在高位或低位剧烈波动后，上下轨快速收敛。这代表趋势衰竭，即将进入变盘期，此时应离场观望，避免持仓等待。

3. 判断极端行情（盲点用法）
“价格触碰上轨卖出，触碰下轨买入” 这种用法在单边行情中极易亏损。正确判断如下：
在震荡市（三轨走平）：价格触碰上轨是卖出信号，触碰下轨是买入信号。
在单边市（三轨同向）：价格触碰上轨不是卖点，而是趋势加速点，应持仓或加仓；反之触碰下轨也无需恐慌割肉。

4. 两个关键辅助指标
带宽：带宽极窄时，预示即将出现剧烈变盘，其成功率往往高于金叉死叉。
%b 指标：判断价格处于布林带的具体位置。若 %b > 1（价格突破上轨上方）且伴随大成交量，是强势特征；若 %b < 0（价格跌破下轨），属于超跌，但在下跌趋势中可能持续钝化。
"""
class BollingerBandAnalyzer:
    """布林带类 - 计算布林带信号"""
    symbol = ""
    #判断指标阈值
    _DEF_BOLL_BAND_SLOPE_THRESHOLD = 0.1 #默认斜率阈值
    _DEF_BOLL_BAND_BANDWIDTH_THRESHOLD = 10 #默认带宽阈值
    _DEF_BOLL_BAND_SLOPE_PERIOD = 10 #默认斜率周期
    _DEF_BOLL_BAND_VOLUME_PERIOD = 10 #默认成交量周期
    _DEF_BOLL_BAND_VOLUME_THRESHOLD = 1.5 #默认成交量放大值阈值

    #bollinger band数据
    indicator = "boll"
    bollData = None
    bollSignals = {}
    bollKeyList = ["date","ma_60","boll_upper","boll_mid","boll_lower","open","close","high","low","volume","amount","turnover_rate"]
    
    def __init__(self,symbol,df=None):
        self.symbol = symbol
        if df is not None:
            #取部分字段数据
            self.bollData = df[self.bollKeyList]
    
    def calc(self):
        """计算布林带指标"""
        result = {}
        try:
            trendSignals = self.calculaterTrend()
            falseBreakSignals = self.detectFalseBreak()

            trueRevesalSignals = self.detectTrueRevesal()
            longEntrySignals = self.detectLongEntry()
            stopLossSignals = self.detectStopLoss()
            changeTrendSignals = self.detectChangeTrend()
            
            #合并所有信号,暂时没有其他的指标信号
            finalSignals = {}
            #首先处理真转势识别信号
            for YMD,item in trueRevesalSignals.items():
                if YMD in falseBreakSignals:
                    #是假突破,不处理
                    continue
                date = item.get("date","")
                isTrueRevesalEnd = item["isTrueRevesalEnd"]
                if isTrueRevesalEnd:
                    #建立相关信号字典
                    if YMD not in finalSignals:
                        finalSignals[YMD] = {}
                    #获取趋势信号
                    trendSignal = trendSignals.get(YMD,{})

                    #填充信号数据
                    finalSignals[YMD]["symbol"] = self.symbol
                    finalSignals[YMD]["indicator"] = self.indicator
                    finalSignals[YMD]["subtype"] = "boll真转势"
                    finalSignals[YMD]["subtype"] = finalSignals[YMD]["subtype"][0:_DEF_SUBTYPE_LEN]
                    finalSignals[YMD]["suggestion"] = _DEF_SUGGESTION_HOLD
                    bollBandDataString = f"收盘价:{item['close']} [低轨:{item['boll_lower']} 中轨:{item['boll_mid']} 上轨:{item['boll_upper']}]"
                    marketTrend = trendSignal.get("marketTrend",_DEF_MARKET_TREND_NEUTRAL)
                    marketTrendCN = _DEF_BOLL_MARKET_TREND_CN.get(marketTrend,_DEF_MARKET_TREND_NEUTRAL)
                    finalSignals[YMD]["marketTrend"] = marketTrend
                    description = f"{date},{self.symbol}:真转势信号;市场趋势状态:{marketTrendCN},{bollBandDataString}"
                    description = description[:_DEF_DESCRIPTION_LEN]
                    finalSignals[YMD]["description"] = description

                    #填充详细信息, result_json 数据
                    detail = {}
                    detail["isTrueRevesalEnd"] = item["isTrueRevesalEnd"]
                    detail["isBreakVolHigh"] = item["isBreakVolHigh"]
                    detail["isMidDowntrend_3d"] = item["isMidDowntrend_3d"]
                    detail["isTrueRevesal"] = item["isTrueRevesal"]
                    detail["retestMidNoVol"] = item["retestMidNoVol"]

                    finalSignals[YMD]["detail"] = detail

            #处理长趋势信号,入场信号
            for YMD,item in longEntrySignals.items():
                if YMD in falseBreakSignals:
                    #是假突破,不处理
                    continue
                date = item.get("date","")
                isLongEntry = item["isLongEntry"]
                if isLongEntry:
                    existFinalFlag = False
                    #建立相关信号字典
                    if YMD not in finalSignals:
                        finalSignals[YMD] = {}
                    else:
                        existFinalFlag = True
                        
                    #获取趋势信号
                    trendSignal = trendSignals.get(YMD,{})

                    #填充信号数据
                    finalSignals[YMD]["symbol"] = self.symbol
                    finalSignals[YMD]["indicator"] = self.indicator
                    finalSignals[YMD]["suggestion"] = _DEF_SUGGESTION_BUY
                    bollBandDataString = f"收盘价:{item['close']} [低轨:{item['boll_lower']} 中轨:{item['boll_mid']} 上轨:{item['boll_upper']}]"
                    marketTrend = trendSignal.get("marketTrend",_DEF_MARKET_TREND_NEUTRAL)
                    marketTrendCN = _DEF_BOLL_MARKET_TREND_CN.get(marketTrend,_DEF_MARKET_TREND_NEUTRAL)
                    subType = "boll长线入场"
                    subType = subType[0:_DEF_SUBTYPE_LEN]
                    if existFinalFlag:
                        # 增加信号数据
                        finalSignals[YMD]["subtype"] += f";{subType}"
                        description = finalSignals[YMD]["description"]
                        description += f";长线可以入场(回踩中轨+缩量),建议买入,{bollBandDataString}"
                        description = description[:_DEF_DESCRIPTION_LEN]
                        finalSignals[YMD]["description"] = description
                    else:
                        # 新增信号数据
                        finalSignals[YMD]["subtype"] = subType
                        description = f"{date},{self.symbol}:长线可以入场(回踩中轨+缩量),建议买入;市场趋势状态:{marketTrendCN},{bollBandDataString}"
                        description = description[:_DEF_DESCRIPTION_LEN]
                        finalSignals[YMD]["description"] = description
                        finalSignals[YMD]["marketTrend"] = marketTrend

                    #填充详细信息, result_json 数据
                    detail = {}
                    detail["isPullback_mid"] = item.get("isPullback_mid","")
                    detail["isVolShrink"] = item.get("isVolShrink","")
                    detail["isBullBackgroud"] = item.get("isBullBackgroud","")

                    finalSignals[YMD]["detail"] = detail

            #处理止损信号
            for YMD,item in stopLossSignals.items():
                if YMD in falseBreakSignals:
                    #是假突破,不处理
                    continue
                date = item.get("date","")
                isStopLoss = item["isStopLoss"]
                if isStopLoss:
                    #建立相关信号字典
                    existFinalFlag = False
                    if YMD not in finalSignals:
                        finalSignals[YMD] = {}
                    else:
                        existFinalFlag = True

                    #获取趋势信号
                    trendSignal = trendSignals.get(YMD,{})

                    #填充信号数据
                    finalSignals[YMD]["symbol"] = self.symbol
                    finalSignals[YMD]["indicator"] = self.indicator
                    finalSignals[YMD]["suggestion"] = _DEF_SUGGESTION_SELL
                    bollBandDataString = f"收盘价:{item['close']} [低轨:{item['boll_lower']} 中轨:{item['boll_mid']} 上轨:{item['boll_upper']}]" 
                    marketTrend = trendSignal.get("marketTrend",_DEF_MARKET_TREND_NEUTRAL)
                    marketTrendCN = _DEF_BOLL_MARKET_TREND_CN.get(marketTrend,_DEF_MARKET_TREND_NEUTRAL)
                    subType = "建议止损"
                    subType = subType[0:_DEF_SUBTYPE_LEN]
                    if existFinalFlag:
                        # 增加信号数据
                        finalSignals[YMD]["subtype"] += f";{subType}"
                        description = finalSignals[YMD]["description"]
                        description += f";止损(连续跌破中轨),建议卖出,{bollBandDataString}"
                        description = description[:_DEF_DESCRIPTION_LEN]
                        finalSignals[YMD]["description"] = description
                    else:
                        # 新增信号数据
                        finalSignals[YMD]["subtype"] = subType
                        description = f"{date},{self.symbol}:止损(连续跌破中轨),建议卖出;市场趋势状态:{marketTrendCN},{bollBandDataString}"
                        description = description[:_DEF_DESCRIPTION_LEN]
                        finalSignals[YMD]["description"] = description
                        finalSignals[YMD]["marketTrend"] = marketTrend

                    #填充详细信息, result_json 数据
                    detail = {}
                    detail["consucutiveBelow"] = item.get("consucutiveBelow","")
                    detail["isVolHigh"] = item.get("isVolHigh","")
                    detail["isMidTurnDown"] = item.get("isMidTurnDown","")

                    finalSignals[YMD]["detail"] = detail

            # 处理变盘信号
            for YMD,item in changeTrendSignals.items():
                if YMD in falseBreakSignals:
                    #是假突破,不处理
                    continue
                date = item.get("date","")
                turningPointType = item["turningPointType"]
                if turningPointType:
                    existFinalFlag = False
                    #建立相关信号字典
                    if YMD not in finalSignals:
                        finalSignals[YMD] = {}
                    else:
                        existFinalFlag = True

                    #获取趋势信号
                    trendSignal = trendSignals.get(YMD,{})

                    #填充信号数据
                    finalSignals[YMD]["symbol"] = self.symbol
                    finalSignals[YMD]["indicator"] = self.indicator
                    finalSignals[YMD]["suggestion"] = _DEF_SUGGESTION_HOLD
                    bollBandDataString = f"收盘价:{item['close']} [低轨:{item['boll_lower']} 中轨:{item['boll_mid']} 上轨:{item['boll_upper']}]"
                    marketTrend = trendSignal.get("marketTrend",_DEF_MARKET_TREND_NEUTRAL)
                    marketTrendCN = _DEF_BOLL_MARKET_TREND_CN.get(marketTrend,_DEF_MARKET_TREND_NEUTRAL)
                    subType = "变盘"
                    subType = subType[0:_DEF_SUBTYPE_LEN]
                    if existFinalFlag:
                        # 增加信号数据
                        finalSignals[YMD]["subtype"] += f";{subType}"
                        description = finalSignals[YMD]["description"]
                        if turningPointType == _DEF_MARKET_TREND_UP:
                            description += f";变盘(上),建议买入,{bollBandDataString}"
                        elif turningPointType == _DEF_MARKET_TREND_DOWN:
                            description += f";变盘(下),建议卖出,{bollBandDataString}"
                        else:
                            description += f";变盘(未知),建议保持,{bollBandDataString}"
                        description = description[:_DEF_DESCRIPTION_LEN]
                        finalSignals[YMD]["description"] = description
                    else:
                        # 新增信号数据
                        finalSignals[YMD]["subtype"] = subType
                        if turningPointType == _DEF_MARKET_TREND_UP:
                            description = f"变盘(向上),建议买入"
                        elif turningPointType == _DEF_MARKET_TREND_DOWN:
                            description = f"变盘(向下),建议卖出"
                        elif turningPointType == _DEF_MARKET_TREND_RANGE:
                            description = f"变盘(震荡),建议保持持仓"
                        else:
                            description = f"变盘(未知),建议观望,{bollBandDataString}"
                        description = f"{date},{self.symbol}:{description};市场趋势状态:{marketTrendCN},{bollBandDataString}"
                        description = description[:_DEF_DESCRIPTION_LEN]
                        finalSignals[YMD]["description"] = description
                        finalSignals[YMD]["marketTrend"] = marketTrend

                    #填充详细信息, result_json 数据
                    detail = {}
                    finalSignals[YMD]["detail"] = detail

            # 准备输出结果
            result["trendSignals"] = trendSignals # 趋势信号
            result["falseBreakSignals"] = falseBreakSignals
            result["trueRevesalSignals"] = trueRevesalSignals
            result["longEntrySignals"] = longEntrySignals
            result["stopLossSignals"] = stopLossSignals

            result["finalSignals"] = finalSignals

            self.bollSignals = result   
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"

        return result

    #1.趋势判断（中轨斜率）
    def calculaterTrend(self,slopePeriod=10):
        """
        判断市场趋势状态
        
        参数:
            包含'boll_mid''列的DataFrame
            slopePeriod: 斜率计算周期（长线建议10）
        返回:
            result: 包含市场趋势状态的字典
            key: date
            value: 包含趋势状态的字典

        使用建议
        趋势状态	适用策略	禁用策略
        UPTREND	回踩中轨买入、突破上轨加仓	逆势做空、触碰上轨卖出
        DOWNTREND	反弹中轨卖出、跌破下轨加空	逆势做多、触碰下轨买入
        RANGE	上下轨高抛低吸	追突破、长期持仓
        NEUTRAL	观望等待	开新仓
        核心原则：在UPTREND和DOWNTREND状态下，放弃逆势操作；在RANGE状态下，放弃趋势跟踪策略。

        """
        result = {}
        try:
            df = self.bollData        
            # 计算中轨斜率（百分比变化）
            df['mid_slope'] = (df['boll_mid'] - df['boll_mid'].shift(slopePeriod)) / df['boll_mid'].shift(slopePeriod) * 100
            
            # 计算带宽（用于震荡判断）
            df['bandwidth_pct'] = (df['boll_upper'] - df['boll_lower']) / df['boll_mid'] * 100
            
            # df = df.dropna()

            # 定义趋势条件
            df['isUptrend'] = (
                (df['boll_mid'] > df['boll_mid'].shift(1)) &                # 中轨向上
                (df['mid_slope'] > self._DEF_BOLL_BAND_SLOPE_THRESHOLD) &   # 斜率正
                (df['close'] > df['boll_mid'])                              # 价格在中轨上方
            )
            
            df['isDowntrend'] = (
                (df['boll_mid'] < df['boll_mid'].shift(1)) &                 # 中轨向下
                (df['mid_slope'] < -self._DEF_BOLL_BAND_SLOPE_THRESHOLD) &   # 斜率负
                (df['close'] < df['boll_mid'])                               # 价格在中轨下方
            )
            
            # 震荡市：斜率平缓 + 带宽收窄
            df['isRange'] = (
                (abs(df['mid_slope']) < self._DEF_BOLL_BAND_SLOPE_THRESHOLD) &   # 斜率平缓
                (df['bandwidth_pct'] < self._DEF_BOLL_BAND_BANDWIDTH_THRESHOLD)  # 带宽小于10%（可调整）
            )
            
            # 合并趋势标签
            df['trend'] = _DEF_MARKET_TREND_NEUTRAL
            df.loc[df['isUptrend'], 'trend'] = _DEF_MARKET_TREND_UP
            df.loc[df['isDowntrend'], 'trend'] = _DEF_MARKET_TREND_DOWN
            df.loc[df['isRange'], 'trend'] = _DEF_MARKET_TREND_RANGE

            self.bollData = df

            #转成字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                saveSet = {'date':currentDate,'marketTrend':row['trend']}
                saveSet["mid_slope"] = row["mid_slope"]
                saveSet["bandwidth_pct"] = row["bandwidth_pct"]
                saveSet["isUptrend"] = row["isUptrend"]
                saveSet["isDowntrend"] = row["isDowntrend"]
                saveSet["isRange"] = row["isRange"]
                saveSet["close"] = row["close"]
                saveSet["boll_lower"] = row["boll_lower"]
                saveSet["boll_mid"] = row["boll_mid"]
                saveSet["boll_upper"] = row["boll_upper"]
                result[currentYMD] = saveSet
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        
        return result
    
    #2.假跌破检测（缩量跌破+快速收回）
    def detectFalseBreak(self,volumePeriod=10):        
        """
        检测假突破 （缩量跌破+快速收回）
        参数:
            df: 包含'close', 'boll_mid', 'volume'列的DataFrame
            volume_period: 均量周期
        返回:
            result: 包含假跌破确认信号的字典
            key: date
            value: 包含假跌破确认信号的字典
        """
        result = {}       
        try:
            df = self.bollData
            # 计算成交量均线
            df['vol_ma'] = df['volume'].rolling(window=volumePeriod).mean()
            # 缩量定义：成交量低于均量
            df['isVolShrink'] = df['volume'] < df['vol_ma']
            # 跌破中轨：前一日收盘价在中轨上方，今日收盘价在中轨下方
            df['isBreakMid'] = (df['close'] < df['boll_mid']) & (df['close'].shift(1) > df['boll_mid'])
            # 假跌破：缩量跌破+快速收回
            df['isFalseBreak'] = df['isVolShrink'] & df['isBreakMid']
            # 收回中轨：前一日在中轨下方，今日在中轨上方
            df['isRecoverMid'] = (df['close'] > df['boll_mid']) & (df['close'].shift(1) < df['boll_mid'])
            # 收回放量：放量阳线收回
            df['isRecoverVol'] = df['isRecoverMid'] & (df['volume'] > df['vol_ma']) & (df['close'] > df['open']) #阳线

            #计算过去5天内是否有假跌破
            df['isFalseBreak_5d'] = df['isFalseBreak'].rolling(window=5).max().fillna(0)
            
            # 完整假跌破确认：有假跌破 + 收回放量 + 当前处于多头趋势
            df['isFalseBreakConfirm'] = (
                    (df['isFalseBreak_5d'] == 1) & 
                    df['isRecoverVol'] & 
                    df['isUptrend']
                )
                
            # 转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                isFalseBreakConfirm = row['isFalseBreakConfirm']
                if isFalseBreakConfirm:
                    saveSet = {'date':currentDate,'isFalseBreakConfirm':isFalseBreakConfirm}
                    saveSet["vol_ma"] = row["vol_ma"]
                    saveSet["isVolShrink"] = row["isVolShrink"]
                    saveSet["isFalseBreak_5d"] = row["isFalseBreak_5d"]
                    saveSet["isRecoverVol"] = row["isRecoverVol"]
                    saveSet["isUptrend"] = row["isUptrend"]
                    saveSet["close"] = row["close"]
                    saveSet["boll_lower"] = row["boll_lower"]
                    saveSet["boll_mid"] = row["boll_mid"]
                    saveSet["boll_upper"] = row["boll_upper"]
                    result[currentYMD] = saveSet
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        
        return result

    #3.真转势识别（放量跌破+反抽无力）
    def detectTrueRevesal(self,volumePeriod=10):
        """
        识别真转势（趋势反转）信号
    
        参数:
            df: 包含'close', 'boll_mid', 'volume'列的DataFrame
            volume_period: 均量周期
        返回:
            df: 添加真转势信号列的DataFrame
        """
        result = {}       
        try:
            df = self.bollData
            df["vol_ma"] = df["volume"].rolling(window=volumePeriod).mean()

            # 放量跌破：成交量放大1.5倍以上
            df["isBreakVolHigh"] = (df["volume"] > df["vol_ma"] * self._DEF_BOLL_BAND_VOLUME_THRESHOLD) & (df["close"] < df["boll_mid"])

            # 中轨持续向下,(3日累计)
            df["isMidDowntrend_3d"] = (df["boll_mid"] < df["boll_mid"].shift(3)) & (df["boll_mid"].shift(3) < df["boll_mid"].shift(6))

            # 真趋势: 放量跌破 + 中轨向下
            df["isTrueRevesal"] = df["isBreakVolHigh"] & df["isMidDowntrend_3d"]

            # 反抽中轨但无量
            df['retestMidNoVol'] = (
                    (df['close'] > df['boll_mid']) &                         # 价格回到中轨上方
                    (df['volume'] < df['vol_ma']) &                          # 缩量
                    (df['close'] < df['high'].shift(1)) &                    # 未突破前高
                    df['isMidDowntrend_3d']                                       # 中轨仍向下
             )

            # 趋势结束信号：真转势 或 反抽受阻
            df['isTrueRevesalEnd'] = df['isTrueRevesal'] | df['retestMidNoVol']

            #转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                isTrueRevesalEnd = row['isTrueRevesalEnd']
                if isTrueRevesalEnd:
                    saveSet = {'date':currentDate,'isTrueRevesalEnd':isTrueRevesalEnd}
                    saveSet["vol_ma"] = row["vol_ma"]
                    saveSet["isBreakVolHigh"] = row["isBreakVolHigh"]
                    saveSet["isMidDowntrend_3d"] = row["isMidDowntrend_3d"]
                    saveSet["isTrueRevesal"] = row["isTrueRevesal"]
                    saveSet["retestMidNoVol"] = row["retestMidNoVol"]
                    saveSet["close"] = row["close"]
                    saveSet["boll_lower"] = row["boll_lower"]
                    saveSet["boll_mid"] = row["boll_mid"]
                    saveSet["boll_upper"] = row["boll_upper"]
                    result[currentYMD] = saveSet
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"

        return result

    # 4.长线进场信号（回踩中轨+缩量）
    def detectLongEntry(self,slopeThreshold=0.5):
        """
        识别长线进场信号
        参数:
            df: 包含'close', 'boll_mid', 'volume'列的DataFrame
        返回:

        # 宽松版本（默认）- 捕捉任何微弱的上升趋势
        条件：slopeThreshold > 0.1

        # 中等版本 - 要求5天上涨0.5%以上
        条件：slopeThreshold > 0.5

        # 严格版本（适合长线）- 要求5天上涨1%以上
        条件：slopeThreshold > 1.0

        # 极端严格（只做强趋势）- 要求5天上涨3%以上
        条件：slopeThreshold > 3.0
    
        """
        result = {}
        try:
            df = self.bollData
            # 在 calculaterTrend 中计算了vol_ma和mid_slope,这里直接使用
            # df["vol_ma"] = df["volume"].rolling(window=volumePeriod).mean()
            # df['mid_slope'] = (df['boll_mid'] - df['boll_mid'].shift(slopePeriod)) / df['boll_mid'].shift(slopePeriod) * 100

            # 计算实体占比（锤头线或阳线实体占K线比例）
            df["bodyRatio"] = (df["close"] - df["low"]) / (df["high"] - df["low"] + 1e-10)

            # 回踩中轨：最低价跌破中轨，收盘价站回中轨，实体占比>0.7
            df["isPullback_mid"] = (df["low"] < df["boll_mid"]) & (df["close"] > df["boll_mid"]) & (df["bodyRatio"] > 0.7)

            # 缩量回踩：成交量小于10日均量
            df["isVolShrink"] = df["isPullback_mid"] & (df["volume"] < df["vol_ma"])

            # 多头背景(中轨向上且斜率正)
            df["isBullBackgroud"] = (df["boll_mid"] > df["boll_mid"].shift(5)) & (df["mid_slope"] > slopeThreshold)

            # 长线买点
            df["isLongEntry"] = df["isVolShrink"] & df["isBullBackgroud"]

            #转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                isLongEntry = row['isLongEntry']
                if isLongEntry:
                    saveSet = {'date':currentDate,'isLongEntry':isLongEntry}
                    saveSet["isPullback_mid"] = row["isPullback_mid"]
                    saveSet["isVolShrink"] = row["isVolShrink"]
                    saveSet["isBullBackgroud"] = row["isBullBackgroud"]
                    saveSet["close"] = row["close"]
                    saveSet["boll_lower"] = row["boll_lower"]
                    saveSet["boll_mid"] = row["boll_mid"]
                    saveSet["boll_upper"] = row["boll_upper"]
                    result[currentYMD] = saveSet
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        
        return result
        
    # 5.持仓与止损（连续跌破中轨）
    def detectStopLoss(self):
        """
        止损信号：连续2天收盘价跌破中轨 + 趋势向下
        参数:
            df: 包含'close', 'boll_mid', 'volume'列的DataFrame
        返回:
        """
        result = {}
        try:
            df = self.bollData
            # 当日跌破中轨：收盘价跌破中轨
            df["isBreakMid"] = (df["close"] < df["boll_mid"])

            # 连续2天跌破中轨：收盘价跌破中轨，且趋势向下
            df["consucutiveBelow"] = df["isBreakMid"] & df["isBreakMid"].shift(1)

            # 放量或中轨拐头时止损有效性更高
            df['isVolHigh'] = df['volume'] > df['vol_ma']

            df['isMidTurnDown'] = ((df['boll_mid'] < df['boll_mid'].shift(3)) & 
                    (df['boll_mid'].shift(3) < df['boll_mid'].shift(6)))

            df['isStopLoss'] = df['consucutiveBelow'] & (df['isVolHigh'] | df['isMidTurnDown'])

            # 转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                isStopLoss = row['isStopLoss']
                if isStopLoss:
                    saveSet = {'date':currentDate,'isStopLoss':isStopLoss}
                    saveSet["consucutiveBelow"] = row["consucutiveBelow"]
                    saveSet["isVolHigh"] = row["isVolHigh"]
                    saveSet["isMidTurnDown"] = row["isMidTurnDown"]
                    saveSet["close"] = row["close"]
                    saveSet["boll_lower"] = row["boll_lower"]
                    saveSet["boll_mid"] = row["boll_mid"]
                    saveSet["boll_upper"] = row["boll_upper"]
                    result[currentYMD] = saveSet
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        
        return result
    
    #带宽指标——用于识别震荡末期,布林带带宽指标（变盘预警）
    def detectChangeTrend(self,lookbackPeriod=100, threshold_pct=0.3, max_bandwidth=5):
        """
        带宽:=(UPPER-LOWER)/MID*100; {百分比带宽}
        极窄区:=带宽<HHV(带宽,100)*0.3 AND 带宽<5; {带宽处于历史30%分位以下}
        变盘预警:极窄区 AND (CLOSE>REF(HIGH,1) OR CLOSE<REF(LOW,1));
        参数:
            df: 包含'bandwidth_pct'列的DataFrame
        返回:
        """
        result = {}
        try:
            df = self.bollData

            # 极窄区识别（历史30%分位）
            df['hhv_bandwidth_pct'] = df['bandwidth_pct'].rolling(window=lookbackPeriod, min_periods=1).max()
            df['extremeNarrow'] = (df['bandwidth_pct'] < df['hhv_bandwidth_pct'] * threshold_pct) & (df['bandwidth_pct'] < max_bandwidth)

            # 变盘信号
            df['breakoutUp'] = df['close'] > df['high'].shift(1)
            df['breakoutDown'] = df['close'] < df['low'].shift(1)
            df['breakout'] = df['breakoutUp'] | df['breakoutDown']

            df['turningPointAlert'] = df['extremeNarrow'] & df['breakout']
            df['turningPointUpAlert'] = df['extremeNarrow'] & df['breakoutUp']
            df['turningPointDownAlert'] = df['extremeNarrow'] & df['breakoutDown']

            # 转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                turningPointAlert = row["turningPointAlert"]
                turningPointUpAlert = row["turningPointUpAlert"]
                turningPointDownAlert = row["turningPointDownAlert"]
                if turningPointAlert:
                    if turningPointUpAlert:
                        turningPointType = _DEF_MARKET_TREND_UP
                    if turningPointDownAlert:
                        turningPointType = _DEF_MARKET_TREND_DOWN
                    saveSet = {'date':currentDate,'turningPointType':turningPointType}
                    saveSet["close"] = row["close"]
                    saveSet["boll_lower"] = row["boll_lower"]
                    saveSet["boll_mid"] = row["boll_mid"]
                    saveSet["boll_upper"] = row["boll_upper"]
                    result[currentYMD] = saveSet
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        
        return result

"""
RSI类 - 计算RSI信号
RSI（相对强弱指数）是投资中非常经典的技术指标，主要用于衡量价格涨跌的力度和速度，帮助判断超买或超卖状态。

数值范围：0 – 100
常用周期：14天（也可根据交易风格调整，如短线用7天或9天）
核心逻辑：通过比较一定时期内的平均收盘涨数和平均收盘跌数，来评估多空力量。

这是RSI最基础、最常用的功能。
超买区（RSI > 70）： 说明价格近期上涨过快，买方力量可能已过度释放，价格有回调或下跌的风险。此时应考虑卖出或减仓。
超卖区（RSI < 30）： 说明价格近期下跌过猛，卖方力量可能已衰竭，价格有反弹或上涨的可能。此时应考虑买入或加仓。
注意：在强劲的牛市中，RSI可能长时间停留在70以上；在深度的熊市中，也可能长时间低于30。此时直接反向操作风险较大，需要结合其他方法。

顶背离（看跌信号）：
价格：创出新高（比前一个高点高）
RSI：未能创出新高，反而形成更低的峰顶
含义：上涨动能不足，是强烈的卖出信号。
底背离（看涨信号）：
价格：创出新低（比前一个低点低）
RSI：未能创出新低，反而形成更高的谷底
含义：下跌动能衰竭，是强烈的买入信号。

2. 中轴线（50线）判断强弱
RSI > 50：市场处于强势区域，多头主导，可优先考虑买入或持有。
RSI < 50：市场处于弱势区域，空头主导，应谨慎或考虑卖出。
RSI 上穿50： 可视为短期走强的初步信号。
RSI 下破50： 可视为短期走弱的初步信号。

3. 形态分析
像分析K线形态一样，你也可以分析RSI线的形态。
头肩顶/底、双重顶/底 等经典形态出现在RSI上时，其突破信号比K线形态有时更早出现。
例如：RSI形成“头肩顶”形态并跌破颈线，是卖出信号。

4. 金叉与死叉（结合两条不同周期的RSI线）
你可以设置两条RSI线，例如：
快线：周期为6天的 RSI（更敏感）
慢线：周期为12或24天的 RSI（更平滑）
金叉（买入信号）：快线从下方向上穿越慢线，通常发生在超卖区或从低位回升时。
死叉（卖出信号）：快线从上方向下穿越慢线，通常发生在超买区或从高位回落时。

四、实际操作流程示例
假设你准备交易一只股票，可以按以下步骤综合判断：
看大局：确认50线的位置。若RSI在50上方，只考虑做多（买入），反之亦然。
找机会：等待RSI进入超卖区（<30），或出现底背离形态。
等确认：此时不要立即买入。等待RSI向上突破30或金叉出现，或价格K线出现反转信号（如锤子线、阳包阴）。
定买卖：
买入：信号确认后入场。
卖出：当RSI进入超买区（>70）、出现顶背离、或死叉时卖出。
设止损：买入时，可将止损设在底背离对应的价格低点下方。

五、RSI的局限性
没有完美的指标，RSI也有几个主要缺点：
钝化：在单边暴涨或暴跌行情中，RSI会长期停留在超买/超卖区，导致过早卖出或买入，错过主升浪/主跌浪。
震荡市更有效：RSI在震荡行情中表现极佳，但在强趋势行情中表现一般。
需要确认：RSI是先行或同步指标，最好结合成交量、均线、MACD 等其他工具共同验证。

总结:
判断方法	买入信号	卖出信号
超买/超卖	RSI < 30 且开始掉头向上	RSI > 70 且开始掉头向下
背离	价格新低，RSI未新低（底背离）	价格新高，RSI未新高（顶背离）
中轴线	RSI 从下向上突破 50	RSI 从上向下跌破 50
金叉/死叉	快线上穿慢线（尤其在50以下）	快线下穿慢线（尤其在50以上）

核心建议：不建议单独使用RSI，把它当作一个“提醒工具”或“过滤器”效果会更好。比如，只在RSI显示超卖时才考虑买入，只在RSI显示超买时才考虑卖出，然后结合趋势线和成交量等做最终决策。

"""
class RSIAnalyzer:
    """RSI类 - 计算RSI信号"""
    symbol = ""
    #判断指标阈值
    _DEF_RSI_OVERBOUGHT_THRESHOLD = 70 #默认超买阈值
    _DEF_RSI_OVERSOLD_THRESHOLD = 30 #默认超卖阈值
    
    #RSI数据
    indicator = "rsi"
    rsiData = None
    rsiSignals = {}
    rsiKeyList = ["date","ma_60","rsi_6","rsi_12","rsi_24","open","close","high","low","volume","amount","turnover_rate"]
    
    def __init__(self,symbol,df=None):
        self.symbol = symbol
        if df is not None:
            #取部分字段数据
            self.rsiData = df[self.rsiKeyList]
    
    def calc(self):
        """计算RSI指标"""
        result = {}
        try:
            #计算主要技术指标
            midLineCrossSignals = self.check_mid_line_cross()

            overboughtOversoldSignals = self.check_overbought_oversold()
            divergenceSignals = self.check_divergence()
            goldenDeathCrossSignals = self.check_golden_death_cross()

            #综合技术指标和判断
            """
            看大局：确认50线的位置。若RSI在50上方，只考虑做多（买入），反之亦然。
            找机会：等待RSI进入超卖区（<30），或出现底背离形态。
            等确认：此时不要立即买入。等待RSI向上突破30或金叉出现，或价格K线出现反转信号（如锤子线、阳包阴）。
            定买卖：
            买入：信号确认后入场。
            卖出：当RSI进入超买区（>70）、出现顶背离、或死叉时卖出。
            设止损：买入时，可将止损设在底背离对应的价格低点下方。
            """
            finalSignals = {}
            for YMD,overSignal in overboughtOversoldSignals.items():
                overbought = overSignal["overbought"]
                overboughtCross = overSignal["overboughtCross"]
                oversold = overSignal["oversold"]
                oversoldCross = overSignal["oversoldCross"]
                overStatus = overSignal["overStatus"]

                # 看大局：确认50线的位置。若RSI在50上方，只考虑做多（买入），反之亦然。
                midLineSignal = midLineCrossSignals.get(YMD,{})
                crossAboveMid = midLineSignal.get("crossAboveMid",False)
                crossBelowMid = midLineSignal.get("crossBelowMid",False)
                crossStrong = midLineSignal.get("crossStrong",False)
                crossWeak = midLineSignal.get("crossWeak",False)
                crossStatus = midLineSignal.get("crossStatus","")
 
                # 背离信号
                divergenceSignal = divergenceSignals.get(YMD,{})
                bearishDivergence = False #顶背离
                bullishDivergence = False #底背离
                if divergenceSignal:
                    bearishDivergence = divergenceSignal["bearishDivergence"] #顶背离
                    bullishDivergence = divergenceSignal["bullishDivergence"] #底背离

                # 金叉/死叉信号
                goldenDeathCrossSignal = goldenDeathCrossSignals.get(YMD,{})
                goldenCross = False #金叉
                deathCross = False #死叉
                if goldenDeathCrossSignal:
                    goldenCross = goldenDeathCrossSignal["goldenCross"] #金叉
                    deathCross =  goldenDeathCrossSignal["deathCross"] #死叉
                    
                if overbought:
                    # 卖出：当RSI进入超买区（>70）、出现顶背离、或死叉时卖出
                    suggestion = _DEF_SUGGESTION_SELL
                    if overbought:
                        if YMD not in finalSignals:
                            finalSignals[YMD] = {}
                        #填充信号数据
                        finalSignals[YMD]["symbol"] = self.symbol
                        finalSignals[YMD]["indicator"] = self.indicator
                        finalSignals[YMD]["subtype"] = "rsi超买"
                        descString = f"RSI超买区,RSI值:{overSignal['rsi']}"
                        if bearishDivergence:
                            finalSignals[YMD]["subtype"] += ";顶背离"
                            descString += ";顶背离"
                        if deathCross:
                            finalSignals[YMD]["subtype"] += ";死叉"
                            descString += ";死叉"
                        finalSignals[YMD]["subtype"] = finalSignals[YMD]["subtype"][0:_DEF_SUBTYPE_LEN]
                        finalSignals[YMD]["suggestion"] = suggestion
                        extString = f"收盘价:{overSignal['close']}"
                        description = f"{overSignal['date']},{self.symbol}:建议卖出,{descString};{extString},当RSI进入超买区（>70）、出现顶背离、或死叉时卖出;在单边暴涨或暴跌行情中，RSI会长期停留在超买/超卖区，导致过早卖出或买入"
                        description = description[:_DEF_DESCRIPTION_LEN]
                        finalSignals[YMD]["description"] = description

                        #填充详细信息, result_json 数据
                        detail = {}
                        detail["overSignal"] = overSignal
                        finalSignals[YMD]["detail"] = detail

                if oversold and crossStrong:  
                    # 找机会, 等待RSI进入超卖区（<30),买入
                    # 等确认：此时不要立即买入。等待RSI向上突破30或金叉出现
                    suggestion = _DEF_SUGGESTION_BUY
                    confirmData = self.findCrossSignal(YMD,suggestion,goldenDeathCrossSignals)
                    confimYMD = confirmData.get("YMD","")
                    if confimYMD:
                        confirmDate = confirmData.get("date","")
                        #填充信号数据
                        finalSignals[YMD]["symbol"] = self.symbol
                        finalSignals[YMD]["indicator"] = self.indicator
                        finalSignals[YMD]["suggestion"] = suggestion
                        finalSignals[YMD]["subtype"] = "rsi超卖+金叉"
                        finalSignals[YMD]["subtype"] = finalSignals[YMD]["subtype"][0:_DEF_SUBTYPE_LEN]
                        descString = f"RSI超卖区+{confirmDate}:金叉,RSI值:{overSignal['rsi']}"
                        extString = f"收盘价:{overSignal['close']}"
                        description = f"{overSignal['date']},{self.symbol}:建议买入,{descString};{extString},当RSI进入超卖区（<30）,RSI向上突破30或金叉出现时买入;在单边暴涨或暴跌行情中，RSI会长期停留在超买/超卖区，导致过早卖出或买入"
                        description = description[:_DEF_DESCRIPTION_LEN]
                        finalSignals[YMD]["description"] = description

                        #填充详细信息, result_json 数据
                        detail = {}
                        detail["overSignal"] = overSignal
                        finalSignals[YMD]["detail"] = detail
                        pass

            #顶背离卖出情况
            for YMD, divergenceSignal in divergenceSignals.items():
                if YMD not in finalSignals:
                    finalSignals[YMD] = {}
                else:
                    # 检查是否已给出卖出信号
                    currSuggestion = finalSignals[YMD]["suggestion"]
                    if currSuggestion == _DEF_SUGGESTION_SELL:
                        continue
                #填充信号数据
                finalSignals[YMD]["symbol"] = self.symbol
                finalSignals[YMD]["indicator"] = self.indicator
                finalSignals[YMD]["subtype"] = "rsi顶背离"
                finalSignals[YMD]["subtype"] = finalSignals[YMD]["subtype"][0:_DEF_SUBTYPE_LEN]
                finalSignals[YMD]["suggestion"] = suggestion
                descString = f"RSI顶背离,RSI值:{divergenceSignal['rsi']}"
                extString = f"收盘价:{divergenceSignal['close']}"
                description = f"{divergenceSignal['date']},{self.symbol}:建议卖出,{descString};{extString},当RSI进入超买区（>70）、出现顶背离、或死叉时卖出"
                description = description[:_DEF_DESCRIPTION_LEN]
                finalSignals[YMD]["description"] = description

                #填充详细信息, result_json 数据
                detail = {}
                detail["divergenceSignal"] = divergenceSignal
                finalSignals[YMD]["detail"] = detail
                pass

            #死叉卖出情况
            for YMD, goldenDeathSignal in goldenDeathCrossSignals.items():
                if YMD not in finalSignals:
                    finalSignals[YMD] = {}
                else:
                    # 检查是否已给出卖出信号
                    currSuggestion = finalSignals[YMD]["suggestion"]
                    if currSuggestion == _DEF_SUGGESTION_SELL:
                        continue
                crossStatus = goldenDeathSignal.get("crossStatus","")
                #填充信号数据
                finalSignals[YMD]["symbol"] = self.symbol
                finalSignals[YMD]["indicator"] = self.indicator
                finalSignals[YMD]["subtype"] = "rsi死叉"
                finalSignals[YMD]["subtype"] = finalSignals[YMD]["subtype"][0:_DEF_SUBTYPE_LEN]
                finalSignals[YMD]["suggestion"] = suggestion
                descString = f"RSI死叉,RSI值:{goldenDeathSignal['rsi']},{crossStatus}"
                extString = f"收盘价:{goldenDeathSignal['close']}"
                description = f"{goldenDeathSignal['date']},{self.symbol}:建议卖出,{descString};{extString},当RSI进入超买区（>70）、出现顶背离、或死叉时卖出"
                description = description[:_DEF_DESCRIPTION_LEN]
                finalSignals[YMD]["description"] = description

                #填充详细信息, result_json 数据
                detail = {}
                detail["goldenDeathSignal"] = goldenDeathSignal
                finalSignals[YMD]["detail"] = detail

            result["overboughtOversoldSignals"] = overboughtOversoldSignals
            result["midLineCrossSignals"] = midLineCrossSignals
            result["divergenceSignals"] = divergenceSignals  
            result["goldenDeathCrossSignals"] = goldenDeathCrossSignals  

            result["finalSignals"] = finalSignals
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    #查找未来金叉信号
    def findCrossSignal(self,startYMD,suggestion,goldenDeathCrossSignals,extNum=3):
        """
        等待RSI向上突破30或金叉出现，或价格K线出现反转信号（如锤子线、阳包阴）。    
        参数:
            startYMD (str): 日期，格式为"YYYYMMDD"
            suggestion (str): 信号类型，"buy"或"sell"
            goldenDeathCrossSignals (dict): 金叉/死叉信号字典，键为日期，值为信号字典
            extNum (int): 扩展天数，默认3,检查未来3天是否有交叉信号,对周信号, 没有意义
        
        返回:
            dict: 包含确认信号的字典，键为日期，值为信号字典
        """
        result = {}
        try:
            YMDList = misc.getDaysAfter(extNum,startYMD)
            for YMD in YMDList:
                goldenDeathCrossSignal = goldenDeathCrossSignals.get(YMD,{})
                if suggestion == _DEF_SUGGESTION_BUY and goldenDeathCrossSignal:
                    if goldenDeathCrossSignal:
                        date = goldenDeathCrossSignal["date"]
                        crossStatus = goldenDeathCrossSignal["crossStatus"]
                        goldenCross = goldenDeathCrossSignal["goldenCross"]
                        close = goldenDeathCrossSignal["close"]
                        rsi = goldenDeathCrossSignal["rsi"]
                        if goldenCross:
                            overDate = misc.YMD2HumanDate(startYMD)
                            descString = f"{overDate}:超卖区 + {date}:金叉"
                            result = {"YMD":YMD,"date":date,"subtype":"rsi超卖区+金叉","description":descString,"goldenCross":goldenCross,"suggestion":suggestion,"crossStatus":crossStatus,"close":close}    
                            break
                        if rsi > self._DEF_RSI_OVERSOLD_THRESHOLD:
                            overDate = misc.YMD2HumanDate(startYMD)
                            descString = f"{overDate}:超卖区 + {date}:RSI向上突破{self._DEF_RSI_OVERSOLD_THRESHOLD}"
                            result = {"YMD":YMD,"date":date,"subtype":"rsi超卖区+RSI向上突破{self._DEF_RSI_OVERSOLD_THRESHOLD}","description":descString,"suggestion":suggestion,"crossStatus":crossStatus,"close":close}    
                            break
                elif suggestion == _DEF_SUGGESTION_SELL and goldenDeathCrossSignal:
                    if goldenDeathCrossSignal:
                        date = goldenDeathCrossSignal["date"]
                        crossStatus = goldenDeathCrossSignal["crossStatus"]
                        deathCross = goldenDeathCrossSignal["deathCross"]
                        close = goldenDeathCrossSignal["close"]
                        if deathCross:
                            overDate = misc.YMD2HumanDate(startYMD)
                            descString = f"{overDate}:超买区 + {date}:死叉"
                            result = {"YMD":YMD,"date":date,"subtype":"rsi超买区+死叉","description":descString,"deathCross":deathCross,"suggestion":suggestion,"crossStatus":crossStatus,"close":close}    
                            break
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    #检查超买超卖状态
    def check_overbought_oversold(self,rsiCol="rsi_12"):
        """
        检查超买/超卖状态
        
        参数:
            rsiCol (str): RSI指标列名，默认"rsi_12"
        
        返回:
            dict: 包含超买超卖信号的字典，键为日期，值为信号字典
        """
        result = {}
        try:
            df = self.rsiData

            # 初始化超买超卖信号列
            overboughtCol = f'{rsiCol}_overbought'
            overboughtCrossCol = f'{rsiCol}_overbought_cross'
            oversoldCol = f'{rsiCol}_oversold'
            oversoldCrossCol = f'{rsiCol}_oversold_cross'

            # 超买信号：RSI > 超买阈值 且从上方下穿
            df[overboughtCol] = (df[rsiCol] > self._DEF_RSI_OVERBOUGHT_THRESHOLD)
            df[overboughtCrossCol] = (df[rsiCol] > self._DEF_RSI_OVERBOUGHT_THRESHOLD) & (df[rsiCol].shift(1) <= self._DEF_RSI_OVERBOUGHT_THRESHOLD)
            
            # 超卖信号：RSI < 超卖阈值 且从下方上穿
            df[oversoldCol] = (df[rsiCol] < self._DEF_RSI_OVERSOLD_THRESHOLD)
            df[oversoldCrossCol] = (df[rsiCol] < self._DEF_RSI_OVERSOLD_THRESHOLD) & (df[rsiCol].shift(1) >= self._DEF_RSI_OVERSOLD_THRESHOLD)
            
            # 转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                #判断超买超卖状态
                overbought = row[overboughtCol]
                overboughtCross = row[overboughtCrossCol]
                oversold = row[oversoldCol]
                oversoldCross = row[oversoldCrossCol]
                if overbought or oversold:
                    saveSet = {'date':currentDate}
                    if overbought:
                        saveSet["overStatus"] = "超买"
                    else:
                        saveSet["overStatus"] = "超卖"
                    # 超买信号
                    saveSet["overbought"] = overbought
                    saveSet["overboughtCross"] = overboughtCross
                    # 超卖信号
                    saveSet["oversold"] = oversold
                    saveSet["oversoldCross"] = oversoldCross
                    # 原始数据
                    saveSet["close"] = row["close"]
                
                    saveSet[rsiCol] = row[rsiCol]
                    saveSet["rsi"] = saveSet[rsiCol]

                    result[currentYMD] = saveSet

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    #检查背离信号
    def check_divergence(self, rsiCol="rsi_12",lookback = 20):
        """
        检查背离信号（顶背离和底背离）bearishDivergence, bullishDivergence
        
        参数:
            rsiCol (str): RSI指标列名，默认"rsi_12"
            lookback (int): 回溯周期（默认20）
        
        返回:
            dict: 包含背离信号的字典，键为日期，值为信号字典
        """
        result = {}
        try:
            df = self.rsiData

            # 初始化背离列
            bearishDivergenceCol = f'{rsiCol}_bearish_divergence'
            bullishDivergenceCol = f'{rsiCol}_bullish_divergence'
            # 初始化背离信号列
            df[bearishDivergenceCol] = False
            df[bullishDivergenceCol] = False
            
            for i in range(lookback, len(df)):
                # 获取窗口内的数据
                price_window = df["close"].iloc[i-lookback:i+1]
                rsi_window = df[rsiCol].iloc[i-lookback:i+1]
                
                # 当前价格和RSI
                current_price = price_window.iloc[-1]
                current_rsi = rsi_window.iloc[-1]
                
                # 找窗口内的高点和低点
                price_high_idx = price_window.idxmax()
                price_low_idx = price_window.idxmin()
                rsi_high_idx = rsi_window.idxmax()
                rsi_low_idx = rsi_window.idxmin()
                
                # 顶背离：价格创新高，RSI未创新高
                if (price_window.iloc[-1] > price_window.iloc[-2] and 
                    current_price > price_window.loc[price_high_idx] and
                    current_rsi < rsi_window.loc[rsi_high_idx]):
                    df.loc[df.index[i], bearishDivergenceCol] = True
                
                # 底背离：价格创新低，RSI未创新低
                if (price_window.iloc[-1] < price_window.iloc[-2] and 
                    current_price < price_window.loc[price_low_idx] and
                    current_rsi > rsi_window.loc[rsi_low_idx]):
                    df.loc[df.index[i], bullishDivergenceCol] = True
                
            #转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                bearishDivergence = row[bearishDivergenceCol]
                bullishDivergence = row[bullishDivergenceCol]
                if bearishDivergence or bullishDivergence:
                    saveSet = {'date':currentDate}
                    # 背离信号
                    saveSet["bearishDivergence"] = bearishDivergence #顶背离
                    saveSet["bullishDivergence"] = bullishDivergence #底背离
                    # 原始数据
                    saveSet["close"] = row["close"]
                    saveSet[rsiCol] = row[rsiCol]
                    saveSet["rsi"] = saveSet[rsiCol]

                    result[currentYMD] = saveSet
        
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    def check_mid_line_cross(self, rsi_col = 'rsi_12', mid_line=50):
        """
        检查中轴线穿越信号
        
        参数:
            rsi_col (str): RSI列名，默认"rsi_12"
            mid_line (int): 中轴线位置（默认50）
        
        返回:
            dict: 包含中轴线穿越信号的字典，键为日期，值为信号字典
        """
        result = {}
        try:
            df = self.rsiData

            crossAboveMidCol = f'{rsi_col}_cross_above_mid'
            crossBelowMidCol = f'{rsi_col}_cross_below_mid'
            crossStrongCol = f'{rsi_col}_strong'
            crossWeakCol = f'{rsi_col}_weak'
            
            # 上穿中轴线
            df[crossAboveMidCol] = (df[rsi_col] > mid_line) & (df[rsi_col].shift(1) <= mid_line)
          
            # 下穿中轴线
            df[crossBelowMidCol] = (df[rsi_col] < mid_line) & (df[rsi_col].shift(1) >= mid_line)
            
            # 强弱区域标记
            df[crossStrongCol] = (df[rsi_col] > mid_line)
            df[crossWeakCol] = (df[rsi_col] < mid_line)

            #转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                saveSet = {}
                saveSet["crossAboveMid"] = row[crossAboveMidCol]
                saveSet["crossBelowMid"] = row[crossBelowMidCol]
                saveSet["crossStrong"] = row[crossStrongCol]
                saveSet["crossWeak"] = row[crossWeakCol]

                if saveSet["crossWeak"]:
                    saveSet["crossStatus"] = "弱区域"
                if saveSet["crossStrong"]:
                    saveSet["crossStatus"] = "强区域"
                saveSet["close"] = row["close"]
                saveSet[rsi_col] = row[rsi_col]

                result[currentYMD] = saveSet

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        
        return result

    # 检查金叉/死叉信号
    def check_golden_death_cross(self, fast_rsi = 'rsi_6', slow_rsi = 'rsi_12'):
        """
        检查金叉/死叉信号
        
        参数:
            fast_rsi (str): 快线RSI列名，默认"rsi_6"
            slow_rsi (str): 慢线RSI列名，默认"rsi_12"
        
        返回:
            dict: 包含金叉/死叉信号的字典，键为日期，值为信号字典
        """
        result = {}
        try:
            df = self.rsiData
            goldenCrossCol = f'golden_cross_{fast_rsi}_{slow_rsi}'
            deathCrossCol = f'death_cross_{fast_rsi}_{slow_rsi}'
        
            # 金叉：快线上穿慢线
            df[goldenCrossCol] = (df[fast_rsi] > df[slow_rsi]) & (df[fast_rsi].shift(1) <= df[slow_rsi].shift(1))
            # 死叉：快线下穿慢线
            df[deathCrossCol] = (df[fast_rsi] < df[slow_rsi]) & (df[fast_rsi].shift(1) >= df[slow_rsi].shift(1))

            # 转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                goldenCross = row[goldenCrossCol]
                deathCross = row[deathCrossCol]
                if goldenCross or deathCross:
                    saveSet = {'date':currentDate}
                    saveSet["crossStatus"] = "金叉" if goldenCross else "死叉"
                    # 金叉/死叉信号
                    saveSet["goldenCross"] = goldenCross
                    saveSet["deathCross"] = deathCross
                    # 原始数据
                    saveSet["close"] = row["close"]
                    saveSet["rsi"] = row[slow_rsi]
                    saveSet[fast_rsi] = row[fast_rsi]
                    saveSet[slow_rsi] = row[slow_rsi]

                    result[currentYMD] = saveSet

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
                
        return result


# OBV类,能量潮指标
class OBVAnalyzer:
    """OBV类 - 计算OBV信号"""   
    symbol = ""
   
    #RSOBV数据
    indicator = "obv"
    obvData = None
    obvSignals = {}
    obvKeyList = ["date","ma_60","obv","open","close","high","low","volume","amount","turnover_rate"]
    
    def __init__(self,symbol,df=None):
        self.symbol = symbol
        if df is not None:
            #取部分字段数据
            self.obvData = df[self.obvKeyList]

    def calc(self):
        result = {}
        try:
            divergenceSignals = self.find_divergence()
            trendSignals = self.obv_trend_analysis()
            obvSignals = self.generate_obv_signals()
            self.obvSignals = obvSignals

            # 综合判断, 生成finalSignals
            finalSignals = {}
            for YMD,obvSignal in obvSignals.items():
                #填充信号数据
                finalSignals[YMD] = {}
                suggestion = obvSignal["suggestion"]
                finalSignals[YMD]["symbol"] = self.symbol
                finalSignals[YMD]["indicator"] = self.indicator
                finalSignals[YMD]["subtype"] = "obv" + obvSignal["reason_cn"]
                descString = f"{obvSignal["reason_cn"]},OBV值:{obvSignal['obv']}"
                finalSignals[YMD]["subtype"] = finalSignals[YMD]["subtype"][0:_DEF_SUBTYPE_LEN]
                finalSignals[YMD]["suggestion"] = suggestion
                extString = f"收盘价:{obvSignal['close']}"
                if suggestion == _DEF_SUGGESTION_BUY:
                    description = f"{obvSignal['date']},{self.symbol}:建议买入,{descString};{extString}"
                elif suggestion == _DEF_SUGGESTION_SELL:
                    description = f"{obvSignal['date']},{self.symbol}:建议卖出,{descString};{extString}"
                else:
                    description =  f"{obvSignal['date']},{self.symbol}:无建议"
                description = description[:_DEF_DESCRIPTION_LEN]
                finalSignals[YMD]["description"] = description

                #填充详细信息, result_json 数据
                detail = {}
                detail["obvSignal"] = obvSignal
                finalSignals[YMD]["detail"] = detail

            result["finalSignals"] = finalSignals

            result["divergenceSignals"] = divergenceSignals
            result["trendSignals"] = trendSignals
            result["obvSignals"] = obvSignals
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    def find_divergence(self, lookback=20, threshold=0.7):
        """
        检测OBV与价格之间的背离
        
        参数:
            lookback: 回溯周期，默认20天
            threshold: 背离强度阈值，默认0.7
        
        返回:
            dict: 包含背离信号的字典，键为日期，值为信号字典
        """
        result = {}
        try:
            df = self.obvData
            
            # 计算局部高点和低点（简化版）
            df['price_high'] = df['close'].rolling(window=lookback, center=True).max()
            df['price_low'] = df['close'].rolling(window=lookback, center=True).min()
            df['obv_high'] = df['obv'].rolling(window=lookback, center=True).max()
            df['obv_low'] = df['obv'].rolling(window=lookback, center=True).min()
            
            # 初始化信号列
            df['bullish_divergence'] = False    # 底背离（看涨）
            df['bearish_divergence'] = False    # 顶背离（看跌）
            
            # 顶背离检测：价格新高，OBV未新高
            price_peak_condition = (df['close'] == df['price_high']) & (df['close'] > df['close'].shift(1))
            obv_not_peak = df['obv'] < df['obv_high'] * threshold
            
            # 底背离检测：价格新低，OBV未新低
            price_trough_condition = (df['close'] == df['price_low']) & (df['close'] < df['close'].shift(1))
            obv_not_trough = df['obv'] > df['obv_low'] * (2 - threshold)
            
            df.loc[price_peak_condition & obv_not_peak, 'bearish_divergence'] = True
            df.loc[price_trough_condition & obv_not_trough, 'bullish_divergence'] = True

            # 转换为字典
            for index, row in df.iterrows():
                bearish_divergence = row["bearish_divergence"]
                bullish_divergence = row["bullish_divergence"]
                if bearish_divergence or bullish_divergence:
                    currentDate = row['date']
                    currentYMD = currentDate.replace('-','')
                    saveSet = {'date':currentDate}
                    saveSet["bullish_divergence"] = bullish_divergence
                    saveSet["bearish_divergence"] = bearish_divergence
                    result[currentYMD] = saveSet
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    def obv_trend_analysis(self, window=20):
        """
        OBV趋势分析
        
        参数:
            window: 均线周期，默认20
        
        返回:
            dict: 包含趋势信号的字典，键为日期，值为信号字典
        """
        result = {}
        try:
            df = self.obvData
            
            # 计算OBV的移动平均线
            df['obv_ma'] = df['obv'].rolling(window=window).mean()
            
            # 计算OBV斜率（趋势强度）
            df['obv_slope'] = df['obv'].diff(window) / window
            
            # 趋势判断
            df['obv_trend'] = _DEF_MARKET_TREND_NEUTRAL
            df.loc[df['obv'] > df['obv_ma'], 'obv_trend'] = _DEF_MARKET_TREND_UP    # OBV在均线上方,牛市
            df.loc[df['obv'] < df['obv_ma'], 'obv_trend'] = _DEF_MARKET_TREND_DOWN    # OBV在均线下方,熊市
            
            # 趋势强度
            df['trend_strength'] = 'weak'
            df.loc[df['obv_slope'] > df['obv_slope'].std(), 'trend_strength'] = 'strong'
            df.loc[df['obv_slope'] < -df['obv_slope'].std(), 'trend_strength'] = 'strong'

            # 转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                saveSet = {'date':currentDate}
                saveSet["obv_trend"] = row["obv_trend"]
                saveSet["trend_strength"] = row["trend_strength"]
                saveSet["obv_slope"] = row["obv_slope"]
                result[currentYMD] = saveSet

            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    def generate_obv_signals(self):
        """
        基于OBV生成交易信号
        
        返回:
            dict: 包含交易信号的字典，键为日期，值为信号字典
        """
        result = {}
        try:
            df = self.obvData
        
            # 买入信号
            df['buy_signal'] = 0
            
            # 条件1：底背离
            df.loc[df['bullish_divergence'] == True, 'buy_signal'] = 1
            df.loc[df['buy_signal'] == 1, 'reason_cn'] = "底背离"
            # 条件2：OBV金叉（上穿均线）
            df.loc[(df['obv'] > df['obv_ma']) & (df['obv'].shift(1) <= df['obv_ma'].shift(1)), 'buy_signal'] = 1
            df.loc[df['buy_signal'] == 1, 'reason_cn'] = "金叉"
            # 卖出信号
            df['sell_signal'] = 0
            # 条件1：顶背离
            df.loc[df['bearish_divergence'] == True, 'sell_signal'] = 1
            df.loc[df['sell_signal'] == 1, 'reason_cn'] = "顶背离"    
            # 条件2：OBV死叉（下穿均线）
            df.loc[(df['obv'] < df['obv_ma']) & (df['obv'].shift(1) >= df['obv_ma'].shift(1)), 'sell_signal'] = 1
            df.loc[df['sell_signal'] == 1, 'reason_cn'] = "死叉"
            
            # 信号过滤：避免重复信号
            df['signal'] = 0
            df.loc[df['buy_signal'] == 1, 'signal'] = 1    # 买入
            df.loc[df['sell_signal'] == 1, 'signal'] = -1   # 卖出
            
            # 提取有效信号（去重）
            signals = df[df['signal'] != 0][['date','close', 'obv', 'signal','reason_cn']].copy()

            # 转换为字典
            for index, row in signals.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                saveSet = {'date':currentDate}
                currSignal = row["signal"]

                if currSignal == 1:
                    saveSet["suggestion"] = _DEF_SUGGESTION_BUY
                elif currSignal == -1:
                    saveSet["suggestion"] = _DEF_SUGGESTION_SELL
                else:
                    #非买入/卖出信号,跳过
                    continue
                saveSet["signal"] = row["signal"]
                # 保存close和obv
                saveSet["close"] = row["close"]
                saveSet["obv"] = row["obv"]
                saveSet["reason_cn"] = row["reason_cn"]
                result[currentYMD] = saveSet
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result


# CCI类,能量潮指标
"""
CCI类,能量潮指标
CCI指标（商品通道指标）是一个比较“特立独行”的技术分析工具。它最大的特点是没有上下限，在捕捉极端行情和趋势转折时非常灵敏，能有效解决RSI、KDJ等指标在单边市中容易“钝化”的问题。
它的核心逻辑，是衡量当前价格偏离正常范围的程度，用一句话概括就是：在+100和-100的“常态区”里看戏，一旦突破这两个关键数值，就是“戏肉”来了。
CCI指标的用法分为两大派别：顺势法（追趋势）和逆势法（抓反转）。你可以根据市场行情灵活切换：

方法	关键信号	操作解读	适用场景
顺势法
（追涨杀跌）	CCI向上突破+100	强势行情确立，是明确的买入信号，可顺势跟进	市场处于明显的单边上涨或下跌趋势中
CCI向下突破-100	弱势行情确立，是明确的卖出或做空信号	
逆势法
（高抛低吸）	CCI从+100上方掉头跌破+100	超买状态可能结束，是卖出或止盈信号，准备落袋为安	市场处于震荡或趋势不明朗的行情中
CCI从-100下方回升突破-100	超卖状态可能结束，是买入或抄底信号，准备入场	
常态区观望	CCI在 -100 到 +100 之间	价格处于正常波动，没有明确方向。此时用KDJ等震荡指标效果更好，CCI的参考价值不大	盘整阶段

三大实战进阶技巧
除了看数值，以下几个技巧能帮你更好地把握买卖点：
1.背离信号（胜率最高的用法）：这是CCI最可靠的信号之一。当价格创出新高，但CCI的高点却比之前低，这叫顶背离，是强烈的卖出信号，预示上涨动能衰竭。反之，价格创新低，但CCI的低点却在抬高，这叫底背离，是买入信号。CCI的背离信号通常比RSI更灵敏。
参数设置：默认参数是14日或20日。你可以根据自己的交易风格调整：
2.短线/激进：使用7或14周期，信号更敏感，但假信号也多。
3.中长线/稳健：使用20或40周期，信号更平滑，准确率相对更高。
结合均线过滤假信号：为了增加胜率，可以搭配20日均线一起看。例如，当CCI向上突破+100时，如果股价也同时站上20日均线，买入信号会更可靠。反之，如果股价还在均线下方，要警惕是假突破

"""
class CCIAnalyzer:
    """CCI类 - 计算CCI信号"""   
    symbol = ""
   
    #RSOBV数据
    indicator = "cci"
    cciData = None
    cciSignals = {}
    cciKeyList = ["date","ma_20","ma_60","cci","open","close","high","low","volume","amount","turnover_rate"]

    def __init__(self,symbol,df=None):
        self.symbol = symbol
        if df is not None:
            #取部分字段数据
            self.cciData = df[self.cciKeyList]

    def calc(self):
        result = {}
        try:
            divergenceSignals = self.detect_divergence()
            buySellSignals = self.combine_signals_with_divergence()
            
            # 综合判断, 生成finalSignals
            finalSignals = {}
            for YMD,buySellSignal in buySellSignals.items():
                #填充信号数据
                finalSignals[YMD] = {}
                suggestion = buySellSignal["suggestion"]
                finalSignals[YMD]["symbol"] = self.symbol
                finalSignals[YMD]["indicator"] = self.indicator
                finalSignals[YMD]["subtype"] = buySellSignal.get("subtype","")
                descString = f"{buySellSignal["reason_cn"]},CCI值:{buySellSignal['cci']}"
                finalSignals[YMD]["subtype"] = finalSignals[YMD]["subtype"][0:_DEF_SUBTYPE_LEN]
                finalSignals[YMD]["suggestion"] = suggestion
                extString = f"收盘价:{buySellSignal['close']}"
                if suggestion == _DEF_SUGGESTION_BUY:
                    description = f"{buySellSignal['date']},{self.symbol}:建议买入,{descString};{extString}"
                elif suggestion == _DEF_SUGGESTION_SELL:
                    description = f"{buySellSignal['date']},{self.symbol}:建议卖出,{descString};{extString}"    
                else:
                    description =  f"{buySellSignal['date']},{self.symbol}:无建议"
                description = description[:_DEF_DESCRIPTION_LEN]
                finalSignals[YMD]["description"] = description

                #填充详细信息, result_json 数据
                detail = {}
                detail["buySellSignal"] = buySellSignal
                finalSignals[YMD]["detail"] = detail
            
            result["finalSignals"] = finalSignals

            result["buySellSignals"] = buySellSignals
            result["divergenceSignals"] = divergenceSignals
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    def detect_divergence(self, lookback=20):
        """
        检测 CCI 与价格的背离
        返回: 背离信号字典 key: 日期 value: 背离信号 (up, down)
        """
        result = {}
        try:
            df = self.cciData
            
            price_col = "close"
            cci_col = "cci"

            df['divergence'] = 'none'
            
            for i in range(lookback, len(df)):
                # 获取回溯窗口内的数据
                price_window = df[price_col].iloc[i-lookback:i+1]
                cci_window = df[cci_col].iloc[i-lookback:i+1]
                
                current_price = df[price_col].iloc[i]
                current_cci = df[cci_col].iloc[i]
                
                # 找窗口内的局部极值
                price_min_idx = price_window.idxmin()  # 价格最低点的位置
                price_max_idx = price_window.idxmax()  # 价格最高点的位置
                
                cci_at_price_min = df.loc[price_min_idx, cci_col]
                cci_at_price_max = df.loc[price_max_idx, cci_col]
                
                # 底背离检测：当前价格 < 窗口内最低价，且当前CCI > 价格最低点对应的CCI
                if current_price <= price_window.min() and current_cci > cci_at_price_min:
                    df.loc[df.index[i], 'divergence'] = _DEF_MARKET_TREND_UP
                
                # 顶背离检测：当前价格 > 窗口内最高价，且当前CCI < 价格最高点对应的CCI
                if current_price >= price_window.max() and current_cci < cci_at_price_max:
                    df.loc[df.index[i], 'divergence'] = _DEF_MARKET_TREND_DOWN   

            # 转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                divergence = row["divergence"]
                if divergence == "none":
                    continue
                saveSet = {'date':currentDate,'divergence':divergence}
                result[currentYMD] = saveSet
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    def generate_signals(self):
        """
        根据 CCI 指标生成买卖信号
        信号规则:
            - 买入: CCI 从下向上突破 -100 (进入超卖区回升)
            - 卖出: CCI 从上向下跌破 +100 (从超买区回落)
            - 强势买入: CCI 向上突破 +100 (趋势追踪)
            - 强势卖出: CCI 向下跌破 -100 (趋势追踪)
        返回:
            添加 'signal' 列的 DataFrame (1: 买入, -1: 卖出)
        """
        result = {}
        try:
            df = self.cciData
            df['signal'] = 0
            
            cci_col = "cci"

            # 生成条件
            buy_condition = (df[cci_col].shift(1) < -100) & (df[cci_col] >= -100)
            df.loc[buy_condition, 'subtype'] = "cci买入"
            df.loc[buy_condition, 'reason_cn'] = "买入:超卖区回升"
            sell_condition = (df[cci_col].shift(1) > 100) & (df <= 10 <= 100)
            df.loc[sell_condition, 'subtype'] = "cci卖出"
            df.loc[sell_condition, 'reason_cn'] = "卖出:超买区回落"
            strong_buy = (df[cci_col].shift(1) < 100) & (df[cci_col] >= 100)
            df.loc[strong_buy, 'subtype'] = "cci强买入"
            df.loc[strong_buy, 'reason_cn'] = "强势买入:趋势追踪买入"
            strong_sell = (df[cci_col].shift(1) > -100) & (df[cci_col] <= -100)
            df.loc[strong_sell, 'subtype'] = "cci强卖出"
            df.loc[strong_sell, 'reason_cn'] = "强势卖出:趋势追踪卖出"
            
            df.loc[buy_condition, 'signal'] = 1
            df.loc[sell_condition, 'signal'] = -1
            df.loc[strong_buy, 'signal'] = 1
            df.loc[strong_sell, 'signal'] = -1

            # 转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                signal = row["signal"]
                if signal == 0:
                    continue
                elif signal == 1:
                    suggestion = _DEF_SUGGESTION_BUY
                elif signal == -1:
                    suggestion = _DEF_SUGGESTION_SELL                   
                reason_cn = row["reason_cn"]
                if reason_cn == "none":
                    continue
                result[currentYMD] = {'date':currentDate,'signal':signal,'reason_cn':reason_cn,'subtype':row["subtype"],
                        'suggestion':suggestion,"cci":row[cci_col],"close":row["close"]}
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

    def combine_signals_with_divergence(self, lookback=20):
        """
        综合CCI基础信号和背离信号
        策略逻辑：
            1. 买入信号：基础买入信号 + (底背离 或 CCI从超卖区回升)
            2. 卖出信号：基础卖出信号 + (顶背离 或 CCI从超买区回落)
            3. 信号强度分级：1=弱, 2=中, 3=强
        """
        result = {}
        try:
            df = self.cciData
            cci_col = "cci"
            price_col = "close"
            
            # 1. 生成基础信号
            df['base_signal'] = 0
            buy_condition = (df[cci_col].shift(1) < -100) & (df[cci_col] >= -100)
            sell_condition = (df[cci_col].shift(1) > 100) & (df[cci_col] <= 100)
            strong_buy = (df[cci_col].shift(1) < 100) & (df[cci_col] >= 100)
            strong_sell = (df[cci_col].shift(1) > -100) & (df[cci_col] <= -100)
            
            df.loc[buy_condition | strong_buy, 'base_signal'] = 1
            df.loc[sell_condition | strong_sell, 'base_signal'] = -1
            
            # 2. 检测背离信号
            divergenceSignals = self.detect_divergence(lookback)
            #self.cciData 已经添加了背离信号列
            
            # 3. 综合信号（带强度）
            df['signal'] = 0
            df['signal_strength'] = 0  # 1=弱, 2=中, 3=强
            
            for i in range(1, len(df)):
                # 买入信号综合
                if df['base_signal'].iloc[i] == 1:
                    # 强买入：基础信号 + 底背离
                    if df['divergence'].iloc[i] == _DEF_MARKET_TREND_UP:
                        df.loc[df.index[i], 'signal'] = 1
                        df.loc[df.index[i], 'signal_strength'] = 3
                        df.loc[df.index[i], 'subtype'] = "cci强买入"
                        df.loc[df.index[i], 'reason_cn'] = "强买入：基础信号+底背离"
                    # 中买入：基础信号 + CCI在超卖区或刚突破
                    elif df[cci_col].iloc[i] < 0 or df[cci_col].iloc[i-1] < -100:
                        df.loc[df.index[i], 'signal'] = 1
                        df.loc[df.index[i], 'signal_strength'] = 2
                        df.loc[df.index[i], 'subtype'] = "cci中买入"
                        df.loc[df.index[i], 'reason_cn'] = "中买入：基础信号+CCI在超卖区或刚突破"
                    # 弱买入：仅有基础信号
                    else:
                        df.loc[df.index[i], 'signal'] = 1
                        df.loc[df.index[i], 'signal_strength'] = 1
                        df.loc[df.index[i], 'subtype'] = "cci弱买入"
                        df.loc[df.index[i], 'reason_cn'] = "弱买入：仅有基础信号"
                
                # 卖出信号综合
                elif df['base_signal'].iloc[i] == -1:
                    # 强卖出：基础信号 + 顶背离
                    if df['divergence'].iloc[i] == _DEF_MARKET_TREND_DOWN:
                        df.loc[df.index[i], 'signal'] = -1
                        df.loc[df.index[i], 'signal_strength'] = 3
                        df.loc[df.index[i], 'subtype'] = "cci强卖出"
                        df.loc[df.index[i], 'reason_cn'] = "强卖出：基础信号+顶背离"
                    # 中卖出：基础信号 + CCI在超买区或刚跌破
                    elif df[cci_col].iloc[i] > 0 or df[cci_col].iloc[i-1] > 100:
                        df.loc[df.index[i], 'signal'] = -1
                        df.loc[df.index[i], 'signal_strength'] = 2
                        df.loc[df.index[i], 'subtype'] = "cci中卖出"
                        df.loc[df.index[i], 'reason_cn'] = "中卖出：基础信号+CCI在超买区或刚跌破"
                    # 弱卖出：仅有基础信号
                    else:
                        df.loc[df.index[i], 'signal'] = -1
                        df.loc[df.index[i], 'signal_strength'] = 1
                        df.loc[df.index[i], 'subtype'] = "cci弱卖出"
                        df.loc[df.index[i], 'reason_cn'] = "弱卖出：仅有基础信号"
            # 转为字典
            for index,row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                signal = row["signal"]
                if signal == 0:
                    continue
                elif signal == 1:
                    suggestion = _DEF_SUGGESTION_BUY
                elif signal == -1:
                    suggestion = _DEF_SUGGESTION_SELL
                reason_cn = row['reason_cn']
                signal_strength = row['signal_strength']
                result[currentYMD] = {'date':currentDate,'signal':signal,'reason_cn':reason_cn,'suggestion':suggestion,
                            'signal_strength':signal_strength,"subtype":row["subtype"],"cci":row[cci_col],"close":row["close"]}
       
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result

#其他综合指标
"""
指标内部名称: brma (bollinger band + rsi + macd + adx)
indicator = "brma"
---------------------------------------
震荡市：布林带 + RSI（碰上下轨 + 超买超卖）
趋势市：布林带 + MACD（开口 + 同向确认）
---------------------------------------
指标	作用	什么时候看
ADX	判断震荡还是趋势	任何时候，第一步
布林带	定位价格位置和波动率	任何时候
RSI	找超买超卖点	ADX < 20（震荡时）
MACD	确认趋势方向和强度	ADX > 25（单边时）
"""
class BRMAAnalyzer:
    """布林带 + RSI + MACD + ADX类 - 计算指标信号"""   
    symbol = ""

    _DEF_MARKET_RANGE_THRESOLD = 25
    _DEF_MARKET_TREND_THRESOLD = 20

    _DEF_RSI_OVERBOUGHT_THRESHOLD = 70 #默认超买阈值
    _DEF_RSI_OVERSOLD_THRESHOLD = 30 #默认超卖阈值

    #RSOBV数据
    indicator = "brma"
    brmaData = None
    brmaSignals = {}
    brmaKeyList = ["date","ma_20","ma_60","rsi_6","rsi_12","rsi_24","macd_line","macd_signal","macd_histogram",
                    "macd_line_long","macd_signal_long","macd_histogram_long",
                    "dmi_pdi","dmi_mdi","dmi_adx","boll_upper","boll_lower","boll_mid",
                    "open","close","high","low","volume","amount","turnover_rate"]
    
    def __init__(self,symbol,df=None,isLong=False):
        self.symbol = symbol
        self.isLong = isLong
        self.rsiCol = "rsi_12"
        self.maCol = "ma_20"
        if df is not None:
            #取部分字段数据
            self.brmaData = df[self.brmaKeyList]
            self.calculate_atr()

    def calculate_atr(self, period=14):
        """计算ATR"""
        try:
            df = self.brmaData
            df['high_low'] = df['high'] - df['low']
            df['high_close'] = abs(df['high'] - df['close'].shift(1))
            df['low_close'] = abs(df['low'] - df['close'].shift(1))
            df['TR'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
            df['ATR'] = df['TR'].rolling(window=period).mean()
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return df

    def calc(self,existFunc=False):
        result = {}
        try:
            if existFunc:
                signals = self.generateSignals()
            else:
                signals = self.pureGenerateSignals()

            # 综合判断, 生成finalSignals
            finalSignals = {}
            for YMD,signal in signals.items():
                date = misc.YMD2HumanDate(YMD)
                #填充信号数据
                finalSignals[YMD] = {}
                finalSignals[YMD]["symbol"] = self.symbol
                finalSignals[YMD]["indicator"] = self.indicator
                subtype = signal.get("subtype","")
                subtype = subtype[:_DEF_DESCRIPTION_LEN]
                finalSignals[YMD]["subtype"] = subtype
                suggestion = signal["suggestion"]
                finalSignals[YMD]["suggestion"] = suggestion
                description = signal.get("description","")
                if suggestion == _DEF_SUGGESTION_BUY:
                    description = f"{date},{self.symbol}:建议买入,{description}"
                elif suggestion == _DEF_SUGGESTION_SELL:
                    description = f"{date},{self.symbol}:建议卖出,{description}"    
                else:
                    description =  f"{date},{self.symbol}:无建议"
                description = description[:_DEF_DESCRIPTION_LEN]
                finalSignals[YMD]["description"] = description

                #填充详细信息, result_json 数据
                detail = signal.get("detail",{})
                finalSignals[YMD]["detail"] = detail
            
            result["finalSignals"] = finalSignals
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return result

    # 完全本地计算指标信号,不依赖已有代码(仅依赖MACD指标)
    def pureGenerateSignals(self):
        result = {}
        try:
            #准备数据
            symbol = self.symbol
            df = self.brmaData
            #计算MACD指标
            macd = MACDAnalyzer(symbol,df,isLong = self.isLong)
            crossOverSignals = macd.findCrossOver()
            
            marketTrends = self.determineMarketTrend()
            for currentYMD,marketTrend in marketTrends.items():
                currDate = misc.YMD2HumanDate(currentYMD)
                if marketTrend == _DEF_MARKET_TREND_RANGE:
                    # 震荡行情,布林带 + RSI（碰上下轨 + 超买超卖）
                    bollRsiSignal = self.generateBollRsiSignals(currDate)
                    if bollRsiSignal:
                        result[currentYMD] = bollRsiSignal
                    pass
                elif marketTrend == _DEF_MARKET_TREND_TREND:
                    # 趋势市：布林带 + MACD
                    crossOverSignal = crossOverSignals.get(currentYMD,{})
                    if crossOverSignal:
                        bollMacdSignal = self.generateBollMacdSignals(currDate,crossOverSignal)
                        if bollMacdSignal:
                            result[currentYMD] = bollMacdSignal
                    pass
                else:
                    # 中性市,不操作
                    pass           

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return result

    def generateBollRsiSignals(self,date):
        # 震荡行情：布林带 + RSI（碰上下轨 + 超买超卖）
        result = {}
        try:
            df = self.brmaData
            datePos = df[df['date'] == date].index[0]
            closePrice = df.loc[datePos]["close"]
            bollUpper = df.loc[datePos]["boll_upper"]
            bollLower = df.loc[datePos]["boll_lower"]
            rsiVal = df.loc[datePos][self.rsiCol]
            # 买入信号：价格触及下轨且RSI < 30
            if closePrice <= bollLower and rsiVal < self._DEF_RSI_OVERSOLD_THRESHOLD:
                result["date"] = date
                result["marketTrend"] = _DEF_MARKET_TREND_RANGE
                result["suggestion"] = _DEF_SUGGESTION_BUY

                #计算止损止盈
                stopLoss, takeProfit = self.calculate_stop_loss_take_profit(closePrice, datePos, suggestion=result["suggestion"])

                result["reason_cn"] = f"价格触及下轨且{self.rsiCol} < {self._DEF_RSI_OVERSOLD_THRESHOLD}"
                result["subtype"] = f"价格触及下轨RSI<{self._DEF_RSI_OVERSOLD_THRESHOLD}"
                result["description"] = f"价格触及下轨且{self.rsiCol} < {self._DEF_RSI_OVERSOLD_THRESHOLD}; 收盘价:{closePrice},止损:{stopLoss},止盈:{takeProfit};布林带下轨:{bollLower}"
                #填充细节信息
                result["detail"] = {}
                result["detail"]["stopLoss"] = float(stopLoss)
                result["detail"]["takeProfit"] = float(takeProfit) 
                result["detail"]["bollUpper"] = float(bollUpper)
                result["detail"]["bollLower"] = float(bollLower)
                result["detail"]["rsiVal"] = float(rsiVal)
                result["detail"]["rsiThresold"] = float(self._DEF_RSI_OVERSOLD_THRESHOLD)
                result["detail"]["closePrice"] = float(closePrice)
            # 卖出信号：价格触及上轨且RSI > 70
            elif closePrice >= bollUpper and rsiVal > self._DEF_RSI_OVERBOUGHT_THRESHOLD:
                result["date"] = date
                result["marketTrend"] = _DEF_MARKET_TREND_RANGE
                result["suggestion"] = _DEF_SUGGESTION_SELL

                #计算止损止盈
                stopLoss, takeProfit = self.calculate_stop_loss_take_profit(closePrice, datePos, suggestion=result["suggestion"])

                result["reason_cn"] = f"价格触及上轨且{self.rsiCol} > {self._DEF_RSI_OVERBOUGHT_THRESHOLD}"
                result["subtype"] = f"价格触及上轨RSI>{self._DEF_RSI_OVERBOUGHT_THRESHOLD}"
                result["description"] = f"价格触及上轨且{self.rsiCol} > {self._DEF_RSI_OVERBOUGHT_THRESHOLD}; 收盘价:{closePrice},止损:{stopLoss},止盈:{takeProfit};布林带上轨:{bollUpper}"
                #填充细节信息
                result["detail"] = {}
                result["detail"]["stopLoss"] = float(stopLoss)
                result["detail"]["takeProfit"] = float(takeProfit) 
                result["detail"]["bollUpper"] = float(bollUpper)
                result["detail"]["bollLower"] = float(bollLower)
                result["detail"]["rsiVal"] = float(rsiVal)
                result["detail"]["rsiThresold"] = float(self._DEF_RSI_OVERBOUGHT_THRESHOLD)
                result["detail"]["closePrice"] = float(closePrice)
                pass
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return result

    def generateBollMacdSignals(self,date,crossOverSignal):
        result = {}
        try:
            df = self.brmaData
            datePos = df[df['date'] == date].index[0]
            #金叉和死叉信号
            goldenCross = crossOverSignal.get("goldenCross",False)
            deathCross = crossOverSignal.get("deathCross",False)
            marketTrend = crossOverSignal.get("marketTrend",_DEF_MARKET_TREND_TREND)
            closePrice = df.loc[datePos]["close"]
            midPrice = df.loc[datePos]["boll_mid"]
            # 买入信号：价格在MA上方，MACD金叉
            if closePrice > midPrice and goldenCross:
                result["date"] = date
                result["marketTrend"] = marketTrend
                result["suggestion"] = _DEF_SUGGESTION_BUY

                #计算止损止盈
                stopLoss, takeProfit = self.calculate_stop_loss_take_profit(closePrice, datePos, suggestion=result["suggestion"])

                result["reason_cn"] = f"价格在MA上方，MACD金叉"
                result["subtype"] = f"价格在MA上方MACD金叉"
                result["description"] = f"价格在MA上方，MACD金叉; 收盘价:{closePrice},止损:{stopLoss},止盈:{takeProfit};布林带中轨:{midPrice}"
                #填充细节信息
                result["detail"] = {}
                result["detail"]["stopLoss"] = float(stopLoss)
                result["detail"]["takeProfit"] = float(takeProfit) 
                result["detail"]["bollMid"] = float(midPrice)
                result["detail"]["closePrice"] = float(closePrice)
                result["detail"]["goldenCross"] = goldenCross
                result["detail"]["deathCross"] = deathCross

            # 卖出信号：价格在MA下方，MACD死叉
            elif closePrice < maPrice and deathCross:
                result["date"] = date
                result["marketTrend"] = marketTrend
                result["suggestion"] = _DEF_SUGGESTION_SELL

                #计算止损止盈
                stopLoss, takeProfit = self.calculate_stop_loss_take_profit(closePrice, datePos, suggestion=result["suggestion"])

                result["reason_cn"] = f"价格在MA下方，MACD死叉"
                result["subtype"] = f"价格在MA下方MACD死叉"
                result["description"] = f"价格在MA下方，MACD死叉; 收盘价:{closePrice},止损:{stopLoss},止盈:{takeProfit};布林带中轨:{midPrice}"
                #填充细节信息
                result["detail"] = {}
                result["detail"]["stopLoss"] = float(stopLoss)
                result["detail"]["takeProfit"] = float(takeProfit) 
                result["detail"]["bollMid"] = float(midPrice)
                result["detail"]["closePrice"] = float(closePrice)
                result["detail"]["goldenCross"] = goldenCross
                result["detail"]["deathCross"] = deathCross
                pass
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return result

    # 利用已有代码, 生成BRMA指标信号, 综合判断
    def generateSignals(self):
        result = {}
        try:
            #准备数据
            symbol = self.symbol
            df = self.brmaData

            #计算布林带指标
            boll = BollingerBandAnalyzer(symbol,df)
            bollSignals = boll.calc()

            #计算MACD指标
            macd = MACDAnalyzer(symbol,df,isLong = self.isLong)
            macdSignals = macd.calc()

            #计算RSI指标
            rsi = RSIAnalyzer(symbol,df)
            rsiSignals = rsi.calc()

            #判断震荡还是趋势市
            # 第一步：看ADX判断行情性质
            marketTrends = self.determineMarketTrend()
            for currentYMD,marketTrend in marketTrends.items():
                bollTrendSignals = bollSignals.get("trendSignals",{})
                bollTrendSignal = bollTrendSignals.get(currentYMD,{})
                if bollTrendSignal:
                    #确保布林带趋势与ADX趋势一致
                    bollMarketTrend = bollTrendSignal.get("marketTrend","")
                if marketTrend == _DEF_MARKET_TREND_RANGE and bollMarketTrend == _DEF_MARKET_TREND_RANGE:
                    # 震荡行情,布林带 + RSI（碰上下轨 + 超买超卖）
                    bollRsiSignals = self.getBollRsiSignals(currentYMD,bollSignals,rsiSignals)
                    if bollRsiSignals:
                        result[currentYMD] = bollRsiSignals
                    pass
                elif marketTrend == _DEF_MARKET_TREND_TREND and bollMarketTrend in [_DEF_MARKET_TREND_UP,_DEF_MARKET_TREND_DOWN]:
                    # 趋势市：布林带 + MACD（开口 + 同向确认）
                    bollMacdSignals = self.getBollMacdSignals(currentYMD,bollSignals,macdSignals)
                    if bollMacdSignals:
                        result[currentYMD] = bollMacdSignals
                    pass
                else:
                    # 中性市,不操作
                    pass           
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return result

    def determineMarketTrend(self):
        result = {}
        try:
            df = self.brmaData
            # ADX < 20，且±DI纠缠 → 震荡 → 切换到 布林带+RSI
            # ADX > 25，且±DI分叉 → 单边 → 切换到 布林带+MACD
            for index,row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                adx = row["dmi_adx"]
                pmi = row["dmi_pdi"]
                mmi = row["dmi_mdi"]
                if adx < self._DEF_MARKET_RANGE_THRESOLD and pmi < mmi:
                    result[currentYMD] = _DEF_MARKET_TREND_RANGE
                elif adx > self._DEF_MARKET_RANGE_THRESOLD and pmi > mmi:
                    result[currentYMD] = _DEF_MARKET_TREND_TREND
                else:
                    result[currentYMD] = _DEF_MARKET_TREND_NEUTRAL
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return result

    def getBollRsiSignals(self,YMD,bollSignals,rsiSignals):
        # 震荡行情：布林带 + RSI（碰上下轨 + 超买超卖）
        result = {}
        try:
            # 看RSI判断超买超卖
            bollFinalSignals = bollSignals.get("finalSignals",{})
            bollFinalSignal = bollFinalSignals.get(YMD,{})
            rsiFinalSignals = rsiSignals.get("finalSignals",{})
            rsiFinalSignal = rsiFinalSignals.get(YMD,{})
            rsiOverboutOversoldSignals = rsiSignals.get("overboughtOversoldSignals",{})
            rsiOverSignal = rsiOverboutOversoldSignals.get(YMD,{})
            if bollFinalSignal and rsiFinalSignal:
                bollSuggestion = bollFinalSignal.get("suggestion","")
                rsiSuggestion = rsiFinalSignal.get("suggestion","")
                if bollSuggestion == rsiSuggestion:
                    # 同向确认
                    rsiSubtype = rsiFinalSignal.get("subtype","")
                    rsiDescription = rsiFinalSignal.get("description","")
                    pos = rsiDescription.find(":")
                    if pos > 0:
                        rsiDescription = rsiDescription[pos+1:]
                    # 合并描述,输出数据
                    result["date"] = misc.YMD2HumanDate(YMD)
                    result["suggestion"] = bollSuggestion
                    result["subtype"] = bollFinalSignal.get("subtype","") + "+" + rsiSubtype
                    result["description"] = bollFinalSignal.get("description","") + "+" + rsiDescription
                    result["marketTrend"] = bollFinalSignal.get("marketTrend","")
                    pass
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return result

    def getBollMacdSignals(self,YMD,bollSignals,macdSignals):
        result = {}
        try:
            bollFinalSignals = bollSignals.get("finalSignals",{})
            bollFinalSignal = bollFinalSignals.get(YMD,{})
            macdFinalSignals = macdSignals.get("finalSignals",{})
            macdFinalSignal = macdFinalSignals.get(YMD,{})
            if bollFinalSignal and macdFinalSignal:
                bollSuggestion = bollFinalSignal.get("suggestion","")
                macdSuggestion = macdFinalSignal.get("suggestion","")
                if bollSuggestion == macdSuggestion:
                    # 同向确认
                    macdSubtype = macdSuggestion.get("subtype","")
                    macdDescription = macdSuggestion.get("description","")
                    pos = macdDescription.find(":")
                    if pos > 0:
                        macdDescription = macdDescription[pos+1:]
                    # 合并描述,输出数据
                    result["date"] = misc.YMD2HumanDate(YMD)
                    result["suggestion"] = bollSuggestion
                    result["subtype"] = bollFinalSignal.get("subtype","") + "+" + macdSubtype
                    result["description"] = bollFinalSignal.get("description","") + "+" + macdDescription
                    result["marketTrend"] = bollFinalSignal.get("marketTrend","")
                    pass
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return result

    def calculate_stop_loss_take_profit(self, entry_price, entry_index, suggestion='buy'):
        """
        计算动态止损止盈
        返回: (stop_loss, take_profit)
        """
        initial_stop = 0
        take_profit = 0
        try:
            df = self.brmaData
            atr = df['ATR'].iloc[entry_index]
            
            if suggestion == 'buy': # 做多
                # 初始止损：入场价 - 2倍ATR
                initial_stop = entry_price - 2 * atr
                # 止盈：入场价 + 4倍ATR
                take_profit = entry_price + 4 * atr
            else:
                # 做空
                initial_stop = entry_price + 2 * atr
                take_profit = entry_price - 4 * atr
            initial_stop = round(initial_stop, 2)
            take_profit = round(take_profit, 2)
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return initial_stop, take_profit

    def update_trailing_stop(self, entry_index, current_index, current_price, current_stop, suggestion='buy'):
        """
        更新移动止损
        使用布林中轨或ATR跟踪
        """
        new_stop = 0
        try:
            df = self.brmaData
            if suggestion == 'buy':
                # 方法1：布林中轨止损
                ma_stop = df['boll_mid'].iloc[current_index] * 0.995  # 中轨下方0.5%
                
                # 方法2：ATR跟踪止损（每涨1ATR，止损上移0.5ATR）
                entry_price = df['close'].iloc[entry_index]
                price_move = current_price - entry_price
                atr = df['ATR'].iloc[entry_index]
                atr_stop = entry_price - 2 * atr + (price_move / atr) * 0.5 * atr
                atr_stop = min(atr_stop, current_price - 0.5 * atr)  # 不低于当前价-0.5ATR
                
                # 取两者中较高的作为止损
                new_stop = max(ma_stop, atr_stop, current_stop)               
            else:
                # 做空
                ma_stop = df['boll_mid'].iloc[current_index] * 1.005
                entry_price = df['close'].iloc[entry_index]
                price_move = entry_price - current_price
                atr = df['ATR'].iloc[entry_index]
                atr_stop = entry_price + 2 * atr - (price_move / atr) * 0.5 * atr
                atr_stop = max(atr_stop, current_price + 0.5 * atr)
                
                new_stop = min(ma_stop, atr_stop, current_stop)    
            new_stop = round(new_stop, 2)
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return new_stop


class RSIMACDAnalyzer:
    """RSI + MACD类 - 计算指标信号"""   
    """RSI和MACD指标结合类计算指标信号"""
    symbol = ""
   
    #RSOBV数据
    indicator = "rsma"
    rsmaData = None
    rsmaSignals = {}
    # macdKeyList = ["date","macd_line","macd_signal","macd_histogram","open","close","high","low","volume","amount","turnover_rate"]
    # macdLongKeyList = ["date","macd_line_long","macd_signal_long","macd_histogram_long","open","close","high","low","volume","amount","turnover_rate"]
    # rsiKeyList = ["date","ma_60","rsi_6","rsi_12","rsi_24","open","close","high","low","volume","amount","turnover_rate"]
    rsmaKeyList = ["date","ma_60","rsi_6","rsi_12","rsi_24","macd_line","macd_signal","macd_histogram","open","close","high","low","volume","amount","turnover_rate"]
    rsmaLongKeyList = ["date","ma_60","rsi_6","rsi_12","rsi_24","macd_line_long","macd_signal_long","macd_histogram_long","open","close","high","low","volume","amount","turnover_rate"]
    
    def __init__(self,symbol,df=None,isLong=False):
        self.symbol = symbol
        if df is not None:
            #取部分字段数据
            if isLong:
                self.rsmaData = df[self.rsmaLongKeyList]
                self.rsmaData.rename(columns={'macd_line_long':'dif','macd_signal_long':'dea','macd_histogram_long':'macd'},inplace=True)
            else:
                self.rsmaData = df[self.rsmaKeyList]
                self.rsmaData.rename(columns={'macd_line':'dif','macd_signal':'dea','macd_histogram':'macd'},inplace=True)
                

#其他综合指标
# OBV+ MA + RSI 类
class OBVMARSIAnalyzer:
    """OBV+ MA + RSI类 - 计算指标信号"""   
    """OBV和RSI指标结合类计算指标信号"""
    symbol = ""

    _DEF_RSI_OVERBOUGHT_THRESHOLD = 70 #默认超买阈值
    _DEF_RSI_OVERSOLD_THRESHOLD = 30 #默认超卖阈值   
   
    _DEF_VALUME_RATIO = 1.2 #放量倍数

    #RSOBV数据
    indicator = "obmr"
    obmrData = None
    obmrSignals = {}
    # macdKeyList = ["date","macd_line","macd_signal","macd_histogram","open","close","high","low","volume","amount","turnover_rate"]
    # macdLongKeyList = ["date","macd_line_long","macd_signal_long","macd_histogram_long","open","close","high","low","volume","amount","turnover_rate"]
    # rsiKeyList = ["date","ma_60","rsi_6","rsi_12","rsi_24","open","close","high","low","volume","amount","turnover_rate"]
    obmrKeyList = ["date","ma_20","ma_60","obv","rsi_6","rsi_12","rsi_24","macd_line","macd_signal","macd_histogram","open","close","high","low","volume","amount","turnover_rate"]
    obmrLongKeyList = ["date","ma_20","ma_60","obv","rsi_6","rsi_12","rsi_24","macd_line_long","macd_signal_long","macd_histogram_long","open","close","high","low","volume","amount","turnover_rate"]
    
    def __init__(self,symbol,df=None,isLong=True,window=20):
        self.symbol = symbol
        if df is not None:
            #取部分字段数据
            if isLong:
                self.obmrData = df[self.obmrLongKeyList]
                self.obmrData.rename(columns={'macd_line_long':'dif','macd_signal_long':'dea','macd_histogram_long':'macd'},inplace=True)
            else:
                self.obmrData = df[self.obmrKeyList]
                self.obmrData.rename(columns={'macd_line':'dif','macd_signal':'dea','macd_histogram':'macd'},inplace=True)
        #prepare data
        df = self.obmrData
        #计算OBV移动平均线
        df['obv_ma'] = df['obv'].rolling(window=window).mean()
        #计算ma_trend
        self.maCol = 'ma_20'
        df['ma_trend'] = np.where(df['close'] > df[self.maCol], 1, -1)
        # RSI状态标记
        self.rsiCol = 'rsi_12'
        df['rsi_oversold'] = df[self.rsiCol] < self._DEF_RSI_OVERSOLD_THRESHOLD
        df['rsi_overbought'] = df[self.rsiCol] > self._DEF_RSI_OVERBOUGHT_THRESHOLD

    def calc(self):
        result = {}
        try:
            self.detect_divergence() #检测背离必须先计算
            signals = self.generate_signals() #生成信号,必须在检测背离后计算

            # 综合判断, 生成finalSignals
            finalSignals = {}
            for YMD,signal in signals.items():
                #填充信号数据
                finalSignals[YMD] = {}
                finalSignals[YMD]["symbol"] = self.symbol
                finalSignals[YMD]["indicator"] = self.indicator
                finalSignals[YMD]["subtype"] = signal.get("subtype","")
                suggestion = signal["suggestion"]
                descString = f"{signal["reason_cn"]}"
                finalSignals[YMD]["subtype"] = finalSignals[YMD]["subtype"][0:_DEF_SUBTYPE_LEN]
                finalSignals[YMD]["suggestion"] = suggestion
                extString = f"收盘价:{signal['close']}"
                if suggestion == _DEF_SUGGESTION_BUY:
                    description = f"{signal['date']},{self.symbol}:建议买入,{descString};{extString}"
                elif suggestion == _DEF_SUGGESTION_SELL:
                    description = f"{signal['date']},{self.symbol}:建议卖出,{descString};{extString}"       
                else:
                    description =  f"{signal['date']},{self.symbol}:无建议"
                description = description[:_DEF_DESCRIPTION_LEN]
                finalSignals[YMD]["description"] = description

                #填充详细信息, result_json 数据
                detail = {}
                detail["signal"] = signal
                finalSignals[YMD]["detail"] = detail
            
            result["finalSignals"] = finalSignals
            
            pass
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result


    def detect_divergence(self, lookback=20):
        """
        检测OBV与价格的背离, 并添加背离背离标记, 只在内部使用
        
        参数:
            lookback: 回溯周期
        返回: 
            包含背离标记的DataFrame
        """
        result = self.obmrData
        df = self.obmrData
        try:
            # 计算局部极值
            df['price_high'] = df['close'].rolling(lookback, center=True).max()
            df['price_low'] = df['close'].rolling(lookback, center=True).min()
            df['obv_high'] = df['obv'].rolling(lookback, center=True).max()
            df['obv_low'] = df['obv'].rolling(lookback, center=True).min()
            
            # 顶背离：价格新高，OBV未新高
            price_peak = (df['close'] == df['price_high']) & (df['close'] > df['close'].shift(1))
            obv_not_peak = df['obv'] < df['obv_high']
            df['bearish_divergence'] = (price_peak & obv_not_peak).astype(int)
            
            # 底背离：价格新低，OBV未新低
            price_trough = (df['close'] == df['price_low']) & (df['close'] < df['close'].shift(1))
            obv_not_trough = df['obv'] > df['obv_low']
            df['bullish_divergence'] = (price_trough & obv_not_trough).astype(int)  

            result = df

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
        return result
       
    def generate_signals(self):
        """
        生成交易信号（核心策略逻辑）
        
        买入条件（需同时满足）：
        1. OBV底背离 OR OBV上穿OBV均线
        2. 价格站上MA20
        3. RSI < 30（超卖区）
        
        卖出条件（需同时满足）：
        1. OBV顶背离 OR OBV下穿OBV均线
        2. 价格跌破MA20
        3. RSI > 70（超买区）
        """
        result = {}
        try:
            df = self.obmrData
            df['reason_cn'] = "none"
            # 买入信号
            buy_condition_1 = (df['bullish_divergence'] == 1) | \
                            ((df['obv'] > df['obv_ma']) & 
                            (df['obv'].shift(1) <= df['obv_ma'].shift(1)))
            df.loc[buy_condition_1, 'reason_cn'] = "OBV底背离 OR OBV上穿OBV均线"
            
            buy_condition_2 = df['close'] > df[self.maCol]
            df.loc[buy_condition_2, 'reason_cn'] = f"价格站上MA{self.maCol}"
            buy_condition_3 = df[self.rsiCol] < self._DEF_RSI_OVERSOLD_THRESHOLD
            df.loc[buy_condition_3, 'reason_cn'] = f"超卖区 RSI < {self._DEF_RSI_OVERSOLD_THRESHOLD}"   
            
            df['buy_signal'] = (buy_condition_1 & buy_condition_2 & buy_condition_3).astype(int)
            
            # 卖出信号
            sell_condition_1 = (df['bearish_divergence'] == 1) | \
                            ((df['obv'] < df['obv_ma']) & 
                                (df['obv'].shift(1) >= df['obv_ma'].shift(1)))
            df.loc[sell_condition_1, 'reason_cn'] = "OBV顶背离 OR OBV下穿OBV均线"
            df.loc[sell_condition_1, 'subtype'] = "顶背离/下穿OBV均线"
            
            sell_condition_2 = df['close'] < df[self.maCol]
            df.loc[sell_condition_2, 'reason_cn'] = f"价格跌破MA{self.maCol}"
            df.loc[sell_condition_2, 'subtype'] = f"价格跌破MA{self.maCol}"

            sell_condition_3 = df[self.rsiCol] > self._DEF_RSI_OVERBOUGHT_THRESHOLD
            df.loc[sell_condition_3, 'reason_cn'] = f"超买区 RSI > {self._DEF_RSI_OVERBOUGHT_THRESHOLD}"
            df.loc[sell_condition_3, 'subtype'] = f"超买区RSI>{self._DEF_RSI_OVERBOUGHT_THRESHOLD}"
            df['sell_signal'] = (sell_condition_1 & sell_condition_2 & sell_condition_3).astype(int)
            
            # 综合信号
            df['signal'] = 0
            df.loc[df['buy_signal'] == 1, 'signal'] = 1
            df.loc[df['sell_signal'] == 1, 'signal'] = -1

            #转换为字典
            for index, row in df.iterrows():
                currentDate = row['date']
                currentYMD = currentDate.replace('-','')
                signal = row["signal"]
                if signal == 0:
                    continue
                elif signal == 1:
                    suggestion = _DEF_SUGGESTION_BUY
                elif signal == -1:
                    suggestion = _DEF_SUGGESTION_SELL                   
                reason_cn = row["reason_cn"]
                if reason_cn == "none":
                    continue
                result[currentYMD] = {'date':currentDate,'signal':signal,'reason_cn':reason_cn,'suggestion':suggestion,
                           "subtype":row["subtype"],"close":row["close"]}
        
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}, detail:{traceback.format_exc()}"
        return result


#总体类,负责调用其他类
class StockTS: 
    """技术指标计算 - 负责调用其他类"""
    df = None
    symbol = None
    floatKeyList = ['open','close','high','low','volume','amount','amplitude','turnover_rate',\
    'ma_5','ma_10','ma_20','ma_60','macd_line','macd_signal','macd_histogram','macd_line_long','macd_signal_long','macd_histogram_long',\
    'boll_upper','boll_mid','boll_lower','ene_upper','ene_lower','ene_mid','dmi_pdi','dmi_mdi','dmi_adx',\
    'dma_line','ama_line','sar','kdj_k','kdj_d','kdj_j','rsi_6','rsi_12','rsi_24','cci','wr_6','wr_14',\
    'bias_5','bias_10','bias_20','obv']
    indicatorDropKeyList = ['stock_code','hashval','label1','label2','label3','memo','regID','regYMDHMS','modifyID','modifyYMDHMS','dispFlag','delFlag']
    
    def __init__(self):
        pass

    #格式转换类
    def DICT2DF(self,DICT):
        """将字典转换为DataFrame"""
        return pd.DataFrame(DICT)

    def DF2DICT(self,DF):
        """将DataFrame转换为字典"""
        return DF.to_dict(orient='list')

    #输入数据是技术指标字典
    def calcTechnicalSignals(self,symbol,indicators):
        result = {}
        try:
            """计算技术指标信号"""
            #检查输入数据是否为空
            if not indicators:
                return result
            self.symbol = symbol
            df = self.DICT2DF(indicators)
            try:
                df = df.drop(self.indicatorDropKeyList, axis=1)
            except Exception as e:
                pass
           
            if df is not None:
                #转换date为float类型
                for key in self.floatKeyList:
                    try:
                        df[key] = df[key].astype(float)
                    except Exception as e:
                        df[key] = 0
                #转换为DataFrame
                self.df = df

                #计算BRMA指标
                brmaSignals = self.calcBRMA(symbol,self.df)
                result["brmaSignals"] = brmaSignals
                #计算OBMR指标
                obmrSignals = self.calcOBMR(symbol,self.df)
                result["obmrSignals"] = obmrSignals               
                #计算CCI指标
                cciSignals = self.calcCCI(symbol,self.df)
                result["cciSignals"] = cciSignals
                #计算OBV信号
                obvSignals = self.calcOBVSignal(symbol,self.df)
                result["obvSignals"] = obvSignals
                #计算RSI背离指标
                rsiSignals = self.calcRSISignal(symbol,self.df)
                result["rsiSignals"] = rsiSignals
                #计算布林带指标
                bollSignals = self.calcBollingerBand(symbol,self.df)
                result["bollSignals"] = bollSignals
                #计算macd指标
                macdSignals = self.calcMACD(symbol,self.df)
                result["macdSignals"] = macdSignals
                #计算KDJ指标
                kdjSignals = self.calcKDJ(symbol,self.df)               
                result["kdjSignals"] = kdjSignals

        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
            # _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        return result


    def calcMACD(self,symbol,df):
        """计算macd指标"""
        macd = MACDAnalyzer(symbol,df)
        macdSignals = macd.calc()
        return macdSignals

    def calcKDJ(self,symbol,df):
        """计算KDJ指标"""
        kdj = KDJAnalyzer(symbol,df)
        kdjSignals = kdj.calc()
        return kdjSignals

    def calcBollingerBand(self,symbol,df):
        """计算布林带指标"""
        boll = BollingerBandAnalyzer(symbol,df)
        bollSignals = boll.calc()
        return bollSignals

    def calcRSISignal(self,symbol,df):
        """计算背离指标"""
        rsi = RSIAnalyzer(symbol,df)
        rsiSignals = rsi.calc()
        return rsiSignals

    def calcOBVSignal(self,symbol,df):
        """计算OBV信号"""
        obv = OBVAnalyzer(symbol,df)
        obvSignals = obv.calc()
        return obvSignals
    
    def calcCCI(self,symbol,df):
        """计算CCI指标"""
        cci = CCIAnalyzer(symbol,df)
        cciSignals = cci.calc()
        return cciSignals

    def calcOBMR(self,symbol,df):
        """计算OBMR指标"""
        obmmr = OBVMARSIAnalyzer(symbol,df)
        obmSignals = obmmr.calc()
        return obmSignals

    def calcBRMA(self,symbol,df):
        """计算BRMA指标"""
        brma = BRMAAnalyzer(symbol,df)
        brmaSignals = brma.calc()
        return brmaSignals
       

def test():
    """改进的测试函数"""
    ts = StockTS()

    pass

# 使用示例
if __name__ == "__main__":
    pass
    test()
