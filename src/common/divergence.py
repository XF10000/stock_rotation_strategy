"""
背离检测模块 - 基于TA-Lib指标
检测价格与技术指标之间的背离现象
"""

_VERSION="20260127"

import os
import sys

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)
if sys.getdefaultencoding() != 'utf-8':
    pass


import numpy as np
import pandas as pd
import talib 


_processorPID = os.getpid()

# 检测RSI背离
def detect_rsi_divergence(price, rsi, lookback = 13):
    """
    检测RSI背离
    
    Args:
        price: 价格序列
        rsi: RSI序列
        lookback: 回溯周期，默认13
        
    Returns:
        Dict[str, bool]: {'top_divergence': 顶背离, 'bottom_divergence': 底背离}
    """
    result = {}
    try:
        #格式判断和转换
        if not isinstance(price, pd.Series):
            price = pd.Series(price)
        if not isinstance(rsi, pd.Series):
            rsi = pd.Series(rsi)
        # 参数验证
        if isinstance(price, pd.Series) and isinstance(rsi, pd.Series) and len(price) >= lookback + 1:           
            # 获取最近的数据
            recent_price = price.tail(lookback + 1)
            recent_rsi = rsi.tail(lookback + 1)
            
            # 检测顶背离和底背离
            top_divergence = _detect_top_divergence(recent_price, recent_rsi)
            bottom_divergence = _detect_bottom_divergence(recent_price, recent_rsi)
           
            result = {
                'top_divergence': top_divergence,
                'bottom_divergence': bottom_divergence
            }
            
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"

    return result


def _detect_top_divergence(price: pd.Series, indicator: pd.Series) -> bool:
    """检测顶背离：价格创新高，指标未创新高"""
    result = False
    try:
        # 当前价格是否为回溯期内最高价
        current_price = price.iloc[-1]
        max_price = price.max()
        price_at_high = abs(current_price - max_price) < 0.01
        
        # 当前指标是否低于回溯期内最高指标值
        current_indicator = indicator.iloc[-1]
        max_indicator = indicator.max()
        indicator_below_high = current_indicator < max_indicator * 0.98
        
        if price_at_high and indicator_below_high:
            result = True
        
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"

    return result


def _detect_bottom_divergence(price: pd.Series, indicator: pd.Series) -> bool:
    """检测底背离：价格创新低，指标未创新低"""
    result = False
    try:
        # 当前价格是否为回溯期内最低价
        current_price = price.iloc[-1]
        min_price = price.min()
        price_at_low = abs(current_price - min_price) < 0.01
        
        # 当前指标是否高于回溯期内最低指标值
        current_indicator = indicator.iloc[-1]
        min_indicator = indicator.min()
        indicator_above_low = current_indicator > min_indicator * 1.02
        
        if price_at_low and indicator_above_low:
            result = True
        
    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"

    return result


# 检测MACD背离
def detect_macd_divergence(price, macd_hist, lookback=13) :
    """
    检测MACD背离
    
    Args:
        price: 价格序列
        macd_hist: MACD柱状图序列
        lookback: 回溯周期
        
    Returns:
        Dict[str, bool]: {'top_divergence': 顶背离, 'bottom_divergence': 底背离}
    """
    result = {}
    try:
        #格式判断和转换
        if not isinstance(price, pd.Series):
            price = pd.Series(price)
        if not isinstance(macd_hist, pd.Series):
            macd_hist = pd.Series(macd_hist)
            
        if isinstance(price, pd.Series) and isinstance(macd_hist, pd.Series) and len(price) >= lookback + 1:
        
            # 获取最近的数据
            recent_price = price.tail(lookback + 1)
            recent_macd = macd_hist.tail(lookback + 1)
            
            # 检测背离
            top_divergence = _detect_top_divergence(recent_price, recent_macd)
            bottom_divergence = _detect_bottom_divergence(recent_price, recent_macd)
            
            result = {
                'top_divergence': top_divergence,
                'bottom_divergence': bottom_divergence
            }

    except Exception as e:
        errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
               
    return result


# 检测多个高低点序列
def detect_multiple_divergences(price: pd.Series, indicator: pd.Series, lookback: int = 13):
    """
    检测多个背离点，而不仅仅是最近一个
    """
    divergences = []
    for i in range(lookback, len(price)):
        # 滑动窗口检测
        window_price = price[i-lookback:i+1]
        window_indicator = indicator[i-lookback:i+1]

        # 检测背离
        top_divergence = _detect_top_divergence(window_price, window_indicator)
        bottom_divergence = _detect_bottom_divergence(window_price, window_indicator)
        
        divergence = {
            'top_divergence': top_divergence,
            'bottom_divergence': bottom_divergence
        }
        if top_divergence or bottom_divergence:
            divergence['index'] = i
            divergences.append(divergence)

    return divergences


# 检测RSI背离
class RSIDivergence:
    """RSI背离检测器"""
    
    def __init__(self, rsi_period=14, lookback=20):
        self.rsi_period = rsi_period
        self.lookback = lookback  # 用于检测背离的回看周期
    
    def detect_divergence(self, price, rsi=None):
        """
        检测RSI背离
        
        参数:
        price: 价格序列
        rsi: RSI序列，如果为None则计算
        
        返回:
        bullish_div: 看涨背离信号
        bearish_div: 看跌背离信号
        """
        if rsi is None:
            rsi = talib.RSI(price, timeperiod=self.rsi_period)
        
        n = len(price)
        bullish = np.zeros(n, dtype=bool)
        bearish = np.zeros(n, dtype=bool)
        
        for i in range(self.lookback, n):
            # 获取回看窗口
            price_window = price[i-self.lookback:i+1]
            rsi_window = rsi[i-self.lookback:i+1]
            
            # 移除NaN值
            valid_mask = ~np.isnan(rsi_window)
            if np.sum(valid_mask) < 5:  # 至少需要5个有效点
                continue
                
            price_window = price_window[valid_mask]
            rsi_window = rsi_window[valid_mask]
            
            # 寻找极值点
            price_low_idx = np.argmin(price_window)
            price_high_idx = np.argmax(price_window)
            rsi_low_idx = np.argmin(rsi_window)
            rsi_high_idx = np.argmax(rsi_window)
            
            # 检测看涨背离（底背离）
            if price_low_idx == len(price_window) - 1:  # 当前是价格最低点
                if rsi_low_idx < len(rsi_window) - 1:  # RSI低点不是当前点
                    # 确认价格创新低而RSI未创新低
                    if (price_window[-1] < np.min(price_window[:-1]) and 
                        rsi_window[-1] > np.min(rsi_window[:-1])):
                        bullish[i] = True
            
            # 检测看跌背离（顶背离）
            if price_high_idx == len(price_window) - 1:  # 当前是价格最高点
                if rsi_high_idx < len(rsi_window) - 1:  # RSI高点不是当前点
                    # 确认价格创新高而RSI未创新高
                    if (price_window[-1] > np.max(price_window[:-1]) and 
                        rsi_window[-1] < np.max(rsi_window[:-1])):
                        bearish[i] = True
        
        return bullish, bearish
    
    def detect_advanced_divergence(self, price, rsi=None):
        """
        高级RSI背离检测，包含隐藏背离
        """
        if rsi is None:
            rsi = talib.RSI(price, timeperiod=self.rsi_period)
        
        n = len(price)
        results = {
            'bullish_regular': np.zeros(n, dtype=bool),    # 常规看涨背离
            'bearish_regular': np.zeros(n, dtype=bool),    # 常规看跌背离
            'bullish_hidden': np.zeros(n, dtype=bool),     # 隐藏看涨背离
            'bearish_hidden': np.zeros(n, dtype=bool)      # 隐藏看跌背离
        }
        
        for i in range(self.lookback, n):
            if i < self.lookback * 2:  # 需要足够的数据
                continue
                
            # 获取两个窗口用于确认趋势
            short_window = self.lookback // 2
            long_window = self.lookback
            
            # 短窗口检测
            price_short = price[i-short_window:i+1]
            rsi_short = rsi[i-short_window:i+1]
            
            # 长窗口检测
            price_long = price[i-long_window:i+1]
            rsi_long = rsi[i-long_window:i+1]
            
            # 移除NaN
            valid_short = ~np.isnan(rsi_short)
            valid_long = ~np.isnan(rsi_long)
            
            if np.sum(valid_short) < 3 or np.sum(valid_long) < 5:
                continue
                
            price_short = price_short[valid_short]
            rsi_short = rsi_short[valid_short]
            price_long = price_long[valid_long]
            rsi_long = rsi_long[valid_long]
            
            # 检测常规背离
            if price_long[-1] == np.min(price_long):  # 价格新低
                if rsi_long[-1] > np.min(rsi_long[:-1]):  # RSI未新低
                    results['bullish_regular'][i] = True
                    
            if price_long[-1] == np.max(price_long):  # 价格新高
                if rsi_long[-1] < np.max(rsi_long[:-1]):  # RSI未新高
                    results['bearish_regular'][i] = True
            
            # 检测隐藏背离
            # 隐藏看涨背离：价格低点抬高，但RSI低点降低（上升趋势中的调整）
            if (price_short[-1] > price_short[0] and  # 价格低点抬高
                rsi_short[-1] < np.min(rsi_short[:-1])):  # RSI创新低
                results['bullish_hidden'][i] = True
            
            # 隐藏看跌背离：价格高点降低，但RSI高点升高（下降趋势中的反弹）
            if (price_short[-1] < price_short[0] and  # 价格高点降低
                rsi_short[-1] > np.max(rsi_short[:-1])):  # RSI创新高
                results['bearish_hidden'][i] = True
        
        return results


if __name__ == "__main__":
    # 测试代码
    pass