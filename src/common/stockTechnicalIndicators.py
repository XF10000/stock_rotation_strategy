#! /usr/bin/env python3
#encoding: utf-8

#Filename: stockTechnicalIndicators.py  
#Author: Steven Lian's team/xie_frank@163.com
#E-mail:  steven.lian@gmail.com  
#Date: 2019-08-01
#Description:   技术指标计算类 - 封装常用金融技术分析指标
# 原始代码来源于github 
# https://github.com/mpquant/Python-Financial-Technical-Indicators-Pandas/
# 经过改造为class 类型
# 1. 所有函数均为公开方法，无需添加下划线前缀
# 2. 所有函数均为文档字符串注释，方便查看函数说明

__VERSION="20260314"

import os

import numpy as np
import pandas as pd


_processorPID = os.getpid()

#stock technical indicators -- class 
class StockTI:
    """技术指标计算类 - 封装常用金融技术分析指标"""
    
    def __init__(self):
        pass

    #格式转换类
    def DICT2DF(self,DICT):
        """将字典转换为DataFrame"""
        return pd.DataFrame(DICT)

    def DF2DICT(self,DF):
        """将DataFrame转换为字典"""
        return DF.to_dict(orient='list')

    def PRICE2NP(self,data):
        """将价格数据列表转换为numpy数组"""
        DATE = np.array([item['date'] for item in data])
        CLOSE = np.array([item['close'] for item in data])
        HIGH = np.array([item['high'] for item in data])
        LOW = np.array([item['low'] for item in data])
        OPEN = np.array([item['open'] for item in data])
        VOLUME = np.array([item['volume'] for item in data])
        self.DATE = DATE
        self.CLOSE = CLOSE
        self.HIGH = HIGH
        self.LOW = LOW
        self.OPEN = OPEN
        self.VOLUME = VOLUME
        return DATE,CLOSE, HIGH, LOW, OPEN, VOLUME
    
    #------------------ 0 level：core tools function --------------------------------------------      
    def RD(self,N, D=3):
        """四舍五入到指定小数位数"""
        return np.round(N, D)
        
    def RET(self,S, N=1):
        """返回最后N个元素"""
        return np.array(S)[-N]
        
    def ABS(self,S):
        """绝对值"""
        return np.abs(S)
        
    def MAX(self,S1, S2):
        """最大值"""
        return np.maximum(S1, S2)
    
    def MIN(self,S1, S2):
        """最小值"""
        return np.minimum(S1, S2)
        
    def MA(self,S, N):
        """移动平均线"""
        return pd.Series(S).rolling(N).mean().values
        
    def REF(self,S, N=1):
        """引用N周期前的数据"""
        return pd.Series(S).shift(N).values
        
    def DIFF(self,S, N=1):
        """差分"""
        return pd.Series(S).diff(N).values
        
    def STD(self,S, N):
        """标准差"""
        return pd.Series(S).rolling(N).std(ddof=0).values
        
    def IF(self,S_BOOL, S_TRUE, S_FALSE):
        """条件判断"""
        return np.where(S_BOOL, S_TRUE, S_FALSE)
        
    def SUM(self,S, N):
        """求和"""
        if N > 0:
            return pd.Series(S).rolling(N).sum().values
        else:
            return pd.Series(S).cumsum().values
        
    def HHV(self,S, N):
        """周期内最高值"""
        return pd.Series(S).rolling(N).max().values
        
    def LLV(self,S, N):
        """周期内最低值"""
        return pd.Series(S).rolling(N).min().values
        
    def EMA(self,S, N):
        """指数移动平均"""
        return pd.Series(S).ewm(span=N, adjust=False).mean().values
        
    def SMA(self,S, N, M=1):
        """简单移动平均（加权）"""
        return pd.Series(S).ewm(alpha=M/N, adjust=True).mean().values
        
    def AVEDEV(self,S, N):
        """平均绝对偏差"""
        return pd.Series(S).rolling(N).apply(lambda x: (np.abs(x - x.mean())).mean()).values
        
    def SLOPE(self,S, N, RS=False):
        """计算斜率"""
        M = pd.Series(S[-N:])
        poly = np.polyfit(M.index, M.values, deg=1)
        Y = np.polyval(poly, M.index)
        if RS:
            return Y[1]-Y[0], Y
        return Y[1]-Y[0]
    
    #------------------   1 level：逻辑判断函数 ----------------------------------
    
    def COUNT(self,S_BOOL, N):
        """统计满足条件的次数"""
        return self.SUM(S_BOOL, N)
        
    def EVERY(self,S_BOOL, N):
        """判断是否一直满足条件"""
        R = self.SUM(S_BOOL, N)
        return self.IF(R == N, True, False)
        
    def LAST(self,S_BOOL, A, B):
        """判断过去一段时间是否一直满足条件"""
        if A < B:
            A = B
        return S_BOOL[-A:-B].sum() == (A-B)
        
    def EXIST(self,S_BOOL, N=5):
        """判断是否存在满足条件的情况"""
        R = self.SUM(S_BOOL, N)
        return self.IF(R > 0, True, False)
        
    def BARSLAST(self,S_BOOL):
        """计算上一次满足条件到现在的周期数"""
        M = np.argwhere(S_BOOL)
        if M.size > 0:
            return len(S_BOOL) - int(M[-1]) - 1
        return -1
        
    def FORCAST(self,S, N):
        """线性回归预测值"""
        K, Y = self.SLOPE(S, N, RS=True)
        return Y[-1] + K
        
    def CROSS(self, S1, S2):
        """判断金叉/死叉（上穿/下穿）"""
        CROSS_BOOL = self.IF(S1 > S2, True, False)
        return (self.COUNT(CROSS_BOOL > 0, 2) == 1) * CROSS_BOOL
    
    #------------------   2 level：Technical Indicators ---------------------------------
    
    def MACD(self,CLOSE, SHORT=12, LONG=26, M=9):
        """MACD指标"""
        DIF = self.EMA(CLOSE, SHORT) - self.EMA(CLOSE, LONG)
        DEA = self.EMA(DIF, M)
        MACD = (DIF - DEA) * 2
        return self.RD(DIF), self.RD(DEA), self.RD(MACD)
        
    def KDJ(self,CLOSE, HIGH, LOW, N=9, M1=3, M2=3):
        """KDJ指标"""
        RSV = (CLOSE - self.LLV(LOW, N)) / (self.HHV(HIGH, N) - self.LLV(LOW, N)) * 100
        K = self.EMA(RSV, (M1*2-1))
        D = self.EMA(K, (M2*2-1))
        J = K*3 - D*2
        return K, D, J
        
    def RSI(self,CLOSE, N=24):
        """RSI指标"""
        DIF = CLOSE - self.REF(CLOSE, 1)
        return self.RD(self.SMA(self.MAX(DIF, 0), N) / self.SMA(self.ABS(DIF), N) * 100)
    
    def WR(self,CLOSE, HIGH, LOW, N=6, N1=14):
        """威廉指标"""
        WR = (self.HHV(HIGH, N) - CLOSE) / (self.HHV(HIGH, N) - self.LLV(LOW, N)) * 100
        WR1 = (self.HHV(HIGH, N1) - CLOSE) / (self.HHV(HIGH, N1) - self.LLV(LOW, N1)) * 100
        return self.RD(WR), self.RD(WR1)
        
    def BIAS(self,CLOSE, L1=5, L2=10, L3=20):
        """乖离率指标"""
        BIAS1 = (CLOSE - self.MA(CLOSE, L1)) / self.MA(CLOSE, L1) * 100
        BIAS2 = (CLOSE - self.MA(CLOSE, L2)) / self.MA(CLOSE, L2) * 100
        BIAS3 = (CLOSE - self.MA(CLOSE, L3)) / self.MA(CLOSE, L3) * 100
        return self.RD(BIAS1), self.RD(BIAS2), self.RD(BIAS3) 
        
    def ENE(self,CLOSE, HIGH, LOW, N=12, K=0.6, M=14):
        """
        ENS-S
        ENE (轨道线 / Envelope) 指标
        参考算法: UPPER = MA(CLOSE, N) + K * ATR(M)
                LOWER = MA(CLOSE, N) - K * ATR(M)
        :param CLOSE: 收盘价序列
        :param HIGH: 最高价序列
        :param LOW: 最低价序列
        :param N: 用于计算中轨的均线周期，默认12
        :param K: 轨道宽度倍数，默认0.6
        :param M: 用于计算ATR的周期，默认14
        :return: 上轨 (UPPER), 中轨 (MID), 下轨 (LOWER)
        """
        # 1. 计算中轨：N日的移动平均线
        MID = self.MA(CLOSE, N)

        # 2. 计算ATR (平均真实波幅)，需要用到 HIGH, LOW, CLOSE
        #    这里调用你 MyTT 库中的 ATR 函数
        atr_value = self.ATR(CLOSE, HIGH, LOW, M)

        # 3. 计算上下轨
        UPPER = MID + K * atr_value
        LOWER = MID - K * atr_value

        return self.RD(UPPER), self.RD(MID), self.RD(LOWER)


    def BOLL(self,CLOSE, N=20, P=2):
        """布林带指标"""
        MID = self.MA(CLOSE, N)
        UPPER = MID + self.STD(CLOSE, N) * P
        LOWER = MID - self.STD(CLOSE, N) * P
        return self.RD(UPPER), self.RD(MID), self.RD(LOWER)
    
    def PSY(self,CLOSE, N=12, M=6):
        """心理线指标"""
        PSY = self.COUNT(CLOSE > self.REF(CLOSE, 1), N) / N * 100
        PSYMA = self.MA(PSY, M)
        return self.RD(PSY), self.RD(PSYMA)
        
    def CCI(self,CLOSE, HIGH, LOW, N=14):
        """CCI指标"""
        TP = (HIGH + LOW + CLOSE) / 3
        return (TP - self.MA(TP, N)) / (0.015 * self.AVEDEV(TP, N))
        
    def ATR(self,CLOSE, HIGH, LOW, N=20):
        """平均真实波幅"""
        TR = self.MAX(self.MAX((HIGH - LOW), self.ABS(self.REF(CLOSE, 1) - HIGH)), 
                      self.ABS(self.REF(CLOSE, 1) - LOW))
        return self.MA(TR, N)
        
    def BBI(self,CLOSE, M1=3, M2=6, M3=12, M4=20):
        """多空指标"""
        return (self.MA(CLOSE, M1) + self.MA(CLOSE, M2) + 
                self.MA(CLOSE, M3) + self.MA(CLOSE, M4)) / 4
        
    def DMI(self, CLOSE, HIGH, LOW, M1=14, M2=6):
        """
        DMI 趋向指标（修正版）
        
        参数:
        CLOSE: 收盘价序列
        HIGH: 最高价序列
        LOW: 最低价序列
        M1: 周期，默认14（用于计算TR、+DM、-DM）
        M2: 平滑周期，默认6（用于计算ADX）
        
        返回:
        PDI（上升方向线）, MDI（下降方向线）, ADX（趋向平均值）, ADXR（ADX的平滑）
        """
        # 1. 计算每日TR（真实波幅）
        # TR = MAX(最高-最低, ABS(最高-昨收), ABS(最低-昨收))
        TR_day = self.MAX(
            self.MAX(HIGH - LOW, self.ABS(HIGH - self.REF(CLOSE, 1))),
            self.ABS(LOW - self.REF(CLOSE, 1))
        )
        
        # 2. 计算每日+DM和-DM
        HD = HIGH - self.REF(HIGH, 1)  # 当日最高 - 前日最高
        LD = self.REF(LOW, 1) - LOW    # 前日最低 - 当日最低
        
        # +DM: 当日最高价比前日高，且涨幅大于跌幅
        DM_plus_day = self.IF((HD > 0) & (HD > LD), HD, 0)
        
        # -DM: 当日最低价比前日低，且跌幅大于涨幅
        DM_minus_day = self.IF((LD > 0) & (LD > HD), LD, 0)
        
        # 3. 计算M1天的移动平均值
        TR_ma = self.MA(TR_day, M1)           # TR的移动平均
        DM_plus_ma = self.MA(DM_plus_day, M1)  # +DM的移动平均
        DM_minus_ma = self.MA(DM_minus_day, M1) # -DM的移动平均
        
        # 4. 计算PDI和MDI（注意处理除零情况）
        # 当TR_MA为0时，PDI和MDI设为0
        PDI = np.zeros_like(TR_ma)
        MDI = np.zeros_like(TR_ma)
        
        for i in range(len(TR_ma)):
            if TR_ma[i] != 0 and not np.isnan(TR_ma[i]):
                PDI[i] = DM_plus_ma[i] * 100 / TR_ma[i]
                MDI[i] = DM_minus_ma[i] * 100 / TR_ma[i]
        
        # 5. 计算DX和ADX
        # DX = ABS(MDI - PDI) / (MDI + PDI) * 100
        DX = np.zeros_like(PDI)
        for i in range(len(PDI)):
            if (PDI[i] + MDI[i]) != 0 and not np.isnan(PDI[i] + MDI[i]):
                DX[i] = np.abs(MDI[i] - PDI[i]) / (PDI[i] + MDI[i]) * 100
        
        # ADX = MA(DX, M2)
        ADX = self.MA(DX, M2)
        
        # 6. 计算ADXR = (ADX + REF(ADX, M2)) / 2
        ADXR = (ADX + self.REF(ADX, M2)) / 2
        
        return self.RD(PDI), self.RD(MDI), self.RD(ADX), self.RD(ADXR)
    
    def TURTLES(self,HIGH, LOW, N):
        """海龟通道"""
        UP = self.HHV(HIGH, N)
        DOWN = self.LLV(LOW, N)
        MID = (UP + DOWN) / 2
        return UP, MID, DOWN
        
    def KTN(self,CLOSE, HIGH, LOW, N=20, M=10):
        """肯特纳通道"""
        MID = self.EMA((HIGH + LOW + CLOSE) / 3, N)
        ATRN = self.ATR(CLOSE, HIGH, LOW, M)
        UPPER = MID + 2 * ATRN
        LOWER = MID - 2 * ATRN
        return UPPER, MID, LOWER
        
    def TRIX(self,CLOSE, M1=12, M2=20):
        """TRIX指标"""
        TR = self.EMA(self.EMA(self.EMA(CLOSE, M1), M1), M1)
        TRIX = (TR - self.REF(TR, 1)) / self.REF(TR, 1) * 100
        TRMA = self.MA(TRIX, M2)
        return TRIX, TRMA
        
    def VR(self,CLOSE, VOL, M1=26):
        """成交量变异率"""
        LC = self.REF(CLOSE, 1)
        return self.SUM(self.IF(CLOSE > LC, VOL, 0), M1) / self.SUM(self.IF(CLOSE <= LC, VOL, 0), M1) * 100
        
    def EMV(self,HIGH, LOW, VOL, N=14, M=9):
        """简易波动指标"""
        VOLUME = self.MA(VOL, N) / VOL
        MID = 100 * (HIGH + LOW - self.REF(HIGH + LOW, 1)) / (HIGH + LOW)
        EMV = self.MA(MID * VOLUME * (HIGH - LOW) / self.MA(HIGH - LOW, N), N)
        MAEMV = self.MA(EMV, M)
        return EMV, MAEMV
        
    def DPO(self,CLOSE, M1=20, M2=10, M3=6):
        """区间震荡指标"""
        DPO = CLOSE - self.REF(self.MA(CLOSE, M1), M2)
        MADPO = self.MA(DPO, M3)
        return DPO, MADPO
        
    def BRAR(self,OPEN, CLOSE, HIGH, LOW, M1=26):
        """BRAR情绪指标"""
        AR = self.SUM(HIGH - OPEN, M1) / self.SUM(OPEN - LOW, M1) * 100
        BR = self.SUM(self.MAX(0, HIGH - self.REF(CLOSE, 1)), M1) / self.SUM(self.MAX(0, self.REF(CLOSE, 1) - LOW), M1) * 100
        return AR, BR
        
    def DMA(self,CLOSE, N1=10, N2=50, M=10):
        """平行线差指标"""
        DIF = self.MA(CLOSE, N1) - self.MA(CLOSE, N2)
        DIFMA = self.MA(DIF, M)
        return DIF, DIFMA
        
    def MTM(self,CLOSE, N=12, M=6):
        """动量指标"""
        MTM = CLOSE - self.REF(CLOSE, N)
        MTMMA = self.MA(MTM, M)
        return MTM, MTMMA
        
    def MASS(self,HIGH, LOW, N1=9, N2=25, M=6):
        """梅斯线"""
        MASS = self.SUM(self.MA(HIGH - LOW, N1) / self.MA(self.MA(HIGH - LOW, N1), N1), N2)
        MA_MASS = self.MA(MASS, M)
        return MASS, MA_MASS
        
    def ROC(self,CLOSE, N=12, M=6):
        """变动率指标"""
        ROC = 100 * (CLOSE - self.REF(CLOSE, N)) / self.REF(CLOSE, N)
        MAROC = self.MA(ROC, M)
        return ROC, MAROC
        
    def EXPMA(self,CLOSE, N1=12, N2=50):
        """指数平均数"""
        return self.EMA(CLOSE, N1), self.EMA(CLOSE, N2)
        
    def OBV(self,CLOSE, VOL):
        """能量潮"""
        return self.SUM(self.IF(CLOSE > self.REF(CLOSE, 1), VOL, 
                                 self.IF(CLOSE < self.REF(CLOSE, 1), -VOL, 0)), 0) / 10000
        
    def MFI(self,CLOSE, HIGH, LOW, VOL, N=14):
        """资金流量指标"""
        TYP = (HIGH + LOW + CLOSE) / 3
        V1 = self.SUM(self.IF(TYP > self.REF(TYP, 1), TYP * VOL, 0), N) / \
             self.SUM(self.IF(TYP < self.REF(TYP, 1), TYP * VOL, 0), N)
        return 100 - (100 / (1 + V1))
        
    def ASI(self,OPEN, CLOSE, HIGH, LOW, M1=26, M2=10):
        """振动升降指标"""
        LC = self.REF(CLOSE, 1)
        AA = self.ABS(HIGH - LC)
        BB = self.ABS(LOW - LC)
        CC = self.ABS(HIGH - self.REF(LOW, 1))
        DD = self.ABS(LC - self.REF(OPEN, 1))
        
        R = self.IF((AA > BB) & (AA > CC), AA + BB/2 + DD/4,
                    self.IF((BB > CC) & (BB > AA), BB + AA/2 + DD/4, CC + DD/4))
        
        X = (CLOSE - LC + (CLOSE - OPEN)/2 + LC - self.REF(OPEN, 1))
        SI = 16 * X / R * self.MAX(AA, BB)
        ASI = self.SUM(SI, M1)
        ASIT = self.MA(ASI, M2)
        return ASI, ASIT

    def SAR_Standard(self,HIGH, LOW, N=4, STEP=2, MAXP=20):
        """
        SAR 标准版（简化为仅返回SAR值）
        
        参数:
        HIGH: 最高价序列
        LOW: 最低价序列
        N: 周期，默认4
        STEP: 加速因子步长（百分比），默认2
        MAXP: 加速因子最大值（百分比），默认20
        
        返回:
        SAR序列
        """
        length = len(HIGH)
        if length < N + 1:
            return np.array([np.nan] * length)
        
        af_step = STEP / 100.0
        af_max = MAXP / 100.0
        
        sar = np.zeros(length)
        sar[:N] = np.nan
        
        # 初始趋势判断（用第N天数据）
        if HIGH[N] > HIGH[N-1]:
            # 多头
            trend = 1
            sar[N] = np.min(LOW[:N+1])
            ep = np.max(HIGH[:N+1])
            af = af_step
        else:
            # 空头
            trend = -1
            sar[N] = np.max(HIGH[:N+1])
            ep = np.min(LOW[:N+1])
            af = af_step
        
        for i in range(N+1, length):
            if trend == 1:  # 多头
                sar[i] = sar[i-1] + af * (ep - sar[i-1])
                sar[i] = min(sar[i], np.min(LOW[max(0, i-2):i+1]))
                
                if LOW[i] < sar[i]:  # 反转
                    trend = -1
                    sar[i] = np.max(HIGH[max(0, i-N):i+1])
                    ep = np.min(LOW[max(0, i-N):i+1])
                    af = af_step
                else:
                    if HIGH[i] > ep:
                        ep = HIGH[i]
                        af = min(af + af_step, af_max)
            
            else:  # 空头
                sar[i] = sar[i-1] + af * (ep - sar[i-1])
                sar[i] = max(sar[i], np.max(HIGH[max(0, i-2):i+1]))
                
                if HIGH[i] > sar[i]:  # 反转
                    trend = 1
                    sar[i] = np.min(LOW[max(0, i-N):i+1])
                    ep = np.max(HIGH[max(0, i-N):i+1])
                    af = af_step
                else:
                    if LOW[i] < ep:
                        ep = LOW[i]
                        af = min(af + af_step, af_max)
        
        return self.RD(sar)

    def CCI_Optimized(self,CLOSE, HIGH, LOW, N=14):
        """
        CCI 顺势指标（优化版，处理各种边界条件）
        
        参数:
        CLOSE: 收盘价序列
        HIGH: 最高价序列
        LOW: 最低价序列
        N: 计算周期，默认14
        
        返回:
        CCI序列
        """
        length = len(CLOSE)
        if length < N:
            # 数据不足时返回空值
            return np.array([np.nan] * length)
        
        # 计算典型价格
        TP = (HIGH + LOW + CLOSE) / 3
        
        # 初始化CCI数组
        cci = np.zeros(length)
        cci[:N-1] = np.nan  # 前N-1天数据不足
        
        # 计算每个有效位置的CCI
        for i in range(N-1, length):
            # 获取窗口数据
            tp_window = TP[i-N+1:i+1]
            
            # 计算窗口均值
            ma_tp = np.mean(tp_window)
            
            # 计算平均绝对偏差
            md = np.mean(np.abs(tp_window - ma_tp))
            
            # 计算CCI
            if md != 0:
                cci[i] = (TP[i] - ma_tp) / (0.015 * md)
            else:
                cci[i] = 0  # 如果偏差为0，说明价格无波动
    
        return self.RD(cci)

    def OBV_Optimized(self,CLOSE, VOL):
        """
        OBV 能量潮指标（优化版，使用向量化计算）
        
        参数:
        CLOSE: 收盘价序列
        VOL: 成交量序列
        
        返回:
        OBV序列
        """
        # 计算涨跌方向
        # 上涨: 1, 下跌: -1, 平盘: 0
        direction = np.sign(np.diff(CLOSE, prepend=CLOSE[0]))
        
        # 方向为0（平盘）时，成交量不计入
        signed_volume = direction * VOL
        
        # 累积求和得到OBV
        obv = np.cumsum(signed_volume)
        
        return self.RD(obv)  

    #--- 计算技术指标--- 
    # 输入参数:
    #   dataList: 股票历史数据列表, 每个元素为一个字典, 包含 date,open,high,low,close,volume
    #   数据是按照date 升序排列的
    # 输出参数:
    #   result: 技术指标列表, 每个元素为一个字典, 包含 date, macd,dif,dea,k,d,j等
    def calculateTechnicalIndicators(self,stockHistoryDataList):
        """计算技术指标"""
        result = []
        try:
            # 计算各指标所需的最小周期
            min_periods = {
                'ma': 20,
                'macd': 26,
                'boll': 20,
                'ene': 14,
                'dmi': 14,
                'dma': 50,
                'sar': 5,
                'kdj': 9,
                'rsi': 24,
                'cci': 14,
                'bias': 20,
                'wr': 14,
                'obv': 1
            }
            
            start_idx = max(min_periods.values()) + 10  # 取最大周期+10作为起始索引
            totalDataCount = len(stockHistoryDataList)
            if totalDataCount < (start_idx + 6):
                return result

            #获取symbol 
            self.symbol = stockHistoryDataList[0]["stock_code"]

            # 转换为numpy数组
            dateArr,closeArr, highArr, lowArr, openArr, volumeArr = self.PRICE2NP(stockHistoryDataList)

            #趋势型指标
            # 计算MA
            ma5Arr = self.MA(closeArr, 5)
            ma10Arr = self.MA(closeArr, 10)
            ma20Arr = self.MA(closeArr, 20)

            # 计算MACD
            difArr, deaArr, macdArr = self.MACD(closeArr)

            # 计算BOLL
            bollUpperArr, bollMidArr, bollLowerArr = self.BOLL(closeArr)

            # 计算ENS-S/ENE
            eneUpperArr, eneMidArr, eneLowerArr = self.ENE(closeArr, highArr, lowArr)

            # 计算DMI, PDI, MDI, ADX, ADXR
            dmiPdiArr, dmiMdiArr, dmiAdxArr, dmiAdxrArr = self.DMI(closeArr, highArr, lowArr)

            # 计算DMA
            dmaArr,amaArr = self.DMA(closeArr)
            dmaArr = np.nan_to_num(dmaArr, nan=0)
            amaArr = np.nan_to_num(amaArr,nan=0)

            # 计算SAR
            sarArr = self.SAR_Standard(highArr, lowArr)
                        
            #摆动性指标
            # 计算KDJ
            kArr, dArr, jArr = self.KDJ(closeArr, highArr, lowArr)

            # 计算RSI
            rsi6Arr = self.RSI(closeArr,6)
            rsi12Arr = self.RSI(closeArr, 12)
            rsi24Arr = self.RSI(closeArr, 24)

            #其他摆动指标
            # 计算CCI
            cciArr = self.CCI_Optimized(closeArr, highArr, lowArr)

            # 计算BIAS
            bias5Arr, bias10Arr, bias20Arr = self.BIAS(closeArr)

            # 计算WR
            wr6Arr, wr14Arr = self.WR(closeArr, highArr, lowArr)

            # 计算OBV
            obvArr = self.OBV_Optimized(closeArr, volumeArr)

            for i in range(start_idx, totalDataCount):
                result.append({
                    # 股票代码
                    "stock_code": self.symbol,
                    "symbol": self.symbol,

                    "date": str(dateArr[i]),

                    "close": round(float(closeArr[i]),2),

                    "ma_5": round(float(ma5Arr[i]),2),
                    "ma_10": round(float(ma10Arr[i]),2),
                    "ma_20": round(float(ma20Arr[i]),2),

                    "macd_line": round(float(difArr[i]),2),
                    "macd_signal": round(float(deaArr[i]),2),
                    "macd_histogram": round(float(macdArr[i]),2),

                    "boll_upper": round(float(bollUpperArr[i]),2),
                    "boll_mid": round(float(bollMidArr[i]),2),
                    "boll_lower": round(float(bollLowerArr[i]),2),
                    
                    "ene_upper": round(float(eneUpperArr[i]),2),
                    "ene_mid": round(float(eneMidArr[i]),2),
                    "ene_lower": round(float(eneLowerArr[i]),2),
                    
                    "dmi_pdi": round(float(dmiPdiArr[i]),2),
                    "dmi_mdi": round(float(dmiMdiArr[i]),2),
                    "dmi_adx": round(float(dmiAdxArr[i]),2),
                    "dmi_adxr": round(float(dmiAdxrArr[i]),2),
                    
                    "dma_line": round(float(dmaArr[i]),2),
                    "ama_line": round(float(amaArr[i]),2),

                    "sar": round(float(sarArr[i]),2),
                    
                    "kdj_k": round(float(kArr[i]),2),
                    "kdj_d": round(float(dArr[i]),2),
                    "kdj_j": round(float(jArr[i]),2),
                    
                    "rsi_6": round(float(rsi6Arr[i]),2),
                    "rsi_12": round(float(rsi12Arr[i]),2),
                    "rsi_24": round(float(rsi24Arr[i]),2),

                    "cci": round(float(cciArr[i]),2),

                    "bias_5": round(float(bias5Arr[i]),2),
                    "bias_10": round(float(bias10Arr[i]),2),
                    "bias_20": round(float(bias20Arr[i]),2),

                    "wr_6": round(float(wr6Arr[i]),2),
                    "wr_14": round(float(wr14Arr[i]),2),
                    
                    "obv": round(float(obvArr[i]),2),

                    "volume": round(float(volumeArr[i]),2),
                })
            
        except Exception as e:
            errMsg = f"PID: {_processorPID},errMsg:{str(e)}"
            _LOG.error(f"{errMsg}, {traceback.format_exc()}")
        return result


def test():
    """改进的测试函数"""
    tt = StockTI()
    
    # 构建测试数据
    test_data = []
    for i in range(30):
        test_data.append({
            'date': f'2024-01-{i+1:02d}',
            'open': 10 + i * 0.5,
            'high': 11 + i * 0.5,
            'low': 9 + i * 0.5,
            'close': 10 + i * 0.5,
            'volume': 1000000 + i * 10000
        })
    
    # 测试calculateTechnicalIndicators
    result = tt.calculateTechnicalIndicators(test_data)
    print(f"计算结果数量: {len(result)}")
    if result:
        print("最后一条数据:")
        for key, value in result[-1].items():
            print(f"  {key}: {value}")


# 使用示例
if __name__ == "__main__":
    pass
    test()
