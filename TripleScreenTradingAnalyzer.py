import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mdates
from KrxDbLoader import KrxDb

def TripleScreenTradingAnalyzer(tickerName = 'SamsungElec', startDt ='2021-01-01'):

    db = KrxDb()
    priceDf = db.GetKrxPrice(tickerName, startDt)

    ema65 = priceDf.Close.ewm(span=65).mean() #13 week EMA
    ema60 = priceDf.Close.ewm(span=60).mean() #12 week EMA
    ema130 = priceDf.Close.ewm(span=130).mean() #26 week EMA
    macd = ema60 - ema130 #Moving average convergence/divergence
    signal = macd.ewm(span=45).mean() #9 week EMA
    macdhist = macd - signal
    priceDf = priceDf.assign(ema65=ema65, ema60=ema60, ema130=ema130, 
                            macd=macd, signal=signal, macdhist=macdhist).dropna()

    priceDf['Number'] = priceDf.Dt.map(mdates.date2num)
    ohlc = priceDf[['Number','Open','High','Low','Close']]

    MaxHigh = priceDf.High.rolling(window=14, min_periods=1).max()
    MinLow = priceDf.Low.rolling(window=14, min_periods=1).min()

    fastK = (priceDf.Close - MinLow) / (MaxHigh - MinLow) * 100
    slowD = fastK.rolling(window=3).mean()
    priceDf = priceDf.assign(fastK=fastK, slowD=slowD).dropna()

    plt.figure(figsize=(9, 9))

    p1 = plt.subplot(3, 1, 1)
    matplotlib.rcParams['font.family'] ='Malgun Gothic'
    matplotlib.rcParams['axes.unicode_minus'] =False
    plt.title(f'Triple Screen Trading ({tickerName})')
    plt.grid(True)
    candlestick_ohlc(p1, ohlc.values, width=.6, colorup='red', colordown='blue')
    p1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.plot(priceDf.Number, priceDf['ema65'], color='c', label='EMA65')
    for i in range(1, len(priceDf.Close)):
        if priceDf.ema65.values[i-1] < priceDf.ema65.values[i] and \
            priceDf.slowD.values[i-1] >= 20 and priceDf.slowD.values[i] < 20:
            plt.plot(priceDf.Number.values[i], priceDf.Close.min()*0.99, 'r^') 
        elif priceDf.ema65.values[i-1] > priceDf.ema65.values[i] and \
            priceDf.slowD.values[i-1] <= 80 and priceDf.slowD.values[i] > 80:
            plt.plot(priceDf.Number.values[i], priceDf.Close.min()*0.99, 'bv') 
    plt.legend(loc='best')

    p2 = plt.subplot(3, 1, 2)
    plt.grid(True)
    p2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.bar(priceDf.Number, priceDf['macdhist'], color='m', label='MACD-Hist')
    plt.plot(priceDf.Number, priceDf['macd'], color='b', label='MACD')
    plt.plot(priceDf.Number, priceDf['signal'], 'g--', label='MACD-Signal')
    plt.legend(loc='best')

    p3 = plt.subplot(3, 1, 3)
    plt.grid(True)
    p3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.plot(priceDf.Number, priceDf['fastK'], color='c', label='%K')
    plt.plot(priceDf.Number, priceDf['slowD'], color='k', label='%D')
    plt.yticks([0, 20, 80, 100])
    plt.legend(loc='best')
    plt.show()

if __name__ == "__main__":
    TripleScreenTradingAnalyzer(tickerName = '삼성전자', startDt ='2021-01-01')
