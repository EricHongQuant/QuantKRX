
import requests as rq
from bs4 import BeautifulSoup
import re
from io import BytesIO
import pandas as pd
import numpy as np
import pymysql
import json
import time
from tqdm import tqdm
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from datetime import date, datetime


class KrxDbUpdater:
    
    def __init__(self):
        
        self.con = pymysql.connect(user = 'root',
                                  passwd = '******',
                                  port = 3307,
                                  host = '127.0.0.1',
                                  db = 'KrxStock',
                                  charset = 'utf8')
    
        with self.con.cursor() as curs:
            sql = """
            create table if not exists KrxTicker (
                TickerCode varchar(8) not null,
                TickerName varchar(32),
                MarketName varchar(8),
                Close float,
                MarketCap float,
                DataDt date,
                EPS float,
                ForwardEPS float,
                BPS float,
                DivPerShare float,
                Category varchar(16),
                primary key(TickerCode, DataDt)
                )
            """
            curs.execute(sql)
            
            sql = """
            create table if not exists KrxSector (
                IdxCode varchar(3),
                TickerCode varchar(6),
                TickerName varchar(20),
                SectorName varchar(10),
                Seq int,
                DataDt date,
                primary key(TickerCode, DataDt)
                )
            """
            curs.execute(sql)
            
            sql = """
            create table if not exists KrxPrice (
                Dt date,
                Open double,
                High double,
                Low double,
                Close double,
                Volume double,
                TickerCode varchar(6),
                primary key(Dt, TickerCode)
                )
            """
            curs.execute(sql)
            
        self.con.commit()
        
        self.engine = create_engine('mysql+pymysql://root:141113@127.0.0.1:3307/krxstock')
    
    def __del__(self):
        self.con.close()
        self.engine.dispose()
        
    def GetRecentBizDay(self):
    
        url = 'https://finance.naver.com/sise/sise_deposit.nhn'
        data = rq.get(url)
        dataHtml = BeautifulSoup(data.content, features = 'html.parser')
        
        parseDay = dataHtml.select_one('div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text
            
        bizDay = ''.join(re.findall('[0-9]+', parseDay))
        
        return bizDay

    def UpdateKrxTicker(self, bizDay):
    
        '''*************'''
        ''' Market '''
        '''*************'''
        
        ''' KOSPI'''
        
        gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'   # from [Header] tab in developer tools
        gen_otp_stk = {
            'mktId': 'STK',
            'trdDd': bizDay,
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
        } # from [Payload] tab in developer tools
        headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}    # from [Header] tab in developer tools
        
        otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text
        
        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)
        
        sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding = 'EUC-KR')
        
        
        ''' KOSDAQ'''
        
        gen_otp_ksq = {
            'mktId': 'KSQ',
            'trdDd': bizDay,
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
        }
        otp_ksq = rq.post(gen_otp_url, gen_otp_ksq, headers=headers).text
        
        down_sector_ksq = rq.post(down_url, {'code': otp_ksq}, headers=headers)
        
        sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding='EUC-KR')
        
        
        ''' Combning '''
        
        krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop = True)
        krx_sector['종목명'] = krx_sector['종목명'].str.strip() #dropping space
        krx_sector['DataDt'] = bizDay
        
        
        '''*************'''
        ''' Indiv index '''
        '''*************'''
        
        gen_otp_data = {
            'searchType': '1',
            'mktId': 'ALL',
            'trdDd': bizDay,
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
        }
        
        otp = rq.post(gen_otp_url, gen_otp_data, headers=headers).text
        
        krx_ind = rq.post(down_url, {'code': otp}, headers=headers)
        
        krx_ind = pd.read_csv(BytesIO(krx_ind.content), encoding='EUC-KR')
        krx_ind['종목명'] = krx_ind['종목명'].str.strip()
        krx_ind['DataDt'] = bizDay
        
        
        '''***********************************************'''
        ''' Merging data - Classification and Indiv index '''
        '''***********************************************'''
        diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명'])))
        KrxTicker = pd.merge(krx_sector,
                              krx_ind,
                              on = krx_sector.columns.intersection(krx_ind.columns).tolist(),
                              how = 'outer')
        
        KrxTicker['Category'] = np.where(KrxTicker['종목명'].str.contains('스팩|제[0-9]+호'), 'SPAC',
                                      np.where(KrxTicker['종목코드'].str[-1:] != '0', '우선주',
                                               np.where(KrxTicker['종목명'].str.endswith('리츠'), 'REITs',
                                                        np.where(KrxTicker['종목명'].isin(diff),  'Others',
                                                        'Common stock'))))
        KrxTicker = KrxTicker.reset_index(drop = True)
        KrxTicker.columns = KrxTicker.columns.str.replace(' ', '')
        KrxTicker = KrxTicker[['종목코드', '종목명', '시장구분', '종가',
                                 '시가총액', 'DataDt', 'EPS', '선행EPS', 'BPS', '주당배당금', 'Category']]
        KrxTicker.columns = ['TickerCode', 'TickerName', 'MarketName', 'Close', 
                      'MarketCap', 'DataDt', 'EPS', 'ForwardEPS', 'BPS', 'DivPerShare', 'Category']
        KrxTicker = KrxTicker.replace({np.nan: None})
        KrxTicker['DataDt'] = pd.to_datetime(KrxTicker['DataDt'])
        
       
        mycursor = self.con.cursor()
        query = """
            insert into KrxTicker (TickerCode, TickerName, MarketName, Close, MarketCap, DataDt, EPS, ForwardEPS, BPS, DivPerShare, Category)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
            on duplicate key update
            TickerName = new.TickerName, MarketName = new.MarketName, Close = new.Close, MarketCap = new.MarketCap, EPS = new.EPS, 
            ForwardEPS = new.ForwardEPS, BPS = new.BPS, DivPerShare = new.DivPerShare, Category = new.Category;
        """
        
        args = KrxTicker.values.tolist()
        
        mycursor.executemany(query, args)
        self.con.commit()
        
    def UpdateIndustryClass(self, bizDay):
        
        
        '''*******************************************************'''
        ''' WICS (Wise Industry Classification Standard) Crawling '''
        '''*******************************************************'''
        
        '''Gathering all sector information'''
        
        url = f'''http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={bizDay}&sec_cd=G10'''
        data = rq.get(url).json()

        sectorCd = []
        for n in data['sector']:
            sectorCd.append(n['SEC_CD'])
            
        sectorDfList = []
        
        for i in tqdm(sectorCd):
            url = f'''http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={bizDay}&sec_cd={i}'''    
            data = rq.get(url).json()
            dataDf = pd.json_normalize(data['list'])
        
            sectorDfList.append(dataDf)
        
            time.sleep(2)
        
        krxSector = pd.concat(sectorDfList, axis = 0)
        krxSector = krxSector[['IDX_CD', 'CMP_CD', 'CMP_KOR', 'SEC_NM_KOR', 'SEQ']]
        krxSector.columns = ['IdxCode', 'TickerCode', 'TickerName', 'SectorName', 'Seq', ]
        krxSector['DataDt'] = bizDay
        krxSector['DataDt'] = pd.to_datetime(krxSector['DataDt'])
        
        
        mycursor = self.con.cursor()
        query = """
            insert into KrxSector (IdxCode, TickerCode, TickerName, SectorName, Seq, DataDt)
            values (%s,%s,%s,%s,%s,%s) as new
            on duplicate key update
            IdxCode = new.IdxCode, TickerName = new.TickerName, SectorName = new.SectorName, Seq = new.Seq
        """
        
        args = krxSector.values.tolist()
        
        mycursor.executemany(query, args)
        self.con.commit()
        
        
    def UpdateKrxPrice(self, bizDay):
        
        '''*******************************'''
        ''' Adjusted stock price crawling '''
        '''*******************************'''
        
        ''' Getting ticker '''
        
        tickerList = pd.read_sql("""
            select * 
            from KrxTicker
            where DataDt = (select max(DataDt) from KrxTicker) 
            	and Category = 'Common stock';
        """, con = self.engine)
        
        
        ''' Query to save in Sql Db '''
        
        query = """
            insert into KrxPrice (Dt, Open, High, Low, Close, Volume, TickerCode)
            values (%s,%s,%s,%s,%s,%s,%s) as new
            on duplicate key update
            Open = new.Open, High = new.High, Low = new.Low,
            Close = new.Close, Volume = new.Volume;
        """
        
        errorList = []
        
        mycursor = self.con.cursor()
        
        ''' Getting All ticker price '''
        
        for i in tqdm(range(0, len(tickerList))):
        
            # Ticker selection
            ticker = tickerList['TickerCode'][i]
        
            # StartDt and EndDt selection
            StartDt = (datetime.strptime(bizDay, '%Y%m%d') + relativedelta(years=-5)).strftime("%Y%m%d")
            EndDt = bizDay
        
            # If an error occurs, ignore and move on to the next loop
            try:
        
                url = f'''https://fchart.stock.naver.com/siseJson.nhn?symbol={ticker}&requestType=1
                &startTime={StartDt}&endTime={EndDt}&timeframe=day'''
        
                data = rq.get(url).content
                priceDfRaw = pd.read_csv(BytesIO(data))
        
                # Data cleaning
                priceDf = priceDfRaw.iloc[:, 0:6]
                priceDf.columns = ['Dt', 'Open', 'High', 'Low', 'Close', 'Volume']
                priceDf = priceDf.dropna()
                priceDf['Dt'] = priceDf['Dt'].str.extract('(\d+)')
                priceDf['Dt'] = pd.to_datetime(priceDf['Dt'])
                priceDf['TickerCode'] = ticker
        
                # Saving Data in Db
                args = priceDf.values.tolist()
                mycursor.executemany(query, args)
                self.con.commit()
        
            except:
        
                # If error occurs, save in the errorList and move on
                errorList.append(ticker)
        
            # Delay 2 seconds between loops
            time.sleep(2)
        
        print(f'\nError occured for these tickers{len(errorList)} ea:\n', errorList)
        
if __name__ == "__main__":
    DbUpdater = KrxDbUpdater()
    bizDay = DbUpdater.GetRecentBizDay()
    DbUpdater.UpdateKrxTicker(bizDay)
    DbUpdater.UpdateIndustryClass(bizDay)
    DbUpdater.UpdateKrxPrice(bizDay)
