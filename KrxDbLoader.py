import pandas as pd
from sqlalchemy import create_engine
import pymysql
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import re

class KrxDb:
    def __init__(self):
        
        self.engine = create_engine('mysql+pymysql://root:141113@127.0.0.1:3307/krxstock')
        
    def __del__(self):
        
        self.engine.dispose()

    def GetKrxTicker(self, dataDt=None):
        """ dataDt str: YYYYmmdd or YYYY-mm-dd
                        if not available, latest date in DB """
                        
        if not dataDt:
            query = """select * 
            from KrxTicker
            where DataDt = (select max(DataDt) from KrxTicker)"""
        else:
            query = f"""select * 
            from KrxTicker
            where DataDt = '{dataDt}'"""
        
        tickerDf = pd.read_sql(query, con = self.engine)
        
        return tickerDf

    def GetKrxSector(self, dataDt=None):
        """ dataDt str: YYYYmmdd or YYYY-mm-dd
                        if not available, latest date in DB """
        
        if not dataDt:
            query = """select * 
            from KrxSector
            where DataDt = (select max(DataDt) from KrxSector)"""
        else:
            query = f"""select * 
            from KrxSector
            where DataDt = '{dataDt}'"""
        
        sectorDf = pd.read_sql(query, con = self.engine)
        
        return sectorDf

    def GetKrxPrice(self, ticker=None, startDt=None, endDt=None):
        """ ticker: TickerCode
            startDt: YYYYmmdd or YYYY-mm-dd
            endDt str: YYYYmmdd or YYYY-mm-dd
                        if not available, latest date in DB """
        
        if not startDt:
            startDt = (datetime.today() + relativedelta(years=-1)).strftime('%Y%m%d')
        
        if not endDt:
            endDt = datetime.today().strftime('%Y%m%d')
        
        query = f"""select * 
        from KrxPrice
        where Dt >= '{startDt}' and Dt <= '{endDt}'"""
        
        if ticker:
            query += ' and TickerCode = ticker'
            
        priceDf = pd.read_sql(query, con = self.engine)
        
        return priceDf





        
