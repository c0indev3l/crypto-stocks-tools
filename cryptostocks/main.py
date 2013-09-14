#!/usr/bin/env python

"""
    Python script to filter stocks from Crypto::Stocks
    See API description https://cryptostocks.com/api

    Copyright (C) 2013 "Working4coins" <working4coins@gmail.com>
    You can donate: https://sites.google.com/site/working4coins/donate

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>
"""

# ToDo:
# - get bid
# - get security id
# - get url security

import os
import argparse
#import urllib2
from urlparse import urljoin
import requests

import json
import pandas as pd
import numpy as np
#import dateutil.parser
import datetime
import pytz

class API_Request:
    def __init__(self, base_path, api_url, values={}, api_type='json', flag_download=True):
        self.flag_download = flag_download
        
        self.api_type = api_type
        
        self.api_url = api_url
        self.short_filename = api_url #.replace('/','_')

        self.values = values
        
        self.base_path = base_path        
        
        self.api_base_url = "https://cryptostocks.com/api/"

    def update(self):
        self.get_data()
        self.convert_to_DataFrame()
        self.calculate()

    def get_data(self):
        self.filename = os.path.join(self.base_path, "data_in/{api_url}{dict_val}.{api_type}".format(api_url=self.short_filename, dict_val=self.dict2str(self.values) ,api_type=self.api_type))

        if self.flag_download:
            self.download()
            self.write_data()
        else:
            self.read_data()

    def download(self):
        self.url = urljoin(self.api_base_url, self.api_url)
        self.url = self.url + '.' + self.api_type
        print("Downloading {api_url} from {url} (please wait)".format(api_url=self.api_url, url=self.url))
        print("    parameters= {d}".format(d=self.values))
        
        #req = requests.get(self.url, params=self.values)
        req = requests.get(self.url, params=self.values, verify=False)
        # to fix requests.exceptions.SSLError: [Errno 1] _ssl.c:504: error:14090086:SSL routines:SSL3_GET_SERVER_CERTIFICATE:certificate verify failed
        
        self.raw_data = req.content
        
        if self.api_type=='json':
            self.data = json.loads(self.raw_data)
        else:
            raise(Exception("Undefined API type"))
        
    def write_data(self):
        print("Writing {api_url} to {filename}".format(api_url=self.api_url, filename=self.filename))
        myFile = open(self.filename, 'w')
        myFile.write(self.raw_data)
        myFile.close()

    def read_data(self):
        print("Reading {api_url} from {filename}".format(api_url=self.api_url, filename=self.filename))
        myFile = open(self.filename, 'r')
        self.raw_data = myFile.read()
        self.data = json.loads(self.raw_data)
        myFile.close()

    def dict2str(self, d):
        str = ''
        for (key, value) in d.iteritems():
            #str = str + '_' + key + '-' + value
            str = str + '-' + key + '_' + value
        return(str)

    def pretty_print(self):
        if self.api_type=='json':
            print(json.dumps(self.data, sort_keys=True, indent=2))
        else:
            raise(Exception("Undefined API type"))

    def convert_to_DataFrame(self):
        pass

    def calculate(self):
        pass

    def to_excel(self):
        filename = os.path.join(self.base_path, "data_out/{api_url}{dict_val}.xls".format(api_url=self.short_filename, dict_val=self.dict2str(self.values)))
        self.df.to_excel(filename)

class API_Request_get_security_info(API_Request):
    def __init__(self, base_path, ticker, api_type='json', flag_download=True):
        api_url = 'get_security_info'
        values = {'ticker': ticker}
        
        API_Request.__init__(self, args.basepath, api_url, values, api_type='json', flag_download=flag_download)

class API_Request_get_dividend_for_security(API_Request):
    def __init__(self, base_path, ticker, days=30, daysoffset=0, api_type='json', flag_download=True):
        api_url = 'get_dividend_for_security'
        #values = {'ticker': ticker, 'days': str(days)}
        values = {'ticker': ticker, 'after_id': str(-1)} # to get all dividend

        self.ticker = ticker
        self.days = days
        self.daysoffset = daysoffset
        
        API_Request.__init__(self, args.basepath, api_url, values, api_type='json', flag_download=flag_download)

    def convert_to_DataFrame(self):
        if self.data['return_code']==0:
            try:
                if len(self.data['dividends'])>0:
                    self.df = pd.DataFrame(self.data['dividends'])
                else:
                    self.df = pd.DataFrame(columns=['dividend_per_share', 'id', 'number_shares', 'timestamp'])
            except:
                raise(Exception("Can't convert get_dividend to DataFrame"))
        else:
            raise(Exception("return_code!=0"))

        # convert unicode to float
        self.df['dividend_per_share'] = self.df['dividend_per_share'].astype(float)
        
        #self.df['timestamp'] = self.df['timestamp'].map(lambda s: dateutil.parser.parse(s))
        #self.df['timestamp'] = self.df['timestamp'].map(lambda s: datetime.datetime.fromtimestamp(int(s), tz=pytz.UTC)) #convert timestamp to datetime
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])

        # filter to keep only dividend from now - daysoffset to now - days - daysoffset
        #dtnow = datetime.datetime.now().replace(tzinfo=pytz.UTC)
        dtnow = datetime.datetime.utcnow()
        dt2 = dtnow - datetime.timedelta(days=self.daysoffset)
        dt1 = dt2 - datetime.timedelta(days=self.days)
        print("dividends from {dt1} to {dt2}".format(dt1=dt1, dt2=dt2))
        self.df = self.df[(self.df['timestamp']>=dt1) & (self.df['timestamp']<=dt2)]

    def calculate(self):
        self.df['total_dividend'] = self.df['dividend_per_share'] * self.df['number_shares']
        #self.df['dividend_per_share'] = self.df['total_dividend']/self.df['number_shares']

        self.dividend_per_share_total = float(self.df['dividend_per_share'].sum())
        self.number_shares_total = float(self.df['number_shares'].sum())
        self.total_dividend_total = self.df['total_dividend'].sum()
        if self.number_shares_total!=0:
            self.dividend_per_share_avg = self.total_dividend_total/self.number_shares_total
        else:
            self.dividend_per_share_avg = 0.0 # None

        self.dividends_nb = len(self.df)

class API_Request_get_list_of_securities(API_Request):
    def __init__(self, base_path, api_type='json', flag_download=True):
        api_url = 'get_list_of_securities'
        
        #values = {'currency': 'BTC'}
        values = {} # all currencies
        
        API_Request.__init__(self, base_path, api_url, values, api_type, flag_download=flag_download)
        
        self.update()
        
        self.convert_to_DataFrame()
        
        #print(self.df)
        self.df.index = self.df['ticker'] # use ticker as index
        #self.df = self.df.set_index(self.df['ticker'])
        
        #['ticker', 'name', 'currency', 'number_public_shares', 'highest_bid', 'lowest_ask', 'last_price', 'volume_24h', 'volume_7d', 'volume_30d']
        self.df['lowest_ask'] = np.nan
        self.df['highest_bid'] = np.nan
        
        self.df['dividend_per_share'] = np.nan
        self.df['dividends_nb'] = 0
                
        if args.days!=None:
            days = int(args.days)
        else:
            days = 30

        if args.daysoffset!=None:
            daysoffset = int(args.daysoffset)
        else:
            daysoffset = 0
        
        for ticker in self.tickers():
            print("="*10 + " " + ticker + " " + "="*10)
            
            security_info = API_Request_get_security_info(base_path, ticker, api_type, flag_download)
            security_info.update()
            
            #print("="*10 + " " + ticker + " (" + security_info.data['currency'] + ") " + " " + "="*10)
            
            print("currency: {currency}".format(currency=security_info.data['currency']))
            #print("name: {name}".format(name=security_info.data['name'])) # ToFix: 
            
            #print("{name} ({currency})".format(
            #    name = security_info.data['name'],
            #    currency = security_info.data['currency']
            #))
            
            try:
                lowest_ask = float(security_info.data['lowest_ask'])
            except:
                lowest_ask = None
            #print("lowest_ask={val}".format(val=lowest_ask))
            self.df['lowest_ask'][ticker] = lowest_ask

            try:
                highest_bid = float(security_info.data['highest_bid'])
            except:
                highest_bid = None
            #print("highest_bid={val}".format(val=highest_bid))
            self.df['highest_bid'][ticker] = highest_bid
            
            print("")
            
            dividend_for_security = API_Request_get_dividend_for_security(base_path, ticker, days, daysoffset, api_type, flag_download)
            dividend_for_security.update()
            print(dividend_for_security.df)
            #dividend_for_security.to_excel()
            print('')
            print("dividend_per_share_total={dividend_per_share_total}".format(dividend_per_share_total=dividend_for_security.dividend_per_share_total))
            print("dividend_per_share_avg={dividend_per_share_avg}".format(dividend_per_share_avg=dividend_for_security.dividend_per_share_avg))
            #print("dividend_per_share={val}".format(val=dividend_per_share))
            self.df['dividend_per_share'][ticker] = dividend_for_security.dividend_per_share_total
            self.df['dividends_nb'][ticker] = dividend_for_security.dividends_nb
            
            print("")

        print("")
        print("="*15 + " " + "SUMMARY" + " " + "="*15)

        self.df = self.df.rename(columns={
            'lowest_ask': 'ask',
            'highest_bid': 'bid'
        })
        
        #print(self.df)
        
        # Calculate
        self.df['SpreadRelPC'] = 200.0 * (self.df['ask'] - self.df['bid']) / (self.df['ask'] + self.df['bid'])
        
        self.df['DividendPerPricePC'] = self.df['dividend_per_share']/self.df['ask'] * 100.0
        self.df['DividendPerPricePC'] = self.df['DividendPerPricePC'].fillna(0.0)

        # URL
        #self.df['url'] = self.df.index
        #self.df['url'] = 'https://cryptostocks.com/securities/' + self.df['url']
        
        # Filter
        if args.currency!=None:
            self.df = self.df[self.df['currency']==args.currency]

        if args.onlywithdividend:
            self.df = self.df[self.df['dividend_per_share']>0]

        if args.onlywithask:
            self.df = self.df[self.df['ask']>0]

        if args.maxspreadrel != None:
            self.df = self.df[self.df['SpreadRelPC'] <= float(args.maxspreadrel)]
        
        # Sort
        self.df = self.df.sort('DividendPerPricePC', ascending=True)
        
        df = self.df[['name', 'currency', 'ask', 'bid', 'dividends_nb', 'dividend_per_share', 'SpreadRelPC', 'DividendPerPricePC']]
        print(df)
        
        filename = os.path.join(self.base_path, "data_out/data.xls")
        self.df.to_excel(filename)
        

        #filename = os.path.join(self.base_path, "data_out/data_{days}.csv".format(days=days))
        #self.df.to_csv(filename)
        #filename = os.path.join(self.base_path, "data_out/data_{days}.xls".format(days=days))
        #self.df.to_excel(filename)

    def convert_to_DataFrame(self):
        if self.data['return_code']==0:
            try:
                self.df = pd.DataFrame(self.data['securities'])
            except:
                raise(Exception("Can't convert to DataFrame"))
        
        else:
            raise(Exception("return_code!=0"))

    def tickers(self):
        for ticker in self.df['ticker']:
                yield ticker

class CryptoStocksFilter:
    def __init__(self, args):    
        self.args = args
        
        flag_download = not self.args.nodownload
        api_req = API_Request_get_list_of_securities(args.basepath, api_type='json', flag_download=flag_download)
        api_req.to_excel()
        
        if args.printraw:
            api_req.pretty_print()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Use the following parameters')
    parser.add_argument('--nodownload', action="store_true", help="use this flag to avoid downloading orderbook (will use a previously downloaded file)")
    parser.add_argument('--printraw', action="store_true", help="use this flag to pretty print raw data")
    #parser.add_argument('--printsecu', action="store_true", help="use this flag to print securities")
    parser.add_argument('--onlywithdividend', action="store_true", help="use this flag to filter securities that send dividend")
    parser.add_argument('--onlywithask', action="store_true", help="use this flag to filter securities with ask price")
    parser.add_argument('--maxspreadrel', action="store", help="use this flag to filter securities with relative spread lower than maxspread %")
    parser.add_argument('--currency', action="store", help="use this flag to select a currency (BTC, LTC, DVC...)")
    parser.add_argument('--days', action="store", help="use this flag to select days duration for dividend calculation")
    parser.add_argument('--daysoffset', action="store", help="use this flag to select days offset for dividend calculation")
    args = parser.parse_args()
    
    args.basepath = os.path.dirname(__file__)
    
    stocks = CryptoStocksFilter(args)
