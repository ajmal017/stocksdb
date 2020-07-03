#!/usr/local/bin/python3

import re
import sys
import csv
import urllib.request
import argparse
import sqlite3
import logging
import datetime
import pandas_datareader
import typing

class Stock:
	def __init__(self, conn, a, b, c, d, e, f, g, h):
		self.tickersymbol = a
		self.name = b
		_nasdaqlisted = (' - Common Stock' in self.name)
		if _nasdaqlisted:
			self.category = c
			self.testissue = ('N' != d)
			self.financialstatus = e
			self.roundlotsize = f
			self.etf = ('N' != g)
			self.nextshares = ('N' != h)
			self.exchange = None
			self.cqs = None
			self.nasdaqsymbol = None
		else:
			self.exchange = c
			self.cqs = d
			self.etf = ('N' != e)
			self.roundlotsize = f
			self.testissue = ('N' != g)
			self.nasdaqsymbol = h
			self.category = None
			self.financialstatus = None
			self.nextshares = None
		self.c = conn.cursor()
	def __repr__(self):
		retval = '<Stock : {}>'.format(self.name)
		return retval
	def db_access(self, field, init, delta):
		assert(isinstance(field, str) and field in ['open', 'high', 'low', 'close', 'volume', 'adjclose'])
		assert(None == init or isinstance(init, int))
		assert(None == delta or isinstance(delta, int))
		_init = (19800101 if None == init else init)
		_fini = int(datetime.datetime.today().strftime('%Y%m%d') if None == delta else (datetime.datetime.strptime(str(_init), '%Y%m%d') + datetime.timedelta(days=delta)).strftime('%Y%m%d'))
		if _fini < _init: _init, _fini = _fini, _init
		retval = c.execute('SELECT date, {} FROM prices WHERE tickersymbol=? AND date>=? AND date <=?'.format(field), [self.tickersymbol, _init, _fini])
		return tuple(retval.fetchall())
	def open(self, init=None, fini=None): return self.db_access(sys._getframe().f_code.co_name, init, fini)
	def high(self, init=None, fini=None): return self.db_access(sys._getframe().f_code.co_name, init, fini)
	def low(self, init=None, fini=None): return self.db_access(sys._getframe().f_code.co_name, init, fini)
	def close(self, init=None, fini=None): return self.db_access(sys._getframe().f_code.co_name, init, fini)
	def volume(self, init=None, fini=None): return self.db_access(sys._getframe().f_code.co_name, init, fini)
	def adjclose(self, init=None, fini=None): return self.db_access(sys._getframe().f_code.co_name, init, fini)

def fetcher(conn:sqlite3.Connection,
			commonstock:dict,
			dryrun:bool=False):
	def db_insertprices(tickersymbol:str,
						prices:csv.reader,
						c:sqlite3.Cursor):
		if commonstock[tickersymbol].testissue:
			logging.info('commonstock[{}].testissue == True... not fetching'.format(tickersymbol)) 
			return
		_bulk = []
		for _p in prices:
			_date = int(datetime.datetime.strptime(_p[0], '%Y-%m-%d').strftime('%Y%m%d'))
			_bulk.append(tuple([tickersymbol, _date] + _p[1:]))
		_init = datetime.datetime.now()
		logging.info('_bulk[0] : {}'.format(_bulk[0]))
		c.executemany('INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?, ?)', _bulk)
		_fini = datetime.datetime.now()
		logging.info('{} inserted... ({} sec)'.format(tickersymbol, (_fini - _init)))
	c = conn.cursor()
	stocks = list(commonstock.keys())
	while 0 < len(stocks):
		tickersymbol = stocks.pop(0)
		c.execute('SELECT MAX(date) FROM prices WHERE tickersymbol=?', [tickersymbol])
		_x = c.fetchall()[0][0]
		if None == _x:
			_startdate = '1980-01-01'
		else:
			_startdate = datetime.datetime.strptime(str(_x), '%Y%m%d')
			if datetime.datetime.today() - _startdate < datetime.timedelta(days=1):
				continue
			_startdate += datetime.timedelta(days=1)
			_startdate = _startdate.strftime('%Y-%m-%d')
		_enddate = datetime.datetime.today()
		_enddate = _enddate.strftime('%Y-%m-%d')
		if _startdate == _enddate: continue
		try:
			logging.info('dr.data.get_data_yahoo({}, start={}, end={})...'.format(tickersymbol, _startdate, _enddate))
			_df = pandas_datareader.data.get_data_yahoo(tickersymbol, start=_startdate, end=_enddate)
			prices_raw = _df.to_csv()
			prices_txt = prices_raw.splitlines()
			prices = prices_txt[1:]
			reader = csv.reader(prices, delimiter=',', quoting=csv.QUOTE_NONE)
			db_insertprices(tickersymbol, reader, c)
			if not dryrun: conn.commit()
		except sqlite3.OperationalError:
			exc = sys.exc_info()
			if 'database is locked' in str(exc[1]): stocks.append(tickersymbol)
		except:
			exc = sys.exc_info()
			logging.error('{}: sys.exc_info()[:-1] : {} ; {} data not fetched'.format(__name__, exc[:-1], tickersymbol))
			if 'Service Unavailable' in str(exc[1]):
				logging.info('{} restored to stocks[]...'.format(tickersymbol))
				stocks.append(tickersymbol)
				logging.info('Sleeping...')
				time.sleep(15)
				logging.info('Resuming...')
	c.close()
def init(dbfilename:str,
		 timeout:int=5):
	retval = None
	try:
		retval = sqlite3.connect(dbfilename, timeout=timeout)
		c = retval.cursor()
		c.execute('CREATE TABLE IF NOT EXISTS prices(tickersymbol STR, date INTEGER, open REAL, high REAL, low REAL, close REAL, volume INTEGER, adjclose REAL, UNIQUE(tickersymbol, date))')
		c.close()
		logging.info('Successfully connected to database ({})'.format(dbfilename))
	except:
		logging.critical('Failed to connect to database ({})'.format(dbfilename))
	return retval
def fini(conn:sqlite3.Connection):
	try:
		conn.close()
		logging.info('Successfully disconnected from database.')
	except:
		logging.critical('Failed to disconnect from database.')
def update(conn:sqlite3.Connection,
		   dryrun:bool,
		   updatetickersymbols:bool=True,
		   blacklist:typing.Iterable=['NUANV']):
	if (updatetickersymbols):
		response = urllib.request.urlopen('ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt')
		nasdaqlisted_raw = response.read()
		nasdaqlisted_txt = ''.join(map(chr, nasdaqlisted_raw)).splitlines()
		response = urllib.request.urlopen('ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt')
		otherlisted_raw = response.read()
		otherlisted_txt = ''.join(map(chr, otherlisted_raw)).splitlines()
	else:
		f = open('./nasdaqlisted.txt', 'r')
		nasdaqlisted_txt = f.readlines()
		f.close()
		f = open('./otherlisted.txt', 'r')
		nasdaqlisted_txt = f.readlines()
		f.close()
	nasdaqtradable = filter(lambda a: ' - Common Stock' in a, nasdaqlisted_txt)
	nasdaqtradable = filter(lambda a: all(map(lambda b: not b in a, blacklist)), nasdaqtradable)
	nasdaqreader = csv.reader(nasdaqtradable, delimiter='|', quoting=csv.QUOTE_NONE)
	othertradable = filter(lambda a: ' Common Stock' in a, otherlisted_txt)
	othertradable = filter(lambda a: all(map(lambda b: not b in a, blacklist)), othertradable)
	otherreader = csv.reader(othertradable, delimiter='|', quoting=csv.QUOTE_NONE)
	commonstock = {**{x[0] : Stock(conn, *x) for x in otherreader}, **{x[0] : Stock(conn, *x) for x in nasdaqreader}}
	fetcher(conn, commonstock, dryrun)
def access(conn:sqlite3.Connection,
		   tickersymbols:list,
		   fields:list,
		   init:int=19800101,
		   delta:int=0):
	ohlcva = ['open', 'high', 'low', 'close', 'volume', 'adjclose']
	assert all([f in ohlcva for f in fields]), 'All fields must be {}.'.format(ohlcva)
	fini = int((datetime.datetime.strptime(str(init), '%Y%m%d') + datetime.timedelta(days=delta)).strftime('%Y%m%d'))
	if fini < init: init, fini = fini, init
	c = conn.cursor()
	query  = 'SELECT date, tickersymbol, {} '.format(', '.join(fields))
	query += 'FROM prices '
	query += 'WHERE date>=? AND date<=? '
	query += ('AND ({})'.format(' OR '.join(len(tickersymbols) * ['tickersymbol=?'])) if 0 < len(tickersymbols) else '')
	retval = c.execute(query, [init, fini] + tickersymbols).fetchall()
	c.close()
	return tuple(retval)
def tickersymbols(conn:sqlite3.Connection):
	c = conn.cursor()
	retval = c.execute('SELECT DISTINCT tickersymbol FROM prices').fetchall()
	c.close()
	return sum(retval, tuple())

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Stock Database')
	parser.add_argument('--dry-run', dest='dryrun', action='store_true')
	parser.add_argument('--update', dest='update', action='store_true')
	parser.add_argument('--tickersymbols', dest='tickersymbols', action='store_true')
	parser.add_argument('--timeout', dest='timeout', type=int, default=5)
	parser.add_argument('--db-filename', dest='dbfilename', type=str, default='prices.db')
	parser.add_argument('--access', dest='access', nargs='+')
	args = parser.parse_args()
	logging.basicConfig(filename='stocksdb.log', format='%(asctime)s %(message)s', datefmt='%Y%m%d %H:%M:%S', level=logging.DEBUG)
	logging.debug('args : {}'.format(args))
	conn = init(args.dbfilename, args.timeout)
	if not conn: sys.exit(1)
	if args.update: update(conn, args.dryrun)
	if args.access:
		tickersymbol = [args.access[0]]
		fields = args.access[1:-2]
		init = int(args.access[-2])
		delta = int(args.access[-1])
		print('{}'.format(access(conn, tickersymbol, fields, init, delta)))
	if args.tickersymbols:
		tickersymbols = tickersymbols(conn)
		print('{} ({})'.format(tickersymbols, len(tickersymbols)))
	fini(conn)