#!/usr/local/bin/python3

import re
import sys
import csv
import functools
import urllib.request
import argparse
import sqlite3
import logging
import datetime
import pandas
import pandas_datareader
import typing

def db_insertprices(tickersymbol:str,
					df:pandas.core.frame.DataFrame,
					c:sqlite3.Cursor):
	_bulk = [x.split(',') for x in df.dropna().to_csv().split()[1:]]
	_bulk = map(lambda t, o, h, l, c, v: (tickersymbol, int(t.replace('-', '')), float(o), float(h), float(l), float(c), round(float(v))), *zip(*_bulk))
	_bulk = list(_bulk)
	_init = datetime.datetime.now()
	logging.info('_bulk[0] : {}'.format(_bulk[0]))
	c.executemany('INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?)', _bulk)
	_fini = datetime.datetime.now()
	logging.info('{} inserted... ({} sec)'.format(tickersymbol, (_fini - _init)))
def fetcher(conn:sqlite3.Connection,
			tickersymbols:list,
			source:str,
			dryrun:bool=False):
	c = conn.cursor()
	while 0 < len(tickersymbols):
		tickersymbol = tickersymbols.pop(0)
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
			if 'stooq' == source:
				logging.info('pandas_datareader.data.DataReader({}, "stooq")...'.format(tickersymbol))
				_df = pandas_datareader.data.DataReader(tickersymbol, 'stooq')
			elif 'yahoo' == source:
				logging.info('pandas_datareader.DataReader({}, "yahoo")...'.format(tickersymbol))
				_df = pandas_datareader.DataReader(tickersymbol, 'yahoo', _startdate, _enddate).drop(columns='Adj Close')
			else:
				assert False, 'Invalid source ({}).'.format(source)
			db_insertprices(tickersymbol, _df, c)
			if not dryrun: conn.commit()
		except sqlite3.OperationalError:
			exc = sys.exc_info()
			if 'database is locked' in str(exc[1]): tickersymbols.append(tickersymbol)
		except:
			exc = sys.exc_info()
			logging.error('{}: sys.exc_info()[:-1] : {} ; {} data not fetched'.format(__name__, exc[:-1], tickersymbol))
			if 'Service Unavailable' in str(exc[1]):
				logging.info('{} restored to tickersymbols[]...'.format(tickersymbol))
				tickersymbols.append(tickersymbol)
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
		c.execute('CREATE TABLE IF NOT EXISTS prices(tickersymbol STR, date INTEGER, open REAL, high REAL, low REAL, close REAL, volume INTEGER, UNIQUE(tickersymbol, date))')
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
def fetchtickersymbols(updatetickersymbols:bool=True,
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
	retval  = list(map(lambda x: x[0], nasdaqreader))
	retval += list(map(lambda x: x[0], otherreader))
	return retval
def update(conn:sqlite3.Connection,
		   dryrun:bool,
		   updatetickersymbols:bool=True,
		   blacklist:typing.Iterable=['NUANV']):
	tickersymbols = fetchtickersymbols(updatetickersymbols, blacklist)
	logging.info('len(tickersymbols)  : {}'.format(len(tickersymbols)))
	fetcher(conn, tickersymbols, 'yahoo')
def access(conn:sqlite3.Connection,
		   tickersymbols:list,
		   fields:list,
		   init:int=19800101,
		   delta:int=0):
	ohlcv = ['open', 'high', 'low', 'close', 'volume']
	assert all([f in ohlcv for f in fields]), 'All fields must be {}.'.format(ohlcv)
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
	assert conn, 'Unable to established connection to database!'
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