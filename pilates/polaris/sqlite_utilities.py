#!/usr/bin/python
# Filename: run_convergence.py

import sys
import os
import json
import sqlite3
import csv
import traceback
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def execute_sql_script(db_name, script):
	logger.info('Executing Sqlite3 script: %s on database: %s' % (db_name, script))
	with open(str(script), 'r') as sql_file:
		sql_script = sql_file.read()

	db = sqlite3.connect(str(db_name))
	cursor = db.cursor()
	try:
		cursor.executescript(sql_script)
		db.commit()
	except sqlite3.Error as err:
		logger.error('SQLite error: %s' % (' '.join(err.args)))
		logger.error("Exception class is: ", err.__class__)
		logger.error('SQLite traceback: ')
		exc_type, exc_value, exc_tb = sys.exc_info()
		logger.error(traceback.format_exception(exc_type, exc_value, exc_tb))
	db.close()


def execute_sql_script_with_attach(db_name, script, attach_db_names, attach_db_aliases=None):
	logger.info('Executing Sqlite3 script: %s on database: %s' % (db_name, script))
	
	attach_list = []
	if isinstance(attach_db_names, list):
		attach_list = attach_db_names
	else:
		attach_list.append(attach_db_names)
	attach_aliases = []
	if isinstance(attach_db_aliases, list):
		attach_aliases = attach_db_aliases
	else:
		if attach_db_aliases:
			attach_aliases.append(attach_db_names)
	
			
	with open(str(script), 'r') as sql_file:
		sql_script = sql_file.read()

	attach_names = []
	alias_names = []
	default_alias = 'a'
	for i, n in enumerate(attach_list):
		if not isinstance(n, Path):
			logger.error('execute_sql_script requires a path for attach_db_name')
		attach_names.append(str(n.resolve()))
		if i < len(attach_aliases):
			alias_names.append(attach_aliases[i])
		else:
			alias_names.append(default_alias)
			default_alias = chr(ord(default_alias) + 1)

	db = sqlite3.connect(str(db_name))
	cursor = db.cursor()	
	try:
		for i, attach_name in enumerate(attach_names):
			attach_alias = alias_names[i]
			print ('ATTACH DATABASE "' + attach_name + '" as ' + str(attach_alias) + ';')
			cursor.execute('ATTACH DATABASE "' + attach_name + '" as ' + str(attach_alias) + ';')
		cursor.executescript(sql_script)
		db.commit()
	except sqlite3.Error as err:
		logger.error('SQLite error: %s' % (' '.join(err.args)))
		logger.error("Exception class is: ", err.__class__)
		logger.error('SQLite traceback: ')
		exc_type, exc_value, exc_tb = sys.exc_info()
		logger.error(traceback.format_exception(exc_type, exc_value, exc_tb))
	db.close()
	
	
def dump_table_to_csv(db, table, csv_name, write_headers=True):
	db_input = sqlite3.connect(str(db))
	sql3_cursor = db_input.cursor()
	query = 'SELECT * FROM ' + table
	sql3_cursor.execute(query)
	with open(str(csv_name), 'w') as out_csv_file:
		csv_out = csv.writer(out_csv_file, lineterminator='\n')  # gets rid of blank lines - defaults to \r\n
		if write_headers:
			# write header
			csv_out.writerow([d[0] for d in sql3_cursor.description])
		# write data
		for result in sql3_cursor:
			csv_out.writerow(result)
	db_input.close()