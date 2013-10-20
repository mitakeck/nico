#--coding:utf8

import json
import MySQLdb
import os
import MeCab
import time

def e(s):
	""" 文字列を SQL 文へエスケープ処理をして返却する 
	args:
		s: エスケープしたい文字列
	return:
		<type 'unicode'>
	"""
	if type(s) == unicode or type(s) == str:
		return MySQLdb.escape_string(s.encode("utf-8"));
	else:
		return "";

def db_connection(db, user, passwd):
	""" データベースに接続
	args:
		db: データベース名
		user: ユーザ名
		passwd: パスワード
	return:
		connection:
		cursor:
	"""
	connection = MySQLdb.connect(db = "nico", user = "root", passwd = "admin");
	cursor = connection.cursor();

	return connection, cursor;

def db_disconnection(connection, cursor):
	""" データベースとの接続を解除
	args:
		connection:
		cursor:
	"""
	connection.commit();
	cursor.close();
	connection.close();

def parse_json(json_string):
	""" json 形式の文字列をオブジェクトに変換する
	args:
		json_string: json形式の文字列
	return:
		json オブジェクト
	"""
	return json.loads(json_string);

def db_create_table(connection, cursor, tablename):
	""" テーブルを作成する
	args:
		connection:
		cursor:
		tablename: テーブル名
	return:
		テーブル名
	"""
	tablename = tablename.split(".")[0];
	sql = "create table %s (  id int(17) NOT NULL AUTO_INCREMENT, comment varchar(255) NOT NULL, command varchar(255) , vpos int(17) NOT NULL, no int(17) NOT NULL, date int(17) NOT NULL,PRIMARY KEY (id));" % (e(tablename));
	cursor.execute(sql);
	connection.commit();
	return tablename;

def db_is_exist_table(connection, cursor, tablename):
	""" テーブルの存在チェック
	args:
		connection:
		cursor:
		tablename: 存在を確認したいテーブル名
	return:
		0: 存在しない
		-1: 存在する
	"""
	tablename = tablename.split(".")[0];
	sql = "select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME = '%s';" % (e(tablename));
	cursor.execute(sql);
	result = cursor.fetchall();
	if(len(result) == 0):
		return 0;
	else:
		return -1;


def parse_table_name(string):
	return e(string.split(".")[0]);

def get_nearly_vpos_comment(connection, cursor, tablename, vpos, offset):
	start_vpos = max(0, vpos-offset);
	end_vpos = vpos + offset;
	sql = "select * from %s where vpos between '%s' and '%s';" % (e(tablename), start_vpos, end_vpos);
	cursor.execute(sql);
	result = cursor.fetchall();
	return result;

if __name__ == "__main__":
	connection, cursor = db_connection("nico", "root", "admin");	

	path_to_file = u'/root/nico';
	for folder in  os.listdir(path_to_file):
		if not os.path.isdir(path_to_file+'/'+folder):
			continue;
	
		for filename in os.listdir(path_to_file+'/'+folder):
			filepath = filename;
			filename = path_to_file+'/'+folder+'/'+filename;
			print filename

			file = open(filename, 'rt');
			tablename = parse_table_name(filepath);
			if not db_is_exist_table(connection, cursor, tablename):
				db_create_table(connection, cursor, tablename);
			
				for line in file:
					json_data = json.loads(line);
					query = "insert into %s (command, comment, no, vpos, date) values ('%s', '%s', '%s', '%s', '%s');" % ( e(tablename), e(json_data["command"]), e(json_data["comment"]), json_data["no"], json_data["vpos"], json_data["date"] );
					cursor.execute(query);
				
				cursor.execute("select * from " + tablename + ";");
				result = cursor.fetchall();
				connection.commit();
				print len(result);

		time.sleep(5.0);
	
	



 	db_disconnection(connection, cursor);
