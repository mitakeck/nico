#--coding:utf8

import json
import MySQLdb
import os
import MeCab


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
	connection = MySQLdb.connect(db = db, user = user, passwd = passwd);
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
	sql = """
            CREATE TABLE %s 
            ( 
                id int(17) NOT NULL AUTO_INCREMENT,
                comment varchar(255) NOT NULL, 
                command varchar(255) , 
                vpos int(17) NOT NULL, 
                no int(17) NOT NULL, 
                date int(17) NOT NULL,PRIMARY KEY (id)
            );
            """ % (e(tablename));
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
	sql = """
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '%s';
            """ % (e(tablename));
	cursor.execute(sql);
	result = cursor.fetchall();
	if(len(result) == 0):
		return 0;
	else:
		return -1;

def db_insert_data(connection, cursor, tablename, data):
	if db_is_exist_table(connection, cursor, tablename):
 		query = """
                    INSERT INTO %s ( command, comment, no, vpos, date )
                    VALUES ('%s', '%s', '%s', '%s', '%s');
                    """ % ( e(tablename), e(data["command"]), e(data["comment"]), data["no"], data["vpos"], data["date"] );
		cursor.ececute(sql);
	else:
		print "table not exists.";

def parse_table_name(string):
	""" ファル名からテーブル名に変換するユーティリティ関数
	args:
		string:　ファイル名 / ファイルパス
	return :
		string テーブル名
	"""
	filename = string.split("/");
 	filename = filename[len(filename)-1];
 	return e(filename.split(".")[0]);

def get_comment_where_nearly_vpos(connection, cursor, tablename, vpos, offset):
	""" vpos の前後 offset(ms) のコメントを取得する
	args:
		connection:
		cursor:
		tablename:	テーブル名
		vpos:		取得したいコメントの再生時間
		offset:		前後何(ms)のコメントを取得するか
	return:
		コメントデータオブジェクト配列
	"""
	start_vpos = max(0, vpos-offset);
	end_vpos = vpos + offset;
	sql = """
            SELECT * FROM %s 
            WHERE vpos 
            BETWEEN '%s' AND '%s';
            """ % (e(tablename), start_vpos, end_vpos);
	cursor.execute(sql);
	result = cursor.fetchall();
	return result;

if __name__ == "__main__":

	mecab = MeCab.Tagger();
	connection, cursor = db_connection("nico", "root", "admin");	
	result = get_comment_where_nearly_vpos(connection, cursor, "sm19240845", 24100, 5000);

	word = {};
 	for comment in result:
		data = [];
		data.append(comment[1])
		node = mecab.parseToNode("\n".join(data))
		while node:
			posid = node.posid;
			surface = node.surface;
			if posid==10 or posid==11 or posid==12:
				if not word.has_key(surface):
					word[surface] = 1;
				else:
					word[surface] += 1;
				
			node = node.next;
	
	for w, c in sorted(word.items(), key=lambda x:x[1]):
		print w;
		print c;

 	db_disconnection(connection, cursor);
