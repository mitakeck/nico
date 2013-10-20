#--coding:utf8

import json
import MySQLdb
import os

def e(s):
	if type(s) == unicode or type(s) == str:
		return MySQLdb.escape_string(s.encode("utf-8"));
	else:
		return "";

connection = MySQLdb.connect(db = "nico", user = "root", passwd = "admin");
cursor = connection.cursor();

for filename in os.listdir('/root/nico/0000/'):
	file = open('/root/nico/0000/'+filename, 'rt');
	print filename;
	
	for line in file:
		json_data = json.loads(line);
		print type(json_data["command"]);
		query = "insert into %s (command, comment, no, vpos, date) values ('%s', '%s', '%s', '%s', '%s');" % ( e(filename), e(json_data["command"]), e(json_data["comment"]), json_data["no"], json_data["vpos"], json_data["date"] );
		print json_data["no"];
		cursor.execute(query);
	
	cursor.execute("select * from " + filename + ";");
	result = cursor.fetchall();
	connection.commit();
	print len(result);

cursor.close();
connection.close();
	
