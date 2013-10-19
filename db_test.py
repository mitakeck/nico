import MySQLdb


connection = MySQLdb.connect(db = "nico", user = "root", passwd = "admin");
cursor = connection.cursor();

cursor.execute("select * from sm9;");
result = cursor.fetchall();
for row in result:
	print row

print len(result);

cursor.close();
connection.close();

