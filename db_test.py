import MySQLdb


connection = MySQLdb.connect(db = "nico", user = "root", passwd = "admin");
cursor = connection.cursor();

cursor.execute("select * from sm2451;");
result = cursor.fetchall();
for row in result:
	print row[0];
	print row[1];

print len(result);

cursor.close();
connection.close();

