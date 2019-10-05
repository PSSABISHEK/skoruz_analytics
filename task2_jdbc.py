import jaydebeapi

remote_filepath = r"F:\Internship\Skoruz\HDFS\Simba_Hive_JDBC\SIMBAHiveJDBC41.jar"
local_server = "172.16.1.30"
local_filepath = r"/home/prd_user/SIMBAHiveJDBC41.jar"
remote_server = "india.skoruz.com"
conn = jaydebeapi.connect("com.simba.hive.jdbc41.HS2Driver", "jdbc:hive2://" + remote_server + ":10000/copy",
                          {'user': "hive", 'password': ""}, remote_filepath)
curs = conn.cursor()
curs.execute('SELECT * FROM test_psql')
result = curs.fetchall()
print(result)
conn.close()