import jaydebeapi

remote_server = "india.skoruz.com"
remote_filepath = r"F:\Internship\Skoruz\HDFS\Simba_Hive_JDBC\SIMBAHiveJDBC41.jar"

conn = jaydebeapi.connect("com.simba.hive.jdbc41.HS2Driver", "jdbc:hive2://" + remote_server + ":10000/copy",
                         {'user': "hive", 'password': ""}, remote_filepath)
cur = conn.cursor()
cur.execute("SELECT * FROM test_psql")
result = cur.fetchall()
print(result)
conn.close()