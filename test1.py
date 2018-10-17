from pyspark.sql import SparkSession
#from pyspark import SparkContext, SparkConf
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration for clean data") \
    .enableHiveSupport() \
    .getOrCreate()

database =  'service_system_db'
table = 'exit_jour'
limit = ' '
sql = "select * from {database}.{table} where n_ex_date <20180631 and n_ex_date >20180601 {limit}".format(database=database, table=table, limit=limit)
print(sql)
spark.sql(sql).show()
spark.stop()