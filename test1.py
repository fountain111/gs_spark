from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, when
import pyspark.sql.functions as func
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration for clean data") \
    .enableHiveSupport() \
    .getOrCreate()

database =  'service_system_db'
table = 'exit_jour'
limit = ' '
sql = "select * from {database}.{table} where n_ex_date < \
 20180631 and n_ex_date >20180601 {limit}".format(database=database, table=table, limit=limit)

new_column_1 = expr(
    """IF(c_ex_license != c_card_license, 1, 0)"""
)

new_license_color = expr(
    """ c_en_license+c_en_color"""
)

print(sql)
df = spark.sql(sql)
df.select
df = df.withColumn("same_license", new_column_1)

#df.select('same_license','c_card_license','c_ex_license').show()

df.groupBy("c_ex_license").agg(func.sum("same_license")).show()

spark.stop()