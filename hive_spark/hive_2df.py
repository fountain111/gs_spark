from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession


class Hive_2df():
    def __init__(self,configs):
        self.dataframes = {}
        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL Hive integration for clean data") \
            .enableHiveSupport() \
            .getOrCreate()
        database = configs['database']
        limit = 'limit 1000'
        for table in configs:
            if table == 'database':
                continue
            else:
                sql = "select * from {database}.{table} {limit}".format(database=database,table=table,limit=limit)
                print(sql)
                self.dataframes[table] = spark.sql(sql)

                #self.dataframes[table].show()
                print(self.dataframes[table])


def main():
    pass




if __name__ == '__main__':
    main()
    '''
    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)
    sqlContext = HiveContext(sc)
    my_dataframe = sqlContext.sql("Select  * from exit_entry_jour limit 100")
    print(my_dataframe)
    '''
    # sc_conf.setAppName("finance-similarity-app")
    # sc_conf.setMaster('spark://10.126.240.2:21066/')
    # sc_conf.setMaster("local[*]")
    # sc_conf.set('spark.executor.memory', '2g')
    # sc_conf.set('spark.executor.cores', '4')
    # sc_conf.set('spark.cores.max', '40')
    # sc_conf.set('spark.logConf', True)
    # print(sc_conf.getAll())
    # sc = SparkContext(conf=sc_conf)
    # sc = SparkContext(conf=conf)
    # sqlContext = HiveContext(sc)
    # my_dataframe = sqlContext.sql("Select  * from exit_entry_jour limit 100")
    # my_dataframe.show()
    # print('test')
    # print(my_dataframe)


