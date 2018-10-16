from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf



class Hive_2df():
    def __init__(self,configs):
        self.dataframes = {}

        self.spark = SparkSession \
            .builder \
            .appName("Python Spark SQL Hive integration for clean data") \
            .enableHiveSupport() \
            .getOrCreate()
        #self.conf = SparkConf()
        #self.sc = SparkContext(conf=self.conf)

        database = configs['database']
        limit = 'limit 2'
        for table in configs:
            if table == 'database':
                continue
            else:
                sql = "select * from {database}.{table} {limit}".format(database=database,table=table,limit=limit)
                print(sql)
                self.dataframes[table] = self.spark.sql(sql)



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


