from hive_spark.hive_2df import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf




class Clean_Rule():

    def __init__(self):
        # angle_av = udf(lambda (x, y): -10 if x == 0 else math.atan2(y / x) * 180 / np.pi, DecimalType(20, 10))

        self.compare_license = udf (lambda x,y:0 if x == y else 1)

    def _init(self,configs):
        '''

        row:一行行处理
        column:按列处理
        :param configs:
        '''
        list_ = []
        hive = Hive_2df(configs=configs)
        database = configs['database']
        for dataset, rules in configs.items():
            if dataset =='database':
                continue
            else:
                for row_or_col,rule_list in rules.items():
                    if row_or_col=='row':
                        for rule in rule_list:
                            print('rule',rule)
                            df = hive.dataframes[dataset]
                            df.withColumn('same_licnese',self.compare_license(df.c_card_license,df.c_ex_license)).select('c_card_license','c_ex_license','same_licnese').show()

                    elif row_or_col=='column':
                        continue

        hive.spark.stop()

        return list_

    def test_print(self,x):
        print(x)


    def _compare_license(self, record):
        # 出入口车牌不同标记位,相同标记为1,不同标记为0

        # return 1:same,0:not same,进出口车牌，如四位相同即判定是同一张车牌 ,None:这个label不需要
        if record['c_card_license'] == record['c_ex_license']:
            return 1
        else:
            return 0
        return

    def date(self,record):
        #print(record)
        pass


