from hive_spark.hive_2df import *

import pyspark.sql.functions.udf

#compare =udf(lambda x,y: 0 if (x == y) else 1)

class Clean_Rule():
    def _init(self,configs):
        '''

        row:一行行处理
        column:按列处理
        :param configs:
        '''
        #angle_av = udf(lambda (x, y): -10 if x == 0 else math.atan2(y / x) * 180 / np.pi, DecimalType(20, 10))

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
                            print(rule)
                            #hive.dataframes[dataset].withColumn('same_licnese',self.compare_license).select('c_card_license','c_ex_license','same_licnese').

                    elif row_or_col=='column':
                        continue

        hive.spark.stop()

        return list_

    def test_print(self,x):
        print(x)

    def compare_license(self, record):
        # 出入口车牌不同标记位,相同标记为1,不同标记为0

        # return 1:same,0:not same,进出口车牌，如四位相同即判定是同一张车牌 ,None:这个label不需要
        list_ = []
        if record['c_card_license'] == record['c_ex_license']:
            return 1
        else:
            return 0
        return record

    def date(self,record):
        #print(record)
        pass


