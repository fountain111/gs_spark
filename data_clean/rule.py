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

        list_ = []
        hive = Hive_2df(configs=configs)
        df = hive.dataframes['table_name']

        for row_or_col in configs['rules'].itmes():
            if row_or_col=='row':
                pass
            elif:
                row_or_col=='column':
                pass

            for rule in configs['rules'][row_or_col]:
                rule(df)




        hive.spark.stop()

        return list_

    def test_print(self,x):
        print(x)

    def _license_color(self,df)

    def compare_license(self, en = 'c_en_license',ex='c_ex_license'):
        # 出入口车牌不同标记位,相同标记为1,不同标记为0

        # return 1:same,0:not same,进出口车牌，如四位相同即判定是同一张车牌 ,None:这个label不需要
        if df['c_card_license'] == record['c_ex_license']:
            return 1
        else:
            return 0
        return column

    def date(self,record):
        #print(record)
        pass


