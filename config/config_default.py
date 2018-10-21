

#
from data_clean.rule import *


def main():

    rule = Clean_Rule()

    # KEY:表名,value:规则(处理的函数名)
    configs = {
        # 表名:rule name
        #  |axis_jour
        #  | entry_jour  |
        #  | exit_jour   |
        #  | wt_fare_tx
        {'database_name':'service_system_db'},
        {'table_name':'exit_entry_jour'},
        {'dada_range':('20180601','20180631')},
        {'rules':{'row':[rule.test_print,],'column':()}}
    }










    rule._init(configs=configs)

    pass

if __name__ == '__main__':
    #sc = SparkContext(appName="CollectFemaleInfo")?

    main()