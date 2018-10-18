

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
        'database':'service_system_db',

        'axis_jour': {'row': [rule.date,], 'column': [rule.date,]},
        #月份
        #'date':{'20180601','20180631'}

        #行为或属性

        # 'B':rule.date

    }
    rule._init(configs=configs)

    pass

if __name__ == '__main__':
    #sc = SparkContext(appName="CollectFemaleInfo")?

    main()