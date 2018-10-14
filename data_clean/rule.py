from hive_spark.hive_2df import *
class Clean_Rule():
    def _init(self,configs):
        '''

        row:一行行处理
        column:按列处理
        :param configs:
        '''
        list_ = []
        hive = Hive_2df(configs=configs)
        hive_df = hive.dataframes
        database = configs['database']
        for dataset, rules in configs.items():
            if dataset =='database':
                continue
            else:
                df = hive_df[dataset]# 数据库格式转到df,或者干脆不转,看到时候测试谁快
                for row_or_col,rule_list in rules.items():
                    if row_or_col=='row':
                        for record in df:# 一行行读取,rule是apply到行的
                            for rule in rule_list:
                                #print(rule)
                                #print(record)
                                record = rule(record)
                        list_.append(record)
                    elif row_or_col=='column':
                        continue



        return list_


    def license(self, record):
        # return 1:same,0:not same,进出口车牌，如四位相同即判定是同一张车牌 ,None:这个label不需要

        drop_license = ['浙A00000', '浙A11111', '浙A12345', '赣555555', '沪222222', '浙A000DD', '浙A000DD']

        if record['C_CARD_LICENSE'][0:7] in drop_license:
            return None
        if record['C_EX_LICENSE'][0:7] in drop_license:
            return None
        en_licnese = record['C_CARD_LICENSE'][2:7]
        ex_license = record['C_EX_LICENSE'][2:7]

        positions = ((0, 1, 2, 3), (1, 2, 3, 4), (0, 2, 3, 4), (0, 1, 3, 4), (0, 1, 2, 4))
        if en_licnese != ex_license:
            for pos in positions:
                same_pos = 0
                for index in pos:
                    if en_licnese[index] == ex_license[index]:
                        same_pos += 1
                if same_pos == 4:
                    # print(en_licnese,ex_license)
                    return 0, record['C_CARD_LICENSE'][0:10], record['C_EX_LICENSE'][0:10]

            return 1, record['C_CARD_LICENSE'][0:10], record['C_EX_LICENSE'][0:10]

        return 0, record['C_CARD_LICENSE'][0:10], record['C_EX_LICENSE'][0:10]

    def date(self,record):
        #print(record)
        pass


