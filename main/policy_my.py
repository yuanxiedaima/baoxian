# -*- coding:utf-8 -*-
# Desc:This is Code Desc


from numpy import double
from pyspark.sql import SparkSession, Row
import pandas as pd
import os
from pyspark.sql.functions import pandas_udf
import pandas as pd
from decimal import Decimal

os.environ['SPARK_HOME'] = '/export/server/spark'
PYSPARK_PYTHON = "/root/anaconda3/bin/python3.8"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

# 步骤4的计算lx
def calc_lx(pdf: pd.DataFrame):
    # 此pdf的记录行有可能乱序，所以手动按policy_year排序
    pdf.sort_values('policy_year', inplace=True, ignore_index=True)
    for i in range(0, len(pdf)):
        # i==0表示policy_year=第一年
        if (i == 0):
            pdf.loc[[i], ['lx']] = 1
        # i不等于0，表示policy_year是第二年及以后
        else:
            # 对lx保留12位小数，否则每轮小数位会暴增。
            pdf.loc[[i], ['lx']] = (pdf.loc[i - 1]['lx'] * (1 - pdf.loc[i - 1]['qx'])).quantize(
                Decimal('0.000000000000'))
    return pdf

# 步骤4优化版本
def calc_lx_fast(pdf:pd.DataFrame) -> pd.DataFrame:
    # 此pdf的记录行有可能乱序，所以手动按policy_year排序
    pdf.sort_values('policy_year', inplace=True, ignore_index=True)
    temp_list=[] #暂存每一轮的lx有效保单数
    temp_qx=0    #将当前的qx死亡率,暂存,给下轮用
    temp_lx=0    #将当前的lx有效保单数,暂存,给下轮用
    for index,row in pdf.iterrows():
        # i==0表示policy_year=第一年
        if(index==0):
            temp_list.append(1)
            temp_lx=1
            temp_qx=row['qx']
        # i不等于0，表示policy_year是第二年及以后
        else:
            temp_lx=(temp_lx*(1-temp_qx)).quantize(Decimal('0.000000000000'))
            temp_list.append(temp_lx)
            temp_qx=row['qx']
    pdf['lx']=temp_list
    return pdf



#计算步骤五的3个字段-速度慢但是好理解
def calc_lx_d_dx_d_dx_ci(pdf: pd.DataFrame):
    # 对DataFrame的policy_year进行排序
    pdf.sort_values(by='policy_year', ignore_index=True, inplace=True)
    for i in range(0, len(pdf)):
        if (i == 0):
            pdf.loc[[i],['lx_d']] =1
            pdf.loc[[i],['dx_d']] =pdf.loc[i]['qx_d']
            pdf.loc[[i],['dx_ci']]=pdf.loc[i]['qx_ci']
        else:
            lx_d=pdf.loc[i - 1]['lx_d'] - pdf.loc[i - 1]['dx_d'] - pdf.loc[i - 1]['dx_ci']
            pdf.loc[[i], ['lx_d']] =lx_d
            # # quantize对结果保留12位小数，否则每轮小数位会暴增。
            pdf.loc[[i], ['dx_d']] =(lx_d*pdf.loc[i]['qx_d']).quantize(Decimal('0.000000000000'))
            pdf.loc[[i], ['dx_ci']] =(lx_d*pdf.loc[i]['qx_ci']).quantize(Decimal('0.000000000000'))
    return pdf


#计算步骤五的3个字段-提速版

def calc_lx_d_dx_d_dx_ci_fast(pdf: pd.DataFrame):
    # 对DataFrame的policy_year进行排序
    pdf.sort_values(by='policy_year', ignore_index=True, inplace=True)
    temp_list_lx_d=[]
    temp_list_dx_d=[]
    temp_list_dx_ci=[]
    temp_lx_d=0#暂存当年的lx_d，给下轮用
    temp_dx_d=0#暂存当年的dx_d，给下轮用
    temp_dx_ci=0#暂存当年的dx_ci，给下轮用
    for index,row in pdf.iterrows():
        if (index == 0):
            temp_lx_d=1
            temp_dx_d=row['qx_d']
            temp_dx_ci=row['qx_ci']
            temp_list_lx_d.append(1)
            temp_list_dx_d.append(temp_dx_d)
            temp_list_dx_ci.append(temp_dx_ci)
        else:
            temp_lx_d=temp_lx_d-temp_dx_d-temp_dx_ci
            temp_list_lx_d.append(temp_lx_d)
            # quantize对结果保留12位小数，否则每轮小数位会暴增。
            temp_dx_d=(temp_lx_d*row['qx_d']).quantize(Decimal('0.000000000000'))
            temp_list_dx_d.append(temp_dx_d)
            temp_dx_ci=(temp_lx_d*row['qx_ci']).quantize(Decimal('0.000000000000'))
            temp_list_dx_ci.append(temp_dx_ci)
    pdf['lx_d']=temp_list_lx_d
    pdf['dx_d']=temp_list_dx_d
    pdf['dx_ci']=temp_list_dx_ci

    return pdf


def executeSQLFile(filename):
    # 第一步 加载文件
    with open('../resources/'+filename) as f:
        read_data = f.read()
    arr = read_data.split(';')  # 用 ; 去切割SQL文本
    for sql in arr:
        # 先打印出来看看
        print(sql, ';')

        # 第二步 执行SQL语句,但是因为对SQL文本用 ; 进行切割,要清洗掉干扰字符串

        # 如果sql文本字符串中，既有注释又有有效的SQL语句，那么送给spark.sql("...")混合可以运行。
        # 但是如果sql文本字符串内容只有注释却没有有效的SQL语句，那么执行送给spark.sql("--...")会报错。
        # 将SQL语句的注释的行，过滤掉。
        sql_list = sql.splitlines()  # splitlines:将字符串按换行切割
        # 清洗掉全空的字符串和开头为 -- 也就是注释,但是,我们不考虑多行注释哈
        filtered = filter(lambda line: len(line.strip()) > 0 and \
                                       not line.lstrip().startswith('--'), sql_list)

        list2 = list(filtered)
        if (len(list2) > 0):
            # 核心语句
            df = spark.sql(sql)  # 直接传sql就行,因为带着注释的SQL语句,spark.sql也能跑



            if (list2[0].lstrip().startswith('select')):
                # 是查询语句,就show一下
                df.show()

        if(sql.strip()=='--步骤4'):
            # 先从SQL转换到python
            df1=spark.sql('''select *,cast(0 as decimal(17,12)) as lx from prem_src3''')  #lx是我们要算的字段,再prem_src3中还没有这个字段,所以自己加

            # todo
            df2=df1.groupby('age_buy','sex','ppp').applyInPandas(calc_lx_fast,schema=df1.schema)

            #再从python回归到SQL
            df2.createOrReplaceTempView('prem_src4')
            # 显示一下,用来检验
            # spark.sql("select * from prem_src4 where age_buy=18 and sex='M' and ppp=10 order by policy_year").show()

        if (sql.strip() == '--步骤5'):
            # 先从SQL转换到python
            df1 = spark.sql('''select *,
                                            cast(0 as decimal(17, 12)) as lx_d, 
                                            cast(0 as decimal(17, 12)) as dx_d, 
                                            cast(0 as decimal(17, 12)) as dx_ci
                                       from prem_src4''')
            # todo 下面的函数calc_lx_d_dx_d_dx_ci有慢速版和快速版，替换函数名即可
            df2 = df1.groupby('age_buy', 'sex', 'ppp').applyInPandas(calc_lx_d_dx_d_dx_ci_fast, schema=df1.schema)
            # 再从python回归到SQL
            df2.createOrReplaceTempView('prem_src5')
            # # 将df2的表数据写到hive磁盘，因为prem_src5只是一个临时视图,只在python里有,所以我要把它存到磁盘中,真真正正落地到hive中去
            df2.write.mode('overwrite').saveAsTable('insurance_dw.prem_src5')

            spark.sql("""select age_buy,sex,ppp,policy_year,lx_d,dx_d,dx_ci
                                 from insurance_dw.prem_src5 where age_buy=18 and sex='M' and ppp=10 order by policy_year""").show()


if __name__ == '__main__':
#     第0步,创建spark对象
    spark=SparkSession.builder\
        .appName('test')\
        .master('local[*]') \
        .config('hive.metastore.uris','thrift://node1:9083') \
        .config('spark.sql.warehouse.dir','/user/hive/warehouse') \
        .enableHiveSupport()\
        .getOrCreate()
       # 两个config和enableHiveSupport 这三个参数是spark对象和hive进行集成需要配置的

#     第一步,读取文件---->抽取成函数,这样就可以复用了
    executeSQLFile('3_prem_my.sql')
    # executeSQLFile('4_prem_my.sql')





