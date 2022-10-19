
set spark.sql.shuffle.partitions=4;
--配置禁止精度损失
set spark.sql.decimalOperations.allowPrecisionLoss=false;

--第一大步骤：创建起始维度数据集，只有维度，没有指标
--1-计算核心复合主键，性别，缴费期间，投保年龄，保单年度
--1.1创建性别数据集
create or replace temporary view sex_table as
select stack(2, 'M', 'F') as sex;

--1.2创建缴费期间数据集
create or replace temporary view ppp_table as
select stack(4,10,15,20,30) as ppp;

-- select * from ppp_table;

--1.3生成一个序列，后期方便表示投保年龄
create or replace temporary view age_buy_table as
    select explode(sequence(18,60)) as age_buy;

select * from age_buy_table;

--1.4生成一个序列，后期方便表示保单年度
create or replace temporary view policy_year_table as
    select explode(sequence(0,88)) as policy_year;--因为最大保单年度是88年

select * from policy_year_table;

--1.5生成固定假设参数数据集
create or replace temporary view input as
select 106    t_age,           --满期年龄(terminate age)
       0.035  interest_rate,   --预定利息率(interest rate prem&rsv)
       0.055  interest_rate_cv,--现金价值预定利息率（interest rate cv）
       0.0004 acci_qx,--意外身故死亡发生率(accident_qx)
       0.115  rdr,--风险贴现率（risk discount rate)
       10000  sa,--基本保险金额(baisc sum assured)
       1      average_size,--平均规模(average size)
       1      mortratio_prem_0,--mort ratio(prem)
       1      mortratio_rsv_0,--mort ratio(rsv)
       1      mortratio_cv_0,--mort ratio(cv)
       1      ci_ratio,--ci ratio
       6      b_time1_b,--生存金给付时间(1)—begain
       59     b_time1_t,--生存金给付时间(1)-terminate
       0.1    b_ratio_1,--生存金给付比例(1)
       60     b_time2_b,--生存金给付时间(2)-begain
       106    b_time2_t,--生存金给付时间(2)-terminate
       0.1    b_ratio_2,--生存金给付比例(2)
       70     mb_time,--祝寿金给付时间
       0.2    mb_ration,--祝寿金给付比例
       0.7    rb_per,--可分配盈余分配给客户的比例
       0.7    tb_per,--未分配盈余分配给客户的比例
       1      disability_ratio,--残疾给付保险金保额倍数
       0.1    nursing_ratio,--长期护理保险金保额倍数
       75     nursing_age--长期护理保险金给付期满年龄
;


--将上面的4个数据集做笛卡尔积，作出所有的组合情况
create or replace temporary view prem_src0 as
    select a.age_buy,--投保年龄
       i.nursing_age,--长期护理保险金给付年龄
       s.sex,--性别
       i.t_age,--满期年龄
       p.ppp,--缴费期
       i.t_age - age_buy             as bpp,--保险期间、保障期间
       i.interest_rate,--银行预定利息率
       i.sa,--基本保险金额
       y.policy_year,--保单年度
       a.age_buy + y.policy_year - 1 as age--未来对应保单年度的年龄
from sex_table s
join ppp_table p on 1 = 1
join age_buy_table a on a.age_buy >= 18 and a.age_buy <= 70 - p.ppp --投保年龄必须大于18岁,且小于70岁-缴费时间
join policy_year_table y on y.policy_year >= 1 and y.policy_year <= 106 - a.age_buy --保单年度生效起为第一年,之后为106-投保年龄,最大为88年
join input i on 1 = 1;

select * from prem_src0;

--步骤1 计算是否在缴费期内ppp_、bpp_字段
create or replace temporary view prem_src1 as
    select *,
           `if`(policy_year<=ppp,1,0) as ppp_,--是否在缴费期间内 ppp_
           `if`(policy_year<=bpp,1,0) as bpp_--是否在保险期间内
from prem_src0;

--筛选数据，与Excel做比对
select * from prem_src1
         where age_buy = 18 and sex = 'M' and ppp=10 order by policy_year;
--步骤2 计算死亡率qx、kx、qx_ci字段
create or replace temporary view prem_src2 as
select p.*,
       case when p.age<=105 then if(p.sex='M',m.cl1,m.cl2)
            else 0
       end*i.mortratio_prem_0*bpp_ as qx,--死亡率
       case when p.age<=105 then if(p.sex='M',d.k_male,d.k_female)
            else 0
       end*bpp_ as kx,--残疾死亡占死亡的比例
       if(p.sex='M',d.male,d.female)*bpp_ as qx_ci --残疾率
 from prem_src1 as p
join insurance_ods.mort_10_13 as m on m.age=p.age
join insurance_ods.dd_table as d on d.age=p.age
join input i on 1=1;

--筛选出一部分数据，与Excel做比对
select age_buy,sex,ppp,policy_year,qx,kx,qx_ci from prem_src2 where age_buy=18 and sex='M' and ppp=10 order by policy_year;


--步骤3 计算qx_d字段
--由于decimal类型间做乘法或除法会精度损失，需要进行设置allowPrecisionLoss=false，见顶部
create or replace temporary view prem_src3 as
    select *,
               cast(case when age=105 then qx-qx_ci
                         else qx*(1-kx)
                     end*bpp_ as decimal(17,12)) as qx_d --扣除残疾的死亡率
from prem_src2;

--筛选数据，与Excel做比对
select age_buy,sex,ppp,policy_year,qx_d  from prem_src3 where age_buy = 18 and sex = 'M' and ppp=10 order by policy_year;

--步骤4;
--步骤4在python代码中计算，不在当前SQL文件中计算;
--步骤5;
--步骤5在python代码中计算，不在当前SQL文件中计算;

--步骤6 计算cx字段

-- select pow(2,3) as x;
select sqrt(16) as x;
create or replace temporary view prem_src6 as
select *,
       dx_d/pow(1+interest_rate,age+1) as cx
  from insurance_dw.prem_src5;
-- 与Excel中的局部数据做比对
select age_buy,
       sex,
       ppp,
       policy_year,cx
from prem_src6 where age_buy=18 and sex='M' and ppp=10 order by policy_year;
--步骤7 计算 cx_、ci_cx字段
create or replace temporary view prem_src7 as
select *,
       cx*pow(1+interest_rate,0.5) as cx_,
       dx_ci/pow(1+interest_rate,age+1) as ci_cx
 from prem_src6;
-- 与Excel中的局部数据做比对
select age_buy,
       sex,
       ppp,
       policy_year,cx_,ci_cx
from prem_src7 where age_buy=18 and sex='M' and ppp=10 order by policy_year;

--步骤8 计算ci_cx_、dx、dx_d_字段
create or replace temporary view prem_src8 as
select *,
       ci_cx*pow(1+interest_rate,0.5) as ci_cx_,
       lx/pow(1+interest_rate,age) as dx,
       lx_d/pow(1+interest_rate,age)  as dx_d_
 from prem_src7;
-- 与Excel中的局部数据做比对
select age_buy,
       sex,
       ppp,
       policy_year,
       ci_cx_,dx,dx_d_
from prem_src8 where age_buy=18 and sex='M' and ppp=10 order by policy_year;

--步骤9 计算附加费用率expense、db1、db2_factor字段
SELECT element_at(array('a', 'b', 'c'), 3) as x;
create or replace temporary  view prem_src9 as
select p.*,
       i.Nursing_Ratio, --先要,给第十步用
       case when p.policy_year=1 then r.r1
            when p.policy_year=2 then r.r2
            when p.policy_year=3 then r.r3
            when p.policy_year=4 then r.r4
            when p.policy_year=5 then r.r5
            when p.policy_year>=6 then r.r6_
       end*ppp_ as expense, --附加费用率
       --简化写法
       --element_at(array(r1,r2,r3,r4,r5,r6_) ,if(p.policy_year>=6,6,p.policy_year) )* ppp_ as expense2,
       i.Disability_Ratio*bpp_ as db1,--残疾给付
       if(age<p.Nursing_Age,1,0)*i.Nursing_Ratio as db2_factor
 from prem_src8 as p
join insurance_ods.pre_add_exp_ratio as r
on p.ppp=r.ppp
join input as i on 1=1;
-- 与Excel中的局部数据做比对
select age_buy,
       sex,
       ppp,
       policy_year,
       expense,db1,db2_factor
from prem_src9 where age_buy=18 and sex='M' and ppp=10 order by policy_year;
--步骤10 计算db2、db3、db4、db5字段
SELECT least(10, 9, 100, 4, 3) as x;
create or replace temporary view prem_src10 as
select *,
       sum(dx*db2_factor)over(partition by age_buy,sex,ppp order by policy_year rows between current row and unbounded following)/dx as db2,
       if(age>=Nursing_Age,1,0)*Nursing_Ratio as db3, --养老关爱金
       least(ppp,policy_year) as db4,
       (sum(dx*ppp_) over(partition by age_buy,sex,ppp order by policy_year rows between 1 following and unbounded following)
            /dx)*pow(1+interest_rate,0.5) as db5--豁免保费因子
 from prem_src9;
-- 与Excel中的局部数据做比对
select age_buy,
       sex,
       ppp,
       policy_year,
       db2,db3,db4,db5
from prem_src10 where age_buy=18 and sex='M' and ppp=10 order by policy_year;

--将结果插入prem_src表中
-- desc insurance_dw.prem_src;
-- 上一句用来拿取prem_src中的字段名,并且顺序也是对的;
insert overwrite table insurance_dw.prem_src
select age_buy
     , nursing_age
     , sex
     , t_age
     , ppp
     , bpp
     , interest_rate
     , sa
     , policy_year
     , age
     , qx
     , kx
     , qx_d
     , qx_ci
     , dx_d
     , dx_ci
     , lx
     , lx_d
     , cx
     , cx_
     , ci_cx
     , ci_cx_
     , dx
     , dx_d_
     , ppp_
     , bpp_
     , expense
     , db1
     , db2_factor
     , db2
     , db3
     , db4
     , db5
from prem_src10;




--步骤11 聚合计算9个中间参数
create or replace temporary view prem_std1 as
select age_buy,
       sex,
       ppp,
       bpp,
       sa,
       sum(t11_A) as T11,
       sum(v11_A) as V11,
       sum(w11_A) as W11,
       sum(q11_A) as Q11,
       sum(t9_A)  as T9,
       sum(v9_A)  as V9,
       sum(s11_A) as S11,
       sum(x11_A) as X11,
       sum(y11_A) as Y11
from (select age_buy,
             sex,
             ppp,
             bpp,
             sa,
             policy_year,
             if(policy_year = 1,
                0.5 * ci_cx_ * db1 * pow(1 + interest_rate, -0.25),
                ci_cx_ * db1)                                       as t11_A,
             if(policy_year = 1,
                0.5 * ci_cx_ * db2 * pow(1 + interest_rate, -0.25),
                ci_cx_ * db2)                                       as v11_A,
             dx * db3                                               as w11_A,
             dx * ppp_                                              as q11_A,
             if(policy_year = 1, 0.5 * ci_cx_ * pow(1 + interest_rate, 0.25), 0) as t9_A,
             if(policy_year = 1, 0.5 * ci_cx_ * pow(1 + interest_rate, 0.25), 0) as v9_A,
             dx * expense         as s11_A,
             cx_ * db4            as x11_A,
             ci_cx_ * db5         as y11_A
      from insurance_dw.prem_src) as t
group by age_buy, sex, ppp, sa, bpp;

select * from prem_std1 where age_buy=18 and sex='M' and ppp=10;

-- 步骤12 计算期交保费
create or replace temporary view prem_std2 as
select age_buy, sex, ppp,bpp,
       SA*(T11+V11+W11)/(Q11-T9-V9-S11-X11-Y11) as prem
from prem_std1 ;
select * from prem_std2 where age_buy=18 and sex='M' and ppp=10;

--对结果做比对
select a.age_buy, a.sex, a.ppp,
       a.prem as my_prem,
       b.prem as real_prem,
       abs(a.prem-b.prem) as diff_prem, --取绝对值验证
       abs(a.prem-b.prem)/b.prem as rate_prem--取相对偏移度验证
from prem_std2 a
join insurance_ods.prem_std_real b
on a.age_buy=b.age_buy
and a.sex=b.sex
and a.ppp=b.ppp;
--保存到结果表prem_std
insert overwrite table insurance_dw.prem_std
select * from prem_std2;
--到这里就完成了大半了,并且到这里进行第一轮上线操作,主要计算了保费因子表(prem_src),标准保费结果表(prem_std);

