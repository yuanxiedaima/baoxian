use interview_db;
set hive.exec.mode.local.auto=true;
create table T1
(
    rec_no    int,
    ci_no     int,
    cust_type string,
    cre_dt    string,
    cus_sts   string
);
insert overwrite table T1 values
(123,111111,'01','2010-11-15','Y'),
(234,222222,'02','2011-09-01','N'),
(345,333333,'02','2012-01-09','Y'),
(456,444444,'01','2012-09-08','Y');
select * from T1;



create table T2
(
    ci_no     int,
    ac_no string,
    bal    decimal(7,2)
);
insert overwrite table T2 values
(222222,'123456789',1000.28),
(333333,'123454321',5000.00);

select * from T2;

--请编写sql统计在9月份开户且账户余额不为0的有效客户数。
select distinct T1.ci_no
from t1
join t2 on t1.ci_no=t2.ci_no
where month(cre_dt)=9
  and cus_sts='Y'
and bal!=0;
--方案二，join前预先减少2个表的数据量
select t1.ci_no
from (select * from t1 where month(cre_dt)=9 and cus_sts='Y') t1
join (select ci_no,sum(bal) sum_bal from t2 group by ci_no having sum_bal!=0) t2
on t1.ci_no=t2.ci_no;

