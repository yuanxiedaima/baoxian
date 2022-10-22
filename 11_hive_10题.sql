show databases ;
create database if not exists test_sql;
use test_sql;
-- 一些语句会走 MapReduce，所以慢。 可以开启本地化执行的优化。
set hive.exec.mode.local.auto=true;-- (默认为false)
--第1题：访问量统计
CREATE TABLE test_sql.test1 (
		userId string,
		visitDate string,
		visitCount INT )
	ROW format delimited FIELDS TERMINATED BY "\t";

INSERT overwrite TABLE test_sql.test1
VALUES
    ( 'u01', '2017/1/21', 5 ),
    ( 'u02', '2017/1/23', 6 ),
    ( 'u03', '2017/1/22', 8 ),
    ( 'u04', '2017/1/20', 3 ),
    ( 'u01', '2017/1/23', 6 ),
    ( 'u01', '2017/2/21', 8 ),
    ( 'u02', '2017/1/23', 6 ),
    ( 'u01', '2017/2/22', 4 );

select * from test1;
select *,
       sum(cnt)over(partition by userId order by month) as total
 from
(select userId,
       date_format(replace(visitDate,'/','-') ,'yyyy-MM') as month,
       sum(visitCount) as cnt
  from test1
group by userId,date_format(replace(visitDate,'/','-') ,'yyyy-MM')) as t
;

-- 第2题：电商场景TopK统计
CREATE TABLE test_sql.test2 (
						 user_id string,
						 shop string )
			ROW format delimited FIELDS TERMINATED BY '\t';
INSERT INTO TABLE test_sql.test2 VALUES
( 'u1', 'a' ),
( 'u2', 'b' ),
( 'u1', 'b' ),
( 'u1', 'a' ),
( 'u3', 'c' ),
( 'u4', 'b' ),
( 'u1', 'a' ),
( 'u2', 'c' ),
( 'u5', 'b' ),
( 'u4', 'b' ),
( 'u6', 'c' ),
( 'u2', 'c' ),
( 'u1', 'b' ),
( 'u2', 'a' ),
( 'u2', 'a' ),
( 'u3', 'a' ),
( 'u5', 'a' ),
( 'u5', 'a' ),
( 'u5', 'a' );
--（1）每个店铺的UV（访客数）
select * from test2;
-- UV和PV
-- PV是访问当前网站所有的次数
-- UV是访问当前网站的客户数(需要去重)
--方案一
select shop,count(distinct user_id) uv from test2 group by shop;
--方案二，效率会高，因为t子查询会提前去重减少数据量
select shop,
       count(user_id) cnt
 from
(select shop,user_id from test2 group by shop,user_id) as t
group by shop;
--(2)每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
with t1 as (select shop,
                   user_id,
                   count(1) cnt
            from test2
            group by shop, user_id),
    t2 as (
        select *,
               row_number() over (partition by shop order by cnt desc) as rn
          from t1
    )
select * from t2 where rn<=3;

-- 第3题：订单量统计
CREATE TABLE test_sql.test3 (
			dt string,
			order_id string,
			user_id string,
			amount DECIMAL ( 10, 2 ) )
ROW format delimited FIELDS TERMINATED BY '\t';

INSERT overwrite TABLE test_sql.test3 VALUES
 ('2017-01-01','10029028','王五',33.57),
 ('2017-01-01','10029029','王五',33.57),
 ('2017-01-01','100290288','田七',33.57),
 ('2017-02-02','10029088','王五',33.57),
 ('2017-02-02','100290281','王五',33.57),
 ('2017-02-02','100290282','赵六',33.57),
 ('2017-11-03','100290888','赵六',55.58),
 ('2017-11-02','10290282','张三',234),
 ('2017-11-08','10290432','小明',239),
 ('2018-11-02','10290284','李四',234);

select * from test_sql.test3;
-- 	(1)给出 2017年每个月的订单数、用户数、总成交金额。
select date_format(dt,'yyyy-MM') as month,
       count(distinct order_id) as cnt_orders,
       count(distinct user_id) as cnt_users,
       sum(amount) as sum_amount
    from test3
group by date_format(dt,'yyyy-MM');
-- 	(2)给出2017年11月的新客数(指在11月才有第一笔订单)
select count(user_id) cnt from
(select *,
        row_number() over (partition by user_id order by dt) as rn
 from test3 where year(dt)<=2017) t
where rn=1 and month(dt)=11;
--写法二
select count(1)
from
(select user_id, min(date_format(dt, 'yyyy-MM')) min_month
from test3
where year(dt) <= 2017
group by user_id
having min_month = '2017-11') t



