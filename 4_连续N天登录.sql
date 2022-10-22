use bigdata;
set hive.exec.mode.local.auto=true;
create table game(name string,  `date` string);
insert overwrite table game values
('张三','2021-01-01'),
('张三','2021-01-02'),
('张三','2021-01-03'),
('张三','2021-01-02'),

('张三','2021-01-07'),
('张三','2021-01-08'),
('张三','2021-01-09'),

('李四','2021-01-01'),
('李四','2021-01-02'),
('王五','2021-01-03'),
('王五','2021-01-02'),
('王五','2021-01-02');

select * from game;
--用方案一【推荐】
with t1 as (
    select distinct * from game
),
    t2 as (
        select *,
               row_number() over (partition by name order by `date`) as rn
          from t1
    ),
    t3 as (
        select *,
               date_sub(`date`,rn) as date2
          from t2
    ),
    t4 as (
        select date2,name,
               count(1) as cnt
         from t3
         group by date2,name
    )
select distinct name from t4 where cnt>=3;
--方案二【了解】
with t1 as (
    select distinct * from game
),t2 as (
    select *,
           date_add(`date`,3-1) as dt2,
           lead(`date`,2)over(partition by name order by `date`) as dt3
     from t1
)select distinct name from t2 where dt2=dt3;

--广州银行
create table c_t
(
    card_nbr string,
    c_month  string,
    c_date   string,
    c_type   string,
    c_atm    decimal
);
insert overwrite table c_t values
                               (1,'2022-01','2022-01-01','网购',100),
                               (1,'2022-01','2022-01-02','网购',200),
                               (1,'2022-01','2022-01-03','网购',300),
                               (1,'2022-01','2022-01-15','网购',100),
                               (1,'2022-01','2022-01-16','网购',200),
                               (2,'2022-01','2022-01-06','网购',500),
                               (2,'2022-01','2022-01-07','网购',800),
                               (1,'2022-02','2022-02-01','网购',100),
                               (1,'2022-02','2022-02-02','网购',200),
                               (1,'2022-02','2022-02-03','网购',300),
                               (2,'2022-02','2022-02-06','网购',500),
                               (2,'2022-02','2022-02-07','网购',800);

select * from c_t order by card_nbr,c_date;
-- 卡号  月份   连续最大天数
-- 001   1        4
-- 001   2        3
-- 002   2        5
with t1 as (
    select distinct card_nbr,c_month,c_date from c_t
),
    t2 as (
        select *,
               date_sub(c_date,row_number() over (partition by card_nbr,c_month order by c_date)) as dt2
         from t1
    ),
    t3 as (
        select card_nbr,c_month,
               count(1) as cnt
        from t2
        group by dt2,card_nbr,c_month
    )
select card_nbr,c_month,max(cnt) as max_cnt from t3 group by card_nbr,c_month;


--脉脉
-- 表1 dau   记录了每日脉脉活跃用户的uid和不同模块的活跃时长
create table dau(d string, uid int, module string, active_duration int);
insert overwrite table dau
values ('2020-01-01', 1, 'jobs', 324),
       ('2020-01-01', 2, 'feeds', 445),
       ('2020-01-01', 3, 'im', 345),
       ('2020-01-02', 2, 'network', 765),
       ('2020-01-02', 3, 'jobs', 342);
select * from dau;
-- 在过去一个月内,曾连续两天活跃的用户
with t1 as (
    select distinct d,uid from dau where d between  date_sub(`current_date`(),29) and `current_date`()
),
    t2 as (
        select *,
               date_sub(d,row_number() over (partition by uid order by d)) as temp
         from t1
    ),
    t3 as (
        select temp,uid,count(1) cnt from t2 group by temp,uid having cnt>=2
    )
select distinct uid from t3;