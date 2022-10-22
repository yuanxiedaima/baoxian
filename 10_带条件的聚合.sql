use interview_db;
set hive.exec.mode.local.auto=true;
create table sale_order(
    order_id bigint comment '订单ID',
    user_id bigint comment '用户ID',
    order_status int ,
    create_time string,
    last_update_time string,
    product_id bigint,
    product_num bigint
);
create table user_info(
    user_id bigint comment '用户ID,唯一主键',
    sex string,
    age int
);
--问题：用一条SQL生成完整的用户画像表，包含如下字段：
--user_id,  sex,  age,  d7order_num,   d14_order_num，后面两个字段分别为近7天订单数量，近14天订单数量。
--1传统的方法
select a.user_id,
       a.sex,
       a.age,
       a.d7order_num ,
       b.d14_order_num
from
(select u.user_id,
       u.sex,
       u.age,
       count(o.order_id) d7order_num  --近7天订单数量
from user_info u
left join sale_order o on u.user_id=o.user_id
where o.create_time between `7天前` and `今天`
group by u.user_id,u.sex,u.age) as a
right join
(select u.user_id,
       u.sex,
       u.age,
       count(o.order_id) d14_order_num  --近14天订单数量
from user_info u
left join sale_order o on u.user_id=o.user_id
where o.create_time between `14天前` and `今天`
group by u.user_id,u.sex,u.age) as b
on a.user_id=b.user_id
;
--优化方案二
select u.user_id,
       u.sex,
       u.age,
       count(if(o.create_time between `7天前` and `今天`,o.order_id,null)) d7order_num,  --近7天订单数量
       count(if(o.create_time between `10天前` and `今天`,o.order_id,null)) xx, --近10天订单数量
       count(if(o.create_time between `12天前` and `今天`,o.order_id,null)), --近12天订单数量
       count(if(o.create_time between `7天前` and `今天` and order_status='4',o.order_id,null)),--近7天订单,且是已完成的数量
       count(if(o.create_time between `14天前` and `今天`,o.order_id,null)) d14_order_num  --近14天订单数量
from user_info u
left join sale_order o on u.user_id=o.user_id
where o.create_time between `14天前` and `今天`
group by u.user_id,u.sex,u.age;

--优化方案三
select * from user_info;
select * from sale_order;
select u.user_id,
       u.sex,
       u.age,
       o.d7order_num,
       o.d14_order_num
from user_info u
left join (select user_id,
                  count(if(create_time between '7天前' and '今天',order_id,null)) d7order_num,  --近7天订单数量
                  count(if(create_time between '14天前' and '今天',order_id,null)) d14_order_num  --近14天订单数量
             from sale_order
            where create_time between '14天前' and '今天'
            group by user_id
            ) o on u.user_id=o.user_id