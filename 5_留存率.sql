use bigdata;
set hive.exec.mode.local.auto=true;
create table if not exists tb_cuid_1d
(
    cuid         string comment '用户的唯一标识',
    os           string comment '平台',
    soft_version string comment '版本',
    event_day    string comment '日期',
    timestamp1    int comment '用户访问时间戳',
    duration     decimal comment '用户访问时长',
    ext          array<string> comment '扩展字段'
);
insert overwrite table tb_cuid_1d values
 (1,'android',1,'2020-04-01',1234567,100,`array`('')),
 (1,'android',1,'2020-04-02',1234567,100,`array`('')),
 (1,'android',1,'2020-04-08',1234567,100,`array`('')),
 (2,'android',1,'2020-04-01',1234567,100,`array`('')),
 (3,'android',1,'2020-04-02',1234567,100,`array`(''));

 select * from  tb_cuid_1d;
--写出用户表 tb_cuid_1d的 20200401 的次日、次7日留存的具体HQL ：
--一条sql统计出以下指标 （4.1号uv，4.1号在4.2号的留存uv，4.1号在4.8号的留存uv）;
--方案1，逻辑简单，但是效率低下
select count(a.cuid) as uv1,
       count(b.cuid) as uv2,
       count(c.cuid) as uv8
    from
(select distinct cuid from tb_cuid_1d where event_day='2020-04-01') as a
left join (select distinct cuid from tb_cuid_1d where event_day='2020-04-02') as b on a.cuid=b.cuid
left join (select distinct cuid from tb_cuid_1d where event_day='2020-04-08') as c on a.cuid=c.cuid;

--方案二，代码稍微复杂，但是效率高
select count(cuid) uv1,--4月1号UV
       count(if(cnt2>0,1,null)) uv2,--4月2号的留存UV
       count(if(cnt8>0,1,null)) uv8--4月8号的留存UV
 from
(select cuid,
       count(if(event_day='2020-04-01',1,null)) as cnt1, --4月1号登录情况
       count(if(event_day='2020-04-02',1,null)) as cnt2, --4月2号登录情况
       count(if(event_day='2020-04-08',1,null)) as cnt8 --4月8号登录情况
 from tb_cuid_1d where event_day in ('2020-04-01','2020-04-02','2020-04-08')
group by cuid
having cnt1>0) as t;




--腾讯视频号游戏直播
drop table if exists tableA;
create table tableA
(ds string comment '(日期)'  ,device string,user_id string,is_active int) ;
insert overwrite table  tableA values
('2020-03-01','ios','0001',0),
('2020-03-01','ios','0002',1),
('2020-03-01','ios','0004',1),
('2020-03-01','android','0003',1),
('2020-03-02','ios','0001',0),
('2020-03-02','ios','0002',0),
('2020-03-02','android','0003',1),
('2020-03-02','ios','0005',1) ,
('2020-03-02','ios','0004',1) ;

select * from tableA;
-- 20200301的ios设备用户活跃的次日留存率是多少？
select count(a.user_id) cnt1,
       count(b.user_id) cnt2,
       count(b.user_id)/count(a.user_id) as rate
from
(select user_id from tableA where ds='2020-03-01' and device='ios' and is_active=1) as a
left join (select user_id from tableA where ds='2020-03-02' and device='ios' and is_active=1) b on a.user_id=b.user_id

