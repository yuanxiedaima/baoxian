show databases ;
use test_db;
show tables;

--是否自动转换为mapjoin
set hive.auto.convert.join = true;
--因为上面是false，则另3个参数失效，所以无需设置另3个参数
--下面只有2个表，其中大表90M，小表7M低于25M 。
--下面的执行计划会看到【Reduce Operator Tree】，不会触发map join机制
--关闭mapjoin耗时1分43秒
--开启mapjoin耗时1分11秒
explain select * from bigtable a
left join smalltable1 b on a.sid=b.sid



--下面的执行计划，有3个表，其中大表90M，b表7M低于25M，c表7M也低于25M，
--7+7=14M，合起来<25M,也就是'合起来足够小'，所以仍然会看到【map join operator】
explain select * from bigtable a
left join smalltable1 b on a.sid=b.sid
left join smalltable2 c on a.sid=c.sid;

--- 操作3
 --是否自动转换为mapjoin
set hive.auto.convert.join = true;
--小表的最大文件大小，默认为25000000，即25M，现手动改成10M
set hive.mapjoin.smalltable.filesize = 10000000;

--下面的执行计划，有3个表，其中大表90M，b表7M低于10M，c表7M也低于10M，
--7+7=14M，合起来>10M，也就是'合起来不够小',所以会看到【reduce join operator】,不会触发map join
explain select * from bigtable a
left join smalltable1 b on a.sid=b.sid
left join smalltable2 c on a.sid=c.sid;

--操作四
--是否自动转换为mapjoin
set hive.auto.convert.join = true;
--小表的最大文件大小，默认为25000000，即25M
set hive.mapjoin.smalltable.filesize = 25000000;
--是否将多个mapjoin合并为一个
set hive.auto.convert.join.noconditionaltask = true;
--多个mapjoin转换为1个时，所有小表的文件大小总和的最大值。
set hive.auto.convert.join.noconditionaltask.size = 10000000;

--下面的执行计划，有3个表，其中大表90M，b表7M低于25M，c表7M也低于25M，
--7+7=14M，合起来虽然<25M，但是>10M，会触发map join，但是会有多个stage阶段。
--耗时1分2秒
explain select * from bigtable a
left join smalltable1 b on a.sid=b.sid
left join smalltable2 c on a.sid=c.sid;

--操作五
--是否自动转换为mapjoin
set hive.auto.convert.join = true;
--小表的最大文件大小，默认为25000000，即25M
set hive.mapjoin.smalltable.filesize = 25000000;
--是否将多个mapjoin合并为一个
set hive.auto.convert.join.noconditionaltask = true;
--多个mapjoin转换为1个时，所有小表的文件大小总和的最大值。
set hive.auto.convert.join.noconditionaltask.size = 20000000;

--下面的执行计划，有3个表，其中大表90M，b表7M低于25M，c表7M也低于25M，
--7+7=14M，合起来既<25M，又<20M（故意手动提升10M到20M），不仅会触发map join，又会将多个stage阶段合并成1个stage。
--耗时1分1秒
explain select * from bigtable a
left join smalltable1 b on a.sid=b.sid
left join smalltable2 c on a.sid=c.sid;

select concat('null',rand()) as id


