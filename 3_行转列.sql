use bigdata;
set hive.exec.mode.local.auto=true;
create table table2(year int,month int ,amount double) ;
 insert overwrite table table2 values
           (1991,1,1.1),
           (1991,2,1.2),
           (1991,3,1.3),
           (1991,4,1.4),
           (1992,1,2.1),
           (1992,2,2.2),
           (1992,3,2.3),
           (1992,4,2.4);
select * from table2;

--group by + sum(if语句)
select year,
       sum(if(month=1,amount,0)) as m1,
       sum(if(month=2,amount,0)) as m2,
       sum(if(month=3,amount,0)) as m3,
       sum(if(month=4,amount,0)) as m4
from table2
group by year;
--上面的一句可以拆成2句
select year,
       sum(a1) as m1,
       sum(a2) as m2,
       sum(a3) as m3,
       sum(a4) as m4
from
(select *,
       if(month=1,amount,0) as a1, --伪列
       if(month=2,amount,0) as a2, --伪列
       if(month=3,amount,0) as a3, --伪列
       if(month=4,amount,0) as a4 --伪列
  from table2) as t
group by year;
--上面2句简写为1句
select year,
       sum(if(month=1,amount,0)) as m1,
       sum(if(month=2,amount,0)) as m2,
       sum(if(month=3,amount,0)) as m3,
       sum(if(month=4,amount,0)) as m4
from table2
group by year;
--腾讯游戏
create table table1(DDate string, shengfu string) ;
insert overwrite table table1 values
       ('2015-05-09', "胜"),
       ('2015-05-09', "胜"),
       ('2015-05-09', "负"),
       ('2015-05-09', "负"),
       ('2015-05-10', "胜"),
       ('2015-05-10', "负"),
       ('2015-05-10', "负");
select * from table1;

select DDate,
       sum(if(shengfu='胜',1,0)) as `胜`,
       sum(if(shengfu='负',1,0)) as `负`
from table1
group by DDate;
--也可以用count来处理
select DDate,
       count(if(shengfu='胜',1,null)) as `胜`
from table1
group by DDate;

select DDate,
       count(x) as `胜`,
       count(y) as `负`
from
(select *,
        if(shengfu='胜',1,null) as x,
        if(shengfu='负',1,null) as y
 from table1) as t
group by DDate;

--华泰证券2
create table student(sid int, sname string, gender string, class_id int);
insert overwrite table student
values (1, '张三', '女', 1),
       (2, '李四', '女', 1),
       (3, '王五', '男', 2);

select * from student;

create table  course (cid int, cname string, teacher_id int);
insert overwrite table course
values (1, '生物', 1),
       (2, '体育', 1),
       (3, '物理', 2);
select * from course;

create table score (sid int, student_id int, course_id int, number int);
insert overwrite table score
values (1, 1, 1, 90),
       (2, 1, 2, 68),
       (3, 2, 2, 89);
select * from score;


--查询课程编号“2”的成绩比课程编号“1”低的所有同学的学号、姓名。
select st.*
from (select student_id,
             sum(if(course_id = 1, number, 0)) as `生物分`,
             sum(if(course_id = 2, number, 0)) as `体育分`
      from score sc
      group by student_id
      having `生物分` > `体育分`) as t
join student st on t.student_id = st.sid

--腾讯QQ
create table tableA(qq string, game string)
insert overwrite table tableA values
       (10000, 'a'),
       (10000, 'b'),
       (10000, 'c'),
       (20000, 'c'),
       (20000, 'd');

create table tableB(qq string, game string) ;
insert overwrite table tableB values
(10000, 'a_b_c'),
(20000, 'c_d');

SELECT * from tableA;
SELECT * from tableB;
--行转列
select qq,
       concat_ws('_',collect_list(game)) as games
from tableA
group by qq;
--列转行,hive需要配合侧视图
select b.qq,
       temp.game1
  from tableB as b
lateral view explode(split(game,'_')) temp  as game1
;






