use bigdata;
set hive.exec.mode.local.auto=true;
create table emp(empno string ,ename string,hiredate string,sal int ,deptno string);
insert overwrite table emp values
('7521', 'WARD', '1981-2-22', 1250, 30),
('7566', 'JONES', '1981-4-2', 2975, 20),
('7876', 'ADAMS', '1987-7-13', 1100, 20),
('7369', 'SMITH', '1980-12-17', 800, 20),
('7934', 'MILLER', '1982-1-23', 1300, 10),
('7844', 'TURNER', '1981-9-8', 1500, 30),
('7782', 'CLARK', '1981-6-9', 2450, 10),
('7839', 'KING', '1981-11-17', 5000, 10),
('7902', 'FORD', '1981-12-3', 3000, 20),
('7499', 'ALLEN', '1981-2-20', 1600, 30),
('7654', 'MARTIN', '1981-9-28', 1250, 30),
('7900', 'JAMES', '1981-12-3', 950, 30),
('7788', 'SCOTT', '1987-7-13', 3000, 20),
('7698', 'BLAKE', '1981-5-1', 2850, 30);

select * from emp;
--求出每个部门工资最高的前三名员工，并计算这些员工的工资占所属部门总工资的百分比。
select *,
       round(sal/all_sal,2) as rate
  from
(select *,
       row_number() over (partition by deptno order by sal desc) as rn,
       sum(sal) over(partition by deptno) as all_sal--计算每个部门的总薪资
from emp ) as t
where rn<=3
;
--小米电商
create table t_order (user_id string,
                      order_id string,
                      ctime string,
                      city_id string,
                      sale_num int ,
                      sku_id string) ;
with t1 as (
    select ctime,city_id,
           count(distinct order_id) as cnt
      from t_order
      group by ctime,city_id
),
    t2 as (
        select *,
               row_number() over (partition by ctime order by cnt desc) as rn
         from t1
    )
select * from t2 where rn<=10
;
