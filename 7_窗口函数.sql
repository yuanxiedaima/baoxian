use bigdata;
set hive.exec.mode.local.auto=true;
drop table emp;
create table emp(name string , month string, amt int);
insert overwrite table emp values ('张三', '01', 100),
       ('李四', '02', 120),
       ('王五', '03', 150),
       ('赵六', '04', 500),
       ('张三', '05', 400),
       ('李四', '06', 350),
       ('王五', '07', 180),
       ('赵六', '08', 400);
with t1 as (
    select name,
           sum(amt) as sum_amt
    from emp group by name
)
select *,
       row_number() over (order by sum_amt desc ) as rn,
       round(sum_amt*100/sum(sum_amt)over(),2)||'%' as rate
  from t1 order by rn;

drop table emp;
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
select *,
       sum(cnt)over(order by year /*rows between unbounded preceding and current row */) as total
from
(select year(hiredate) as year,
       count(empno) as cnt
from emp group by year(hiredate)) as t;