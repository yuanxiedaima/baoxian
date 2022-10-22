create table table1(id int,name varchar(20) ,age int);
insert into table1 values
(1,'zs',20),
(2,'ls',21),
(3,'ww',30);

create table table2(id int,name varchar(20) ,age int);
insert into table2 values
(1,'zs',20),
(2,'小明',21),
(3,'小红',30);


#下面可知，先执行on，再执行where
select *
from table1 a
left join table2 b
on a.id=b.id
where a.name=b.name;

-- 下面的条数可能会比上面多。
select *
from table1 a
left join table2 b
on a.id=b.id
and a.name=b.name;


-- 下面用inner join 各得到结果，结果是一样的
select *
from table1 a
join table2 b
on a.id=b.id
where a.name=b.name;

select *
from table1 a
join table2 b
on a.id=b.id
and a.name=b.name;