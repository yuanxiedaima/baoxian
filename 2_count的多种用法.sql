-- count(*),
-- count(1),
-- count(cid),
-- count(name),
-- count(distinct cid),
-- count(distinct name)的区别

create or replace temporary view test2(cid,name) as values
(1,'zs'),
(1,'ls'),
(1,'ww'),
(1,null),
(2,'aa'),
(2,'bb'),
(2,'cc'),
(2,'cc'),
(2,null);

select * from test2;

select count(*) a,--8
       count(1) b,--8
       count(cid) c,--8
       count(name) d,--6
       count(distinct cid) e,--2
       count(distinct name) f,--6
       count(distinct cid,name) g --6
from test2;

 select distinct concat(cid,'_',name) as x from test2