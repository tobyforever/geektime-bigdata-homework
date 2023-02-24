# geektime-bigdata-homework

准备hive建表
```sql
DROP TABLE IF EXISTS `student`;

CREATE TABLE `student` (`s_id` varchar(20),`s_name` varchar(20),`s_birth` varchar(20),`s_sex` varchar(10)) row format delimited fields terminated by ',' lines terminated by '\n';

load data local inpath '/opt/install/data/student.csv' into table student;



DROP TABLE IF EXISTS `course`;
CREATE TABLE `course` (
  `c_id` varchar(20),
  `c_name` varchar(20),
  `t_id` varchar(20)
)
row format delimited
fields terminated by ','
lines terminated by '\n';

load data local inpath '/opt/install/data/course.csv' into table course;


DROP TABLE IF EXISTS `teacher`;
CREATE TABLE `teacher`(
`t_id` varchar(20),
`t_name` varchar(20)
)
row format delimited
fields terminated by ','
lines terminated by '\n';

load data local inpath '/opt/install/data/teacher.csv' into table teacher;


DROP TABLE IF EXISTS `score`;
CREATE TABLE `score`(
`s_id` varchar(20),
`c_id` varchar(20),
`s_score` int
)
row format delimited
fields terminated by ','
lines terminated by '\n';

load data local inpath '/opt/install/data/score.csv' into table score;

```

查询"01"课程比"02"课程成绩高的学生的信息及课程分数

```sql
select s.*,sc1.s_score,sc2.s_score from score sc1
join score sc2 on sc2.s_id=sc1.s_id
join student s on s.s_id=sc1.s_id
where sc1.c_id='01' and sc2.c_id = '02' and sc1.s_score > sc2.s_score;


+---------+-----------+-------------+----------+--------------+--------------+
| s.s_id  | s.s_name  |  s.s_birth  | s.s_sex  | sc1.s_score  | sc2.s_score  |
+---------+-----------+-------------+----------+--------------+--------------+
| 02      | 钱电        | 1990-12-21  | 男        | 70           | 60           |
| 04      | 李云        | 1990-08-06  | 男        | 50           | 30           |
| 09      | 张飞        | 1990-9-25   | 男        | 85           | 80           |
| 10      | 刘备        | 1990-01-25  | 男        | 80           | 56           |
+---------+-----------+-------------+----------+--------------+--------------+
4 rows selected (20.393 seconds)
```

查询"01"课程比"02"课程成绩低的学生的信息及课程分数  
```sql
select s.*,sc1.s_score,sc2.s_score from score sc1
join score sc2 on sc2.s_id=sc1.s_id
join student s on s.s_id=sc1.s_id
where sc1.c_id='01' and sc2.c_id = '02' and sc1.s_score < sc2.s_score;

+---------+-----------+-------------+----------+--------------+--------------+
| s.s_id  | s.s_name  |  s.s_birth  | s.s_sex  | sc1.s_score  | sc2.s_score  |
+---------+-----------+-------------+----------+--------------+--------------+
| 01      | 赵雷        | 1990-01-01  | 男        | 80           | 90           |
| 05      | 周梅        | 1991-12-01  | 女        | 76           | 87           |
+---------+-----------+-------------+----------+--------------+--------------+
2 rows selected (21.184 seconds)
```

查询平均成绩大于等于 60 分的同学的学生编号和学生姓名和平均成绩
```sql
select collect_set(s.s_id)[0],collect_set(s.s_name)[0], avg(sc.s_score) as avgs from score sc
join student s on s.s_id = sc.s_id
group by sc.s_id
having avgs >=60

+------+------+--------------------+
| _c0  | _c1  |        avgs        |
+------+------+--------------------+
| 01   | 赵雷   | 89.66666666666667  |
| 02   | 钱电   | 70.0               |
| 03   | 孙风   | 80.0               |
| 05   | 周梅   | 81.5               |
| 07   | 郑竹   | 93.5               |
| 09   | 张飞   | 88.0               |
| 10   | 刘备   | 64.0               |
| 11   | 关羽   | 90.0               |
+------+------+--------------------+
8 rows selected (49.341 seconds)
```
查询平均成绩小于 60 分的同学的学生编号和学生姓名和平均成绩 (包括有成绩的和无成绩的)
```sql
select collect_set(s.s_id)[0],collect_set(s.s_name)[0], avg(sc.s_score) as avgs from score sc
right join student s on s.s_id = sc.s_id
group by sc.s_id
having avgs <60 or avgs is null

+------+------+---------------------+
| _c0  | _c1  |        avgs         |
+------+------+---------------------+
| 08   | 王菊   | NULL                |
| 04   | 李云   | 33.333333333333336  |
| 06   | 吴兰   | 32.5                |
+------+------+---------------------+
3 rows selected (40.674 seconds)
```
查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
```sql
select collect_set(s.s_id)[0] as s_id,collect_set(s.s_name)[0] as s_name,count(sc.c_id) as coursecount,sum(sc.s_score) as totalscore
 from score sc
 right join student s on s.s_id = sc.s_id 
 group by sc.s_id


+-------+---------+--------------+-------------+
| s_id  | s_name  | coursecount  | totalscore  |
+-------+---------+--------------+-------------+
| 08    | 王菊      | 0            | NULL        |
| 01    | 赵雷      | 6            | 538         |
| 02    | 钱电      | 6            | 420         |
| 03    | 孙风      | 6            | 480         |
| 04    | 李云      | 6            | 200         |
| 05    | 周梅      | 4            | 326         |
| 06    | 吴兰      | 4            | 130         |
| 07    | 郑竹      | 4            | 374         |
| 09    | 张飞      | 6            | 528         |
| 10    | 刘备      | 8            | 512         |
| 11    | 关羽      | 2            | 180         |
+-------+---------+--------------+-------------+
11 rows selected (21.111 seconds)
```


查询"李"姓老师的数量
```sql
select count(t_id) from teacher
where t_name like '李%';

+------+
| _c0  |
+------+
| 1    |
+------+
1 row selected (12.733 seconds)
```
查询学过"张三"老师授课的同学的信息
```sql
select s.* from student s
join score sc on s.s_id =sc.s_id
join course c on c.c_id =sc.c_id
join teacher t on t.t_id = c.t_id
where t.t_name ='张三'

+---------+-----------+-------------+----------+
| s.s_id  | s.s_name  |  s.s_birth  | s.s_sex  |
+---------+-----------+-------------+----------+
| 01      | 赵雷        | 1990-01-01  | 男        |
| 02      | 钱电        | 1990-12-21  | 男        |
| 03      | 孙风        | 1990-05-20  | 男        |
| 04      | 李云        | 1990-08-06  | 男        |
| 05      | 周梅        | 1991-12-01  | 女        |
| 07      | 郑竹        | 1989-07-01  | 女        |
| 09      | 张飞        | 1990-9-25   | 男        |
| 10      | 刘备        | 1990-01-25  | 男        |
+---------+-----------+-------------+----------+
8 rows selected (62.422 seconds)
```
查询没学过"张三"老师授课的同学的信息
```sql
select s2.* from student s2
where s2.s_id not in (
select s.s_id from student s
join score sc on s.s_id =sc.s_id
join course c on c.c_id =sc.c_id
join teacher t on t.t_id = c.t_id
where t.t_name ='张三'
)

+----------+------------+-------------+-----------+
| s2.s_id  | s2.s_name  | s2.s_birth  | s2.s_sex  |
+----------+------------+-------------+-----------+
| 06       | 吴兰         | 1992-03-01  | 女         |
| 08       | 王菊         | 1990-01-20  | 女         |
| 11       | 关羽         | 1990-01-25  | 男         |
+----------+------------+-------------+-----------+
3 rows selected (85.905 seconds)
```
查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
```sql
select s.* from student s
join score sc1 on sc1.s_id=s.s_id
join score sc2 on sc2.s_id=s.s_id
where sc1.c_id='01' and sc2.c_id='02'

+---------+-----------+-------------+----------+
| s.s_id  | s.s_name  |  s.s_birth  | s.s_sex  |
+---------+-----------+-------------+----------+
| 01      | 赵雷        | 1990-01-01  | 男        |
| 02      | 钱电        | 1990-12-21  | 男        |
| 03      | 孙风        | 1990-05-20  | 男        |
| 04      | 李云        | 1990-08-06  | 男        |
| 05      | 周梅        | 1991-12-01  | 女        |
| 09      | 张飞        | 1990-9-25   | 男        |
| 10      | 刘备        | 1990-01-25  | 男        |
+---------+-----------+-------------+----------+
7 rows selected (26.77 seconds)
```
查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
```sql
select s.* from student s
join score sc1 on sc1.s_id=s.s_id
where sc1.c_id='01' and not exists(
select sc2.s_id from score sc2 where sc2.s_id=s.s_id and sc2.c_id = '02'
)

+---------+-----------+-------------+----------+
| s.s_id  | s.s_name  |  s.s_birth  | s.s_sex  |
+---------+-----------+-------------+----------+
| 06      | 吴兰        | 1992-03-01  | 女        |
+---------+-----------+-------------+----------+
1 row selected (64.862 seconds)
```

查询没有学全所有课程的同学的信息
```sql
select count(sc.c_id) as coursecount,collect_set(s.s_name)[0],collect_set(s.s_birth)[0],collect_set(s.s_sex)[0]  from score sc
join student s on sc.s_id=s.s_id
group by sc.s_id 
having coursecount < 4

+--------------+------+-------------+------+
| coursecount  | _c1  |     _c2     | _c3  |
+--------------+------+-------------+------+
| 3            | 赵雷   | 1990-01-01  | 男    |
| 3            | 钱电   | 1990-12-21  | 男    |
| 3            | 孙风   | 1990-05-20  | 男    |
| 3            | 李云   | 1990-08-06  | 男    |
| 2            | 周梅   | 1991-12-01  | 女    |
| 2            | 吴兰   | 1992-03-01  | 女    |
| 2            | 郑竹   | 1989-07-01  | 女    |
| 3            | 张飞   | 1990-9-25   | 男    |
| 1            | 关羽   | 1990-01-25  | 男    |
+--------------+------+-------------+------+
9 rows selected (22.397 seconds)
```

查询至少有一门课与学号为"01"的同学所学相同的同学的信息
```sql
select distinct s.* from score sc2 
join student s on s.s_id = sc2.s_id
where sc2.s_id != '01'
and sc2.c_id in (
select sc.c_id from score sc
where sc.s_id='01'
);

+---------+-----------+-------------+----------+
| s.s_id  | s.s_name  |  s.s_birth  | s.s_sex  |
+---------+-----------+-------------+----------+
| 02      | 钱电        | 1990-12-21  | 男        |
| 03      | 孙风        | 1990-05-20  | 男        |
| 04      | 李云        | 1990-08-06  | 男        |
| 05      | 周梅        | 1991-12-01  | 女        |
| 06      | 吴兰        | 1992-03-01  | 女        |
| 07      | 郑竹        | 1989-07-01  | 女        |
| 09      | 张飞        | 1990-9-25   | 男        |
| 10      | 刘备        | 1990-01-25  | 男        |
+---------+-----------+-------------+----------+
8 rows selected (20.583 seconds)
```
查询和"01"号的同学学习的课程完全相同的其他同学的信息
```sql
select * from student s
where s.s_id in (
    select sc.s_id from score sc
    where sc.s_id !='01'
    group by sc.s_id
    having count(sc.s_id)=(select count(c_id) from score sc where s_id='01')
    and s_id not in (select distinct s_id from score where c_id not in (
        select c_id from score where s_id ='01'
    ))
)

+---------+-----------+-------------+----------+
| s.s_id  | s.s_name  |  s.s_birth  | s.s_sex  |
+---------+-----------+-------------+----------+
| 02      | 钱电        | 1990-12-21  | 男        |
| 03      | 孙风        | 1990-05-20  | 男        |
| 04      | 李云        | 1990-08-06  | 男        |
+---------+-----------+-------------+----------+
3 rows selected (568.65 seconds)
```
查询没学过"张三"老师讲授的任一门课程的学生姓名
```sql
select s1.s_name from student s1
except
(
select s.s_name from score sc
join student s on s.s_id = sc.s_id
join course c on c.c_id = sc.c_id
join teacher t on t.t_id = c.t_id
where t.t_name='张三')

+-------------+
| _u1.s_name  |
+-------------+
| 关羽          |
| 吴兰          |
| 王菊          |
+-------------+
3 rows selected (115.02 seconds)
```
查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
```sql
select sc2.s_id as s_id, collect_set(s.s_name)[0] as name,avg(sc2.s_score) as avgscore from score sc2 
join student s on s.s_id = sc2.s_id
where s.s_id in (
select sc.s_id as failcount from score sc
where sc.s_score<60
group by sc.s_id
having failcount >=2
)
group by sc2.s_id

+-------+-------+---------------------+
| s_id  | name  |      avgscore       |
+-------+-------+---------------------+
| 04    | 李云    | 33.333333333333336  |
| 06    | 吴兰    | 32.5                |
| 10    | 刘备    | 64.0                |
+-------+-------+---------------------+
3 rows selected (71.891 seconds)
```
