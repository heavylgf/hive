
创建目录：
hadoop fs  -mkdir /hive;
hadoop fs  -mkdir /hive/halllogin;
hadoop fs  -mkdir /hive/roomlogin;
dfs -rm /hive/halllogin/;

####联合查询
select * from 
			(select userId from table_a where dt=20160731) a 
		join 
			(select userId from table_b where dt=20160731) b  
		on a.userId=b.userId 
		join 
			(select userId from table_c where dt=20160731) c 
		on a.userId=c.userId

注意：
/data/halllogin/20170110 文件用户是hdfs
/hive/halllogin 目录用户是root
直接可以再root用下 -cp 不用切换用户  cp完成之后文件的用户是root

hive> hadoop fs -cp /data/halllogin/ /hive/halllogin;
hive> hadoop fs -cp /data/roomlogin/ /hive/roomlogin;


Hive删除数据库
DROP DATABASE 语法如下：

DROP DATABASE StatementDROP (DATABASE|SCHEMA) [IF EXISTS] database_name 
[RESTRICT|CASCADE];
下面的查询用于删除数据库。假设要删除的数据库名称为userdb。

hive> DROP DATABASE IF EXISTS userdb;
以下是使用CASCADE查询删除数据库。这意味着要全部删除相应的表在删除数据库之前。

hive> DROP DATABASE IF EXISTS userdb CASCADE;

查看数据库：
show databases;

删除后查看hdfs文件，也已经删除
hdfs dfs -ls /hive/warehouse/

DROP DATABASE IF EXISTS userdb;
create database loginDB;
use loginDB;

这里其实创建的是一个托管表(内部表)
在使用drop表的时候，元数据和数据都会被一起删除
drop table if exists halllogin_detail;
create table halllogin_detail (
uid                   int,
date                  int,
time                  int,
ip                     string, 
group                  int,  
app                    int,
fromapp                int,
game                   int, 
channel                int,
promcode               int,
hardid                 string,    
area_province          string,
area_city              string,
area_district          string)  
PARTITIONED BY (
dt     string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA  INPATH '/hive/halllogin/20170101' overwrite  INTO TABLE halllogin_detail PARTITION (dt='20170101'); 
这个加载操作其实就是文件系统中的文件移动，所以执行速度很快，但是不检查表目录中的文件是否符合为表所声明的模式
如果有数据和模式不匹配，只有在查询的时候才知道。


外部表就不一样
创建外部表时，外部的数据需要在创建表的时候指明：

hive> create external table fz_external_table(id int,name string,age int,tel string)

    > ROW FORMAT DELIMITED

    > FIELDS TERMINATED BY ','

    > STORED AS TEXTFILE

    > location '/user/hive/external/fz_external_table';
建外部表时要加external关键字，我在建表时指定了location，当然也可以不指定，不指定就默认使用hive.metastore.warehouse.dir指定的路径。

可以看到，HDFS中数据文件在表被删除的情况下，还是存在的，也就是说删除外部表，只能删除表数据，并不能删除数据文件。
在使用external关键字之后，hive知道数据并不由自己管理，但是drop外部表的时候，hive是不会碰数据，而只会删除元数据。

内部表和外部表的区别：
创建外部表需要添加 external 字段。而内部表不需要。
删除外部表时，HDFS中的数据文件不会一起被删除。而删除内部表时，表数据及HDFS中的数据文件都会被删除。

  
动态分区设置参数：
先使用set查看参数： set hive.exec.dynamic.partition.mode
set hive.exec.dynamic.partition.mode=nonstrict


将hive表划分成桶:
clustered by子句来指定划分桶所用的列和要划分的桶的个数
create table explode(dummy string)
clustered by (id) into 4 buckets;

insert语句
select 语句中使用分区值来动态指明分区：
insert overwrite table target
partition (dt)
select col1, col2, dt
	from source;

多表插入：
from source
insert overwrite table tab1
	select col1, col2
insert overwrite table tab2
	select year, count(1)
insert overwrite table tab3
	select year, count(distinct station)
	where temp != 9999
		and (query = 0 or query = 1)
	group by year;

	
create table tab1
as 
select col1, col2
from source;
	
使用like关键字
create table new_table like tab1;


drop table if exists roomlogin_detail;
create table roomlogin_detail (
uid                   int,
date                  int,
time                  int,
ip                     string, 
group                  int,  
app                    int,
utype                  int,
game                   int, 
code                   string,
room                   int,
channel                int,
promcode               int,
hardid                 string,
area_province          string,
area_city              string,
area_district          string)  
PARTITIONED BY (
dt     string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA  INPATH '/hive/roomlogin/20170101' overwrite  INTO TABLE roomlogin_detail PARTITION (dt='20170101'); 

select count(*) from business.halllogin_log where dt>='20171001' and dt<'20171031'; --252626488
select count(*) from business.halllogin_log where dt>='20171001' and dt<='20171031'; --260796239
select count(*) from business.halllogin_log where dt='20171031'; --	8169751

####将hdfs上数据复制到本地文件
get命令
hadoop fs -get /user/hadoop/file localfile
hadoop fs -get hdfs://host:port/user/hadoop/file localfile
hadoop fs -get /hive/halllogin/20170114 20170114
hadoop fs -get /hive/halllogin/20170115 20170115

copyToLocal命令
除了限定目标路径是一个本地文件外，和get命令类似。
使用方法：hadoop fs -copyToLocal [-ignorecrc] [-crc] URI <localdst>

使用上边的hadoop fs -get url localurl 来拷贝大文件到本地的话很容易中途断掉。原因是：文件过大，拷贝时间过长，一旦shell客户端的网络状况不良则会拷贝中断。
（..+ &）将当前任务提交给后台，让后台去执行。从而使这个任务在本地执行，不停止/重启hadoop则不会拷贝中断。 
解决方法：在命令末尾加 & 符号。 
eg：hadoop fs -get /xxx/bb /yyy/aa &


简单的说，-put更宽松，可以把本地或者HDFS上的文件拷贝到HDFS中；而-copyFromLocal则更严格限制只能拷贝本地文件到HDFS中。
也就是说，如果本地和HDFS上都存在相同路径，则-put跟趋于优先取HDFS的源。

####  将本地目录下文件复制到HDFS文件夹下
copyFromLocal命令
记住先切换用户 su hdfs 
hadoop fs -copyFromLocal /opt/20170114 /hive/halllogin;

put命令   
使用方法：hadoop fs -put <localsrc> ... <dst>
从本地文件系统中复制单个或多个源路径到目标文件系统。也支持从标准输入中读取输入写入目标文件系统。
hadoop fs -put localfile /user/hadoop/hadoopfile
hadoop fs -put localfile1 localfile2 /user/hadoop/hadoopdir
hadoop fs -put localfile hdfs://host:port/hadoop/hadoopfile
hadoop fs -put - hdfs://host:port/hadoop/hadoopfile 

####  将数据从hive里导出到本地
insert overwrite local directory 'path' select q1;  
注意：不能使用insert into local directory 'path' select q1 这种方式
      和Hive数据导入不一样，不能使用INSERT INTO命令导出表中数据。

指定输出结果列之间的分隔符
insert overwrite local directory 'path'  row format delimited  fields terminated by '\t' select q1;
insert overwrite local directory '/opt/test'  row format delimited  fields terminated by ',' select uid,date,time,ip, group,code,room from roomlogin_detail WHERE app=1880077 AND `date`>=20170801;
将数据从hive里导出到hdfs  会产生很多小文件
insert overwrite directory 'hdfs_path' select * from dept;


####  hive本地机器执行hive -e导出数据到本地文件：得到的结果是用\t分割的。
http://blog.csdn.net/niityzu/article/details/42238483
hive -e "use logindb;select * from logindb.halllogin_detail where dt='20170114' limit 10" > /opt/bb;  
hive -e " use logindb;select uid, hardid,date,channel,group from logindb.halllogin_detail where app = 3001 and dt>='20170601' and dt<='20171120' " > /opt/app_3001; 

hive -f方式：
hive -f wyp.sql >> local/wyp2.txt
cat wyp.sql
select * from wyp

Hive修改表
Alter Table 语句

语法
声明接受任意属性，我们希望在一个表中修改以下语法。
ALTER TABLE name RENAME TO new_name
ALTER TABLE name ADD COLUMNS (col_spec[, col_spec ...])
ALTER TABLE name DROP [COLUMN] column_name
ALTER TABLE name CHANGE column_name new_name new_type
ALTER TABLE name REPLACE COLUMNS (col_spec[, col_spec ...])

Rename To… 语句
下面是查询重命名表，把 employee 修改为 emp。
hive> ALTER TABLE employee RENAME TO emp;

Change 语句
下面查询重命名使用上述数据的列名和列数据类型：
hive> ALTER TABLE employee CHANGE name ename String;
hive> ALTER TABLE employee CHANGE salary salary Double;

添加列语句
下面的查询增加了一个列名dept在employee表。
hive> ALTER TABLE employee ADD COLUMNS (dept STRING COMMENT 'Department name');

REPLACE语句
以下从employee表中查询删除的所有列，并使用emp替换列：
hive> ALTER TABLE employee REPLACE COLUMNS ( 
   > eid INT empid Int, 
   > ename STRING name String);




# 统计部署实例
select count(1),a.APP_ID from t_app_conf c, t_app a where c.VALID_FLAG = 1 and c.APP_ID = a.APP_ID group by a.APP_ID;

#统计应用名，应用负责人
select a.APP_NAME,u.login_name from t_app a LEFT JOIN s_sg_user u on a.APP_MANAGER = u.id;

#统计最新版本
select max(v.ver_num),a.app_id from t_app a ,t_app_version v where a.APP_ID = v.APP_ID group by a.APP_ID 
order by v.CREATE_DATE desc LIMIT 1;


####  Load：加载文件到hive表：
hive> LOAD DATA LOCAL INPATH '/opt/cloudy/data' INTO TABLE track_log PARTITION （这个字段代表分区表）(ds='2015-08-28'); 
      (ds='2015-08-28' 是指定字段的格式)   加载该目录下全部文件 
或者
hive> LOAD DATA LOCAL INPATH '/opt/cloudy/data/2015082818' INTO TABLE track_log PARTITION (ds='2015-08-28');
如上两个方式是append方式，overwrite 的方式如下：

1、针对本地文件
2015082818和2015082819文件上传到 /usr/local目录下
hive>load data local inpath '/usr/local/2015082818' overwrite into table track_log partition (ds='2015-08-28', hour = '18');
hive>load data local inpath '/usr/local/2015082819' overwrite into table track_log partition (ds='2015-08-28', hour = '19');

hive> LOAD DATA LOCAL INPATH '/opt/cloudy/data/2015082818' overwrite  INTO TABLE track_log PARTITION (ds='2015-08-28',hour='18');
hive> LOAD DATA LOCAL INPATH '/opt/cloudy/data/2015082819' overwrite  INTO TABLE track_log PARTITION (ds='2015-08-28',hour='19');

2、针对hdfs上文件：
hive> LOAD DATA  INPATH '/opt/cloudy/data/2015082818' overwrite  INTO TABLE track_log PARTITION (ds='2015-08-28',hour='18');
hive> LOAD DATA  INPATH '/opt/cloudy/data/2015082819' overwrite  INTO TABLE track_log PARTITION (ds='2015-08-28',hour='19');

su flume
hive -e "LOAD DATA  INPATH '/hive/roomlogin/20170114' overwrite  INTO TABLE roomlogin_detail PARTITION (dt='20170114')"


####  创建一个数据库，指定一个存储路径
create database rpt location '/user/hive/warehouse/rpt'; 
DROP DATABASE IF EXISTS tutorial_database;
use rpt;

DROP table IF EXISTS user;
create table user
(  id bigint,
   name string,
   age bigint
   ) partitioned by (ds string);

drop table if exists halllogin_log;
create table halllogin_log (
id                         string ,
url                        string ,
referer                    string ,
。。。。
buttonPosition             string)  
PARTITIONED BY (
 dt     string,
 hour   string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

show tables;

添加分区:
alter table track_log add partition (hour )
ALTER TABLE table_name ADD PARTITION (partCol = 'value1') location 'loc1';   //示例
ALTER TABLE table_name ADD PARTITION (dt = '2018-01-01'); 
ALTER TABLE table_name ADD IF NOT EXISTS PARTITION (dt='20130101') LOCATION '/user/hadoop/warehouse/table_name/dt=20130101'; //一次添加一个分区

ALTER TABLE page_view ADD PARTITION (dt='2008-08-08', country='us') location '/path/to/us/part080808' PARTITION (dt='2008-08-09', country='us') location '/path/to/us/part080809';  //一次添加多个分区

删除分区:
ALTER TABLE login DROP IF EXISTS PARTITION (dt='2008-08-08');
ALTER TABLE page_view DROP IF EXISTS PARTITION (dt='2008-08-08', country='us');

insert overwrite table t_table1 select * from t_table1 where 1=0;

INSERT OVERWRITE TABLE table PARTITION (partition_name = '')
SELECT ... FROM ...

修改某个分区中的数据
insert overwrite table silverlogdb.silverlog PARTITION (dt='20151120') 
select 
uid    ,
gameid ,
opid   ,
deposit ,
balance ,
ip  ,
date  ,
time  ,
desc  ,
optype ,
diff ,
guid ,
area_province ,
area_city ,
area_district ,
logsource 
from  silverlogdb.silverlog where dt='20151120' and logsource is not null;

修改分区:
ALTER TABLE table_name PARTITION (dt='2008-08-08') SET LOCATION "new location";
ALTER TABLE table_name PARTITION (dt='2008-08-08') RENAME TO PARTITION (dt='20080808');

添加列:
ALTER TABLE table_name ADD COLUMNS (col_name STRING);  //在所有存在的列后面，但是在分区列之前添加一列

修改列
CREATE TABLE test_change (a int, b int, c int);

// will change column a's name to a1
ALTER TABLE test_change CHANGE a a1 INT; 

// will change column a's name to a1, a's data type to string, and put it after column b. The new table's structure is: b int, a1 string, c int
ALTER TABLE test_change CHANGE a a1 STRING AFTER b; 

// will change column b's name to b1, and put it as the first column. The new table's structure is: b1 int, a string, c int
ALTER TABLE test_change CHANGE b b1 INT FIRST; 


修改表属性:
alter table table_name set TBLPROPERTIES ('EXTERNAL'='TRUE');   //内部表转外部表 
alter table table_name set TBLPROPERTIES ('EXTERNAL'='FALSE');  //外部表转内部表

表的重命名:
ALTER TABLE table_name RENAME TO new_table_name

查看分区语句：
hive> show partitions day_hour_table;


Insert有2种方式： 
  1、append  追加，方式基本不用，不安全
  2、overwrite  覆盖（先删后插入），强烈推荐，基本垄断
  
  
Load：加载文件到hive表：
hive> LOAD DATA LOCAL INPATH '/opt/cloudy/data' INTO TABLE track_log PARTITION （这个字段代表分区表）(ds='2015-08-28'); 
      (ds='2015-08-28' 是指定字段的格式)   加载该目录下全部文件 
或者
hive> LOAD DATA LOCAL INPATH '/opt/cloudy/data/2015082818' INTO TABLE track_log PARTITION (ds='2015-08-28');
如上两个方式是append方式，overwrite 的方式如下：

1、针对本地文件
2015082818和2015082819文件上传到 /usr/local目录下
load data local inpath '/usr/local/2015082818' overwrite into table track_log partition (ds='2015-08-28', hour = '18');
load data local inpath '/usr/local/2015082819' overwrite into table track_log partition (ds='2015-08-28', hour = '19');

hive> LOAD DATA LOCAL INPATH '/opt/cloudy/data/2015082818' overwrite  INTO TABLE track_log PARTITION (ds='2015-08-28',hour='18');
hive> LOAD DATA LOCAL INPATH '/opt/cloudy/data/2015082819' overwrite  INTO TABLE track_log PARTITION (ds='2015-08-28',hour='19');

2、针对hdfs上文件：
hive> LOAD DATA  INPATH '/opt/cloudy/data/2015082818' overwrite  INTO TABLE track_log PARTITION (ds='2015-08-28',hour='18');
hive> LOAD DATA  INPATH '/opt/cloudy/data/2015082819' overwrite  INTO TABLE track_log PARTITION (ds='2015-08-28',hour='19');


hive> select * from  track_log limit 10; 
hive> select url,ip from  track_log where hour='19' limit 10;
      select count(*) from track_log where ds='2015-08-28';
	  
hive> select count(*) from track_log;
hive> select count(*) from track_log where ds='2017-07-18' and hour='18';
hive> select count(*) from track_log where ds='2017-07-18' and hour='19';


把hive查询的数据落地到HDFS或hive其他表，如何做？
create database rpt location '/user/hive/warehouse/rpt';  创建一个数据库，指定一个存储路径

创建一个单分区表
create table visit_daily
(  pv bigint,
   uv bigint 
   ) partitioned by (dt string);
   
//将default.track_log表中查出的数据放到visit_daily表中
insert overwrite table    visit_daily partition (ds='2015-08-28')
select 
  count(url) pv ,
  count(distinct guid) uv
  from default.track_log where ds='2015-08-28';

//自己创建一个两级分区表
create table homework_daily   
(  pv bigint,
   uv bigint 
   ) partitioned by (ds string,hour string);
   
//将hour='18'的数据导入到homework_daily表中
insert overwrite table    homework_daily partition (ds='2015-08-28',hour='18')
select 
  count(url) pv ,
  count(distinct guid) uv
  from default.homework_log where ds='2015-08-28'and hour="18";
  
insert overwrite table homework_daily partition (ds='2017-06-20', hour='18')
select 
   count(url) pv,
   count(distinct guid) uv
   from track_log where ds='2015-08-28' and hour='18';
  
//将hour='19'的数据导入到homework_daily表中
insert overwrite table    homework_daily partition (ds='2015-08-28',hour='19')
select 
  count(url) pv ,
  count(distinct guid) uv
  from default.homework_log where ds='2015-08-28'and hour="19";
  
insert overwrite table homework_daily partition (ds='2017-06-20', hour='19')
select 
   count(url) pv,
   count(distinct guid) uv
   from track_log where ds='2015-08-28' and hour='19';
 
 
把hive查询的数据落地到HDFS或本地目录
//导入到HDFS
insert overwrite directory '/user/root/cc1'

导入到本地磁盘：注意要加上local   原先在/opt/cloudy/tmp目录下的文件要被全部覆盖
hive>insert overwrite local directory '/opt/cloudy/tmp'
select 
   count(url) pv,
   count(distinct guid) uv
   from track_log where ds='2015-08-28' and hour='19';

  
课题1：
select ds,hour,count(distinct guid),count(url) from track_log where ds='2015-08-28' 
  group by ds,hour;
    

create  table mangoerp.mangoerp_between_log
 ( pv bigint,
  uv bigint
 ) partitioned by(ds string , hour string);
 
 
 insert overwrite table mangoerp_between_log partition(ds='2015-08-28', hour='18')
  select 
  count(url) pv,
  count(distinct guid) uv
  from mangoerp.mangoerp_log 
  where ds='2015-08-28'and hour='18';
  
  insert overwrite table mangoerp_between_log partition(ds = '2015-08-28', hour = '19')
  select 
  count(url) pv,
  count(distinct guid) uv
  from mangoerp.mangoerp_log
  where ds='2015-08-28'and hour = '19';
  
  directory
  distinct
  
  
  insert overwrite directory '/user/hive/warehouse/mangoerp'
  select 
  count(url) pv,
  count(distinct guid) uv
  from mangoerp_log
  where ds='2015-08-28' and hour='18';
  
  
  insert overwrite local directory '/user/hive/warehouse/mangoerp'
  select 
  count(url) pv,
  count(distinct guid) uv
  from mangoerp_log
  where ds='2015-08-28' and hour='18';
  
  select 
  split(guid,'_')[0],year('2015-08-28'),month('2015-08-28'),day('2015-08-28')
  from mangoerp_log where ds='2015-08-28' and hour='18' limit 10;
    
  select guid, reverse(guid), length(guid),concat(guid,'-','zuijia','-',ip)
  
  
####hive OVER(PARTITION BY)函数用法
   开窗函数指定了分析函数工作的数据窗口大小，这个数据窗口大小可能会随着行的变化而变化，举例如下：
窗口函数之排序：
row_number() over(partition by order by) 顺序排列，如果列为1,1,2，打行号之后是1,2,3。
rank() over(partition by order by)跳跃排列，如果为1,1,2，打行号之后是1,1,3。
dense_rank() over(partition by order by) 合并排列，如果为1,1,2，打行号之后是1,1,2 
   
1：over后的写法：    
   over（order by salary） 按照salary排序进行累计，order by是个默认的开窗函数
   over（partition by deptno）按照部门分区
   over（partition by deptno order by salary）

drop table if exists t2;
create table t2 (
name                   string,
class                  int,
s                      int,
salary                 int, 
dept                   string)
PARTITIONED BY (
dt     string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA  INPATH '/user/root/20171121' overwrite  INTO TABLE t2 PARTITION (dt='20171121'); 

2：开窗的窗口范围：
over（order by salary range between 5 preceding and 5 following）：窗口范围为当前行数据幅度减5加5后的范围内的。

举例：

 

--sum(s)over(order by s range between 2 preceding and 2 following) 表示加2或2的范围内的求和
 select name,class,s, sum(s)over(order by s range between 2 preceding and 2 following) mm from t2
adf        3        45        45  --45加2减2即43到47，但是s在这个范围内只有45
asdf       3        55        55
cfe        2        74        74
3dd        3        78        158 --78在76到80范围内有78，80，求和得158
fda        1        80        158
gds        2        92        92
ffd        1        95        190
dss        1        95        190
ddd        3        99        198
gf         3        99        198

 
 
over（order by salary rows between 5 preceding and 5 following）：窗口范围为当前行前后各移动5行。
举例：

 

--sum(s)over(order by s rows between 2 preceding and 2 following)表示在上下两行之间的范围内
select name,class,s, sum(s)over(order by s rows between 2 preceding and 2 following) mm from t2
adf        3        45        174  （45+55+74=174）
asdf       3        55        252   （45+55+74+78=252）
cfe        2        74        332    （74+55+45+78+80=332）
3dd        3        78        379    （78+74+55+80+92=379）
fda        1        80        419
gds        2        92        440
ffd        1        95        461
dss        1        95        480
ddd        3        99        388
gf         3        99        293
 

 

over（order by salary range between unbounded preceding and unbounded following）或者
over（order by salary rows between unbounded preceding and unbounded following）：窗口不做限制
 

3、与over函数结合的几个函数介绍
row_number()over()、rank()over()和dense_rank()over()函数的使用
下面以班级成绩表t2来说明其应用

t2表信息如下：
cfe        2        74
dss        1        95
ffd        1        95
fda        1        80
gds        2        92
gf         3        99
ddd        3        99
adf        3        45
asdf       3        55
3dd        3        78

select * from                                                                      
    (                                                                           
    select name,class,s,rank()over(partition by class order by s desc) mm from t2
    )                                                                           
    where mm=1；
得到的结果是:
dss        1        95        1
ffd        1        95        1
gds        2        92        1
gf         3        99        1
ddd        3        99        1 

注意：
    1.在求第一名成绩的时候，不能用row_number()，因为如果同班有两个并列第一，row_number()只返回一个结果;
select * from                                                                      
    (                                                                           
    select name,class,s,row_number()over(partition by class order by s desc) mm from t2
    )                                                                           
    where mm=1；
1        95        1  --95有两名但是只显示一个
2        92        1
3        99        1 --99有两名但也只显示一个

    2.rank()和dense_rank()可以将所有的都查找出来：
如上可以看到采用rank可以将并列第一名的都查找出来；
     rank()和dense_rank()区别：
     --rank()是跳跃排序，有两个第二名时接下来就是第四名；
select name,class,s,rank()over(partition by class order by s desc) mm from t2
dss        1        95        1
ffd        1        95        1
fda        1        80        3 --直接就跳到了第三
gds        2        92        1
cfe        2        74        2
gf         3        99        1
ddd        3        99        1
3dd        3        78        3
asdf       3        55        4
adf        3        45        5
     --dense_rank()l是连续排序，有两个第二名时仍然跟着第三名
select name,class,s,dense_rank()over(partition by class order by s desc) mm from t2
dss        1        95        1
ffd        1        95        1
fda        1        80        2 --连续排序（仍为2）
gds        2        92        1
cfe        2        74        2
gf         3        99        1
ddd        3        99        1
3dd        3        78        2
asdf       3        55        3
adf        3        45        4

--sum()over（）的使用
select name,class,s, sum(s)over(partition by class order by s desc) mm from t2 --根据班级进行分数求和
dss        1        95        190  --由于两个95都是第一名，所以累加时是两个第一名的相加
ffd        1        95        190 
fda        1        80        270  --第一名加上第二名的
gds        2        92        92
cfe        2        74        166
gf         3        99        198
ddd        3        99        198
3dd        3        78        276
asdf       3        55        331
adf        3        45        376

first_value() over()和last_value() over()的使用  



--找出这三条电路每条电路的第一条记录类型和最后一条记录类型

SELECT opr_id,res_type,
       first_value(res_type) over(PARTITION BY opr_id ORDER BY res_type) low,
       last_value(res_type) over(PARTITION BY opr_id ORDER BY res_type rows BETWEEN unbounded preceding AND unbounded following) high
  FROM rm_circuit_route
WHERE opr_id IN ('000100190000000000021311','000100190000000000021355','000100190000000000021339')
 ORDER BY opr_id;
 

注：rows BETWEEN unbounded preceding AND unbounded following 的使用
--取last_value时不使用rows BETWEEN unbounded preceding AND unbounded following的结果

 

SELECT opr_id,res_type,
       first_value(res_type) over(PARTITION BY opr_id ORDER BY res_type) low,
       last_value(res_type) over(PARTITION BY opr_id ORDER BY res_type) high
  FROM rm_circuit_route
 WHERE opr_id IN ('000100190000000000021311','000100190000000000021355','000100190000000000021339')
 ORDER BY opr_id;
如下图可以看到，如果不使用

rows BETWEEN unbounded preceding AND unbounded following，取出的last_value由于与res_type进行进行排列，因此取出的电路的最后一行记录的类型就不是按照电路的范围提取了，而是以res_type为范围进行提取了。
 


 

在first_value和last_value中ignore nulls的使用
数据如下：
 

 

取出该电路的第一条记录，加上ignore nulls后，如果第一条是判断的那个字段是空的，则默认取下一条，结果如下所示：

 

--lag() over()函数用法（取出前n行数据）
lag(expresstion,<offset>,<default>)
with a as 
(select 1 id,'a' name from dual
 union
 select 2 id,'b' name from dual
 union
 select 3 id,'c' name from dual
 union
 select 4 id,'d' name from dual
 union
 select 5 id,'e' name from dual
) 
select id,name,lag(id,1,'')over(order by name) from a;

--lead() over()函数用法（取出后N行数据）
lead(expresstion,<offset>,<default>)
with a as 
(select 1 id,'a' name from dual
 union
 select 2 id,'b' name from dual
 union
 select 3 id,'c' name from dual
 union
 select 4 id,'d' name from dual
 union
 select 5 id,'e' name from dual
) 
select id,name,lead(id,1,'')over(order by name) from a;

--ratio_to_report(a)函数用法 Ratio_to_report() 括号中就是分子，over() 括号中就是分母
with a as (select 1 a from dual
           union all
select 1 a from dual
           union  all
select 1 a from dual
           union all
select 2 a from dual
           union all 
select 3 a from dual
           union all
select 4 a from dual
           union all
select 4 a from dual
           union all
select 5 a from dual
           )
select a, ratio_to_report(a)over(partition by a) b from a 
order by a; 

with a as (select 1 a from dual
           union all
select 1 a from dual
           union  all
select 1 a from dual
           union all
select 2 a from dual
           union all 
select 3 a from dual
           union all
select 4 a from dual
           union all
select 4 a from dual
           union all
select 5 a from dual
           )
select a, ratio_to_report(a)over() b from a --分母缺省就是整个占比
order by a; 

with a as (select 1 a from dual
           union all
select 1 a from dual
           union  all
select 1 a from dual
           union all
select 2 a from dual
           union all 
select 3 a from dual
           union all
select 4 a from dual
           union all
select 4 a from dual
           union all
select 5 a from dual
           )
select a, ratio_to_report(a)over() b from a
group by a order by a;--分组后的占比

 

percent_rank用法
计算方法：所在组排名序号-1除以该组所有的行数-1，如下所示自己计算的pr1与通过percent_rank函数得到的值是一样的：
SELECT a.deptno,
       a.ename,
       a.sal,
       a.r,
       b.n,
       (a.r-1)/(n-1) pr1,
       percent_rank() over(PARTITION BY a.deptno ORDER BY a.sal) pr2
  FROM (SELECT deptno,
               ename,
               sal,
               rank() over(PARTITION BY deptno ORDER BY sal) r --计算出在组中的排名序号
          FROM emp
         ORDER BY deptno, sal) a,
       (SELECT deptno, COUNT(1) n FROM emp GROUP BY deptno) b --按部门计算每个部门的所有成员数
 WHERE a.deptno = b.deptno;

 

cume_dist函数
计算方法：所在组排名序号除以该组所有的行数，但是如果存在并列情况，则需加上并列的个数-1，
          如下所示自己计算的pr1与通过percent_rank函数得到的值是一样的：
SELECT a.deptno,
       a.ename,
       a.sal,
       a.r,
       b.n,
       c.rn,
       (a.r + c.rn - 1) / n pr1,
       cume_dist() over(PARTITION BY a.deptno ORDER BY a.sal) pr2
  FROM (SELECT deptno,
               ename,
               sal,
               rank() over(PARTITION BY deptno ORDER BY sal) r
          FROM emp
         ORDER BY deptno, sal) a,
       (SELECT deptno, COUNT(1) n FROM emp GROUP BY deptno) b,
       (SELECT deptno, r, COUNT(1) rn,sal
          FROM (SELECT deptno,sal,
                       rank() over(PARTITION BY deptno ORDER BY sal) r
                  FROM emp)
         GROUP BY deptno, r,sal
         ORDER BY deptno) c --c表就是为了得到每个部门员工工资的一样的个数
 WHERE a.deptno = b.deptno
   AND a.deptno = c.deptno(+)
   AND a.sal = c.sal;
 
 
percentile_cont函数
含义：输入一个百分比（该百分比就是按照percent_rank函数计算的值），返回该百分比位置的平均值
如下，输入百分比为0.7，因为0.7介于0.6和0.8之间，因此返回的结果就是0.6对应的sal的1500加上0.8对应的sal的1600平均
SELECT ename,
       sal,
       deptno,
       percentile_cont(0.7) within GROUP(ORDER BY sal) over(PARTITION BY deptno) "Percentile_Cont",
       percent_rank() over(PARTITION BY deptno ORDER BY sal) "Percent_Rank"
  FROM emp
 WHERE deptno IN (30, 60);
 

若输入的百分比为0.6，则直接0.6对应的sal值，即1500
SELECT ename,
       sal,
       deptno,
       percentile_cont(0.6) within GROUP(ORDER BY sal) over(PARTITION BY deptno) "Percentile_Cont",
       percent_rank() over(PARTITION BY deptno ORDER BY sal) "Percent_Rank"
  FROM emp
 WHERE deptno IN (30, 60);
 
PERCENTILE_DISC函数
功能描述：返回一个与输入的分布百分比值相对应的数据值，分布百分比的计算方法见函数CUME_DIST，如果没有正好对应的数据值，就取大于该分布值的下一个值。
注意：本函数与PERCENTILE_CONT的区别在找不到对应的分布值时返回的替代值的计算方法不同

SAMPLE：下例中0.7的分布值在部门30中没有对应的Cume_Dist值，所以就取下一个分布值0.83333333所对应的SALARY来替代

SELECT ename,
       sal,
       deptno,
       percentile_disc(0.7) within GROUP(ORDER BY sal) over(PARTITION BY deptno) "Percentile_Disc",
       cume_dist() over(PARTITION BY deptno ORDER BY sal) "Cume_Dist"
  FROM emp
 WHERE deptno IN (30, 60);
 
 
 

SELECT t0.type,
       t0.day_id,
       count(distinct t0.m2) as dau_count,
       sum(if(t1.p_day_id - cast(t0.day_id as int) = 1,1,0)) as second,
       count(distinct t2.uid) as payment_count,
       sum(t2.amount_rmb) as pament,
       count(distinct if(t2.p_day_id - cast(t0.day_id as int) <= 6,t0.uid,null)) as seven_pay_count,
       sum(if(t2.p_day_id - cast(t0.day_id as int) <= 6,t2.amount_rmb,0)) as seven_payment
  FROM
(select t0.type,
        t0.day_id,
        t0.imei,
        t1.m2,
        t1.uid
   from
(select type,day_id,imei
   from mz_tmp_1213
 group by type,day_id,imei) t0
join
 ( SELECT t1.pday,
          t1.m2,
          a.join_list, 
          t1.uid
    FROM (select t.pday,
                  t.m2,
                  t.uid,
                  array(t.imei,t.m1) as join_array 
             from hive_qhsdk_logparse_huajiao t
             where pday>=20171204 and pday<=20171210
               and appkey like '34%'
               and t.m1<>'UNKNOWN' and t.imei<>'UNKNOWN' ) t1 lateral view explode(join_array) a as join_list) t1
on trim(t0.imei) = t1.join_list and t0.day_id = t1.pday
group by t0.type,t0.day_id,t0.imei,t1.m2,t1.uid) t0 join (select * from pre_qdas_acteqt where p_day_id >= 20171204 ) t1 on t0.m2 = t1.m2
left join (SELECT p_day_id,
             uid,
             sum(amount_rmb) as amount_rmb
        FROM (select p_day_id,
                     uid,
                     amount_rmb 
                from pre_qdas_huajiao_iap where p_day_id>=20171204
                union all
              select p_day_id,
                     uid,
                     amount_rmb 
                from pre_qdas_huajiao_charge 
               where p_day_id>=20171204
              ) t group by p_day_id,uid) t2 on t0.uid=t2.uid and t0.day_id=t2.p_day_id
group by t0.type,t0.day_id;



collect_set函数：
select uid, collect_set(game) game
	from user_actions
	group by uid;
输出结果：每个uid一行数据，其中game是一个去重的集合
        1752	[16]
		1774	[200,256,259,251]
		2014	[10]
		
collect_list函数：
select uid, collect_list(game) game
	from user_actions
	group by uid;
输出结果：每个uid一行数据，其中game是列表list
        1752	[16,16,18,18,18]
		1774	[200,256,256,259,251]
		2014	[10]

aort_array 对返回的是array的数据 结果进行排序：
select uid, sort_array(collect_list(game)) as contents
	from user_actions
	group by uid;
结果如下：
        1752	[16,16,18,18,18]
		1774	[200,251,256,256,259]
		2014	[10]

select ddd.uid uid,
	   ddd.game game
	from
	(select t1.uid uid,
			t1.game game
		from
		(select aa.uid uid,
				collect_set(game) game
			from logindb.roomlogin_detail aa 
			where aa.dt >= '20180101' and aa.dt <= '20180131' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
			group by uid
		) t1
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on t1.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
		from paydb.basic
		group by uidto 
		having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null
	limit 10;


  
  
  
  
  
  
  
  
    
