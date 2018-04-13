select uidto, 
        month(concat(substr(paydate,1,4),'-',substr(paydate,5,2),'-',substr(paydate,7,2))) paydate
    from basic
    where paydate >= '20170101' 
    group by uidto, paydate;
每个用户有充值的月份


SELECT l1.uid,
    l1.app
FROM halllogin_log l1
WHERE l1.fromapp = 3001
  AND l1.app > 0
  AND l1.app != 3001
  AND l1.uid IN
    (SELECT DISTINCT l2.uid
    FROM halllogin_log l2
    WHERE l2.app > 7000000)
GROUP BY l1.uid,
    l1.app
	
11月11日-11月12日
《王者传奇》7000066
《传奇来了》7000082
《大天使H5》7000074
《杨过小龙女》7000081
select uid, date, time from halllogin_detail where dt>='20171111' and dt<='20171112' and app=7000066 ; 
select uid, date, time from halllogin_detail where dt>='20171111' and dt<='20171112' and app=7000082 ; 
select uid, date, time from halllogin_detail where dt>='20171111' and dt<='20171112' and app=7000074 ; 
select uid, date, time from halllogin_detail where dt>='20171111' and dt<='20171112' and app=7000081 ; 


提前预约的数据，
  11月15日-11月16日登陆《欢乐修仙》游戏用户的用户ID。 7000084
  11月16日-11月17日登陆《仙灵觉醒》游戏用户的用户ID。 7000083

select distinct(uid), date, time from halllogin_detail where dt>='20171115' and dt<='20171116' and app=7000084 ; 

select distinct(uid), date, time from halllogin_detail where dt>='20171116' and dt<='20171117' and app=7000083 ; 


10月的数据
area_city='0910'  10月
select distinct(uid) from logindb.halllogin_detail where dt>='20171001' and dt<='20171031' and area_city='0910'; 


上饶麻将，游戏ID：80
广丰五十K，游戏ID：521
PC和移动分开
10.02-11.01登陆过的用户ID去重导出
游戏也帮我分开

pc端 大厅登录 游戏规律
group > 0 and group not in (6,66,8,88,68,69,55)
移动端 大厅登录 游戏规律
group in (6,66,8,88)

移动端：单包（6,8）+移动平台（tcyapp,66,88）
其他：68,69,55

游戏ID规律
game < 10000  appid = 188+
game = 10000 appid = 3001  代表同城游app
game > 7000000 appid = game

移动平台登录  group in (66,88) 
group in (6,8)代表纯粹的移动游戏   6代表安卓   8代表ios

pc端大厅   根据游戏ID查询用户登录情况
select distinct(uid) from roomlogin_detail WHERE dt>='20171002' and dt<='20171101' and group > 0 and group not in (6,66,8,88,68,69,55) and game=80;
select distinct(uid) from roomlogin_detail WHERE dt>='20171002' and dt<='20171101' and group > 0 and group not in (6,66,8,88,68,69,55) and game=521;

移动端大厅   根据游戏ID查询用户登录情况
select distinct(uid) from halllogin_detail WHERE dt>='20171002' and dt<='20171101' and group in (6,66,8,88) and game=80;
select distinct(uid) from halllogin_detail WHERE dt>='20171002' and dt<='20171101' and group in (6,66,8,88) and game=521;
hive -e "use logindb;select distinct(uid) from logindb.halllogin_detail where dt>='20171002' and dt<='20171101' and app='80' " > /opt/app_80;  
hive -e "use logindb;select distinct(uid) from logindb.halllogin_detail where dt>='20171002' and dt<='20171101' and app='521' " > /opt/app_521; 


halllogin中  date  uid   hardid  channel group  where app = 3001   6月开始 到现在
select uid, hardid,date,channel,group from halllogin_detail where app = 3001 and dt>='20170601' and dt<='20171120' ; --567932786
hive -e " use logindb;select uid, hardid,date,channel,group from logindb.halllogin_detail where app = 3001 and dt>='20170601' and dt<='20171120' " > /opt/app_3001; 


注意：
移动端  只要用户登录了大厅就代表登录了游戏
PC端    用户登录了大厅不一定登录了游戏    要用户登录了房间才代表登录了游戏

所以查询登陆pc大厅的用户  直接到halllogin表中就可以

最近2个月 pc端最近一次登录用户信息
09.22~11.22日登陆用户ID及最后一次登陆时间。
登陆pc大厅的用户
select uid, max(date) from halllogin_detail WHERE dt>='20171022' and dt<='20171122' and group > 0 and group not in (6,66,8,88,68,69,55) group by uid;

导入到HDFS
insert overwrite directory '/user/root/' row format delimited  fields terminated by ',' 
    select uid, max(date) from halllogin_detail WHERE dt>='20171022' and dt<='20171122' and group > 0 and group not in (6,66,8,88,68,69,55) group by uid;

hive -e " use logindb;select uid, max(date) from halllogin_detail WHERE dt>='20171022' and dt<='20171122' and group > 0 and group not in (6,66,8,88,68,69,55) group by uid " > /opt/20171124; 


查询设备号码：日期范围：7月19日之后  移动端的 
imei：
860474030307108
861538010256508
select * from halllogin_detail where hardid='860474030307108' and dt>='20170719';
select * from halllogin_detail where hardid='861538010256508' and dt>='20170719';


halllogin中  11月20- 11月26 移动端大厅登录 group in (6,66,8,88)
uid   date  app   group   去重   导出     
select uid,date,app,group from halllogin_detail where and dt>='20171120' and dt<='20171126';
// 通过4个字段去重
select uid,date,app,group from halllogin_detail where dt>='20171120' and dt<='20171126' and group in (6,66,8,88) group by uid,date,app,group;
// 通过两个字段去重
select uid,date,collect_list(app),collect_list(group) from halllogin_detail where dt='20171126' group BY uid,date;
select uid,date,collect_set(app),collect_set(group) from halllogin_detail group BY uid,date;


这次能不能给我们导出10月28号和12月4号的总登录和新增数据。我们上次拿一周的数据看了下扩散，只能看到7天的。
之后的每周只要7天的。
hive -e " use logindb; select uid,date,app,group from logindb.halllogin_detail where dt>='20171028' and dt<='20171204' and group in (6,66,8,88) group by uid,date,app,group " > /opt/20171204; 

12月4日——12月10日 移动端大厅登录的用户
hive -e " use logindb; select uid,date,app,group from logindb.halllogin_detail where dt>='20171204' and dt<='20171210' and group in (6,66,8,88) group by uid,date,app,group " > /opt/20171211; 


还请协助导出：
在给的渠道号中，先拉取出来从6月15日起注册的用户序号。
然后看这些用户从注册日起的登录游戏的情况，记录下游戏id和名称

创建用户登录游戏时间表，将数据存入其中
drop table if exists uid_detail;
create table uid_detail (
uid                   int,
date                  int)  
PARTITIONED BY (
dt     string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';  // 指定分隔符是空格
LOAD DATA  INPATH '/user/root/uid01' overwrite  INTO TABLE uid_detail PARTITION (dt='20171201');

SELECT ha.uid,
    ha.app
FROM halllogin_detail ha 
where ha.dt>='20170615'
  and ha.uid in
    (SELECT DISTINCT ud.uid
    FROM uid_detail ud
    WHERE ud.dt='20171201')
GROUP BY ha.uid,
    ha.app;
    
hive -e " use logindb; SELECT ha.uid,
    ha.app
FROM halllogin_detail ha 
where ha.dt>='20170615'
  and ha.uid in
    (SELECT DISTINCT ud.uid
    FROM uid_detail ud
    WHERE ud.dt='20171201')
GROUP BY ha.uid,
    ha.app; " > /opt/uid_app_20171201; 
LOAD DATA  INPATH '/hive/halllogin/20170101' overwrite  INTO TABLE uid_detail PARTITION (dt='20170101'); 		 

09.01~10.31pc平台大厅登录，用户id分组之后最后一次登录时间 		 
select uid, max(date) from halllogin_detail WHERE dt>='20170901' and dt<='20171031' and group > 0 and group not in (6,66,8,88,68,69,55) group by uid;

hive -e " use logindb; select uid, max(date) from halllogin_detail WHERE dt>='20170901' and dt<='20171131' and group > 0 and group not in (6,66,8,88,68,69,55) group by uid; " > /opt/20171205; 

移动端
hive -e " use logindb; select distinct(uid) from logindb.halllogin_detail where dt>='20171101' and game=311 " > /opt/game311; 
hive -e " use logindb; select distinct(uid) from logindb.halllogin_detail where dt>='20171101' and game=316 " > /opt/game316; 
hive -e " use logindb; select distinct(uid) from logindb.halllogin_detail where dt>='20171101' and game=421 " > /opt/game421; 
hive -e " use logindb; select distinct(uid) from logindb.halllogin_detail where dt>='20171101' and game=445 " > /opt/game445; 
hive -e " use logindb; select distinct(uid) from logindb.halllogin_detail where dt>='20171101' and game=450 " > /opt/game450; 

pc端
hive -e " use logindb; select distinct(uid) from logindb.roomlogin_detail WHERE dt>='20171101' and group > 0 and group not in (6,66,8,88,68,69,55) and game=23 " > /opt/pc_game23; 
hive -e " use logindb; select distinct(uid) from logindb.roomlogin_detail WHERE dt>='20171101' and group > 0 and group not in (6,66,8,88,68,69,55) and game=311 " > /opt/pc_game311; 
hive -e " use logindb; select distinct(uid) from logindb.roomlogin_detail WHERE dt>='20171101' and group > 0 and group not in (6,66,8,88,68,69,55) and game=316 " > /opt/pc_game316; 
hive -e " use logindb; select distinct(uid) from logindb.roomlogin_detail WHERE dt>='20171101' and group > 0 and group not in (6,66,8,88,68,69,55) and game=421 " > /opt/pc_game421; 
hive -e " use logindb; select distinct(uid) from logindb.roomlogin_detail WHERE dt>='20171101' and group > 0 and group not in (6,66,8,88,68,69,55) and game=445 " > /opt/pc_game445; 
hive -e " use logindb; select distinct(uid) from logindb.roomlogin_detail WHERE dt>='20171101' and group > 0 and group not in (6,66,8,88,68,69,55) and game=450 " > /opt/pc_game450; 


select distinct(uid) from logindb.roomlogin_detail WHERE dt>='20171101' and group > 0 and group not in (6,66,8,88,68,69,55) and game=23;


7月19日之后的
安卓打大A用户，注册中，登陆超过50天， 7月19号9点后
UID，CHANNEL，总游戏时长，设备重复注册，账号注册时间，账号注册IP，账号注册地区，设备注册地区，登陆天数

a表示登录信息：uid,app,登录时间date,ip,channel
b表示注册信息：uid,channel,账号注册ip,账号注册时间,账号注册地区,设备注册地区
现在要查出在某个时间段的注册用户中，算出这些用户的登录天数，账号注册ip,账号注册地区,设备注册地区

_id,channelname,hard1st,date,time,ip,province1,city1,district1,hardprovince1,hardcity1,harddistrict1

drop table if exists uid_detail;
create table uid_detail (
id     int,
ip     string,
channelname string,
date   int,
time   int,
hardid  string,
hard1st string,
province1 string,
city1 string,
district1 string,
hardprovince1 string,
hardcity1 string,
harddistrict1 string) 
PARTITIONED BY (
dt     string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


 keys = ["_id", "ip", "channelname", "date", "time", "hardid", "hard1st",
    "province1", "city1", "district1", "hardprovince1", "hardcity1", "harddistrict1"]
	
LOAD DATA  INPATH '/user/root/regdetails' overwrite  INTO TABLE uid_detail PARTITION (dt='20171212');

select uid.id,uid.ip,uid.channelname,uid.date,uid.time,uid.hard1st,uid.province1,uid.city1,uid.district1,uid.hardprovince1,uid.hardcity1,uid.harddistrict1,count(ha.ha_date) 
from
(SELECT DISTINCT a.uid uid,
	a.date ha_date
FROM halllogin_detail a
where a.dt>='20170719' and a.app=1880221
  and a.uid in
(SELECT DISTINCT b.id FROM uid_detail b WHERE b.dt='20171212')
) ha 
join
	(select id,ip,channelname,date,time,hard1st,province1,city1,district1,hardprovince1,hardcity1,harddistrict1 from uid_detail where dt='20171212') uid on ha.uid = uid.id 
group by uid.id,ip,channelname,date,time,hard1st,province1,city1,district1,hardprovince1,hardcity1,harddistrict1;


select uid.id,uid.ip,uid.channelname,uid.date,uid.time,uid.hardid,uid.hard1st,uid.province1,uid.city1,uid.district1,uid.hardprovince1,uid.hardcity1,uid.harddistrict1,count(ha.ha_date) 
from
(SELECT DISTINCT a.uid uid,
	a.date ha_date
FROM halllogin_detail a
where a.dt>='20170719' and a.app=1880221
  and a.uid in
(SELECT DISTINCT b.id FROM uid_detail b WHERE b.dt='20171213')) ha right outer join
(select id,ip,channelname,date,time,hardid,hard1st,province1,city1,district1,hardprovince1,hardcity1,harddistrict1 from uid_detail where dt='20171213') uid on ha.uid = uid.id 
group by uid.id,ip,channelname,date,time,hardid,hard1st,province1,city1,district1,hardprovince1,hardcity1,harddistrict1;

 
hive -e " use logindb; select uid.id,uid.ip,uid.channelname,uid.date,uid.time,uid.hardid,uid.hard1st,uid.province1,uid.city1,uid.district1,uid.hardprovince1,uid.hardcity1,uid.harddistrict1,count(ha.ha_date) 
from
(SELECT DISTINCT a.uid uid,
    a.date ha_date
FROM halllogin_detail a
where a.dt>='20170719' and a.app=1880221
  and a.uid in
(SELECT DISTINCT b.id FROM uid_detail b WHERE b.dt='20171213')) ha right outer join
(select id,ip,channelname,date,time,hardid,hard1st,province1,city1,district1,hardprovince1,hardcity1,harddistrict1 from uid_detail where dt='20171213') uid on ha.uid = uid.id 
group by uid.id,ip,channelname,date,time,hardid,hard1st,province1,city1,district1,hardprovince1,hardcity1,harddistrict1 " > /opt/uid_20170719_20171213; 		 
 

12月3日——12月9日
登录同城游APP 的uid    app=3001
select distinct uid from halllogin_detail where dt>='20171203' and dt<='20171209' and app = 3001; --
hive -e " use logindb; select distinct uid from halllogin_detail where dt>='20171203' and dt<='20171209' and app = 3001 " > /opt/1203_1209_app_3001; 
登录PC大厅的 uid
select distinct uid from halllogin_detail WHERE dt>='20171203' and dt<='20171209' and group > 0 and group not in (6,66,8,88,68,69,55);
hive -e " select distinct uid from halllogin_detail WHERE dt>='20171203' and dt<='20171209' and group > 0 and group not in (6,66,8,88,68,69,55) " > /opt/PC_1203_1209; 


11月12日-12月12日，登录西安地区（2301），用户ID去重， 分PC和移动
PC端
select distinct uid from halllogin_detail WHERE dt>='20171112' and dt<='20171212' and group > 0 and group not in (6,66,8,88,68,69,55) and area_city='2301';

移动端
select distinct(uid) from halllogin_detail WHERE dt>='20171112' and dt<='20171212' and group in (6,66,8,88) and area_city='2301';



select uid.id,uid.ip,uid.channelname,uid.date,uid.time,uid.hardid,uid.hard1st,uid.province1,uid.city1,uid.district1,uid.hardprovince1,uid.hardcity1,uid.harddistrict1,count(ha.ha_date) 
from
(SELECT DISTINCT a.uid uid,
	a.date ha_date
FROM halllogin_detail a
where a.dt>='20170719' and a.app=1880221
  and a.uid in
(SELECT DISTINCT b.id FROM uid_detail b WHERE b.dt='20171213')) ha right outer join
(select id,ip,channelname,date,time,hardid,hard1st,province1,city1,district1,hardprovince1,hardcity1,harddistrict1 from uid_detail where dt='20171213') uid on ha.uid = uid.id 
group by uid.id,ip,channelname,date,time,hardid,hard1st,province1,city1,district1,hardprovince1,hardcity1,harddistrict1;


select distinct ha.uid 
from halllogin_detail ha
where ha.dt>='20171201' and ha.uid not in
	(SELECT a.uid uid FROM login_uid a WHERE a.dt='201701_08');
	
select distinct ha.uid 
from halllogin_detail ha
where ha.dt>='20171201' and ha.uid not in
	(SELECT a.uid uid FROM login_uid a WHERE a.dt='201709_11');


在前几个月登录过，在12月未登录的用户
hive -e "use logindb; SELECT distinct a.uid 
    FROM (select uid from halllogin_detail WHERE dt>='20171201') ha 
    right JOIN (SELECT a.uid uid FROM login_uid a WHERE a.dt='201709_11') a
    ON ha.uid = a.uid WHERE ha.uid is NULL" > /opt/201709_11;

hive -e "use logindb; SELECT distinct a.uid 
    FROM (select uid from halllogin_detail WHERE dt>='20171201') ha 
    right JOIN (SELECT a.uid uid FROM login_uid a WHERE a.dt='201701_08') a
    ON ha.uid = a.uid WHERE ha.uid is NULL" > /opt/201701_08;

	
20170115-20170121 这周的登录用户，在20170716-20170722的登录情况在20171119-20171125的登录情况，
即：导出1月的第三周的登录用户，去查询这批用户在接下来的7,11月中的第三个星期的登录情况，
同样的，还需要导出
1月的第四周（20170122-20170128）的登录用户，去查询这批用户在接下来的7,11月中的第四周的登录情况，
1月的最后一周（20170129-20170204）的登录用户，去查询这批用户在接下来的7,11月中的最后一周的登录情况

第三周：
7月的第三周
hive -e "use logindb; 
SELECT distinct a.uid, a.channel
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20170716' and ha1.dt<='20170722') ha
right join (select distinct b.uid uid, b.channel channel from halllogin_detail b where b.dt>='20170115' and b.dt<='20170121') a
on ha.uid = a.uid where ha.uid is not NULL" > /opt/201707_week3;  --1345758
11月的第三周
hive -e "use logindb;
SELECT distinct a.uid, a.channel
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20171119' and ha1.dt<='20171125') ha
right join (select distinct b.uid uid, b.channel channel from halllogin_detail b where b.dt>='20170115' and b.dt<='20170121') a
on ha.uid = a.uid where ha.uid is not NULL" > /opt/201711_week3;  --987745

第四周：
7月第四周
hive -e "use logindb; 
SELECT distinct a.uid, a.channel
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20170723' and ha1.dt<='20170729') ha
right join (select distinct b.uid uid, b.channel channel from halllogin_detail b where b.dt>='20170122' and b.dt<='20170128') a
on ha.uid = a.uid where ha.uid is not NULL" > /opt/201707_week4; --1322736
11月第四周
hive -e "use logindb; 
SELECT distinct a.uid, a.channel
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20171126' and ha1.dt<='20171202') ha
right join (select distinct b.uid uid, b.channel channel from halllogin_detail b where b.dt>='20170122' and b.dt<='20170128') a
on ha.uid = a.uid where ha.uid is not NULL" > /opt/201711_week4;

最后一周：
7月
hive -e "use logindb; 
SELECT distinct a.uid, a.channel
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20170730' and ha1.dt<='20170805') ha
right join (select distinct b.uid uid, b.channel channel from halllogin_detail b where b.dt>='20170129' and b.dt<='20170204') a
on ha.uid = a.uid where ha.uid is not NULL" > /opt/201707_after_week; --1301423
11月  没有

SELECT distinct a.uid, a.channel
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20170716' and ha1.dt<='20170722') ha
right join (select distinct b.uid uid, b.channel channel from halllogin_detail b where b.dt>='20170115' and b.dt<='20170121') a
on ha.uid = a.uid where ha.uid is not NULL;

select * from 
(select * from halllogin_detail) ha  
left join (select uid from login_uid where dt='20171215') a 
on ha.uid = a.uid where a.uid is not NULL;


hive -e "use logindb; 
select ha.* from 
	(select 
	uid,
	date,
	time,
	ip, 
	group,  
	app,
	fromapp,
	game, 
	channel,
	promcode,
	hardid,    
	area_province,
	area_city,
	area_district
from halllogin_detail) ha  
left join (select uid from login_uid where dt='20171215') a 
on ha.uid = a.uid where a.uid is not NULL "> /opt/20171215;


hive -e "use logindb; 
SELECT count(0)
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20170716' and ha1.dt<='20170722') ha
right join (select distinct b.uid uid from halllogin_detail b where b.dt>='20170115' and b.dt<='20170121') a
on ha.uid = a.uid where ha.uid is not NULL" > /opt/201707_week3;

hive -e "use logindb;
SELECT count(0)
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20171119' and ha1.dt<='20171125') ha
right join (select distinct b.uid uid from halllogin_detail b where b.dt>='20170115' and b.dt<='20170121') a
on ha.uid = a.uid where ha.uid is not NULL" > /opt/201711_week3;

hive -e "use logindb; 
SELECT count(0)
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20170723' and ha1.dt<='20170729') ha
right join (select distinct b.uid uid from halllogin_detail b where b.dt>='20170122' and b.dt<='20170128') a
on ha.uid = a.uid where ha.uid is not NULL" > /opt/201707_week4;

hive -e "use logindb; 
SELECT count(0)
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20171126' and ha1.dt<='20171202') ha
right join (select distinct b.uid uid from halllogin_detail b where b.dt>='20170122' and b.dt<='20170128') a
on ha.uid = a.uid where ha.uid is not NULL" > /opt/201711_week4;

hive -e "use logindb; 
SELECT count(0)
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20170730' and ha1.dt<='20170805') ha
right join (select distinct b.uid uid from halllogin_detail b where b.dt>='20170129' and b.dt<='20170204') a
on ha.uid = a.uid where ha.uid is not NULL" > /opt/201707_after_week;


2017年9月1日—11月30日期间，PC大厅流失用户ID和手机号码，流失定义为连续27天未登陆
LOAD DATA  INPATH '/user/root/hadoopfile' overwrite  INTO TABLE login_uid PARTITION (dt='201712_25');

hive -e "use logindb; 
	SELECT t0.uid,
		   t0.date
	  FROM (select hd.uid, 
				   max(concat(substr(hd.date,1,4),'-',substr(hd.date,5,2),'-',substr(hd.date,7,2))) date 
			  from halllogin_detail hd  
			 where hd.dt>='20170901' and hd.dt<='20171224' 
			   and hd.group > 0 and hd.group not in (6,66,8,88,68,69,55) 
			  group by hd.uid HAVING datediff(current_date(),date) >= 27) t0
		   JOIN
		   (select lu.uid from login_uid lu where lu.dt='201712_25') t1 ON t0.uid = t1.uid " > /opt/20171225;
		 


届时需要导出相关的数据，
应用id	应用名称
7000066	王者传奇
7000082	玛法英雄
7000081	杨过小龙女
7000074	大天使H5
7000061	诛神黄昏
7000040	葫芦娃
7000006	少年西游记
7000083	仙灵觉醒
      12月22日-12月26日登陆的用户ID。
共8个游戏，8张表
hive -e "use logindb; 
select distinct(uid) from halllogin_detail where dt>='20171222' and dt<='20171226' and app=7000066 " > /opt/7000066;
hive -e "use logindb; 
select distinct(uid) from halllogin_detail where dt>='20171222' and dt<='20171226' and app=7000082 " > /opt/7000082;
hive -e "use logindb; 
select distinct(uid) from halllogin_detail where dt>='20171222' and dt<='20171226' and app=7000081 " > /opt/7000081;
hive -e "use logindb; 
select distinct(uid) from halllogin_detail where dt>='20171222' and dt<='20171226' and app=7000074 " > /opt/7000074;
hive -e "use logindb; 
select distinct(uid) from halllogin_detail where dt>='20171222' and dt<='20171226' and app=7000061 " > /opt/7000061;
hive -e "use logindb; 
select distinct(uid) from halllogin_detail where dt>='20171222' and dt<='20171226' and app=7000040 " > /opt/7000040;
hive -e "use logindb; 
select distinct(uid) from halllogin_detail where dt>='20171222' and dt<='20171226' and app=7000006 " > /opt/7000006;
hive -e "use logindb; 
select distinct(uid) from halllogin_detail where dt>='20171222' and dt<='20171226' and app=7000083 " > /opt/7000083;


group=88，app=3001，在1214-1221期间活跃的用户，在1222（包含）之后再也没有登陆过的用户ID

hive -e "use logindb;
SELECT ha.uid
from
	(select distinct ha1.uid uid from halllogin_detail ha1 where ha1.dt>='20171214' and ha1.dt<='20171221' and group=88 and app=3001) ha
left join 
	(select distinct b.uid uid from halllogin_detail b where b.dt>='20171222' and b.dt<='20171226' and group=88 and app=3001) a
on ha.uid = a.uid where a.uid is NULL " > /opt/1214-1221;
	

营收类游戏：316、421、450、311、23、445
gameid：432+409最近一个月（11月25日-12月25日）的用户ID，排除玩过营收类游戏的用户
宿松同心（id：432）+拖三（id：409）最近一个月（11月25日-12月25日）登录玩家的ID和对应的手机号码（包含移动端+pc端）

select alluids.uid
from 
	(select distinct aa.uid uid 
		from halllogin_detail aa 
			where aa.game in (432,409) and aa.dt>='20171125' and aa.dt<='20171225'
	union all
	select distinct bb.uid uid 
		from roomlogin_detail bb 
			where bb.game in (432,409) and bb.dt>='20171125' and bb.dt<='20171225'
	) alluids
	left join
	(select distinct hd.uid uid
		from halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
	UNION ALL
	select distinct rd.uid uid
		from roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
	) yinshou 
	on alluids.uid = yinshou.uid 
where yinshou.uid is NULL


select  uid from
 (select hd.uid uid
 from halllogin_detail hd 
  where hd.game in (316,421,450,311,23,445) 
 UNION all
 select rd.uid uid
  from roomlogin_detail rd where rd.game in (316,421,450,311,23,445)
 ) yinshou group by uid ;
	

最近15天登陆过的用户情况
LOAD DATA  INPATH '/user/root/hadoopfile' overwrite  INTO TABLE login_uid PARTITION (dt='20171229');
select uid from  halllogin_detail where dt >= date_sub(current_date(),15)

hive -e "use logindb;
SELECT distinct a.uid 
    FROM 
   	 	(select uid from halllogin_detail WHERE dt>='20171215') ha
    right JOIN
   	 	(SELECT a.uid uid FROM login_uid a WHERE a.dt='201712_29') a
    ON ha.uid = a.uid WHERE ha.uid is not NULL " > /opt/20171229;
    

万州麻将（ID：265）在2017年2月24到4月24日期间登录玩家的ID和对应的手机号码（包含移动端+PC端）
导出halllogin和roomlogin中gameid=265的用户，时间范围是2017年2月24到4月24日期间，导成一个文件给我，用户去重
    
hive -e "use logindb;
select distinct uid 
    from halllogin_detail 
      where game=265 and dt>='20170224' 
            and dt<='20170424' and uid not in (select uid from halllogin_detail where game in (316、421、450、311、23、445))"> /opt/265_mobile.csv;

hive -e "use logindb;
select distinct uid 
    from roomlogin_detail 
      where game=265 and dt>='20170224' 
            and dt<='20170424' and uid not in (select uid from roomlogin_detail where game in (316、421、450、311、23、445))"> /opt/265_pc.csv;  


hive -e "use logindb;
select distinct uid 
		from halllogin_detail 
			where group > 0 and group not in (6,66,8,88,68,69,55) 
						and game=265 and dt>='20170224' 
						and dt<='20170424' "> /opt/20170224_20170424_mobile;

hive -e "use logindb;
select distinct uid 
		from halllogin_detail 
			where group in (6,66,8,88) 
						and game=265 and dt>='20170224' 
						and dt<='20170424' "> /opt/20170224_20170424_pc;		
			

导出halllogin中10.31~11.30期间，PC大厅【group>0 and group not in (6,66,8,88,55,68,69)】
中最后一次登陆日期小于等于2017/12/06的用户去重列表
hive -e "use logindb;
select uid,maxdate from ( select uid, max(date) as maxdate 
    from halllogin_detail WHERE dt>='20171031' and dt<='20171130'  
        and group > 0 and group not in (6,66,8,88,68,69,55)
        group by uid) tmp  where maxdate <=20171206"> /opt/pc_nologin_uids.csv;
    

hive -e "use logindb;
select uid, max(date) date
		from halllogin_detail WHERE dt>='20171031' and dt<='20171130'  
				and group > 0 and group not in (6,66,8,88,68,69,55) and date <=20171206 
				group by uid  "> /opt/20171226_uid;


LOAD DATA  INPATH '/user/root/hadoopfile' overwrite  INTO TABLE login_uid PARTITION (dt='201712_25');


SELECT t0.uid,
	   t0.date
  FROM (select hd.uid, 
			   max(concat(substr(hd.date,1,4),'-',substr(hd.date,5,2),'-',substr(hd.date,7,2))) date 
		  from halllogin_detail hd  
		 where hd.dt>='20170901' and hd.dt<='20171224' 
		   and hd.group > 0 and hd.group not in (6,66,8,88,68,69,55) 
		  group by hd.uid HAVING datediff(current_date(),date) >= 27) t0
	   JOIN
	   (select lu.uid from login_uid lu where lu.dt='201712_25') t1 ON t0.uid = t1.uid ;


帮我导出，2017年12月4日-2018年1月4日期间，玩game=31的去重用户ID列表，且这些用户从来没有玩过营收类游戏，累计充值没有超过100元
区分PC端和移动端
移动端：
select ddd.uid
from
    (select alluids.uid uid
    from
    	(select distinct aa.uid uid 
    		from logindb.halllogin_detail aa 
    			where aa.game = 31 and aa.dt >= '20171204' and aa.dt <= '20180104'
    		) alluids
    	left join
    	(select distinct hd.uid uid
    		from logindb.halllogin_detail hd 
    			where hd.game in (316,421,450,311,23,445) 
    		) yinshou	
    	on alluids.uid = yinshou.uid
    where yinshou.uid is null) ddd
left join
    (select uidto
        from paydb.basic
            group by uidto 
            having sum(money) >= 10000
    ) ba
on ddd.uid = ba.uidto
where ba.uidto is null;


pc端
hive -e "
select ddd.uid
from
    (select alluids.uid uid
    from
    	(select distinct aa.uid uid 
    		from logindb.roomlogin_detail aa 
    			where aa.game = 31 and aa.dt >= '20171204' and aa.dt <= '20180104'
    		) alluids
    	left join
    	(select distinct hd.uid uid
    		from logindb.roomlogin_detail hd 
    			where hd.game in (316,421,450,311,23,445) 
    		) yinshou
    	on alluids.uid = yinshou.uid
    where yinshou.uid is null) ddd
left join
    (select uidto
        from paydb.basic
            group by uidto 
            having sum(money) >= 10000
    ) ba
on ddd.uid = ba.uidto
where ba.uidto is null
" > /opt/pc;


select uidto
from paydb.basic
group by uidto having sum(money) < 10000;



导一下附件中用户列表中每个用户的历时充值总额
LOAD DATA  INPATH '/user/root/hadoopfile' overwrite  INTO TABLE login_uid PARTITION (dt='201801_08');

select * from login_uid where dt='201801_08';
select alluids.uid, ba.mon
from 
	(select uid
		from login_uid 
			where dt='201801_08') alluids
	left join
	(select uidto, sum(money) mon
		from paydb.basic
			group by uidto) ba
	on alluids.uid = ba.uidto
	

[1] 报表描述： 
        2017年9月25日—12月31日期间，根据附件ID表，匹配出该时间段充值总数。
        不限制平台，PC&移动双端
        [2] 报表目的：验证回流用户充值情况 
        [3] 补充说明：尽快完成，谢谢！
select * from login_uid where dt='201801_09';

select alluids.uid, ba.mon
from 
	(select uid
		from logindb.login_uid 
			where dt='201801_09') alluids
	left join
	(select uidto, sum(money) mon
		from paydb.basic 
			where paydate>=20170925 and paydate<=20171231
			group by uidto) ba
	on alluids.uid = ba.uidto;
	where ba.mon is not null;
	
	
需求1:
    app：1880338
    date：20171208-20180108
    平台：PC端
    获取上述条件下登陆过的用户，去除充过钱、玩过营收类游戏的用户
    导出文件：1880338.pc.csv
select ddd.uid
from
    (select alluids.uid uid
    from
    	(select distinct aa.uid uid 
    		from logindb.roomlogin_detail aa 
    			where aa.app = 1880338 and aa.dt >= '20171208' and aa.dt <= '20180108'
    		) alluids
    	left join
    	(select distinct hd.uid uid
    		from logindb.roomlogin_detail hd 
    			where hd.game in (316,421,450,311,23,445) 
    		) yinshou
    	on alluids.uid = yinshou.uid
    where yinshou.uid is null) ddd
left join
    (select uidto
        from paydb.basic
            group by uidto 
            having sum(money) > 0
    ) ba
on ddd.uid = ba.uidto
where ba.uidto is null;


需求2:
    app：1880338
    date：20171208-20180108
    平台：移动端
    获取上述条件下登陆过的用户，去除充过钱、玩过营收类游戏的用户
    导出文件：1880338.mobile.csv
select ddd.uid
from
    (select alluids.uid uid
    from
    	(select distinct aa.uid uid 
    		from logindb.halllogin_detail aa 
    			where aa.app = 1880338 and aa.dt >= '20171208' and aa.dt <= '20180108'
    		) alluids
    	left join
    	(select distinct hd.uid uid
    		from logindb.halllogin_detail hd 
    			where hd.game in (316,421,450,311,23,445) 
    		) yinshou	
    	on alluids.uid = yinshou.uid
    where yinshou.uid is null) ddd
left join
    (select uidto
        from paydb.basic
            group by uidto 
            having sum(money) > 0
    ) ba
on ddd.uid = ba.uidto
where ba.uidto is null;



需求3:
    app：1880310
    date：20171208-20180108
    平台：PC端
    获取上述条件下登陆过的用户，去除充过钱、玩过营收类游戏的用户
    导出文件：1880310.pc.csv
hive -e "
select ddd.uid
from
    (select alluids.uid uid
    from
    	(select distinct aa.uid uid 
    		from logindb.roomlogin_detail aa 
    			where aa.app = 1880310 and aa.dt >= '20171208' and aa.dt <= '20180108'
    		) alluids
    	left join
    	(select distinct hd.uid uid
    		from logindb.roomlogin_detail hd 
    			where hd.game in (316,421,450,311,23,445) 
    		) yinshou
    	on alluids.uid = yinshou.uid
    where yinshou.uid is null) ddd
left join
    (select uidto
        from paydb.basic
            group by uidto 
            having sum(money) > 0
    ) ba
on ddd.uid = ba.uidto
where ba.uidto is null
" > /opt/1880310.pc.csv;


需求4:
    app：1880310
    date：20171208-20180108
    平台：移动端
    获取上述条件下登陆过的用户，去除充过钱、玩过营收类游戏的用户
    导出文件：1880310.mobile.csv
hive -e "
select ddd.uid
from
    (select alluids.uid uid
    from
    	(select distinct aa.uid uid 
    		from logindb.halllogin_detail aa 
    			where aa.app = 1880310 and aa.dt >= '20171208' and aa.dt <= '20180108'
    		) alluids
    	left join
    	(select distinct hd.uid uid
    		from logindb.halllogin_detail hd 
    			where hd.game in (316,421,450,311,23,445) 
    		) yinshou	
    	on alluids.uid = yinshou.uid
    where yinshou.uid is null) ddd
left join
    (select uidto
        from paydb.basic
            group by uidto 
            having sum(money) > 0
    ) ba
on ddd.uid = ba.uidto
where ba.uidto is null
" > /opt/1880310.mobile.csv;


为统计同城游APP活跃用户季度留存情况（app=3001），请帮忙导出：
20170115-20170121登录的用户，在20170415-20170421的登录情况
20170122-20170128登录的用户，在20170422-20170428的登录情况
20170129-20170204登录的用户，在20170429-20170505的登录情况
hive -e "
select distinct aa.uid uid 
	from logindb.halllogin_detail aa 
		where aa.app = 3001 and aa.dt >= '20170115' and aa.dt <= '20170121'
" > /opt/3001_0115_0121.csv;

hive -e "
select ha1.uid
from
	(select distinct aa.uid uid 
		from logindb.halllogin_detail aa 
			where aa.app = 3001 and aa.dt >= '20170115' and aa.dt <= '20170121'
			) ha1
	left join
	(select distinct ha.uid uid 
		from logindb.halllogin_detail ha 
			where ha.app = 3001 and ha.dt >= '20170415' and ha.dt <= '20170421'
			) ha2
on ha1.uid = ha2.uid
where ha2.uid is not null
" > /opt/3001_0415_0421.csv;

    			
hive -e "
select distinct aa.uid uid 
	from logindb.halllogin_detail aa 
		where aa.app = 3001 and aa.dt >= '20170122' and aa.dt <= '20170128'
" > /opt/3001_0122_0128.csv;

hive -e "
select ha1.uid
from
	(select distinct aa.uid uid 
		from logindb.halllogin_detail aa 
			where aa.app = 3001 and aa.dt >= '20170122' and aa.dt <= '20170128'
			) ha1
	left join
	(select distinct ha.uid uid 
		from logindb.halllogin_detail ha 
			where ha.app = 3001 and ha.dt >= '20170422' and ha.dt <= '20170428'
			) ha2
on ha1.uid = ha2.uid
where ha2.uid is not null
" > /opt/3001_0422_0428.csv;


hive -e "			
select distinct aa.uid uid 
	from logindb.halllogin_detail aa 
		where aa.app = 3001 and aa.dt >= '20170129' and aa.dt <= '20170204'
" > /opt/3001_0129_0204.csv;	
	
hive -e "
select ha1.uid
from
	(select distinct aa.uid uid 
		from logindb.halllogin_detail aa 
			where aa.app = 3001 and aa.dt >= '20170129' and aa.dt <= '20170204'
			) ha1
	left join
	(select distinct ha.uid uid 
		from logindb.halllogin_detail ha 
			where ha.app = 3001 and ha.dt >= '20170429' and ha.dt <= '20170505'
			) ha2
on ha1.uid = ha2.uid
where ha2.uid is not null
" > /opt/3001_0429_0505.csv;	



需求一：
    app：1880258，  移动端
    date：2017年12月12日 - 2018年1月12日
    登陆用户去重（去除 历史充值大于等于100元，玩过营收类游戏的用户）
    导出文件：1880258.csv

select ddd.uid
from
    (select alluids.uid uid
    from
    	(select distinct aa.uid uid 
    		from logindb.halllogin_detail aa 
    			where aa.app = 1880258 and aa.dt >= '20171212' and aa.dt <= '20180112'
    		) alluids
    	left join
    	(select distinct hd.uid uid
    		from logindb.halllogin_detail hd 
    			where hd.game in (316,421,450,311,23,445) 
    		) yinshou	
    	on alluids.uid = yinshou.uid
    where yinshou.uid is null
    ) ddd
left join
    (select uidto
        from paydb.basic
            group by uidto 
            having sum(money) >= 10000
    ) ba
on ddd.uid = ba.uidto
where ba.uidto is null;


需求二：
    app：1880053， group：66, 88
    date：10月份
    去重用户ＩＤ
    导出文件：1880053.10MONTH.csv
hive -e "
select distinct aa.uid uid 
  	from logindb.halllogin_detail aa 
  		where aa.app = 1880053 and aa.dt >= '20171001' and aa.dt <= '20171031' and aa.group in (66,88)
" > /opt/10MONTH.csv;	

需求三：
    app：1880053， group：66, 88
    date：10月11日
    去重登陆用户ＩＤ
    导出文件：1880053.1011.csv
hive -e "
select distinct aa.uid uid 
  	from logindb.halllogin_detail aa 
  		where aa.app = 1880053 and aa.dt = '20171011' and aa.group in (66,88)
" > /opt/1880053.1011.csv;	

需求四：
    app：1880053， group：66, 88
    date：10月12日
    去重登陆用户ＩＤ
    导出文件：1880053.1012.csv
hive -e "
select distinct aa.uid uid 
  	from logindb.halllogin_detail aa 
  		where aa.app = 1880053 and aa.dt = '20171012' and aa.group in (66,88)
" > /opt/1880053.1012.csv;	

需求四：
    app：1880053， group：66, 88
    date：10月13日
    去重登陆用户ＩＤ
    导出文件：1880053.1013.csv
hive -e "
select distinct aa.uid uid 
  	from logindb.halllogin_detail aa 
  		where aa.app = 1880053 and aa.dt = '20171013' and aa.group in (66,88)
" > /opt/1880053.1013.csv;	


端	       期数			用户ID	统计开始日期	统计截止日期    忽略	登录天数
platform  period  uid    starttime	   endtime	 ignore
drop table if exists filter_detail;
create table filter_detail (
platform                  string,
period                  	string,
uid                  			int,
starttime               	int, 
endtime                  	int,  
ignore                    string)  
PARTITIONED BY (
dt     string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA  INPATH '/user/root/1' overwrite  INTO TABLE filter_detail PARTITION (dt='20180116');


filter_detail  -- 648941
select * from logindb.filter_detail;
select count(0) 
    from logindb.filter_detail fi
		where dt='20180116'
		group by fi.platform, fi.period, fi.uid , fi.starttime, fi.endtime;   -- 648941


select count(0)
from 
    (select fi.platform platform, fi.period period, fi.uid uid, fi.starttime starttime, fi.endtime endtime
		from logindb.filter_detail fi
		where dt='20180116'
			) filter_uid
		left join
		(select distinct  aa.uid uid, aa.date date
		from logindb.halllogin_detail aa
			) alluids
		on filter_uid.uid = alluids.uid;  -- 17801291


select aa.uid uid, aa.date date
		from logindb.halllogin_detail aa
		group by aa.uid, aa.date



select filter_uid.platform, filter_uid.period, filter_uid.uid , filter_uid.starttime, filter_uid.endtime,
		count(alluids.date)
	from 
    (select fi.platform platform, fi.period period, fi.uid uid, fi.starttime starttime, fi.endtime endtime
		from logindb.filter_detail fi
		where dt='20180116'
	) filter_uid
	left join
	(select distinct  aa.uid uid, 
		aa.date date
		from logindb.halllogin_detail aa
	) alluids
	on filter_uid.uid = alluids.uid
where alluids.date >= filter_uid.starttime and
			alluids.date <= filter_uid.endtime
group by filter_uid.platform, filter_uid.period, filter_uid.uid , filter_uid.starttime, filter_uid.endtime


hive -e "
select count(0)
from 
    (select fi.platform platform, fi.period period, fi.uid uid, fi.starttime starttime, fi.endtime endtime
		from logindb.filter_detail fi
		where dt='20180116'
			) filter_uid
		left join
		(select distinct  aa.uid uid, aa.date date
		from logindb.halllogin_detail aa
			) alluids
		on filter_uid.uid = alluids.uid
where alluids.date >= filter_uid.starttime and
			alluids.date <= filter_uid.endtime
" > /opt/11.csv    108548


select 
		fid.platform platform, 
		fid.period period, 
		fid.uid uid, 
		fid.starttime starttime,
		fid.endtime endtime,
		count_uids.count_date count_date
from
		(select fd.platform platform, 
						fd.period period, 
						fd.uid uid, 
						fd.starttime starttime, 
						fd.endtime endtime
		from logindb.filter_detail fd
		where dt='20180116'
			) fid
	  left join
		(select filter_uid.platform platform, filter_uid.period period, filter_uid.uid uid, 
						filter_uid.starttime starttime, filter_uid.endtime endtime,
					 count(alluids.date) count_date
		from
		    (select fi.platform platform, fi.period period, fi.uid uid, fi.starttime starttime, fi.endtime endtime
				from logindb.filter_detail fi
				where dt='20180116'
					) filter_uid
				left join
				(select distinct  aa.uid uid, aa.date date
				from logindb.halllogin_detail aa
					) alluids
				on filter_uid.uid = alluids.uid
		where alluids.date >= filter_uid.starttime and
					alluids.date <= filter_uid.endtime
		group by filter_uid.platform, filter_uid.period, filter_uid.uid , filter_uid.starttime, filter_uid.endtime
		) count_uids 
		on fid.uid = count_uids.uid


select 
		fid.platform platform, 
		fid.period period, 
		fid.uid uid, 
		fid.starttime starttime,
		fid.endtime endtime,
		count_uids.count_date count_date
from
		(select fd.platform platform, 
						fd.period period, 
						fd.uid uid, 
						fd.starttime starttime, 
						fd.endtime endtime
		from logindb.filter_detail fd
		where dt='20180116'
			) fid
		left join
		(select filter_uid.platform platform, 
						filter_uid.period period, 
						filter_uid.uid uid, 
						filter_uid.starttime starttime, 
						filter_uid.endtime endtime,
					  count(alluids.date) count_date
		from
		    (select fi.platform platform, 
		    			  fi.period period, 
		    			  fi.uid uid, 
		    			  fi.starttime starttime, 
		    			  fi.endtime endtime
				from logindb.filter_detail fi
				where dt='20180116'
					) filter_uid
				left join
				(select aa.uid uid, 
								aa.date date
				from logindb.halllogin_detail aa
				group by aa.uid, aa.date
					) alluids
				on filter_uid.uid = alluids.uid
		where alluids.date >= filter_uid.starttime and
					alluids.date <= filter_uid.endtime
		group by filter_uid.platform, filter_uid.period, filter_uid.uid , filter_uid.starttime, filter_uid.endtime
		) count_uids 
		on fid.uid = count_uids.uid
		
		
SELECT t0.platform, 
       t0.period, 
       t0.uid,
       t0.starttime,
       t0.endtime,
       count(DISTINCT if(t1.login_date BETWEEN t0.starttime AND t0.endtime,t1.login_date,NULL))
   FROM
(SELECT fi.platform platform, 
     		fi.period period,
        fi.uid uid,
        fi.starttime starttime,
        fi.endtime endtime
   FROM logindb.filter_detail fi
  WHERE dt='20180116') t0
LEFT JOIN
(select distinct aa.uid, 
        aa.date login_date
    from logindb.halllogin_detail aa
) t1 ON t0.uid = t1.uid
GROUP BY t0.platform, t0.period, t0.uid,t0.starttime,t0.endtime


需求：
    date：20170901-20171231
    group: 6,66,8,88
    指定用户，按月按游戏去重导出
    字段：月份（例如：201709）、应用ID、用户ID
    导出文件：fk.20170901.20171231.csv

select to.uid,
			 t1.app,
			 t1.month
	 from
(select uid 
	from logindb.login_uid 
	where dt = '20180116'
) to
LEFT JOIN
(select uid, app, 
		substr(date,1,6) month 
	from logindb.halllogin_detail 
    where dt >= '20170901' and dt <= '20171231' and group in (6,66,8,88)
    group by uid, app, substr(date,1,6)
) t1 
ON to.uid = t1.uid

hive -e "
" > /opt/fk.20170901.20171231.csv;


【用户充值&消耗数据】同城游APP回流用户信息导出（20180109-20180115）
附件内用户在20180109-20180115期间的充值、消耗总额
LOAD DATA  INPATH '/user/root/1' overwrite  INTO TABLE filter_detail PARTITION (dt='20180116');

附件内用户 时间范围内银子消耗总额
select t1.uid, t1.money, t2.DepositSum
from 
  (select uid,money from dbo.tblTmpUsers2) t1
left join
  (SELECT UserID, SUM(DepositSum) DepositSum from dbo.tblUserDailyCost20180117
  where CreateDate >= 20180109 AND CreateDate <= 20180115 
  group by UserID) t2
on t1.uid = t2.UserID;


附件内用户 时间范围内的充值
select uids.uid, ba.money
	from
		(select distinct uid 
			from logindb.login_uid where dt='20180115'
			) uids
left join
		(select uidto, sum(money) money
    	from paydb.basic 
    		where dt>='201801' and paydate>='20180109' and paydate<='20180115'
        group by uidto 
			) ba
on uids.uid = ba.uidto 
				

附件内用户在同城游pc或者移动端游戏期间的充值、消耗总额，总在线时长，用户最后一次登录时
附件用户中的历史总充值，2017年11月及以后的银子总消耗，2018年的最后一次登陆时间
附件中的用户在2017年11月及以后的银子总消耗
select t1.uid, t2.DepositSum
from 
	(select uid from dbo.tblTmpUsers) t1
left join
	(SELECT UserID, SUM(DepositSum) DepositSum from dbo.tblUserDailyCost20180117
		where CreateDate>=20171101 group by UserID) t2
on t1.uid = t2.UserID;

LOAD DATA  INPATH '/user/root/uid_DepositSum' overwrite  INTO TABLE temporary PARTITION (dt='20180118');


select t0.uid, t0.depositsum, t0.money, t1.date
	from
	(select uid_depositsum.uid uid, uid_depositsum.depositsum depositsum, ba.money money
		from
			(select uid, depositsum 
				from logindb.temporary 
				where dt = '20180118'
				group by uid, depositsum
			) uid_depositsum
	left join
			(select uidto, sum(money) money
				from paydb.basic
		      group by uidto 
			) ba
	on uid_depositsum.uid = ba.uidto
	) t0
left join
	(select uid, max(date) date
		from logindb.halllogin_detail 
	    where dt >= '20180101'
	    group by uid
	) t1
on t0.uid = t1.uid;


需求一：
    app：1880067
    日期范围： 12月18日-1月17日
    过滤历史充值大于100元的用户，过滤玩过营收类游戏的用户
    导出 去重用户ID、最近一次登录时间（date、time）、group
    导出文件名：1880067.1218.0117.csv
    总共用户：uid + max(date) -- 25767    过滤完之后是 -- 22454

移动端：
select	ddd.uid,
		ddd.date,
		ddd.group
	from
	(select  alluids.uid uid,
			 alluids.date date,
			 alluids.group group
		from
		(select  t.uid uid, 
			t.date date,
			t.group group
		  from
		(select aa.uid uid,
				aa.date date, 
				aa.group group,
				row_number() over(partition by aa.uid order by aa.date desc) as rn
		   from logindb.halllogin_detail aa 
		   where aa.app = 1880067 
		   and aa.dt >= '20171218' and aa.dt <= '20180117'
		) t
		where rn = 1
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
	   from paydb.basic
	   group by uidto 
	   having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;
	
	
PC端
select	ddd.uid,
		ddd.date,
		ddd.group
	from
	(select  alluids.uid uid,
			 alluids.date date,
			 alluids.group group
		from
		(select  t.uid uid, 
			t.date date, 
			t.group group
		  from
		(select aa.uid uid,
				aa.date date, 
				aa.group group,
				row_number() over(partition by aa.uid order by aa.date desc) as rn
		   from logindb.roomlogin_detail aa 
		   where aa.game = 67  and aa.group not in (6,66,8,88,68,69,55) and aa.group > 0
		   and aa.dt >= '20171218' and aa.dt <= '20180117'
		) t
		where rn = 1
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
	   from paydb.basic
	   group by uidto 
	   having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;    -- 33016
	
hive -e "
" > /opt/1880067.1218.0117.csv;


需求二：
    app：1880109
    日期范围： 12月18日-1月17日
    过滤历史充值大于100元的用户，过滤玩过营收类游戏的用户
    导出 去重用户ID、最近一次登录时间（date、time）、group
    导出文件名：1880109.1218.0117.csv

移动端：	
select	ddd.uid,
		ddd.date,
		ddd.group
	from
	(select  alluids.uid uid,
			 alluids.date date,
			 alluids.group group
		from
		(select  t.uid uid, 
			t.date date, 
			t.group group
		  from
		(select aa.uid uid,
				aa.date date, 
				aa.group group,
				row_number() over(partition by aa.uid order by aa.date desc) as rn
		   from logindb.halllogin_detail aa 
		   where aa.app = 1880109 
		   and aa.dt >= '20171218' and aa.dt <= '20180117'
		   ) t
		   where rn = 1
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
	   from paydb.basic
	   group by uidto 
	   having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;

	
PC端：
select	ddd.uid,
		ddd.date,
		ddd.group
	from
	(select  alluids.uid uid,
			 alluids.date date,
			 alluids.group group
		from
		(select  t.uid uid, 
			t.date date, 
			t.group group
		  from
		(select aa.uid uid,
				aa.date date, 
				aa.group group,
				row_number() over(partition by aa.uid order by aa.date desc) as rn
		   from logindb.roomlogin_detail aa
		   where aa.game = 109  and aa.group not in (6,66,8,88,68,69,55) and aa.group > 0
		   and aa.dt >= '20171218' and aa.dt <= '20180117') t
		where rn = 1
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
	   from paydb.basic
	   group by uidto 
	   having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;
	

hive -e "
" > /opt/1880109.1218.0117.csv;

需求三：
    game：30
    移动平台
    日期范围： 2017年11月15日-2018年1月15日
    过滤历史充值大于100元的用户，过滤玩过营收类游戏的用户
    导出 去重用户ID
    导出文件名：30.mobile.1115.0115.csv
 
select ddd.uid
	from
	(select alluids.uid uid
		from 
		(select aa.uid uid
			from logindb.halllogin_detail aa 
			where aa.game = 30 and aa.dt >= '20171115' and aa.dt <= '20180115' and aa.group in (6, 66, 8, 88)
			group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
  (select uidto
  	from paydb.basic
    group by uidto 
    having sum(money) >= 10000
  ) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;

hive -e "
" > /opt/30.mobile.1115.0115.csv;	


需求四：
    game：30
    PC平台
    日期范围： 2017年11月15日-2018年1月15日
    过滤历史充值大于100元的用户，过滤玩过营收类游戏的用户
    导出 去重用户ID
    导出文件名：30.pc.1115.0115.csv
select ddd.uid
	from
	(select alluids.uid uid
		from 
		(select aa.uid uid
			from logindb.roomlogin_detail aa 
			where aa.game = 30 and aa.dt >= '20171115' and aa.dt <= '20180115' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
			group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
  (select uidto
  	from paydb.basic
    group by uidto 
    having sum(money) >= 10000
  ) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;
	
hive -e "
" > /opt/30.pc.1115.0115.csv;


(select distinct hd.uid uid
	from halllogin_detail hd 
	where hd.game in (316,421,450,311,23,445) 
UNION ALL
select distinct rd.uid uid
	from roomlogin_detail rd 
	where rd.game in (316,421,450,311,23,445)
) yinshou 




【报表】【手机号】打大A（id：1880221）2017年11月19日—2018年1月18日
指定cityid：3111
登录的玩家ID 和对应的手机号码
区分移动 和PC
过滤充值超过100元的用户和营收类游戏用户

mobile：
select ddd.uid
	from
	(select alluids.uid uid
		from 
		(select aa.uid uid
			from logindb.halllogin_detail aa 
			where aa.game = 221 and aa.dt >= '20171119' and aa.dt <= '20180118' and aa.group in (6, 66, 8, 88) and area_city='3111'
			group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
  (select uidto
  	from paydb.basic
    group by uidto 
    having sum(money) >= 10000
  ) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;
hive -e "
" > /opt/221.mobile.1119.0118.csv;


pc:
select ddd.uid
	from
	(select alluids.uid uid
		from 
		(select aa.uid uid
			from logindb.roomlogin_detail aa 
			where aa.game = 221 and aa.dt >= '20171119' and aa.dt <= '20180118' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55) and area_city='3111'
			group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
  (select uidto
  	from paydb.basic
    group by uidto 
    having sum(money) >= 10000
  ) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;

hive -e "
" > /opt/221.pc.1119.0118.csv;


【报表】登陆明细导出
应用ID:
1880221,1880024,1880450,1880053,1880316,1880421,1880081,1880039,1880392,1880234
日期： 12月1日~1月21日

select aa.*
	from logindb.halllogin_detail aa 
	where aa.dt >= '20171201' and aa.dt <= '20180121' and aa.app in(1880221,1880024,1880450,1880053,1880316,1880421,1880081,1880039,1880392,1880234)

hive -e "
" > /opt/1201-0121.csv;


【报表】同城游pc&移动新流失用户信息导出
移动/PC分开导出
1、12月活跃的但1月1日后未再登录的用户ID；
2、12月注册的新用户中1月1日后未再登录的用户ID；
3、将1和2的用户在12月的登录天数、玩过的游戏名称or游戏ID、充值总金额、12月的充值金额和充值笔数导出

问题：
12月注册用户？
玩过的游戏，充值在一张表？还是分开
充值笔数是12月份还是 整个历史
select uid,  count(distinct uid, date)
	from logindb.halllogin_detail
	where dt = '20171201'  and group in (6, 66, 8, 88) and uid='34085'
group by uid;

select uid FROM logindb.login_uid WHERE dt='12_registere_mobile';
select uid FROM logindb.login_uid WHERE dt='12_registere_pc';

mobile：
select lf.uid, 
	   lf.count_date, 
	   lf.game, 
	   rg.money, 
	   rg.12_money, 
	   rg.count_monry
	from
	(select t3.uid uid, 
		    t3.count_date count_date, 
			t4.game game
		from 
		(select t1.uid uid, 
			    t1.count_date count_date
			FROM
			(select aa.uid uid, 
					count(distinct aa.date) count_date
				from logindb.halllogin_detail aa 
				where aa.dt >= '20171201' and aa.dt <= '20171231' and aa.group in (6, 66, 8, 88)
				group by aa.uid
			) t1
			left join
			(select aa.uid uid
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180101' and aa.dt <= '20180121' and aa.group in (6, 66, 8, 88)
				group by aa.uid
			) t2
			on t1.uid = t2.uid
			where t2.uid is null
		) t3
		left join
		(select bb.uid uid, 
				bb.game game
			from logindb.halllogin_detail bb
			where bb.dt >= '20171201' and bb.dt <= '20171231' and bb.group in (6, 66, 8, 88)
			group by bb.uid, bb.game
		) t4
		on t3.uid = t4.uid
	) lf	
	left join
	(select b1.uidto uidto, 
			b1.money money, 
			b2.12_money 12_money, 
			b2.count_monry count_monry
		from
		(select uidto, 
				sum(money) money
			from paydb.basic
			group by uidto
		) b1
		left join
		(select uidto, 
				sum(money) 12_money, 
				count(money) count_monry
			from paydb.basic
			where dt>= '20170101' and dt <= '20171231' 
			group by uidto
		) b2
		on b1.uidto=b2.uidto
	) rg
	on lf.uid = rg.uidto
UNION ALL	
select 	lf1.uid, 
		lf1.count_date, 
		lf1.game, 
		rg1.money, 
		rg1.12_money, 
		rg1.count_monry
	FROM
	(select tl5.uid uid, 
			tl5.count_date count_date, 
			tl6.game game
		FROM
		(select tl3.uid uid, 
				tl4.count_date	count_date
			from 
			(SELECT tl1.uid uid
				FROM
				(select uid
					FROM logindb.login_uid 
					WHERE dt='12_registere_mobile'
				) tl1
				left join	
				(select hd.uid uid
					FROM logindb.halllogin_detail hd 
					where hd.dt >= '20180101' and hd.dt <= '20180121' and hd.group in (6, 66, 8, 88)
					group by hd.uid
				) tl2
				on tl1.uid = tl2.uid
				where tl2.uid is null
			) tl3
			left join
			(select had.uid uid, 
					count(distinct had.date) count_date
				from logindb.halllogin_detail had 
				where had.dt >= '20171201' and had.dt <= '20171231' and had.group in (6, 66, 8, 88)
				group by had.uid
			) tl4
			on tl3.uid = tl4.uid
		) tl5
		left join
		(select had1.uid uid,
				had1.game game	
			from logindb.halllogin_detail had1 
			where had1.dt >= '20171201' and had1.dt <= '20171231' and had1.group in (6, 66, 8, 88)
			group by had1.uid, had1.game
		) tl6
		on 	tl5.uid = tl6.uid
	) lf1 
	left join
	(select ba1.uidto uidto, 
			ba1.money money, 
			ba2.12_money 12_money, 
			ba2.count_monry count_monry
		from
		(select uidto, 
				sum(money) money
			from paydb.basic
			group by uidto
		) ba1
		left join
		(select uidto, 
				sum(money) 12_money, 
				count(money) count_monry
			from paydb.basic
			where dt>= '20170101' and dt <= '20171231' 
			group by uidto
		) ba2
		on ba1.uidto=ba2.uidto
	) rg1
	on lf1.uid = rg1.uidto

hive -e "
" > /opt/mobile0001.csv;	

pc：
select lf.uid, 
	   lf.count_date, 
	   lf.game, 
	   rg.money, 
	   rg.12_money, 
	   rg.count_monry
	from
	(select t3.uid uid, 
		    t3.count_date count_date, 
			t4.game game
		from 
		(select t1.uid uid, 
			    t1.count_date count_date
			FROM
			(select aa.uid uid, 
					count(distinct aa.date) count_date
				from logindb.roomlogin_detail aa 
				where aa.dt >= '20171201' and aa.dt <= '20171231' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid
			) t1
			left join
			(select aa.uid uid
				from logindb.roomlogin_detail aa 
				where aa.dt >= '20180101' and aa.dt <= '20180121' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid
			) t2
			on t1.uid = t2.uid
			where t2.uid is null
		) t3
		left join
		(select bb.uid uid, 
				bb.game game
			from logindb.roomlogin_detail bb
			where bb.dt >= '20171201' and bb.dt <= '20171231' and bb.group > 0 and  bb.group not in (6,66,8,88,68,69,55)
			group by bb.uid, bb.game
		) t4
		on t3.uid = t4.uid
	) lf
	left join
	(select b1.uidto uidto, 
			b1.money money, 
			b2.12_money 12_money, 
			b2.count_monry count_monry
		from
		(select uidto, 
				sum(money) money
			from paydb.basic
			group by uidto
		) b1
		left join
		(select uidto, 
				sum(money) 12_money, 
				count(money) count_monry
			from paydb.basic
			where dt>= '20170101' and dt <= '20171231' 
			group by uidto
		) b2
		on b1.uidto=b2.uidto
	) rg
	on lf.uid = rg.uidto
UNION ALL	
select 	lf1.uid, 
		lf1.count_date, 
		lf1.game, rg1.money, 
		rg1.12_money, 
		rg1.count_monry
	FROM
	(select tl5.uid uid, 
			tl5.count_date count_date, 
			tl6.game game
		FROM
		(select tl3.uid uid, 
				tl4.count_date	count_date
			from 
			(SELECT tl1.uid uid
				FROM
				(select uid
					FROM logindb.login_uid 
					WHERE dt='12_registere_pc'
				) tl1
				left join	
				(select hd.uid uid
					FROM logindb.roomlogin_detail hd 
					where hd.dt >= '20180101' and hd.dt <= '20180121' and hd.group > 0 and  hd.group not in (6,66,8,88,68,69,55)
					group by hd.uid
				) tl2
				on tl1.uid = tl2.uid
				where tl2.uid is null
			) tl3
			left join
			(select had.uid uid, 
					count(distinct had.date) count_date
				from logindb.roomlogin_detail had 
				where had.dt >= '20171201' and had.dt <= '20171231' and had.group > 0 and  had.group not in (6,66,8,88,68,69,55)
				group by had.uid
			) tl4
			on tl3.uid = tl4.uid
		) tl5
		left join
		(select had1.uid uid,
				had1.game game	
			from logindb.roomlogin_detail had1 
			where had1.dt >= '20171201' and had1.dt <= '20171231' and had1.group > 0 and  had1.group not in (6,66,8,88,68,69,55)
			group by had1.uid, had1.game
		) tl6
		on 	tl5.uid = tl6.uid
	) lf1 
	left join
	(select ba1.uidto uidto, 
			ba1.money money, 
			ba2.12_money 12_money, 
			ba2.count_monry count_monry
		from
		(select uidto, 
				sum(money) money
			from paydb.basic
			group by uidto
		) ba1
		left join
		(select uidto, 
				sum(money) 12_money, 
				count(money) count_monry
			from paydb.basic
			where dt>= '20170101' and dt <= '20171231' 
			group by uidto
		) ba2
		on ba1.uidto=ba2.uidto
	) rg1
	on lf1.uid = rg1.uidto	

hive -e "
" > /opt/pc0001.csv;



【报表】同城游pc&移动17年有效召回用户跟踪
移动/PC分开导出
附件内用户的1月份的登录天数；
附件内用户自2017年9月1日起的充值总额、玩过的游戏ID
mobile：
select t5.uid, 
	   t5.count_date, 
	   t5.game, 
	   t6.money
	FROM
	(select t3.uid uid, 
			t3.count_date count_date, 
			t4.game game
		FROM
		(select t1.uid uid, 
				t2.count_date count_date
			from
			(select uid 
				FROM logindb.login_uid 
				WHERE dt='20180122_mobile'
			) t1
			left join
			(select aa.uid uid, 
					count(distinct aa.date) count_date
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180101' and aa.dt <= '20180121' and aa.group in (6, 66, 8, 88)
				group by aa.uid
			) t2
			on t1.uid = t2.uid
		) t3
		left join
		(select aa.uid uid,
				aa.game game
			from logindb.halllogin_detail aa 
			where aa.dt >= '20170901' and aa.dt <= '20180121' and aa.group in (6, 66, 8, 88)
			group by aa.uid, aa.game
		) t4
		on t3.uid = t4.uid
	) t5
	left join
	(select uidto, 
			sum(money) money
		from paydb.basic
		where paydate>='20170901'
		group by uidto
	) t6
	on t5.uid = t6.uidto
hive -e "
" > /opt/mobile_17_valid_uids.csv;	
	
	
pc：
select t5.uid, 
	   t5.count_date, 
	   t5.game, 
	   t6.money
	FROM
	(select t3.uid uid, t3.count_date count_date, t4.game game
		FROM
		(select t1.uid uid, t2.count_date count_date
			from
			(select uid 
				FROM logindb.login_uid 
				WHERE dt='20180122_pc'
			) t1
			left join
			(select aa.uid uid, 
					count(distinct aa.date) count_date
				from logindb.roomlogin_detail aa 
				where aa.dt >= '20180101' and aa.dt <= '20180121' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid
			) t2
			on t1.uid = t2.uid
		) t3
		left join
		(select aa.uid uid,
			aa.game game
			from logindb.roomlogin_detail aa 
			where aa.dt >= '20170901' and aa.dt <= '20180121' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
			group by aa.uid, aa.game
		) t4
		on t3.uid = t4.uid
	) t5
	left join
	(select uidto, sum(money) money
		from paydb.basic
		where paydate>='20170901'
		group by uidto
	) t6
	on t5.uid = t6.uidto

hive -e "
" > /opt/pc_17_valid_uids.csv;	



【报表】斗地主登陆明细导出
2018年1月12日 - 2018年1月17日
APPID：1880053
GROUP：66, 88
导出字段：uid,app,date,group,province,city,district,channel
每天一个文件
hive -e "
select uid, app, date, group, area_province, area_city, area_district, channel
	from logindb.halllogin_detail  
	where dt = '20180112' and app = 1880053 and group in (66, 88)
" > /opt/20180112.csv;

hive -e "
select uid, app, date, group, area_province, area_city, area_district, channel
	from logindb.halllogin_detail  
	where dt = '20180113' and app = 1880053 and group in (66, 88)
" > /opt/20180113.csv;


hive -e "
select uid, app, date, group, area_province, area_city, area_district, channel
	from logindb.halllogin_detail  
	where dt = '20180114' and app = 1880053 and group in (66, 88)
" > /opt/20180114.csv;


hive -e "
select uid, app, date, group, area_province, area_city, area_district, channel
	from logindb.halllogin_detail  
	where dt = '20180115' and app = 1880053 and group in (66, 88)
" > /opt/20180115.csv;


hive -e "
select uid, app, date, group, area_province, area_city, area_district, channel
	from logindb.halllogin_detail  
	where dt = '20180116' and app = 1880053 and group in (66, 88)
" > /opt/20180116.csv;


hive -e "
select uid, app, date, group, area_province, area_city, area_district, channel
	from logindb.halllogin_detail  
	where dt = '20180117' and app = 1880053 and group in (66, 88)
" > /opt/20180117.csv;


【报表】熟人游戏明细导出
日期：10月, 11月
导出内容：用户ID，应用ID，日期，省市县
（去重导出）

select aa.uid, aa.app, aa.date, aa.area_province, aa.area_city, aa.area_district
	from logindb.halllogin_detail aa 
	where aa.dt >= '20171001' and aa.dt <= '20171130' and aa.app in(
			1880392,1880221,1880365,1880039,1880014,1880779,1880438,1880759,1880449,1880047,1880756,1880079,1880037,1880017,1880254,1880266,1880784,1880781,
			1880078,1880080,1880052,1880778,1880113,1880450,1880053,1880316,1880421,1880081,1880024,1880234,1880337,1880771,1880109,1880521,1880033)

hive -e "
" > /opt/1201-1231.csv;


房卡游戏的每日登陆人数，不区分游戏，全部总计后根据账号去重。
12月1日-1月25日 数据

select  aa.date, count(distinct aa.uid)
	from logindb.halllogin_detail aa 
	where aa.dt >= '20171201' and aa.dt <= '20180125' and aa.app in(
			1880392,1880221,1880365,1880039,1880014,1880779,1880438,1880759,1880449,1880047,1880756,1880079,1880037,1880017,1880254,1880266,1880784,1880781,
			1880078,1880080,1880052,1880778,1880113,1880450,1880053,1880316,1880421,1880081,1880024,1880234,1880337,1880771,1880109,1880521,1880033)
	group by aa.date;


【报表】鄂尔多斯用户手机号
APPID：1880221
日期：2017年11月26日-2018年1月26日
地区：3102
过滤：充值超过100元的用户和营收类游戏的用户
导出去重用户ID列表
文件名：1880221.3102.csv

mobile：
select ddd.uid
	from
	(select alluids.uid uid
		from 
		(select aa.uid uid
			from logindb.halllogin_detail aa 
			where aa.app = 1880221 and aa.dt >= '20171126' and aa.dt <= '20180126' and aa.group in (6, 66, 8, 88) and area_city='3102'
			group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
  (select uidto
  	from paydb.basic
    group by uidto 
    having sum(money) >= 10000
  ) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;
	
hive -e "
" > /opt/221.mobile.1119.0118.csv;


pc:
select ddd.uid
	from
	(select alluids.uid uid
		from 
		(select aa.uid uid
			from logindb.roomlogin_detail aa 
			where aa.app = 1880221 and aa.dt >= '20171126' and aa.dt <= '20180126' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55) and area_city='3102'
			group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
  (select uidto
  	from paydb.basic
    group by uidto 
    having sum(money) >= 10000
  ) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;



【报表】古田三副游戏登录用户
日期：2017年11月15日-2018年1月15日
APPID：1880239
日期范围内登录的玩家ID
区分PC和移动
请过滤结果中的大R用户和营收类游戏的用户
大R用户:充值超过100元的用户
导出文件：1880239.pc.csv 1880239.mobile.csv

mobile：
select ddd.uid
	from
	(select alluids.uid uid
		from 
		(select aa.uid uid
			from logindb.halllogin_detail aa 
			where aa.app = 1880239 and aa.dt >= '20171115' and aa.dt <= '20180115' and aa.group in (6, 66, 8, 88)
			group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
  (select uidto
  	from paydb.basic
    group by uidto 
    having sum(money) >= 10000
  ) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;
	
hive -e "
" > /opt/1880239.mobile.csv;

pc:
select ddd.uid
	from
	(select alluids.uid uid
		from 
		(select aa.uid uid
			from logindb.roomlogin_detail aa 
			where aa.app = 1880239 and aa.dt >= '20171115' and aa.dt <= '20180115' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
			group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
  (select uidto
  	from paydb.basic
    group by uidto 
    having sum(money) >= 10000
  ) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;
	
hive -e "
" > /opt/1880239.pc.csv;



【报表】同城游pc&移动18年第一批有效召回用户跟踪
日期：2018.01.16-2018.01.22
附件<流失用户召回0109.xlsx>内用户在日期范围内的充值总额、登录天数、所玩游戏
导出文件：18y1.csv
mobile:
select t4.uid,
	   t4.count_date,
	   t4.game,
	   t5.money
	from 
	(select t2.uid uid,
		   t2.count_date count_date,
		   t3.game game
		from 
		(select alluids.uid uid,
			   t1.count_date count_date
			from 
			(select uid 
				from logindb.login_uid 
				WHERE dt='20180109'
				group by uid
			) alluids
			left join
			(select uid,
				   count(distinct date) count_date
				from logindb.halllogin_detail
				where dt >= '20180116' and dt <= '20180122' and group in (6, 66, 8, 88)   
				group by uid
			) t1
			on alluids.uid = t1.uid
		) t2
		left join
		(select uid,
				game
			from logindb.halllogin_detail 
			where dt >= '20180116' and dt <= '20180122' and group in (6, 66, 8, 88)
			group by uid, game
		) t3
		on t2.uid = t3.uid
	) t4
	left join
	(select uidto, 
			sum(money) money
		from paydb.basic
		where paydate >= '20180116' and paydate <= '20180122'
		group by uidto
	) t5
	on t4.uid = t5.uidto
	
hive -e "
" > /opt/18y1_mobile.csv;

	
pc:
select t4.uid,
	   t4.count_date,
	   t4.game,
	   t5.money
	from 
	(select t2.uid uid,
		   t2.count_date count_date,
		   t3.game game
		from 
		(select alluids.uid uid,
			   t1.count_date count_date
			from 
			(select uid 
				from logindb.login_uid 
				WHERE dt='20180109'
				group by uid
			) alluids
			left join
			(select uid,
				   count(distinct date) count_date
				from logindb.roomlogin_detail
				where dt >= '20180116' and dt <= '20180122' and group > 0 and  group not in (6,66,8,88,68,69,55)
				group by uid
			) t1
			on alluids.uid = t1.uid
		) t2
		left join
		(select uid,
				game
			from logindb.roomlogin_detail 
			where dt >= '20180116' and dt <= '20180122' and group > 0 and  group not in (6,66,8,88,68,69,55)
			group by uid, game
		) t3
		on t2.uid = t3.uid
	) t4
	left join
	(select uidto, 
			sum(money) money
		from paydb.basic
		where paydate >= '20180116' and paydate <= '20180122'
		group by uidto
	) t5
	on t4.uid = t5.uidto
	
hive -e "
" > /opt/18y1_pc.csv;


【报表】同城游pc&移动18年第二批有效召回用户跟踪
日期：2018.01.22 - 2018.01.28
附件<流失用户召回1.22.xlsx>内用户在日期范围内的充值总额、所玩游戏
要区分PC和移动
导出文件：18y2.mobile.csv 18y2.pc.csv
mobile:
select t2.uid,
	   t2.game,
	   t3.money
	from 
	(select alluids.uid uid,
			t1.game game
		from 
		(select uid 
			from logindb.login_uid
			WHERE dt='20180122_mobile'
			group by uid
		) alluids
		left join
		(select uid,
				game
			from logindb.halllogin_detail
			where dt >= '20180122' and dt <= '20180128' and group in (6, 66, 8, 88)   
			group by uid, game
		) t1
		on alluids.uid = t1.uid
	) t2
	left join
	(select uidto, 
			sum(money) money
		from paydb.basic
		where paydate >= '20180122' and paydate <= '20180128'
		group by uidto
	) t3
	on t2.uid = t3.uidto		
hive -e "
" > /opt/18y2_mobile.csv;

pc:
select t2.uid,
	   t2.game,
	   t3.money
	from 
	(select alluids.uid uid,
			t1.game game
		from 
		(select uid 
			from logindb.login_uid
			WHERE dt='20180122_pc'
			group by uid
		) alluids
		left join
		(select uid,
				game
			from logindb.roomlogin_detail
			where dt >= '20180122' and dt <= '20180128' and group > 0 and  group not in (6,66,8,88,68,69,55)  
			group by uid, game
		) t1
		on alluids.uid = t1.uid
	) t2
	left join
	(select uidto, 
			sum(money) money
		from paydb.basic
		where paydate >= '20180122' and paydate <= '20180128'
		group by uidto
	) t3
	on t2.uid = t3.uidto
	
hive -e "
" > /opt/18y2_pc.csv;



【报表】斗地主12月房卡版对局数据导出需求
APPID：1880053
数据周期：12月1日-12月31日
报表描述：房卡对局数据
按每天、游戏ID、用户ID去重（比如一个用户在12月1号在斗地主的房卡房间里打了30局，只需提供如下字段）
格式：日期 ， 游戏ID，用户ID，对局数
文件名称：1880053.fk.1201.1231.csv

select date,
		gameid,
		uid,
		count(*)
from gsplaytogetherdb.endbout
where dt >= '20171201' and dt <= '20171231' and app='1880053'
group by uid, gameid, date

hive -e "
" > /opt/1880053.fk.1201.1231.csv;



报表】判断指定用户是否登陆过双扣游戏
gameid：24
日期：20180101-20180131
附件<双扣问卷奖励玩家ID.xlsx>中指定的用户，判断在制定日期内是否登陆过指定游戏，需要在roomlogin和halllogin中都查询一下
导出登陆过的用户ID
导出文件：24.1801.csv

select t1.uid
	from 
	(select uid 
		from logindb.login_uid
		where dt='20180201'
	) t1
	left join
	(select uid
		from logindb.halllogin_detail
		where dt >= '20180101' and dt <= '20180131' and game = '24'
		group by uid
	) t2
	on t1.uid = t2.uid
	where t2.uid is not null
union all
select t1.uid
	from 
	(select uid 
		from logindb.login_uid
		where dt='20180201'
	) t1
	left join
	(select uid
		from logindb.roomlogin_detail
		where dt >= '20180101' and dt <= '20180131' and game = '24'
		group by uid
	) t2
	on t1.uid = t2.uid
	where t2.uid is not null

hive -e "
" > /opt/1880053.fk.1201.1231.csv;



【报表】1月22日-2月4日房卡对局数据导出需求
数据周期：1月22日-2月4日

需求一：房卡进入房间数据
按每天、游戏ID、用户ID去重（比如一个用户在1月1号在打大A(id：1880221)的房卡房间进入了20次，只需提供如下字段）
日期， 游戏ID， 用户ID，进入次数
导出文件：fk01.0122.0204.csv
select date,
	gameid,
	uid,
	count(*)
from gsplaytogetherdb.gameactive
where date >= '20180122' and date <= '20180204'
group by uid, gameid, date

hive -e "
" > /opt/fk01.0122.0204.csv;


需求二：房卡对局数据
按每天、游戏ID、用户ID去重（比如一个用户在1月1号在打大A(id：1880221)的房卡房间里打了30局，只需提供如下字段）
日期，游戏ID，用户ID，对局次数
导出文件：fk02.0122.0204.csv
select date,
	gameid,
	uid,
	count(*)
from gsplaytogetherdb.endbout
where date >= '20180122' and date <= '20180204'
group by uid, gameid, date

hive -e "
" > /opt/fk02.0122.0204.csv;

需求三：房卡新增数据 (沈杰处理)
按每天、游戏ID、用户ID
日期，游戏ID，用户ID
导出文件：fk03.0122.0204.csv


【报表】2月5日-2月11日房卡对局数据导出需求
数据周期：2月5日-2月11日
需求一：房卡进入房间数据
按每天、游戏ID、用户ID去重（比如一个用户在1月1号在打大A的房卡房间进入了20次，只需提供如下字段）
日期， 游戏ID， 用户ID，进入次数
导出文件：fk01.0205.0211.csv
select date,
	gameid,
	uid,
	count(*)
from gsplaytogetherdb.gameactive
where date >= '20180205' and date <= '20180211'
group by uid, gameid, date

hive -e "
" > /opt/fk01.0205.0211.csv

需求二：房卡对局数据
按每天、游戏ID、用户ID去重（比如一个用户在1月1号在打大A的房卡房间里打了30局，只需提供如下字段）
日期，游戏ID，用户ID，对局次数
导出文件：fk02.0205.0211.csv
select date,
	gameid,
	uid,
	count(*)
from gsplaytogetherdb.endbout
where date >= '20180205' and date <= '20180211'
group by uid, gameid, date

hive -e "
" > /opt/fk02.0205.0211.csv


需求三：房卡新增数据 (沈杰处理)
按每天、游戏ID、用户ID

日期，游戏ID，用户ID

导出文件：fk03.0122.0204.csv



【报表】查询指定用户的最后一次登陆时间和注册来源
1. 用户ID列表.xlsx
2. 最后一次登陆日期
3. 用户注册时的APPID以及Group
如果查不到就写0

select t2.uid, 
	   t2.date, 
	   t3.app,
	   t3.group
	from
	(select alluids.uid uid,
		   t1.date date
		from
		(select uid 
			from tempdb.login_uid
			WHERE dt='20180226'
			group by uid
		) alluids
		left join
		(select uid,
				max(date) date
			from logindb.halllogin_detail
			group by uid
		) t1
		on alluids.uid = t1.uid
	) t2
	left join
	(select uid,
			app,
			group
		from regdb.regdetail
	) t3
	on t2.uid = t3.uid


【报表】查询登陆用户ID列表
GAMEID: 354
DATE: 2017年12月25日—2018年2月25日
导出在上述日期范围内登陆过应用的用户ID（去重）

需要区分PC和移动
请过滤结果中的大R用户和营收类游戏的用户
大R用户:充值超过100元的用户

导出文件：
354.mobile.csv
354.pc.csv

hive -e "
" > /opt/fk02.0205.0211.csv

移动端：
select ddd.uid	
	from
	(select alluids.uid uid	
		from
		(select aa.uid uid
		   from logindb.halllogin_detail aa 
		   where aa.game = 354 
		   and aa.dt >= '20171225' and aa.dt <= '20180225' and aa.group in (6, 66, 8, 88)
		   group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
		from paydb.basic
		group by uidto 
		having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;

pc:
select ddd.uid	
	from
	(select alluids.uid uid	
		from
		(select aa.uid uid,
		   from logindb.roomlogin_detail aa 
		   where aa.game = 354 
		   and aa.dt >= '20171225' and aa.dt <= '20180225' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		   group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
		from paydb.basic
		group by uidto 
		having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;

	

【报表】同城游pc&移动18年第三批有效召回用户跟踪
时间范围：2018年01月30日-2018年02月08日
附件<第三期流失用户召回.xlsx>中，指定用户在指定期间期间的充值总额、所玩游戏
ps：PC用户查询PC游戏，移动用户查询移动游戏

导出文件名：
mobile.liushi.csv
pc.liushi.csv

LOAD DATA  INPATH '/user/root/uid01' overwrite  INTO TABLE uid_detail PARTITION (dt='20171201');
LOAD DATA  INPATH '/user/root/uid01' overwrite  INTO TABLE uid_detail PARTITION (dt='20171201');

mobile:
select t2.uid,
	   t2.game,
	   t3.money
	from 
	(select alluids.uid uid,
			t1.game game
		from 
		(select uid 
			from tempdb.login_uid
			WHERE dt='20180228_mobile'
			group by uid
		) alluids
		left join
		(select uid,
				game
			from logindb.halllogin_detail
			where dt >= '20180130' and dt <= '20180208' and group in (6, 66, 8, 88)   
			group by uid, game
		) t1
		on alluids.uid = t1.uid
	) t2
	left join
	(select uidto, 
			sum(money) money
		from paydb.basic
		where paydate >= '20180130' and paydate <= '20180208'
		group by uidto
	) t3
	on t2.uid = t3.uidto

hive -e "
" > /opt/mobile.liushi.csv;

pc:
select t2.uid,
	   t2.game,
	   t3.money
	from 
	(select alluids.uid uid,
			t1.game game
		from 
		(select uid 
			from tempdb.login_uid
			WHERE dt='20180228_pc'
			group by uid
		) alluids
		left join
		(select uid,
				game
			from logindb.roomlogin_detail
			where dt >= '20180130' and dt <= '20180208' and group > 0 and  group not in (6,66,8,88,68,69,55)  
			group by uid, game
		) t1
		on alluids.uid = t1.uid
	) t2
	left join
	(select uidto, 
			sum(money) money
		from paydb.basic
		where paydate >= '20180130' and paydate <= '20180208'
		group by uidto
	) t3
	on t2.uid = t3.uidto
	
hive -e "
" > /opt/pc.liushi.csv;


【报表】同城游单包新增用户进入游戏数据情况（2月14日-2月24日）
注册渠道ID
126924364
154907551
184066339
150079879
183891956
注册日期：
2018年2月14日 - 2018年2月24日
查询指定渠道下，指定注册日期内注册的用户，在当日玩过哪些同城游app的游戏（用group in（66，88）判断同城游app的游戏），并分析得出统计结果

channel.3001.games.csv

日期         渠道id       游戏id    用户数量
20180214     126924364    1880234   24546
select alluids.date, 
	   alluids.channel,
	   t1.game,
	   count(alluids.uid)
	from
	(select uid, date, channel
		from regdb.regdetail 
		where date >= '20180214' and date <= '20180224' and channel in (126924364, 154907551, 184066339, 150079879, 183891956)
	) alluids
	left join
	(select uid,
			date,
		   game
		from logindb.halllogin_detail
		where dt >= '20180214' and date <= '20180224' and group in (66, 88)
		group by uid, date, game
	) t1
	on alluids.uid = t1.uid and alluids.date = t1.date
	group by alluids.date, alluids.channel, t1.game


【报表】同城游pc&移动18年第三批有效召回用户跟踪
时间范围：2018年01月22日-2018年02月04日 
		  2018年01月30日-2018年02月12日

附件<1月短信召回数据拉取名单.xlsx>中，指定用户在指定期间期间的充值总额、充值方式，充值次数，所玩游戏，
指定期间的pc用户登录pc大厅的次数，移动用户登录同城游app（3001）的次数。
附件内用户的累计充值总额。

ps：PC用户查询PC游戏，移动用户查询移动游戏

导出文件名：
pc.liushifenxi.csv
mobile.liushifenxi.csv
LOAD DATA  INPATH '/user/root/20180122_0204_mobile' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='20180122_0204_mobile');
LOAD DATA  INPATH '/user/root/20180122_0204_pc' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='20180122_0204_pc');
LOAD DATA  INPATH '/user/root/20180130_0212_mobile' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='20180130_0212_mobile');
LOAD DATA  INPATH '/user/root/20180130_0212_pc' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='20180130_0212_pc');


mobile:
select t2.uid,
	   t2.app_cishu,
	   t2.game,
	   t5.cishu,
	   t5.money,
	   t5.payway
	from 
	(select alluids.uid uid,
			tt.app_cishu app_cishu,
			tt.game game
		from 
		(select uid 
			from tempdb.login_uid
			WHERE dt='20180122_0204_mobile'
			group by uid
		) alluids
		left join
		(select hd.uid uid, 
				hd.app_cishu app_cishu, 
				t1.game game
			from
			(select uid,
					count(app) app_cishu
				from logindb.halllogin_detail
				where dt >= '20180122' and dt <= '20180204' and group in (6, 66, 8, 88) and app = 3001
				group by uid
			) hd
			left join
			(select uid,
					game
				from logindb.halllogin_detail
				where dt >= '20180122' and dt <= '20180204' and group in (6, 66, 8, 88)   
				group by uid, game
			) t1
			on hd.uid = t1.uid
		) tt
		on alluids.uid = tt.uid
		
	) t2
	left join
	(select t3.uidto uidto, t3.cishu cishu, t3.money money, t4.payway payway
		from
		(select uidto,
				count(money) cishu,
				sum(money) money
			from paydb.basic
			where paydate >= '20180122' and paydate <= '20180204'
			group by uidto
		) t3
		left join
		(select uidto, payway
			from paydb.basic
			where paydate >= '20180122' and paydate <= '20180204'
			group by uidto, payway
		) t4
		on t3.uidto = t4.uidto
	) t5
	on t2.uid = t5.uidto


pc:
select t2.uid,
	   t2.app_cishu,
	   t2.game,
	   t5.cishu,
	   t5.money,
	   t5.payway
	from 
	(select alluids.uid uid,
			tt.app_cishu app_cishu,
			tt.game game
		from 
		(select uid 
			from tempdb.login_uid
			WHERE dt='20180122_0204_pc'
			group by uid
		) alluids
		left join
		(select hd.uid uid, 
				hd.app_cishu app_cishu, 
				t1.game game
			from
			(select uid,
					count(*) app_cishu
				from logindb.roomlogin_detail
				where dt >= '20180122' and dt <= '20180204' and group > 0 and  group not in (6,66,8,88,68,69,55)
				group by uid
			) hd
			left join
			(select uid,
					game
				from logindb.roomlogin_detail
				where dt >= '20180122' and dt <= '20180204' and group > 0 and  group not in (6,66,8,88,68,69,55)
				group by uid, game
			) t1
			on hd.uid = t1.uid
		) tt
		on alluids.uid = tt.uid
		
	) t2
	left join
	(select t3.uidto uidto, t3.cishu cishu, t3.money money, t4.payway payway
		from
		(select uidto,
				count(money) cishu,
				sum(money) money
			from paydb.basic
			where paydate >= '20180122' and paydate <= '20180204'
			group by uidto
		) t3
		left join
		(select uidto, payway
			from paydb.basic
			where paydate >= '20180122' and paydate <= '20180204'
			group by uidto, payway
		) t4
		on t3.uidto = t4.uidto
	) t5
	on t2.uid = t5.uidto


	
2018年01月30日-2018年02月12日	
mobile:
select t2.uid,
	   t2.app_cishu,
	   t2.game,
	   t5.cishu,
	   t5.money,
	   t5.payway
	from 
	(select alluids.uid uid,
			tt.app_cishu app_cishu,
			tt.game game
		from 
		(select uid 
			from tempdb.login_uid
			WHERE dt='20180130_0212_mobile'
			group by uid
		) alluids
		left join
		(select hd.uid uid, 
				hd.app_cishu app_cishu, 
				t1.game game
			from
			(select uid,
					count(app) app_cishu
				from logindb.halllogin_detail
				where dt >= '20180130' and dt <= '20180212' and group in (6, 66, 8, 88) and app = 3001
				group by uid
			) hd
			left join
			(select uid,
					game
				from logindb.halllogin_detail
				where dt >= '20180130' and dt <= '20180212' and group in (6, 66, 8, 88)   
				group by uid, game
			) t1
			on hd.uid = t1.uid
		) tt
		on alluids.uid = tt.uid
		
	) t2
	left join
	(select t3.uidto uidto, t3.cishu cishu, t3.money money, t4.payway payway
		from
		(select uidto,
				count(money) cishu,
				sum(money) money
			from paydb.basic
			where paydate >= '20180130' and paydate <= '20180212'
			group by uidto
		) t3
		left join
		(select uidto, payway
			from paydb.basic
			where paydate >= '20180130' and paydate <= '20180212'
			group by uidto, payway
		) t4
		on t3.uidto = t4.uidto
	) t5
	on t2.uid = t5.uidto


pc:
select t2.uid,
	   t2.app_cishu,
	   t2.game,
	   t5.cishu,
	   t5.money,
	   t5.payway
	from 
	(select alluids.uid uid,
			tt.app_cishu app_cishu,
			tt.game game
		from 
		(select uid 
			from tempdb.login_uid
			WHERE dt='20180130_0212_pc'
			group by uid
		) alluids
		left join
		(select hd.uid uid, 
				hd.app_cishu app_cishu, 
				t1.game game
			from
			(select uid,
					count(*) app_cishu
				from logindb.roomlogin_detail
				where dt >= '20180130' and dt <= '20180212' and group > 0 and  group not in (6,66,8,88,68,69,55)
				group by uid
			) hd
			left join
			(select uid,
					game
				from logindb.roomlogin_detail
				where dt >= '20180130' and dt <= '20180212' and group > 0 and  group not in (6,66,8,88,68,69,55)
				group by uid, game
			) t1
			on hd.uid = t1.uid
		) tt
		on alluids.uid = tt.uid
	) t2
	left join
	(select t3.uidto uidto, t3.cishu cishu, t3.money money, t4.payway payway
		from
		(select uidto,
				count(money) cishu,
				sum(money) money
			from paydb.basic
			where paydate >= '20180130' and paydate <= '20180212'
			group by uidto
		) t3
		left join
		(select uidto, payway
			from paydb.basic
			where paydate >= '20180130' and paydate <= '20180212'
			group by uidto, payway
		) t4
		on t3.uidto = t4.uidto
	) t5
	on t2.uid = t5.uidto



【报表】【打电话】打鬼子经典版手机号码导出
GAMEID：215
日期：2018年01月28日-2018年02月28日

请过滤结果中的大R用户和营收类游戏的用户
用户ID、手机号码、最后登录时间
并区分PC端和移动端

215.pc.telephone.csv
215.mobile.telephone.csv

移动端：
select ddd.uid,
	   ddd.date
	from
	(select alluids.uid uid,
			alluids.date date
		from
		(select t.uid uid,
				t.date date
			from
			(select aa.uid uid,
					aa.date date,
					row_number() over(partition by aa.uid order by aa.date desc) as rn
			   from logindb.halllogin_detail aa
			   where aa.game = 215 
			   and aa.dt >= '20180128' and aa.dt <= '20180228' and aa.group in (6, 66, 8, 88)
			) t
			where rn = 1
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
		from paydb.basic
		group by uidto 
		having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;

pc:
select ddd.uid,
	   ddd.date
	from
	(select alluids.uid uid,
			alluids.date date
		from
		(select t.uid uid,
				t.date date
			from
			(select aa.uid uid,
					aa.date date,
					row_number() over(partition by aa.uid order by aa.date desc) as rn
			   from logindb.roomlogin_detail aa 
			   where aa.game = 215 
			   and aa.dt >= '20180128' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
			) t
			where rn = 1
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
		from paydb.basic
		group by uidto 
		having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;


【报表】【发短信】短信和电话邀请 雷州升级1880108
GAMEID：108
日期：2018年2月1日-2018年2月28日

请过滤结果中的大R用户和营收类游戏的用户
登陆的玩家的ID+手机号+最近1个月登录天数
并区分PC端和移动端

108.pc.telephone.csv
108.mobile.telephone.csv

移动端：
select ddd.uid,
	   ddd.count_date
	from
	(select alluids.uid uid,
			alluids.count_date count_date
		from
		(select aa.uid uid,
				count(distinct aa.date) count_date
		   from logindb.halllogin_detail aa 
		   where aa.game = 108 
		   and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group in (6, 66, 8, 88)
		   group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
		from paydb.basic
		group by uidto 
		having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;


pc：
select ddd.uid,
	   ddd.count_date
	from
	(select alluids.uid uid,
			alluids.count_date count_date
		from
		(select aa.uid uid,
				count(distinct aa.date) count_date
		   from logindb.roomlogin_detail aa 
		   where aa.game = 108 
		   and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		   group by aa.uid
		) alluids
		left join
		(select distinct hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
		UNION ALL
		select distinct rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
		) yinshou	
		on alluids.uid = yinshou.uid
		where yinshou.uid is null
	) ddd
	left join
	(select uidto
		from paydb.basic
		group by uidto 
		having sum(money) >= 10000
	) ba
	on ddd.uid = ba.uidto
	where ba.uidto is null;



报表】【发短信】短信和电话邀请 马队 1880673
GAMEID：673
日期：2018年2月1日-2018年2月28日

请过滤结果中的大R用户和营收类游戏的用户
并区分PC端和移动端
用户id   手机号码    最近一个月登录天数   最后登录时间
673.pc.telephone.csv
673.mobile.telephone.csv

移动端：
select ddd.uid,
	   ddd.date,
	   ddd.count_date
	from
	(select t1.uid uid,
			t1.date date,
			t1.count_date count_date
		from
		(select alluids.uid uid,
			    alluids.date date,
			    tt.count_date count_date
			from
			(select t.uid uid,
					t.date date
				from
				(select aa.uid uid,
						aa.date date,
						row_number() over(partition by aa.uid order by aa.date desc) as rn
					from logindb.halllogin_detail aa 
					where aa.game = 673 
					and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group in (6, 66, 8, 88)
				) t
				where rn = 1
			) alluids
			left join
			(select aa.uid uid,
					count(distinct aa.date) count_date
				from logindb.halllogin_detail aa 
				where aa.game = 673 
				and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group in (6, 66, 8, 88)
				group by aa.uid
			) tt
			on alluids.uid = tt.uid
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
	where ba.uidto is null;
	

pc：
select ddd.uid,
	   ddd.date,
	   ddd.count_date
	from
	(select t1.uid uid,
			t1.date date,
			t1.count_date count_date
		from
		(select alluids.uid uid,
			    alluids.date date,
			    tt.count_date count_date
			from
			(select t.uid uid,
					t.date date
				from
				(select aa.uid uid,
						aa.date date,
						row_number() over(partition by aa.uid order by aa.date desc) as rn
					from logindb.roomlogin_detail aa 
					where aa.game = 673 
					and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				) t
				where rn = 1
			) alluids
			left join
			(select aa.uid uid,
					count(distinct aa.date) count_date
				from logindb.roomlogin_detail aa 
				where aa.game = 673 
				and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid
			) tt
			on alluids.uid = tt.uid
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
	where ba.uidto is null;
	

【报表】【发短信】短信和电话邀请 【地区】河南新乡 2215  area_city='2215'
**登录地区： 河南新乡 2215
日期：2018年2月1日-2018年2月28日
请过滤结果中的大R用户和营收类游戏的用户
并区分PC端和移动端
用户id   手机号码    最近一个月登录天数   最后登录时间
2215.pc.telephone.csv
2215.mobile.telephone.csv

移动端：
select ddd.uid,
	   ddd.date,
	   ddd.count_date
	from
	(select t1.uid uid,
			t1.date date,
			t1.count_date count_date
		from
		(select alluids.uid uid,
			    alluids.date date,
			    tt.count_date count_date
			from
			(select t.uid uid,
					t.date date
				from
				(select aa.uid uid,
						aa.date date,
						row_number() over(partition by aa.uid order by aa.date desc) as rn
					from logindb.halllogin_detail aa 
					where aa.area_city = '2215'
					and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group in (6, 66, 8, 88)
				) t
				where rn = 1
			) alluids
			left join
			(select aa.uid uid,
					count(distinct aa.date) count_date
				from logindb.halllogin_detail aa 
				where aa.area_city = '2215'
				and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group in (6, 66, 8, 88)
				group by aa.uid
			) tt
			on alluids.uid = tt.uid
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
	where ba.uidto is null;


pc：
select ddd.uid,
	   ddd.date,
	   ddd.count_date
	from
	(select t1.uid uid,
			t1.date date,
			t1.count_date count_date
		from
		(select alluids.uid uid,
			    alluids.date date,
			    tt.count_date count_date
			from
			(select t.uid uid,
					t.date date
				from
				(select aa.uid uid,
						aa.date date,
						row_number() over(partition by aa.uid order by aa.date desc) as rn
					from logindb.roomlogin_detail aa 
					where aa.area_city = '2215'
					and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				) t
				where rn = 1
			) alluids
			left join
			(select aa.uid uid,
					count(distinct aa.date) count_date
				from logindb.roomlogin_detail aa 
				where aa.area_city = '2215' 
				and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid
			) tt
			on alluids.uid = tt.uid
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
	where ba.uidto is null;


【报表】充值用户的分析	
数据需求：最后一次登录时间、注册时间、17年起首次充值时间、如没有请导出截止目前该用户号上银子总额、17年最后一次充值时间
从17年开始查询
附件内有三份用户id，需求字段分别如下
连续充值用户：
最后登录时间   注册时间  首次充值时间    有充值的月份

1月
最后登录时间   注册时间  17年首次充值时间   17年最后一次充值时间

2月
最后登录时间   注册时间  17年首次充值时间   17年最后一次充值时间

LOAD DATA  INPATH '/user/root/lianxuchongzhi' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='lianxuchongzhi');
LOAD DATA  INPATH '/user/root/yiyue' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='yiyue');
LOAD DATA  INPATH '/user/root/eryue' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='eryue');

select t5.uid uid,
	   t5.zhuce_date zhuce_date,
	   t5.final_date final_date,
	   t5.begin_paydate begin_paydate,
	   t6.paydate paydate
	from
	(select t3.uid uid,
		   t3.zhuce_date zhuce_date,
		   t3.final_date final_date,
		   t4.paydate begin_paydate
		from 
		(select t1.uid uid,
			   t1.zhuce_date zhuce_date,
			   t2.final_date final_date
			from
			(select alluids.uid uid,
				   rd.date zhuce_date
				from
				(select uid 
					from tempdb.login_uid
					WHERE dt='lianxuchongzhi'
					group by uid
				) alluids
				left join
				(select uid,
						date
					from regdb.regdetail
				) rd
				on alluids.uid = rd.uid
			) t1
			left join
			(select t.uid uid,
					t.date final_date
				from
				(select aa.uid uid,
						aa.date date,
						row_number() over(partition by aa.uid order by aa.date desc) as rn
					from logindb.halllogin_detail aa 
					where aa.dt >= '20170101'
				) t
				where rn = 1
			) t2
			on t1.uid = t2.uid
		) t3
		left join
		(select ba.uidto uidto,
				ba.paydate paydate
			from
			(select uidto, 
					paydate,
					row_number() over(partition by uidto order by paydate) as rn
				from paydb.basic
				where paydate >= '20170101'
			) ba
			where rn = 1
		) t4
		on t3.uid = t4.uidto
	) t5
	left join
	(select uidto, 
			month(concat(substr(paydate,1,4),'-',substr(paydate,5,2),'-',substr(paydate,7,2))) paydate
		from paydb.basic
		where paydate >= '20170101' 
		group by uidto, paydate
	) t6
	on t5.uid = t6.uidto
	group by uid, zhuce_date,final_date,begin_paydate, paydate

hive -e "
" > /opt/lianxuchongzhi.csv;	
	

1月
最后登录时间   注册时间  17年首次充值时间   17年最后一次充值时间
select t5.uid,
	   t5.zhuce_date,
	   t5.final_date,
	   t5.begin_paydate,
	   t6.final_paydate
	from
	(select t3.uid uid,
		   t3.zhuce_date zhuce_date,
		   t3.final_date final_date,
		   t4.paydate begin_paydate
		from 
		(select t1.uid uid,
			   t1.zhuce_date zhuce_date,
			   t2.final_date final_date
			from
			(select alluids.uid uid,
				   rd.date zhuce_date
				from
				(select uid 
					from tempdb.login_uid
					WHERE dt='yiyue'
					group by uid
				) alluids
				left join
				(select uid,
						date
					from regdb.regdetail
				) rd
				on alluids.uid = rd.uid
			) t1
			left join
			(select t.uid uid,
					t.date final_date
				from
				(select aa.uid uid,
						aa.date date,
						row_number() over(partition by aa.uid order by aa.date desc) as rn
					from logindb.halllogin_detail aa 
					where aa.dt >= '20170101'
				) t
				where rn = 1
			) t2
			on t1.uid = t2.uid
		) t3
		left join
		(select ba.uidto uidto,
				ba.paydate paydate
			from
			(select uidto, 
					paydate,
					row_number() over(partition by uidto order by paydate) as rn
				from paydb.basic
				where paydate >= '20170101' and paydate <= '20171231'
			) ba
			where rn = 1
		) t4
		on t3.uid = t4.uidto
	) t5
	left join
	(select ba1.uidto uidto,
			ba1.paydate final_paydate
		from
		(select uidto, 
				paydate,
				row_number() over(partition by uidto order by paydate desc) as rn
			from paydb.basic
			where paydate >= '20170101' and paydate <= '20171231'
		) ba1
		where rn = 1
	) t6
	on t5.uid = t6.uidto

hive -e "
" > /opt/yiyue.csv;

2月
最后登录时间   注册时间  17年首次充值时间   17年最后一次充值时间
select t5.uid,
	   t5.zhuce_date,
	   t5.final_date,
	   t5.begin_paydate,
	   t6.final_paydate
	from
	(select t3.uid uid,
		   t3.zhuce_date zhuce_date,
		   t3.final_date final_date,
		   t4.paydate begin_paydate
		from 
		(select t1.uid uid,
			   t1.zhuce_date zhuce_date,
			   t2.final_date final_date
			from
			(select alluids.uid uid,
				   rd.date zhuce_date
				from
				(select uid 
					from tempdb.login_uid
					WHERE dt='eryue'
					group by uid
				) alluids
				left join
				(select uid,
						date
					from regdb.regdetail
				) rd
				on alluids.uid = rd.uid
			) t1
			left join
			(select t.uid uid,
					t.date final_date
				from
				(select aa.uid uid,
						aa.date date,
						row_number() over(partition by aa.uid order by aa.date desc) as rn
					from logindb.halllogin_detail aa 
					where aa.dt >= '20170101'
				) t
				where rn = 1
			) t2
			on t1.uid = t2.uid
		) t3
		left join
		(select ba.uidto uidto,
				ba.paydate paydate
			from
			(select uidto, 
					paydate,
					row_number() over(partition by uidto order by paydate) as rn
				from paydb.basic
				where paydate >= '20170101' and paydate <= '20171231'
			) ba
			where rn = 1
		) t4
		on t3.uid = t4.uidto
	) t5
	left join
	(select ba1.uidto uidto,
			ba1.paydate final_paydate
		from
		(select uidto, 
				paydate,
				row_number() over(partition by uidto order by paydate desc) as rn
			from paydb.basic
			where paydate >= '20170101' and paydate <= '20171231'
		) ba1
		where rn = 1
	) t6
	on t5.uid = t6.uidto

hive -e "
" > /opt/eryue.csv;


【报表】同城游单包新增用户进入游戏数据情况（2月14日-2月24日）
注册渠道id:196384529
注册日期：20180214-20180224
查询指定渠道下，指定注册日期内注册的用户，在当日玩过哪些同城游app的游戏（用group in（66，88）判断同城游app的游戏），并分析得出统计结果
日期    渠道id    游戏id    用户数量
196384529.3001.games.csv

select alluids.date, 
	   alluids.channel,
	   t1.game,
	   count(alluids.uid)
	from
	(select uid, 
			date, 
			channel
		from regdb.regdetail 
		where date >= '20180214' and date <= '20180224' and channel in (196384529)
	) alluids
	left join
	(select uid,
			date,
		    game
		from logindb.halllogin_detail
		where dt >= '20180214' and date <= '20180224' and group in (66, 88)
		group by uid, date, game
	) t1
	on alluids.uid = t1.uid and alluids.date = t1.date
	group by alluids.date, alluids.channel, t1.game

hive -e "

" > /opt/eryue.csv;


【报表】【发短信】短信和电话邀请 【地区】广东-茂名 1015
**登录地区： 广东-茂名 1015
日期：2018年2月1日-2018年2月28日

请过滤结果中的大R用户和营收类游戏的用户
并区分PC端和移动端
用户id    手机号码

移动端：
select ddd.uid uid
	from 
	(select t1.uid uid
		from 
		(select aa.uid uid
			from logindb.halllogin_detail aa 
			where aa.area_city = '1015'
			and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group in (6, 66, 8, 88)
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
	where ba.uidto is null;

pc：
select ddd.uid uid
	from 
	(select t1.uid uid
		from 
		(select aa.uid uid
			from logindb.halllogin_detail aa 
			where aa.area_city = '1015'
			and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
			GROUP BY uid
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
	where ba.uidto is null;


select ddd.uid,
	   ddd.date,
	   ddd.count_date
	from
	(select t1.uid uid,
			t1.date date,
			t1.count_date count_date
		from
		(select alluids.uid uid,
			    alluids.date date,
			    tt.count_date count_date
			from
			(select t.uid uid,
					t.date date
				from
				(select aa.uid uid,
						aa.date date,
						row_number() over(partition by aa.uid order by aa.date desc) as rn
					from logindb.roomlogin_detail aa 
					where aa.area_city = '2215'
					and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				) t
				where rn = 1
			) alluids
			left join
			(select aa.uid uid,
					count(distinct aa.date) count_date
				from logindb.roomlogin_detail aa 
				where aa.area_city = '2215' 
				and aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid
			) tt
			on alluids.uid = tt.uid
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
	where ba.uidto is null;



手机号】18年来用户信息
1.过滤大R和营收类游戏用户。
2.分为4+4共计8张表导出，
3.导出时间及范围：2018年1月1日-2月28日 登录用户
A1：1月PC登录用户手机号
用户id   手机号码
select ddd.uid uid,
	   ddd.telephone telephone
	from
	(select tt.uid uid,
			tt.telephone telephone
		from
		(select t1.uid uid,
				t2.telephone telephone
			from
			(select aa.uid uid
				from logindb.roomlogin_detail aa 
				where aa.dt >= '20180101' and aa.dt <= '20180131' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by uid
			) t1
			left join
			(select uid, 
				   telephone 
				from tempdb.uid_telephone 
				where dt='20180315'
				group by uid, telephone
			) t2
			on t1.uid = t2.uid
			where t2.uid is not null
		) tt
		left join
		(select hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
			group by uid
		UNION ALL
		select rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
			group by uid
		) yinshou	
		on tt.uid = yinshou.uid
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
	
hive -e "
" > /opt/pc.1.telephone.csv;


A2：1月PC登录用户信息（注：用户均为有手机号的用户。）
用户id    游戏名称    游戏id    登录地区
select ddd.uid uid,
	   ddd.game game,
	   ddd.area_city area_city
	from
	(select tt.uid uid,
			tt.game game,
			tt.area_city area_city
		from
		(select t1.uid uid,
			    t1.game game,
				t1.area_city area_city
			from
			(select aa.uid uid,
					aa.game game,
					aa.area_city area_city
				from logindb.roomlogin_detail aa 
				where aa.dt >= '20180101' and aa.dt <= '20180131' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by uid, game, area_city
			) t1
			left join
			(select uid
				from tempdb.uid_telephone 
				where dt='20180315'
				group by uid
			) t2
			on t1.uid = t2.uid
			where t2.uid is not null
		) tt
		left join
		(select hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
			group by uid
		UNION ALL
		select rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
			group by uid
		) yinshou	
		on tt.uid = yinshou.uid
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
hive -e "

" > /opt/pc.1.information.csv;


B1：2月PC登录用户手机号
用户id   手机号码
select ddd.uid uid,
	   ddd.telephone telephone
	from
	(select tt.uid uid,
			tt.telephone telephone
		from
		(select t1.uid uid,
				t2.telephone telephone
			from
			(select aa.uid uid
				from logindb.roomlogin_detail aa 
				where aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by uid
			) t1
			left join
			(select uid, 
				   telephone 
				from tempdb.uid_telephone 
				where dt='20180315'
				group by uid, telephone
			) t2
			on t1.uid = t2.uid
			where t2.uid is not null
		) tt
		left join
		(select hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
			group by uid
		UNION ALL
		select rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
			group by uid
		) yinshou	
		on tt.uid = yinshou.uid
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

hive -e "

" > /opt/pc.2.telephone.csv;

B2：2月PC登录用户信息（注：用户均为有手机号的用户。）
用户id    游戏名称    游戏id    登录地区
select ddd.uid uid,
	   ddd.game game,
	   ddd.area_city area_city
	from
	(select tt.uid uid,
			tt.game game,
			tt.area_city area_city
		from
		(select t1.uid uid,
			    t1.game game,
				t1.area_city area_city
			from
			(select aa.uid uid,
					aa.game game,
					aa.area_city area_city
				from logindb.roomlogin_detail aa 
				where aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by uid, game, area_city
			) t1
			left join
			(select uid
				from tempdb.uid_telephone 
				where dt='20180315'
				group by uid
			) t2
			on t1.uid = t2.uid
			where t2.uid is not null
		) tt
		left join
		(select hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
			group by uid
		UNION ALL
		select rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
			group by uid
		) yinshou	
		on tt.uid = yinshou.uid
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

hive -e "

" > /opt/pc.2.information.csv;

	
C1：1月移动端登录用户手机号
用户id   手机号码

移动端：
select ddd.uid uid,
	   ddd.telephone telephone
	from
	(select tt.uid uid,
			tt.telephone telephone
		from
		(select t1.uid uid,
				t2.telephone telephone
			from
			(select aa.uid uid
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180101' and aa.dt <= '20180131' and aa.group in (6, 66, 8, 88)
				group by uid
			) t1
			left join
			(select uid, 
				    telephone 
				from tempdb.uid_telephone 
				where dt='20180315'
				group by uid, telephone
			) t2
			on t1.uid = t2.uid
			where t2.uid is not null
		) tt
		left join
		(select hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
			group by uid
		UNION ALL
		select rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
			group by uid
		) yinshou	
		on tt.uid = yinshou.uid
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

hive -e "

" > /opt/mobile.1.telephone.csv;

	
C2：1月移动端登录用户信息（注：用户均为有手机号的用户。）
用户id    游戏名称    游戏id    登录地区
select ddd.uid uid,
	   ddd.game game,
	   ddd.area_city area_city
	from
	(select tt.uid uid,
			tt.game game,
			tt.area_city area_city
		from
		(select t1.uid uid,
			    t1.game game,
				t1.area_city area_city
			from
			(select aa.uid uid,
					aa.game game,
					aa.area_city area_city
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180101' and aa.dt <= '20180131' and aa.group in (6, 66, 8, 88)
				group by uid, game, area_city
			) t1
			left join
			(select uid
				from tempdb.uid_telephone 
				where dt='20180315'
				group by uid
			) t2
			on t1.uid = t2.uid
			where t2.uid is not null
		) tt
		left join
		(select hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
			group by uid
		UNION ALL
		select rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
			group by uid
		) yinshou	
		on tt.uid = yinshou.uid
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

hive -e "

" > /opt/mobile.1.information.csv;

D1：2月移动端登录用户手机号
用户id   手机号码
select ddd.uid uid,
	   ddd.telephone telephone
	from
	(select tt.uid uid,
			tt.telephone telephone
		from
		(select t1.uid uid,
				t2.telephone telephone
			from
			(select aa.uid uid
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group in (6, 66, 8, 88)
				group by uid
			) t1
			left join
			(select uid, 
				   telephone 
				from tempdb.uid_telephone 
				where dt='20180315'
				group by uid, telephone
			) t2
			on t1.uid = t2.uid
			where t2.uid is not null
		) tt
		left join
		(select hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
			group by uid
		UNION ALL
		select rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
			group by uid
		) yinshou	
		on tt.uid = yinshou.uid
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
hive -e "

" > /opt/mobile.2.telephone.csv;

D2：2月移动端登录用户信息（注：用户均为有手机号的用户。）
用户id    游戏名称    游戏id    登录地区
select ddd.uid uid,
	   ddd.game game,
	   ddd.area_city area_city
	from
	(select tt.uid uid,
			tt.game game,
			tt.area_city area_city
		from
		(select t1.uid uid,
			    t1.game game,
				t1.area_city area_city
			from
			(select aa.uid uid,
					aa.game game,
					aa.area_city area_city
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180201' and aa.dt <= '20180228' and aa.group in (6, 66, 8, 88)
				group by uid, game, area_city
			) t1
			left join
			(select uid
				from tempdb.uid_telephone 
				where dt='20180315'
				group by uid
			) t2
			on t1.uid = t2.uid
			where t2.uid is not null
		) tt
		left join
		(select hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
			group by uid
		UNION ALL
		select rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
			group by uid
		) yinshou	
		on tt.uid = yinshou.uid
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

hive -e "

" > /opt/mobile.2.information.csv;


导出后命名表格：
pc.1.telephone.csv
pc.1.information.csv
pc.2.telephone.csv
pc.2.information.csv
mobile.1.telephone.csv
mobile.1.information.csv
mobile.2.telephone.csv
mobile.2.information.csv


【明细导出】打大A的杭州注册，登录用户
1.时间：2018年3月7日-3月13日
2.应用：打大A 1880221
3.导出数据：注册明细，登录明细。

注册用户明细：用户序号、游戏ID、应用ID、应用名称、大厅ID、大厅名称、渠道号、渠道名称、所属渠道标签、推广码、总游戏时长、总大厅时长、设备重复注册、设备号、账号注册时间、账号注册IP、账号注册地区、设备注册地区。

登录用户明细：登陆时间、用户序号、游戏ID、应用ID、应用名称、大厅ID、大厅名称、渠道号、渠道名称、所属渠道标签、推广码、设备号、登陆IP、登陆地区。


uid int,  用户序号
date int, 注册时间
time int,注册时间
ip string, 注册ip
group int 大厅ID、
game int,  游戏id
app bigint,  应用id
hard1st string,  设备重复注册 (true 非重复))
hardid string,  设备号
channel int,    渠道号
promcode int,   推广码、
province string,   账号注册地区
city string,       账号注册地区
district string,   账号注册地区
hardarea_province string, 设备注册地区
hardarea_city string,  设备注册地区
hardarea_district string,  设备注册地区
mobilehard_imei string,
mobilehard_imsi string,
mobilehard_sim string,
mobilehard_system string,
mobilehard_wifi string)

应用名称    大厅名称   渠道名称、所属渠道标签 
总游戏时长、总大厅时长    去掉?



【流失用户】1月流失用户导出
1. 用户ID请区分移动和pc
   移动端只记录同城游app    app = 3001
   PC端只记录大厅客户端     aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
2.导出用户群：
pc:1月1日-1月31登录的用户，连续27天未登录的用户
移动:1月1日-1月31登录的用户，连续15天未登录的用户


3.获取到流失的用户id后，导出以下的登录相关字段

|用户id|1月3001登录天数|1月3001登陆次数|大厅在线时长| 玩过那些游戏 |17年充值总金额|1月的充值金额|1月充值笔数|
导出后表格命名字段
pc.3001.csv
mobile.3001.csv
pc.1.game.csv
mobile.1.game.csv

移动端：
select t4.uid,
	   t4.login_cishu,
	   t4.count_date,
	   t4.game,
	   t4.money,
	   t5.1_money,
	   t5.1_chongzhi_cishu
	from
	(select t2.uid uid,
		   t2.login_cishu login_cishu,
		   t2.count_date count_date,
		   t2.game game,
		   t3.money money
		from
		(select t0.uid uid,
			   t0.login_cishu login_cishu,
			   t0.count_date count_date,
			   t1.game game
			from
			(select hd.uid uid,
					count(hd.date) login_cishu,
					count(distinct hd.date) count_date,
					max(concat(substr(hd.date,1,4),'-',substr(hd.date,5,2),'-',substr(hd.date,7,2))) date
				from logindb.halllogin_detail hd  
				where hd.dt>='20180101' and hd.dt<='20180131' 
				and hd.app=3001
				group by hd.uid HAVING datediff(current_date(),date) >= 15
			) t0
			left join
			(select aa.uid uid,
					collect_set(game) game
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180101' and aa.dt <= '20180131' and group in (6, 66, 8, 88)
				group by aa.uid
			) t1
			on t0.uid = t1.uid
		) t2
		left join
		(select uidto, 
				sum(money) money
			from paydb.basic
			where paydate>='20170101'
			group by uidto
		) t3
		on t2.uid = t3.uidto
	) t4
	left join
	(select uidto, 
			sum(money) 1_money,
			count(money) 1_chongzhi_cishu
		from paydb.basic
		where paydate>='20180101' and paydate>='20180131'
		group by uidto
	) t5
	on t4.uid = t5.uidto

hive -e "

" > /opt/mobile.1.game.csv;
	
	
pc：
select t4.uid,
	   t4.login_cishu,
	   t4.count_date,
	   t4.game,
	   t4.money,
	   t5.1_money,
	   t5.1_chongzhi_cishu
	from
	(select t2.uid uid,
		   t2.login_cishu login_cishu,
		   t2.count_date count_date,
		   t2.game game,
		   t3.money money
		from
		(select t0.uid uid,
			   t0.login_cishu login_cishu,
			   t0.count_date count_date,
			   t1.game game
			from
			(select hd.uid uid,
					count(hd.date) login_cishu,
					count(distinct hd.date) count_date,
					max(concat(substr(hd.date,1,4),'-',substr(hd.date,5,2),'-',substr(hd.date,7,2))) date
				from logindb.roomlogin_detail hd  
				where hd.dt>='20180101' and hd.dt<='20180131' 
				and hd.group > 0 and  hd.group not in (6,66,8,88,68,69,55)
				group by hd.uid HAVING datediff(current_date(),date) >= 27
			) t0
			left join
			(select aa.uid uid,
					collect_set(game) game
				from logindb.roomlogin_detail aa 
				where aa.dt >= '20180101' and aa.dt <= '20180131' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid
			) t1
			on t0.uid = t1.uid
		) t2
		left join
		(select uidto, 
				sum(money) money
			from paydb.basic
			where paydate>='20170101'
			group by uidto
		) t3
		on t2.uid = t3.uidto
	) t4
	left join
	(select uidto, 
			sum(money) 1_money,
			count(money) 1_chongzhi_cishu
		from paydb.basic
		where paydate>='20180101' and paydate>='20180131'
		group by uidto
	) t5
	on t4.uid = t5.uidto

hive -e "

" > /opt/pc.1.game.csv;
	
	
【登录用户id明细】斗牌 游戏登录用户明细
1.时间为2月1号-2月16号，  3月1号-3月13号
2.导出游戏 斗牌 1880316  game = 316
3.导出 pc端的房间登录    
4.导出字段为：用户id。

表格名称：
0201-0216.csv
0301-0313.csv


select aa.uid uid
	from logindb.roomlogin_detail aa 
	where aa.dt >= '20180201' and aa.dt <= '20180216' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
	and game = 316
	group by aa.uid
hive -e "

" > /opt/0201-0216.csv;


select aa.uid uid
	from logindb.roomlogin_detail aa 
	where aa.dt >= '20180301' and aa.dt <= '20180313' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
	and game = 316
	group by aa.uid
hive -e "

" > /opt/0301-0313.csv;
	

页游用户信息
均为PC端的游戏
导出游戏：

|游戏名称|游戏ID|
|-|-|
|传奇荣耀|757|
|传奇世界|763|
|传奇盛世| 665|

##其中的12月1日-2月9日有过登录并充值的用户名单；命名为
LOAD DATA  INPATH '/user/root/20180122_0204_mobile' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='20180122_0204_mobile');
LOAD DATA  INPATH '/user/root/665' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='665');
757.paylogin.csv
763.paylogin.csv
665.paylogin.csv
select t0.uid
	from 
	(select aa.uid uid
		from logindb.halllogin_detail aa 
		where aa.dt >= '20171201' and aa.dt <= '20180209' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		and game = 757
		group by aa.uid
	) t0
	left join
	(select uidto
		from paydb.basic
		where paydate >= '20171201' and paydate <= '20180209' and game = 757
		group by uidto 
		having sum(money) > 0
	) ba
	on t0.uid = ba.uidto
	where ba.uidto is not null
hive -e "
" > /opt/757.paylogin.csv;

select t0.uid
	from 
	(select aa.uid uid
		from logindb.halllogin_detail aa 
		where aa.dt >= '20171201' and aa.dt <= '20180209' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		and game = 763
		group by aa.uid
	) t0
	left join
	(select uidto
		from paydb.basic
		where paydate >= '20171201' and paydate <= '20180209' and game = 763
		group by uidto 
		having sum(money) > 0
	) ba
	on t0.uid = ba.uidto
	where ba.uidto is not null
hive -e "
" > /opt/763.paylogin.csv;

select t0.uid
	from 
	(select aa.uid uid
		from logindb.halllogin_detail aa 
		where aa.dt >= '20171201' and aa.dt <= '20180209' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		and game = 665
		group by aa.uid
	) t0
	left join
	(select uidto
		from paydb.basic
		where paydate >= '20171201' and paydate <= '20180209' and game = 665
		group by uidto 
		having sum(money) > 0
	) ba
	on t0.uid = ba.uidto
	where ba.uidto is not null
hive -e "
" > /opt/665.paylogin.csv;


其中的12月1日-2月9日有过登录且在该游戏内充值的用户名单
##基于上述表格用户列表，再导出 2月10日-3月19日期间超过15天及以上未付费的用户。
命名为：
757.nopay.csv
763.nopay.csv
665.nopay.csv
drop table if exists tempdb.game_amount ;
create table tempdb.game_amount (
id     int,
game_total   int,
total_pay int) 
PARTITIONED BY (
dt     string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA  INPATH '/user/root/20180210-20180319' overwrite  INTO TABLE tempdb.legend_game PARTITION (dt='20180210-20180319');

select id, game, amount, date from tempdb.legend_game where dt='20180210-20180319';

表格字段为

|用户ID|游戏|未登录总天数|2月10日-3月19日期间付费总额|2月10日-3月19日在*游戏内的付费总额|
select t3.id,
		t3.game_total,
		t3.total_pay,
		t4.game,
		t4.count_date
	from
	(select t1.id id, 
			t1.game_total game_total, 
			t1.total_pay total_pay
		from
		(select id,
				game_total, 
				total_pay
			from tempdb.game_amount
			where dt ='665'
		) t1
		left join
		(select id, 
				max(concat(substr(bc.date,1,4),'-',substr(bc.date,5,2),'-',substr(bc.date,7,2))) date 
			from tempdb.legend_game bc
			where dt='20180210-20180319' and game = 665
			GROUP BY id
			HAVING datediff('2018-03-19',date) <= 15
		) t2
		on t1.id = t2.id
		where t2.id is null
	) t3
	left join
	(select aa.uid uid,
			collect_set(aa.game) game,
			count(distinct aa.date) count_date
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		group by aa.uid
	) t4
	on t3.id = t4.uid
hive -e "
" > /opt/665.nopay.csv;
	
	
select t3.id,
		t3.game_total,
		t3.total_pay,
		t4.game,
		t4.count_date
	from
	(select t1.id id, 
			t1.game_total game_total, 
			t1.total_pay total_pay
		from
		(select id,
				game_total, 
				total_pay
			from tempdb.game_amount
			where dt ='757'
		) t1
		left join
		(select id, 
				max(concat(substr(bc.date,1,4),'-',substr(bc.date,5,2),'-',substr(bc.date,7,2))) date 
			from tempdb.legend_game bc
			where dt='20180210-20180319' and game = 757
			GROUP BY id
			HAVING datediff('2018-03-19',date) <= 15
		) t2
		on t1.id = t2.id
		where t2.id is null
	) t3
	left join
	(select aa.uid uid,
			collect_set(aa.game) game,
			count(distinct aa.date) count_date
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		group by aa.uid
	) t4
	on t3.id = t4.uid
hive -e "
" > /opt/757.nopay.csv;
	

select t3.id,
		t3.game_total,
		t3.total_pay,
		t4.game,
		t4.count_date
	from
	(select t1.id id, 
			t1.game_total game_total, 
			t1.total_pay total_pay
		from
		(select id,
				game_total, 
				total_pay
			from tempdb.game_amount
			where dt ='763'
		) t1
		left join
		(select id, 
				max(concat(substr(bc.date,1,4),'-',substr(bc.date,5,2),'-',substr(bc.date,7,2))) date 
			from tempdb.legend_game bc
			where dt='20180210-20180319' and game = 763
			GROUP BY id
			HAVING datediff('2018-03-19',date) <= 15
		) t2
		on t1.id = t2.id
		where t2.id is null
	) t3
	left join
	(select aa.uid uid,
			collect_set(aa.game) game,
			count(distinct aa.date) count_date
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		group by aa.uid
	) t4
	on t3.id = t4.uid
hive -e "
" > /opt/763.nopay.csv;



select t8.uid,
		t8.game,
		t8.count_date,
		t8.money,
		t9.757_money
	from
	(select t6.uid uid,
		   t6.game game,
		   t6.count_date count_date,
		   t7.money money
		from
		(select t4.uid uid,
			   t5.game game,
			   t5.count_date count_date
			from
			(select t1.uid uid
				from
				(select t0.uid uid
					from 
					(select aa.uid uid
						from logindb.halllogin_detail aa 
						where aa.dt >= '20171201' and aa.dt <= '20180209' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
						and game = 757
						group by aa.uid
					) t0
					left join
					(select uidto
						from paydb.basic
						where paydate >= '20171201' and paydate <= '20180209' and game = 757
						group by uidto 
						having sum(money) > 0
					) ba
					on t0.uid = ba.uidto
					where ba.uidto is not null
				) t1
				left join
				(select t2.uidto uidto
					from
					(select bc.uidto uidto,
							sum(money) money,
							max(concat(substr(bc.paydate,1,4),'-',substr(bc.paydate,5,2),'-',substr(bc.paydate,7,2))) date
						from paydb.basic bc
						where bc.paydate>='20180210' and bc.paydate<='20180319' 
						group by bc.uidto 
						HAVING datediff('2018-03-19',date) >= 15 and money <= 0
					) t2
				) t3
				on t1.uid = t3.uidto
				where t3.uidto is not null
			) t4
			left join
			(select aa.uid uid,
					collect_set(aa.game) game,
					count(distinct aa.date) count_date
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid, aa.game
			) t5
			on t4.uid = t5.uid
		) t6
		left join
		(select uidto,
				sum(money) money
			from paydb.basic
			where paydate >= '20180210' and paydate <= '20180319'
			group by uidto 
		) t7
		on t6.uid = t7.uidto
	) t8
	left join
	(select uidto,
			sum(money) 757_money
		from paydb.basic
		where game = 757 and paydate >= '20180210' and paydate <= '20180319'
		group by uidto 
	) t9
	on t8.uid = t9.uidto
	
hive -e "
" > /opt/757.nopay.csv;
	
	
	
select t8.uid,
		t8.game,
		t8.count_date,
		t8.money,
		t9.763_money
	from
	(select t6.uid uid,
		   t6.game game,
		   t6.count_date count_date,
		   t7.money money
		from
		(select t4.uid uid,
			   t5.game game,
			   t5.count_date count_date
			from
			(select t1.uid uid
				from
				(select t0.uid uid
					from 
					(select aa.uid uid
						from logindb.halllogin_detail aa 
						where aa.dt >= '20171201' and aa.dt <= '20180209' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
						and game = 763
						group by aa.uid
					) t0
					left join
					(select uidto
						from paydb.basic
						where paydate >= '20171201' and paydate <= '20180209' and game = 763
						group by uidto 
						having sum(money) > 0
					) ba
					on t0.uid = ba.uidto
					where ba.uidto is not null
				) t1
				left join
				(select t2.uidto uidto
					from
					(select bc.uidto uidto,
							sum(money) money,
							max(concat(substr(bc.paydate,1,4),'-',substr(bc.paydate,5,2),'-',substr(bc.paydate,7,2))) date
						from paydb.basic bc
						where bc.paydate>='20180210' and bc.paydate<='20180319' 
						group by bc.uidto 
						HAVING datediff('2018-03-19',date) >= 15 and money <= 0
					) t2
				) t3
				on t1.uid = t3.uidto
				where t3.uidto is not null
			) t4
			left join
			(select aa.uid uid,
					collect_set(aa.game) game,
					count(distinct aa.date) count_date
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid, aa.game
			) t5
			on t4.uid = t5.uid
		) t6
		left join
		(select uidto,
				sum(money) money
			from paydb.basic
			where paydate >= '20180210' and paydate <= '20180319'
			group by uidto 
		) t7
		on t6.uid = t7.uidto
	) t8
	left join
	(select uidto,
			sum(money) 763_money
		from paydb.basic
		where game = 763 and paydate >= '20180210' and paydate <= '20180319'
		group by uidto 
	) t9
	on t8.uid = t9.uidto
	
hive -e "
" > /opt/763.nopay.csv;

	
select t8.uid,
		t8.game,
		t8.count_date,
		t8.money,
		t9.665_money
	from
	(select t6.uid uid,
		   t6.game game,
		   t6.count_date count_date,
		   t7.money money
		from
		(select t4.uid uid,
			   t5.game game,
			   t5.count_date count_date
			from
			(select t1.uid uid
				from
				(select t0.uid uid
					from 
					(select aa.uid uid
						from logindb.halllogin_detail aa 
						where aa.dt >= '20171201' and aa.dt <= '20180209' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
						and game = 665
						group by aa.uid
					) t0
					left join
					(select uidto
						from paydb.basic
						where paydate >= '20171201' and paydate <= '20180209' and game = 665
						group by uidto 
						having sum(money) > 0
					) ba
					on t0.uid = ba.uidto
					where ba.uidto is not null
				) t1
				left join
				(select t2.uidto uidto
					from
					(select bc.uidto uidto,
							sum(money) money,
							max(concat(substr(bc.paydate,1,4),'-',substr(bc.paydate,5,2),'-',substr(bc.paydate,7,2))) date
						from paydb.basic bc
						where bc.paydate>='20180210' and bc.paydate<='20180319' 
						group by bc.uidto 
						HAVING datediff('2018-03-19',date) >= 15 and money <= 0
					) t2
				) t3
				on t1.uid = t3.uidto
				where t3.uidto is not null
			) t4
			left join
			(select aa.uid uid,
					collect_set(aa.game) game,
					count(distinct aa.date) count_date
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid, aa.game
			) t5
			on t4.uid = t5.uid
		) t6
		left join
		(select uidto,
				sum(money) money
			from paydb.basic
			where paydate >= '20180210' and paydate <= '20180319'
			group by uidto 
		) t7
		on t6.uid = t7.uidto
	) t8
	left join
	(select uidto,
			sum(money) 665_money
		from paydb.basic
		where game = 665 and paydate >= '20180210' and paydate <= '20180319'
		group by uidto 
	) t9
	on t8.uid = t9.uidto
	
hive -e "
" > /opt/665.nopay.csv;


其中的12月1日-2月9日有过登录且在该游戏内充值的用户名单
##基于上述表格用户列表，再导出 2月10日-3月19日期间超过15天及以上未登录的用户。
命名为：
757.nologin.csv
763.nologin.csv
665.nologin.csv

|用户ID|游戏|未登录总天数|2月10日-3月19日期间付费总额|2月10日-3月19日在*游戏内的付费总额|
drop table if exists tempdb.legend_game ;
create table tempdb.legend_game (
id     int,
game   int,
game_detail string,
fu_id  int,
amount  int,
date   int,
time   int) 
PARTITIONED BY (
dt     string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA  INPATH '/user/root/20180210-20180319' overwrite  INTO TABLE tempdb.legend_game PARTITION (dt='20180210-20180319');


select t3.id,
		t3.game_total,
		t3.total_pay,
		t4.game,
		t4.count_date
	from
	(select t1.id id, 
			t1.game_total game_total, 
			t1.total_pay total_pay
		from
		(select id,
				game_total, 
				total_pay
			from tempdb.game_amount
			where dt ='665'
		) t1
		left join
		(select bc.uid uid,
				max(concat(substr(bc.date,1,4),'-',substr(bc.date,5,2),'-',substr(bc.date,7,2))) date
			from logindb.halllogin_detail bc
			where bc.dt>='20180210' and bc.dt<='20180319' 
			group by bc.uid
			HAVING datediff('2018-03-19',date) > 15
		) t2
		on t1.id = t2.uid
		where t2.uid is not null
	) t3
	left join
	(select aa.uid uid,
			collect_set(aa.game) game,
			count(distinct aa.date) count_date
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		group by aa.uid
	) t4
	on t3.id = t4.uid
hive -e "
" > /opt/665.nologin.csv;


select t3.id,
		t3.game_total,
		t3.total_pay,
		t4.game,
		t4.count_date
	from
	(select t1.id id, 
			t1.game_total game_total, 
			t1.total_pay total_pay
		from
		(select id,
				game_total, 
				total_pay
			from tempdb.game_amount
			where dt ='757'
		) t1
		left join
		(select bc.uid uid,
				max(concat(substr(bc.date,1,4),'-',substr(bc.date,5,2),'-',substr(bc.date,7,2))) date
			from logindb.halllogin_detail bc
			where bc.dt>='20180210' and bc.dt<='20180319' 
			group by bc.uid
			HAVING datediff('2018-03-19',date) > 15
		) t2
		on t1.id = t2.uid
		where t2.uid is not null
	) t3
	left join
	(select aa.uid uid,
			collect_set(aa.game) game,
			count(distinct aa.date) count_date
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		group by aa.uid
	) t4
	on t3.id = t4.uid
hive -e "
" > /opt/757.nologin.csv;
	
	
select t3.id,
		t3.game_total,
		t3.total_pay,
		t4.game,
		t4.count_date
	from
	(select t1.id id, 
			t1.game_total game_total, 
			t1.total_pay total_pay
		from
		(select id,
				game_total, 
				total_pay
			from tempdb.game_amount
			where dt ='763'
		) t1
		left join
		(select bc.uid uid,
				max(concat(substr(bc.date,1,4),'-',substr(bc.date,5,2),'-',substr(bc.date,7,2))) date
			from logindb.halllogin_detail bc
			where bc.dt>='20180210' and bc.dt<='20180319' 
			group by bc.uid
			HAVING datediff('2018-03-19',date) > 15
		) t2
		on t1.id = t2.uid
		where t2.uid is not null
	) t3
	left join
	(select aa.uid uid,
			collect_set(aa.game) game,
			count(distinct aa.date) count_date
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		group by aa.uid
	) t4
	on t3.id = t4.uid
hive -e "
" > /opt/763.nologin.csv;
	





select t8.uid,
		t8.game,
		t8.count_date,
		t8.money,
		t9.757_money
	from
	(select t6.uid uid,
		   t6.game game,
		   t6.count_date count_date,
		   t7.money money
		from
		(select t4.uid uid,
			   t5.game game,
			   t5.count_date count_date
			from
			(select t1.uid uid
				from
				(select t0.uid uid
					from 
					(select aa.uid uid
						from logindb.halllogin_detail aa 
						where aa.dt >= '20171201' and aa.dt <= '20180209' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
						and game = 757
						group by aa.uid
					) t0
					left join
					(select uidto
						from paydb.basic
						where paydate >= '20171201' and paydate <= '20180209' and game = 757
						group by uidto 
						having sum(money) > 0
					) ba
					on t0.uid = ba.uidto
					where ba.uidto is not null
				) t1
				left join
				(select t2.uid uid
					from
					(select bc.uid uid,
							max(concat(substr(bc.date,1,4),'-',substr(bc.date,5,2),'-',substr(bc.date,7,2))) date
						from logindb.halllogin_detail bc
						where bc.dt>='20180210' and bc.dt<='20180319' 
						group by bc.uid
						HAVING datediff('2018-03-19',date) < 15
					) t2
				) t3
				on t1.uid = t3.uid
				where t3.uid is null
			) t4
			left join
			(select aa.uid uid,
					collect_set(aa.game) game,
					count(distinct aa.date) count_date
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid
			) t5
			on t4.uid = t5.uid
		) t6
		left join
		(select uidto,
				sum(money) money
			from paydb.basic
			where paydate >= '20180210' and paydate <= '20180319'
			group by uidto 
		) t7
		on t6.uid = t7.uidto
	) t8	
	left join
	(select uidto,
			sum(money) 757_money
		from paydb.basic
		where game = 757 and paydate >= '20180210' and paydate <= '20180319'
		group by uidto 
	) t9
	on t8.uid = t9.uidto
	
hive -e "
" > /opt/757.nologin.csv;


select t8.uid,
		t8.game,
		t8.count_date,
		t8.money,
		t9.763_money
	from
	(select t6.uid uid,
		   t6.game game,
		   t6.count_date count_date,
		   t7.money money
		from
		(select t4.uid uid,
			   t5.game game,
			   t5.count_date count_date
			from
			(select t1.uid uid
				from
				(select t0.uid uid
					from 
					(select aa.uid uid
						from logindb.halllogin_detail aa 
						where aa.dt >= '20171201' and aa.dt <= '20180209' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
						and game = 763
						group by aa.uid
					) t0
					left join
					(select uidto
						from paydb.basic
						where paydate >= '20171201' and paydate <= '20180209' and game = 763
						group by uidto 
						having sum(money) > 0
					) ba
					on t0.uid = ba.uidto
					where ba.uidto is not null
				) t1
				left join
				(select t2.uid uid
					from
					(select bc.uid uid,
							max(concat(substr(bc.date,1,4),'-',substr(bc.date,5,2),'-',substr(bc.date,7,2))) date
						from logindb.halllogin_detail bc
						where bc.dt>='20180210' and bc.dt<='20180319' 
						group by bc.uid
						HAVING datediff('2018-03-19',date) < 15
					) t2
				) t3
				on t1.uid = t3.uid
				where t3.uid is null
			) t4
			left join
			(select aa.uid uid,
					collect_set(aa.game) game,
					count(distinct aa.date) count_date
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid
			) t5
			on t4.uid = t5.uid
		) t6
		left join
		(select uidto,
				sum(money) money
			from paydb.basic
			where paydate >= '20180210' and paydate <= '20180319'
			group by uidto 
		) t7
		on t6.uid = t7.uidto
	) t8	
	left join
	(select uidto,
			sum(money) 763_money
		from paydb.basic
		where game = 763 and paydate >= '20180210' and paydate <= '20180319'
		group by uidto 
	) t9
	on t8.uid = t9.uidto	
hive -e "
" > /opt/763.nologin.csv;



select t8.uid,
		t8.game,
		t8.count_date,
		t8.money,
		t9.665_money
	from
	(select t6.uid uid,
		   t6.game game,
		   t6.count_date count_date,
		   t7.money money
		from
		(select t4.uid uid,
			   t5.game game,
			   t5.count_date count_date
			from
			(select t1.uid uid
				from
				(select t0.uid uid
					from 
					(select aa.uid uid
						from logindb.halllogin_detail aa 
						where aa.dt >= '20171201' and aa.dt <= '20180209' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
						and game = 665
						group by aa.uid
					) t0
					left join
					(select uidto
						from paydb.basic
						where paydate >= '20171201' and paydate <= '20180209' and game = 665
						group by uidto 
						having sum(money) > 0
					) ba
					on t0.uid = ba.uidto
					where ba.uidto is not null
				) t1
				left join
				(select t2.uid uid
					from
					(select bc.uid uid,
							max(concat(substr(bc.date,1,4),'-',substr(bc.date,5,2),'-',substr(bc.date,7,2))) date
						from logindb.halllogin_detail bc
						where bc.dt>='20180210' and bc.dt<='20180319' 
						group by bc.uid
						HAVING datediff('2018-03-19',date) < 15
					) t2
				) t3
				on t1.uid = t3.uid
				where t3.uid is null
			) t4
			left join
			(select aa.uid uid,
					collect_set(aa.game) game,
					count(distinct aa.date) count_date
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180210' and aa.dt <= '20180319' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by aa.uid
			) t5
			on t4.uid = t5.uid
		) t6
		left join
		(select uidto,
				sum(money) money
			from paydb.basic
			where paydate >= '20180210' and paydate <= '20180319'
			group by uidto 
		) t7
		on t6.uid = t7.uidto
	) t8	
	left join
	(select uidto,
			sum(money) 665_money
		from paydb.basic
		where game = 665 and paydate >= '20180210' and paydate <= '20180319'
		group by uidto 
	) t9
	on t8.uid = t9.uidto	
hive -e "
" > /opt/665.nologin.csv;
	

	
在同城游app注册，且后续登录在同城游app的用户
注册时间：2016年4月1-5日、2016年9月1-5日、2017年4月1-5日、2017年9月1-5日
登陆时间要求：2018年2月21日-3月20日（28天）登陆天数

导出要求：
用户序号|注册地区|注册大厅号|（28天）登陆天数|
导出表格命名：
201604user.csv
201609user.csv
201704user.csv
201709user.csv


select t3.uid uid,
	   t3.group group,
	   t3.province province,
	   t3.city city,
	   t3.district district,
	   t3.count_date count_date,
	   t2.telephone
	from
	(select alluids.uid uid,
		   alluids.group group,
		   alluids.province province,
		   alluids.city city,
		   alluids.district district,
		   t1.count_date count_date
		from
		(select uid, 
				group, 
				province,
				city,
				district
			from regdb.regdetail 
			where date >= '20160401' and date <= '20160405' and app = 3001
		) alluids
		left join
		(select aa.uid uid,
			   count(distinct aa.date) count_date
			from logindb.halllogin_detail aa 
			where aa.app = 3001 and aa.dt >= '20180221' and aa.dt <= '20180320'
			group by uid
		) t1
		on alluids.uid = t1.uid
	) t3
	left join
	(select uid, 
			telephone 
		from tempdb.uid_telephone 
		where dt='20180315'
		group by uid, telephone
	) t2
	on t3.uid = t2.uid
hive -e "
" > /opt/201604user.csv;
	
	
select t3.uid uid,
	   t3.group group,
	   t3.province province,
	   t3.city city,
	   t3.district district,
	   t3.count_date count_date,
	   t2.telephone
	from
	(select alluids.uid uid,
		   alluids.group group,
		   alluids.province province,
		   alluids.city city,
		   alluids.district district,
		   t1.count_date count_date
		from
		(select uid, 
				group, 
				province,
				city,
				district
			from regdb.regdetail 
			where date >= '20160901' and date <= '20160905' and app = 3001
		) alluids
		left join
		(select aa.uid uid,
			   count(distinct aa.date) count_date
			from logindb.halllogin_detail aa 
			where aa.app = 3001 and aa.dt >= '20180221' and aa.dt <= '20180320'
			group by uid
		) t1
		on alluids.uid = t1.uid
	) t3
	left join
	(select uid, 
			telephone 
		from tempdb.uid_telephone 
		where dt='20180315'
		group by uid, telephone
	) t2
	on t3.uid = t2.uid
hive -e "
" > /opt/201609user.csv;


select t3.uid uid,
	   t3.group group,
	   t3.province province,
	   t3.city city,
	   t3.district district,
	   t3.count_date count_date,
	   t2.telephone
	from
	(select alluids.uid uid,
		   alluids.group group,
		   alluids.province province,
		   alluids.city city,
		   alluids.district district,
		   t1.count_date count_date
		from
		(select uid, 
				group, 
				province,
				city,
				district
			from regdb.regdetail 
			where date >= '20170401' and date <= '20170405' and app = 3001
		) alluids
		left join
		(select aa.uid uid,
			   count(distinct aa.date) count_date
			from logindb.halllogin_detail aa 
			where aa.app = 3001 and aa.dt >= '20180221' and aa.dt <= '20180320'
			group by uid
		) t1
		on alluids.uid = t1.uid
	) t3
	left join
	(select uid, 
			telephone 
		from tempdb.uid_telephone 
		where dt='20180315'
		group by uid, telephone
	) t2
	on t3.uid = t2.uid
hive -e "
" > /opt/201704user.csv;

select t3.uid uid,
	   t3.group group,
	   t3.province province,
	   t3.city city,
	   t3.district district,
	   t3.count_date count_date,
	   t2.telephone
	from
	(select alluids.uid uid,
		   alluids.group group,
		   alluids.province province,
		   alluids.city city,
		   alluids.district district,
		   t1.count_date count_date
		from
		(select uid, 
				group, 
				province,
				city,
				district
			from regdb.regdetail 
			where date >= '20170901' and date <= '20170905' and app = 3001
		) alluids
		left join
		(select aa.uid uid,
			   count(distinct aa.date) count_date
			from logindb.halllogin_detail aa 
			where aa.app = 3001 and aa.dt >= '20180221' and aa.dt <= '20180320'
			group by uid
		) t1
		on alluids.uid = t1.uid
	) t3
	left join
	(select uid, 
			telephone 
		from tempdb.uid_telephone 
		where dt='20180315'
		group by uid, telephone
	) t2
	on t3.uid = t2.uid
hive -e "
" > /opt/201709user.csv;


连续30天登录
SELECT 
    uid, MAX(dt) - MIN(dt) diff, COLLECT_set (dt) 
FROM
    (SELECT 
        a.uid, a.dt, dt - rn num 
    FROM
        (SELECT 
            uid, 
			date, 
			row_number () over (PARTITION BY uid 
        ORDER BY date) rn 
        FROM
            table_name
        GROUP BY uid, date
		) a
	) a 
	GROUP BY uid, num 

select 
		uid, max(date) - min(date) diff, COLLECT_set (date)
	from
	(SELECT 
			a.uid, a.date, date - rn num 
		from
		(SELECT 
			uid, 
			date, 
			row_number () over (PARTITION BY uid 
		ORDER BY date) rn 
		FROM logindb.halllogin_detail
		WHERE dt>='20180301' and dt <='20180325'
		GROUP BY uid, date
		) a
	) a 
	group by uid, num
	

select uid, diff
	FROM
	(select 
			b.uid, max(b.date) - min(b.date) diff, COLLECT_set (b.date)
		from
		(SELECT 
				a.uid uid, a.date date, a.date - a.rn num 
			from
			(SELECT
				uid,
				date,
				row_number () over (PARTITION BY uid 
			ORDER BY date) rn 
			FROM logindb.halllogin_detail
			WHERE dt>='20180301' and dt <='20180325'
			GROUP BY uid, date
			) a
		) b
		group by b.uid, num
	) c
	WHERE diff = 29

【页游清明节活动数据】VIP用户中有手机号的活跃用户导出
1.有充值行为或累计登录30天以上的用户名单
（1的数据王鑫明已统计好发送过来，名称为：20180326-妖豆VIP用户.xls)
2.在1的基础上获取18年1月1日之后有登录过的用户
3.在2的基础上获取有手机号的用户,**注意，不过滤大R和营收类游戏的用户**
4.导出3 的用户id，1月1日之后登录过的页游，
保存为表格:loginVIP.csv
即最终列表为：

|用户id|登录页游|手机号|

LOAD DATA  INPATH '/user/root/20180326yaodouVIP' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='20180326yaodouVIP');

select t7.uid, 
       t7.telephone,
	   t7.game
	from
	(select t5.uid uid,
			t5.telephone telephone,
			t6.game game 
		from
		(select t3.uid uid,
				t4.telephone telephone
			from
			(select t1.uid uid
				from
				(select uid 
					from tempdb.login_uid 
					where  dt='20180326yaodouVIP'
				) t1
				left join
				(select aa.uid uid
					from logindb.halllogin_detail aa  
					where aa.dt >= '20180101' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
					group by uid
				) t2
				on t1.uid = t2.uid
				where t2.uid is not null
			) t3
			left join
			(select uid, 
					telephone 
				from tempdb.uid_telephone 
				where dt='20180327'
				group by uid, telephone
			) t4
			on t3.uid = t4.uid
			where t4.uid is not null
		) t5
		left join
		(select aa.uid uid,
				aa.game game
			from logindb.halllogin_detail aa  
			where aa.dt >= '20180101' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
			group by uid, game
		) t6
		on t5.uid = t6.uid
	) t7
	RIGHT join
	(select game 
		from tempdb.yegame_detail 
		where dt='yeyou'
	) t8
	on t7.game = t8.game
	where t7.game is not null
hive -e "
" > /opt/loginVIP.csv;

	
	
	
	


	
select t7.uid, 
       t7.telephone,
	   t7.game
	from
	(select t5.uid uid,
			t5.telephone telephone,
			t6.game game 
		from
		(select t3.uid uid,
				t4.telephone telephone
			from
			(select t1.uid uid
				from
				(select uid 
					from tempdb.login_uid 
					where  dt='665'
				) t1
				
				
				
				left join
				(select aa.uid uid
					from logindb.halllogin_detail aa  
					where aa.dt >= '20180101' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
					group by uid
				) t2
				on t1.uid = t2.uid
				where t2.uid is not null
			) t3
			left join
			(select uid, 
					telephone 
				from tempdb.uid_telephone 
				where dt='20180327'
				group by uid, telephone
			) t4
			on t3.uid = t4.uid
			where t4.uid is not null
		) t5
		left join
		(select aa.uid uid,
				aa.game game
			from logindb.halllogin_detail aa  
			where aa.dt >= '20180101' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
			group by uid, game
		) t6
		on t5.uid = t6.uid
	) t7
	RIGHT join
	(select game 
		from tempdb.yegame_detail 
		where dt='yeyou'
	) t8
	on t7.game = t8.game
	where t7.game is not null
hive -e "
" > /opt/loginVIP.csv;

select t1.uid, t2.telephone
	from
	(select uid 
		from tempdb.login_uid 
		where  dt='665'
	) t1
	left join
	(select uid, 
			telephone 
		from tempdb.uid_telephone 
		where dt='20180327'
		group by uid, telephone
	) t2
	on t1.uid = t2.uid

hive -e "
" > /opt/665.csv;

	
drop table if exists tempdb.uid_game;
create table tempdb.uid_game (
uid                   int,
game                  int)  
PARTITIONED BY (
dt     string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','; 
LOAD DATA  INPATH '/user/root/01-03' overwrite  INTO TABLE tempdb.uid_game PARTITION (dt='01-03');
LOAD DATA  INPATH '/user/root/04-06' overwrite  INTO TABLE tempdb.uid_game PARTITION (dt='04-06');
LOAD DATA  INPATH '/user/root/07-09' overwrite  INTO TABLE tempdb.uid_game PARTITION (dt='07-09');
LOAD DATA  INPATH '/user/root/10-12' overwrite  INTO TABLE tempdb.uid_game PARTITION (dt='10-12');

(select t.uid uid,
		t.date date
	from
	(select aa.uid uid,
			aa.date date,
			row_number() over(partition by aa.uid, aa.game order by aa.date desc) as rn
	   from logindb.halllogin_detail aa 
	   where aa.game = 215 
	   and aa.dt >= '20180128' and aa.dt <= '20180228' and aa.group in (6, 66, 8, 88)
	) t
	where rn = 1
	) alluids

	
select t3.uid,
		t3.game,
		t4.app,
		t3.reg_date,
		t4.date
	from
	(select t1.uid uid, 
			t1.game game, 
			t2.date reg_date
		from
		(select uid,
				game
			from tempdb.uid_game 
			where  dt='01-03'
		) t1
		left join
		(select uid, 
				date
			from regdb.regdetail 
			group by uid, date
		) t2
		on t1.uid = t2.uid
	) t3
	left join
	(select t.uid uid,
			t.app app,
			t.date date
		from
		(select aa.uid uid,
				aa.app app,
				aa.date date,
				row_number() over(partition by aa.uid, aa.app order by aa.date desc) as rn
		   from logindb.halllogin_detail aa 
		   where aa.dt >= '20170101' and dt <= '20170331'
		) t
		where rn = 1
	) t4
	on t3.uid = t4.uid and t3.game = t4.app
		
hive -e "
" > /opt/01-03.csv;

select t3.uid,
		t3.game,
		t4.app,
		t3.reg_date,
		t4.date
	from
	(select t1.uid uid, 
			t1.game game, 
			t2.date reg_date
		from
		(select uid,
				game
			from tempdb.uid_game 
			where  dt='04-06'
		) t1
		left join
		(select uid, 
				date
			from regdb.regdetail 
			group by uid, date
		) t2
		on t1.uid = t2.uid
	) t3
	left join
	(select t.uid uid,
			t.app app,
			t.date date
		from
		(select aa.uid uid,
				aa.app app,
				aa.date date,
				row_number() over(partition by aa.uid, aa.app order by aa.date desc) as rn
		   from logindb.halllogin_detail aa 
		   where aa.dt >= '20170401' and dt <= '20170630'
		) t
		where rn = 1
	) t4
	on t3.uid = t4.uid and t3.game = t4.app
hive -e "
" > /opt/04-06.csv;


select t3.uid,
		t3.game,
		t4.app,
		t3.reg_date,
		t4.date
	from
	(select t1.uid uid, 
			t1.game game, 
			t2.date reg_date
		from
		(select uid,
				game
			from tempdb.uid_game 
			where  dt='07-09'
		) t1
		left join
		(select uid, 
				date
			from regdb.regdetail 
			group by uid, date
		) t2
		on t1.uid = t2.uid
	) t3
	left join
	(select t.uid uid,
			t.app app,
			t.date date
		from
		(select aa.uid uid,
				aa.app app,
				aa.date date,
				row_number() over(partition by aa.uid, aa.app order by aa.date desc) as rn
		   from logindb.halllogin_detail aa 
		   where aa.dt >= '20170701' and dt <= '20170930'
		) t
		where rn = 1
	) t4
	on t3.uid = t4.uid and t3.game = t4.app
		
hive -e "
" > /opt/07-09.csv;

select t3.uid,
		t3.game,
		t4.app,
		t3.reg_date,
		t4.date
	from
	(select t1.uid uid, 
			t1.game game, 
			t2.date reg_date
		from
		(select uid,
				game
			from tempdb.uid_game 
			where  dt='10-12'
			group by uid, game
		) t1
		left join
		(select uid, 
				date
			from regdb.regdetail 
			group by uid
		) t2
		on t1.uid = t2.uid
	) t3
	left join
	(select t.uid uid,
			t.app app,
			t.date date
		from
		(select aa.uid uid,
				aa.app app,
				aa.date date,
				row_number() over(partition by aa.uid, aa.app order by aa.date desc) as rn
		   from logindb.halllogin_detail aa 
		   where aa.dt >= '20171001' and dt <= '20171231'
		) t
		where rn = 1
	) t4
	on t3.uid = t4.uid and t3.game = t4.app	
hive -e "
" > /opt/10-12.csv;



【手机号】18年来用户信息
1.过滤大R和营收类游戏用户。
2.分2+2共计4张表导出，
3.导出时间及范围：2018年3月1日-3月28日  登录用户
A1：3月PC登录用户手机号 
|用户id|手机号|
select ddd.uid uid,
	   ddd.telephone telephone
	from
	(select tt.uid uid,
			tt.telephone telephone
		from
		(select t1.uid uid,
				t2.telephone telephone
			from
			(select aa.uid uid
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180301' and aa.dt <= '20180329' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
				group by uid
			) t1
			left join
			(select uid, 
				   telephone 
				from tempdb.uid_telephone 
				where dt='20180327'
				group by uid, telephone
			) t2
			on t1.uid = t2.uid
			where t2.uid is not null
		) tt
		left join
		(select hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
			group by uid
		UNION ALL
		select rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
			group by uid
		) yinshou	
		on tt.uid = yinshou.uid
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
	
hive -e "
" > /opt/pc.3.telephone.csv;

A2：3月PC登录用户信息（注：用户均为有手机号的用户。）
|用户id|注册日期|注册地区|3月1日至3月28日期间的登录天数|登录过的游戏|最后一次登录时间|手机号|

select t9.uid uid, 
		t10.count_date,
		t9.end_date end_date, 
		t9.telephone telephone,
		t9.reg_date reg_date,
		t9.province province,
		t9.city city,
		t9.district district,
		t9.game
	from
	(select t7.uid uid, 
			t7.end_date end_date, 
			t7.telephone telephone,
			t7.reg_date reg_date,
			t7.province province,
			t7.city city,
			t7.district district,
			t8.game
		from
		(select t5.uid uid, 
				t5.end_date end_date, 
				t5.telephone telephone,
				t6.date reg_date,
				t6.province province,
				t6.city city,
				t6.district district
			from
			(select t4.uid uid, 
					t4.end_date end_date, 
					t4.telephone telephone
				from
				(select t3.uid uid, 
						t3.end_date end_date, 
						t3.telephone telephone
					from
					(select t1.uid uid,
							t1.date end_date,
							t2.telephone telephone
						from
						(select t.uid uid, 
								t.date date
							from
							(select aa.uid uid,
									aa.date date,
									row_number() over(partition by aa.uid order by aa.date desc) as rn	
								from logindb.halllogin_detail aa 
								where aa.dt >= '20180301' and aa.dt <= '20180329' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
								group by uid, date
							) t
							where rn = 1
						) t1
						left join
						(select uid, 
							   telephone 
							from tempdb.uid_telephone 
							where dt='20180327'
							group by uid, telephone
						) t2
						on t1.uid = t2.uid
						where t2.uid is not null
					) t3
					left join
					(select hd.uid uid
						from logindb.halllogin_detail hd 
						where hd.game in (316,421,450,311,23,445) 
						group by uid
					UNION ALL
					select rd.uid uid
						from logindb.roomlogin_detail rd 
						where rd.game in (316,421,450,311,23,445)
						group by uid
					) yinshou	
					on t3.uid = yinshou.uid
					where yinshou.uid is null
				) t4
				left join
				(select uidto
					from paydb.basic
					group by uidto 
					having sum(money) >= 10000
				) ba
				on t4.uid = ba.uidto
				where ba.uidto is null		
			) t5
			left join
			(select uid, 
					date,
					province,
					city,
					district
				from regdb.regdetail
			) t6
			on t5.uid = t6.uid
		) t7	
		left join
		(select aa.uid uid,
				collect_set(game) game
			from logindb.roomlogin_detail aa 
			where aa.dt >= '20180301' and aa.dt <= '20180329' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
			group by aa.uid
		) t8
		on t7.uid = t8.uid
	) t9
	left join
	(select aa.uid uid,
			count(distinct aa.date) count_date
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180301' and aa.dt <= '20180329' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
		group by aa.uid
	) t10
	on t9.uid = t10.uid	
	
	
	
	
hive -e "
" > /opt/pc.3.information.csv;

C1：3月移动端登录用户手机号
|用户id|手机号|
select ddd.uid uid,
	   ddd.telephone telephone
	from
	(select tt.uid uid,
			tt.telephone telephone
		from
		(select t1.uid uid,
				t2.telephone telephone
			from
			(select aa.uid uid
				from logindb.halllogin_detail aa 
				where aa.dt >= '20180301' and aa.dt <= '20180329' and aa.group in (6, 66, 8, 88)
				group by uid
			) t1
			left join
			(select uid, 
				   telephone 
				from tempdb.uid_telephone 
				where dt='20180327'
				group by uid, telephone
			) t2
			on t1.uid = t2.uid
			where t2.uid is not null
		) tt
		left join
		(select hd.uid uid
			from logindb.halllogin_detail hd 
			where hd.game in (316,421,450,311,23,445) 
			group by uid
		UNION ALL
		select rd.uid uid
			from logindb.roomlogin_detail rd 
			where rd.game in (316,421,450,311,23,445)
			group by uid
		) yinshou	
		on tt.uid = yinshou.uid
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
	
hive -e "
" > /opt/mobile.3.telephone.csv;

C2：3月移动端登录用户信息（注：用户均为有手机号的用户。）
|用户id|注册日期|注册地区|3月1日至3月28日期间的登录天数|登录过的游戏|最后一次登录时间|手机号|

select t7.uid uid, 
		t8.count_date count_date, 
		t7.end_date end_date, 
		t7.telephone telephone,
		t7.reg_date reg_date,
		t7.province province,
		t7.city city,
		t7.district district,
		t8.game
	from
	(select t5.uid uid, 
			t5.end_date end_date, 
			t5.telephone telephone,
			t6.date reg_date,
			t6.province province,
			t6.city city,
			t6.district district
		from
		(select t4.uid uid, 
				t4.end_date end_date, 
				t4.telephone telephone
			from
			(select t3.uid uid, 
					t3.end_date end_date, 
					t3.telephone telephone
				from
				(select t1.uid uid,
						t1.date end_date,
						t2.telephone telephone
					from
					(select t.uid uid, 
							t.date date
						from
						(select aa.uid uid,
								aa.date date,
								row_number() over(partition by aa.uid order by aa.date desc) as rn	
							from logindb.halllogin_detail aa 
							where aa.dt >= '20180301' and aa.dt <= '20180329' and aa.group in (6, 66, 8, 88)
							group by uid, date
						) t
						where rn = 1
					) t1
					left join
					(select uid, 
						   telephone 
						from tempdb.uid_telephone 
						where dt='20180327'
						group by uid, telephone
					) t2
					on t1.uid = t2.uid
					where t2.uid is not null
				) t3
				left join
				(select hd.uid uid
					from logindb.halllogin_detail hd 
					where hd.game in (316,421,450,311,23,445) 
					group by uid
				UNION ALL
				select rd.uid uid
					from logindb.roomlogin_detail rd 
					where rd.game in (316,421,450,311,23,445)
					group by uid
				) yinshou	
				on t3.uid = yinshou.uid
				where yinshou.uid is null
			) t4
			left join
			(select uidto
				from paydb.basic
				group by uidto 
				having sum(money) >= 10000
			) ba
			on t4.uid = ba.uidto
			where ba.uidto is null		
		) t5
		left join
		(select uid, 
				date,
				province,
				city,
				district
			from regdb.regdetail
		) t6
		on t5.uid = t6.uid
	) t7	
	left join
	(select aa.uid uid,
			count(distinct aa.date) count_date,
			collect_set(game) game
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180301' and aa.dt <= '20180329' and aa.group in (6, 66, 8, 88)
		group by aa.uid
	) t8
	on t7.uid = t8.uid

hive -e "
" > /opt/mobile.3.information.csv;


导出后命名表格：
pc.3.telephone.csv
pc.3.information.csv

mobile.3.telephone.csv
mobile.3.information.csv

	
【报表】同城游pc&移动18年第四批有效召回用户跟踪
1. 用户ID请区分移动和pc（附件已分为两个sheet页）
    移动端只记录同城游app(app = 3001)
    PC端只记录大厅客户端  (halllogin_detail 中 group > 0 and  group not in (6,66,8,88,68,69,55))
2.导出用户时间周期：
pc:3月26日-4月1日
移动:3月24日-3月30日

|用户id|登录游戏ID|17年历史充值总额|周期内充值总数|周期内充值次数|周期内大厅登陆次数(count(app))|

导出后表格命名字段
pc.liushi.0402.csv
mobile.liushi.0402.csv
LOAD DATA  INPATH '/user/root/pc' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='pc');
LOAD DATA  INPATH '/user/root/mobile' overwrite  INTO TABLE tempdb.login_uid PARTITION (dt='mobile');

mobile:
select t2.uid,
	   t2.app_cishu,
	   t5.chongzhi_cishu,
	   t5.money,
	   t5.2017_money,
	   t2.game
	from 
	(select alluids.uid uid,
			tt.app_cishu app_cishu,
			tt.game game
		from 
		(select uid 
			from tempdb.login_uid
			WHERE dt='mobile'
			group by uid
		) alluids
		left join
		(select hd.uid uid, 
				hd.app_cishu app_cishu, 
				t1.game game
			from
			(select uid,
					count(app) app_cishu
				from logindb.halllogin_detail
				where dt >= '20180324' and dt <= '20180330' and app = 3001
				group by uid
			) hd
			left join
			(select uid,
					collect_set(game) game
				from logindb.halllogin_detail
				where dt >= '20180324' and dt <= '20180330' and app = 3001
				group by uid
			) t1
			on hd.uid = t1.uid
		) tt
		on alluids.uid = tt.uid	
	) t2
	left join
	(select t3.uidto uidto, 
			t3.chongzhi_cishu chongzhi_cishu, 
			t3.money money, 
			t4.2017_money 2017_money
		from
		(select uidto,
				count(money) chongzhi_cishu,
				sum(money) money
			from paydb.basic
			where paydate >= '20180324' and paydate <= '20180330'
			group by uidto
		) t3
		left join
		(select uidto, 
				sum(money) 2017_money
			from paydb.basic
			where paydate >= '20170101' and paydate <= '20171231'
			group by uidto
		) t4
		on t3.uidto = t4.uidto
	) t5
	on t2.uid = t5.uidto
hive -e "
" > /opt/mobile.liushi.0402.csv;


pc: 3月26日-4月1日
select t2.uid,
	   t2.app_cishu,
	   t5.chongzhi_cishu,
	   t5.money,
	   t5.2017_money,
	   t2.game
	from 
	(select alluids.uid uid,
			tt.app_cishu app_cishu,
			tt.game game
		from 
		(select uid 
			from tempdb.login_uid
			WHERE dt='pc'
			group by uid
		) alluids
		left join
		(select hd.uid uid, 
				hd.app_cishu app_cishu, 
				t1.game game
			from
			(select uid,
					count(app) app_cishu
				from logindb.halllogin_detail
				where dt >= '20180326' and dt <= '20180401' and group > 0 and  group not in (6,66,8,88,68,69,55)
				group by uid
			) hd
			left join
			(select uid,
					collect_set(game) game
				from logindb.roomlogin_detail
				where dt >= '20180326' and dt <= '20180401' and group > 0 and  group not in (6,66,8,88,68,69,55)
				group by uid
			) t1
			on hd.uid = t1.uid
		) tt
		on alluids.uid = tt.uid	
	) t2
	left join
	(select t3.uidto uidto, 
			t3.chongzhi_cishu chongzhi_cishu, 
			t3.money money, 
			t4.2017_money 2017_money
		from
		(select uidto,
				count(money) chongzhi_cishu,
				sum(money) money
			from paydb.basic
			where paydate >= '20180326' and paydate <= '20180401'
			group by uidto
		) t3
		left join
		(select uidto, 
				sum(money) 2017_money
			from paydb.basic
			where paydate >= '20170101' and paydate <= '20171231'
			group by uidto
		) t4
		on t3.uidto = t4.uidto
	) t5
	on t2.uid = t5.uidto
hive -e "
" > /opt/pc.liushi.0402.csv;







select t2.uid,
	   t2.app_cishu,
	   t5.chongzhi_cishu,
	   t5.money,
	   t5.2017_money,
	   t2.game
	from 
	(select alluids.uid uid,
			tt.app_cishu app_cishu,
			tt.game game
		from 
		(select uid 
			from tempdb.login_uid
			WHERE dt='pc'
			group by uid
		) alluids
		left join
		(select hd.uid uid, 
				hd.app_cishu app_cishu, 
				t1.game game
			from
			(select uid,
					count(app) app_cishu
				from logindb.roomlogin_detail
				where dt >= '20180326' and dt <= '20180401' and group > 0 and  group not in (6,66,8,88,68,69,55)
				group by uid
			) hd
			left join
			(select uid,
					collect_set(game) game
				from logindb.roomlogin_detail
				where dt >= '20180326' and dt <= '20180401' and group > 0 and  group not in (6,66,8,88,68,69,55)
				group by uid
			) t1
			on hd.uid = t1.uid
		) tt
		on alluids.uid = tt.uid	
	) t2
	left join
	(select t3.uidto uidto, 
			t3.chongzhi_cishu chongzhi_cishu, 
			t3.money money, 
			t4.2017_money 2017_money
		from
		(select uidto,
				count(money) chongzhi_cishu,
				sum(money) money
			from paydb.basic
			where paydate >= '20180326' and paydate <= '20180401'
			group by uidto
		) t3
		left join
		(select uidto, 
				sum(money) 2017_money
			from paydb.basic
			where paydate >= '20170101' and paydate <= '20171231'
			group by uidto
		) t4
		on t3.uidto = t4.uidto
	) t5
	on t2.uid = t5.uidto
hive -e "
" > /opt/pc.liushi.0402.csv;


endbout  房卡对局  游戏对局消息处理方案  上报时间点：房间每一局游戏的结束的时候上报一条该数据
endroom    游戏房间解散消息处理方案 上报时间点：房间解散时候上报一条该数据
gameactive    进房间    游戏活跃消息处理方案  上报时间点: 用户进入熟人包房一次,便上报一条该数据
tcyappactive       平台活跃消息处理方案
vaildroom   成功开局  游戏成功组桌消息处理方案  上报时间点：房间第一局游戏的开始的时候上报一条该数据

新用户示例：
打开房卡页面的5.2.7用户且注册日期为2月1日的用户数

select reg_uid.date,
	   count(fangka_uid.uid)
	from
	(select uid 
		FROM gsplaytogetherdb.tcyappactive 
		WHERE vers='5.2.7'
	) fangka_uid
	left join
	(select uid,
			date
		from regdb.regdetail 
		where date = '20180201'
	) reg_uid
	on fangka_uid.uid = reg_uid.uid
	where reg_uid.uid is NOT NULL
	group by reg_uid.date

基于B2，在2月1日至3月31日期间打开房卡房间的用户数
gameactive
select t3.date date,
		t3.count_fangka count_fangka,
		t4.count_dakai_fangka count_dakai_fangka
	from
	(select reg_uid.date date,
		   count(fangka_uid.uid) count_fangka
		from
		(select uid 
			FROM gsplaytogetherdb.tcyappactive 
			WHERE vers='5.2.7'
		) fangka_uid
		left join
		(select uid,
				date
			from regdb.regdetail 
			where date = '20180201'
		) reg_uid
		on fangka_uid.uid = reg_uid.uid
		where reg_uid.uid is NOT NULL
		group by reg_uid.date
	) t3
	left join
	(select t1.date date,
			count(t1.uid) count_dakai_fangka
		from
		(select fangka_uid.uid uid,
				reg_uid.date date
			from
			(select uid 
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7'
			) fangka_uid
			left join
			(select uid,
					date
				from regdb.regdetail 
				where date = '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uid
			from gsplaytogetherdb.gameactive 
			where date >= '20180201' and date <= '20180331'
		) t2
		on t1.uid = t2.uid
		where t2.uid is NOT NULL
		group by t1.date
	) t4
	on 	t3.date = t4.date
	
基于B2，在2月1日至3月31日期间完成房卡对局的用户数
endbout
select t5.date date,
		t5.count_fangka count_fangka,
		t5.count_dakai_fangka count_dakai_fangka,
		t6.count_fangka_duiju count_fangka_duiju
	from
	(select t3.date date,
			t3.count_fangka count_fangka,
			t4.count_dakai_fangka count_dakai_fangka
		from
		(select reg_uid.date date,
			   count(fangka_uid.uid) count_fangka
			from
			(select uid 
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7'
			) fangka_uid
			left join
			(select uid,
					date
				from regdb.regdetail 
				where date = '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
			group by reg_uid.date
		) t3
		left join
		(select t1.date date,
				count(t1.uid) count_dakai_fangka
			from
			(select fangka_uid.uid uid,
					reg_uid.date date
				from
				(select uid 
					FROM gsplaytogetherdb.tcyappactive 
					WHERE vers='5.2.7'
				) fangka_uid
				left join
				(select uid,
						date
					from regdb.regdetail 
					where date = '20180201'
				) reg_uid
				on fangka_uid.uid = reg_uid.uid
				where reg_uid.uid is NOT NULL
			) t1
			left join
			(select uid
				from gsplaytogetherdb.gameactive 
				where date >= '20180201' and date <= '20180331'
			) t2
			on t1.uid = t2.uid
			where t2.uid is NOT NULL
			group by t1.date
		) t4
		on t3.date = t4.date
	) t5	
	left join
	(select t1.date date,
			count(t1.uid) count_fangka_duiju
		from
		(select fangka_uid.uid uid,
				reg_uid.date date
			from
			(select uid 
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7'
			) fangka_uid
			left join
			(select uid,
					date
				from regdb.regdetail 
				where date = '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uid
			from gsplaytogetherdb.endbout 
			where date >= '20180201' and date <= '20180331'
		) t2
		on t1.uid = t2.uid
		where t2.uid is NOT NULL
		group by t1.date
	) t6
	on t5.date = t6.date


基于B2，在2月1日至3月31日期间消耗欢乐币/券用户数
orderdetails
消耗欢乐币  Operatecode  = 205开头
select t7.date date,
		t7.count_fangka count_fangka,
		t7.count_dakai_fangka count_dakai_fangka,
		t7.count_fangka_duiju count_fangka_duiju,
		t8.count_xiaohao_yonghushu count_xiaohao_yonghushu,
		t8.count_xiaohao_cishu count_xiaohao_cishu
	from
	(select t5.date date,
			t5.count_fangka count_fangka,
			t5.count_dakai_fangka count_dakai_fangka,
			t6.count_fangka_duiju count_fangka_duiju
		from
		(select t3.date date,
				t3.count_fangka count_fangka,
				t4.count_dakai_fangka count_dakai_fangka
			from
			(select reg_uid.date date,
				   count(fangka_uid.uid) count_fangka
				from
				(select uid 
					FROM gsplaytogetherdb.tcyappactive 
					WHERE vers='5.2.7'
				) fangka_uid
				left join
				(select uid,
						date
					from regdb.regdetail 
					where date = '20180201'
				) reg_uid
				on fangka_uid.uid = reg_uid.uid
				where reg_uid.uid is NOT NULL
				group by reg_uid.date
			) t3
			left join
			(select t1.date date,
					count(t1.uid) count_dakai_fangka
				from
				(select fangka_uid.uid uid,
						reg_uid.date date
					from
					(select uid 
						FROM gsplaytogetherdb.tcyappactive 
						WHERE vers='5.2.7'
					) fangka_uid
					left join
					(select uid,
							date
						from regdb.regdetail 
						where date = '20180201'
					) reg_uid
					on fangka_uid.uid = reg_uid.uid
					where reg_uid.uid is NOT NULL
				) t1
				left join
				(select uid
					from gsplaytogetherdb.gameactive 
					where date >= '20180201' and date <= '20180331'
				) t2
				on t1.uid = t2.uid
				where t2.uid is NOT NULL
				group by t1.date
			) t4
			on t3.date = t4.date
		) t5	
		left join
		(select t1.date date,
				count(t1.uid) count_fangka_duiju
			from
			(select fangka_uid.uid uid,
					reg_uid.date date
				from
				(select uid 
					FROM gsplaytogetherdb.tcyappactive 
					WHERE vers='5.2.7'
				) fangka_uid
				left join
				(select uid,
						date
					from regdb.regdetail 
					where date = '20180201'
				) reg_uid
				on fangka_uid.uid = reg_uid.uid
				where reg_uid.uid is NOT NULL
			) t1
			left join
			(select uid
				from gsplaytogetherdb.endbout 
				where date >= '20180201' and date <= '20180331'
			) t2
			on t1.uid = t2.uid
			where t2.uid is NOT NULL
			group by t1.date
		) t6
		on t5.date = t6.date
	) t7
	left join
	(select t1.date date,
			count(distinct t1.uid) count_xiaohao_yonghushu,
			count(t2.operatecode) count_xiaohao_cishu
		from
		(select fangka_uid.uid uid,
				reg_uid.date date
			from
			(select uid 
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7'
			) fangka_uid
			left join
			(select uid,
					date
				from regdb.regdetail 
				where date = '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uid, 
				operatecode
			from happycoindb.orderdetails 
			where date >= '20180201' and date <= '20180331'and substr(operatecode,0,3) like '205%'
		) t2
		on t1.uid = t2.uid
		where t2.uid is NOT NULL
		group by t1.date
	) t8
	on t7.date = t8.date	

	
基于B2，在2月1日至3月31日期间打开充值欢乐币用户数
basic	
select t9.date date,
		t9.count_fangka count_fangka,
		t9.count_dakai_fangka count_dakai_fangka,
		t9.count_fangka_duiju count_fangka_duiju,
		t9.count_xiaohao_yonghushu count_xiaohao_yonghushu,
		t9.count_xiaohao_cishu count_xiaohao_cishu,
		t10.count_chongzhi_yonghushu count_chongzhi_yonghushu
	from
	(select t7.date date,
			t7.count_fangka count_fangka,
			t7.count_dakai_fangka count_dakai_fangka,
			t7.count_fangka_duiju count_fangka_duiju,
			t8.count_xiaohao_yonghushu count_xiaohao_yonghushu,
			t8.count_xiaohao_cishu count_xiaohao_cishu
		from
		(select t5.date date,
				t5.count_fangka count_fangka,
				t5.count_dakai_fangka count_dakai_fangka,
				t6.count_fangka_duiju count_fangka_duiju
			from
			(select t3.date date,
					t3.count_fangka count_fangka,
					t4.count_dakai_fangka count_dakai_fangka
				from
				(select reg_uid.date date,
					   count(distinct fangka_uid.uid) count_fangka
					from
					(select uid 
						FROM gsplaytogetherdb.tcyappactive 
						WHERE vers='5.2.7'
					) fangka_uid
					left join
					(select uid,
							date
						from regdb.regdetail 
						where date = '20180201'
					) reg_uid
					on fangka_uid.uid = reg_uid.uid
					where reg_uid.uid is NOT NULL
					group by reg_uid.date
				) t3
				left join
				(select t1.date date,
						count(distinct t1.uid) count_dakai_fangka
					from
					(select fangka_uid.uid uid,
							reg_uid.date date
						from
						(select uid 
							FROM gsplaytogetherdb.tcyappactive 
							WHERE vers='5.2.7'
						) fangka_uid
						left join
						(select uid,
								date
							from regdb.regdetail 
							where date = '20180201'
						) reg_uid
						on fangka_uid.uid = reg_uid.uid
						where reg_uid.uid is NOT NULL
					) t1
					left join
					(select uid
						from gsplaytogetherdb.gameactive 
						where date >= '20180201' and date <= '20180331'
					) t2
					on t1.uid = t2.uid
					where t2.uid is NOT NULL
					group by t1.date
				) t4
				on t3.date = t4.date
			) t5	
			left join
			(select t1.date date,
					count(distinct t1.uid) count_fangka_duiju
				from
				(select fangka_uid.uid uid,
						reg_uid.date date
					from
					(select uid 
						FROM gsplaytogetherdb.tcyappactive 
						WHERE vers='5.2.7'
					) fangka_uid
					left join
					(select uid,
							date
						from regdb.regdetail 
						where date = '20180201'
					) reg_uid
					on fangka_uid.uid = reg_uid.uid
					where reg_uid.uid is NOT NULL
				) t1
				left join
				(select uid
					from gsplaytogetherdb.endbout 
					where date >= '20180201' and date <= '20180331'
				) t2
				on t1.uid = t2.uid
				where t2.uid is NOT NULL
				group by t1.date
			) t6
			on t5.date = t6.date
		) t7
		left join
		(select t1.date date,
				count(distinct t1.uid) count_xiaohao_yonghushu,
				count(t2.operatecode) count_xiaohao_cishu
			from
			(select fangka_uid.uid uid,
					reg_uid.date date
				from
				(select uid 
					FROM gsplaytogetherdb.tcyappactive 
					WHERE vers='5.2.7'
				) fangka_uid
				left join
				(select uid,
						date
					from regdb.regdetail 
					where date = '20180201'
				) reg_uid
				on fangka_uid.uid = reg_uid.uid
				where reg_uid.uid is NOT NULL
			) t1
			left join
			(select uid, 
					operatecode
				from happycoindb.orderdetails 
				where date >= '20180201' and date <= '20180331'and substr(operatecode,0,3) like '205%'
			) t2
			on t1.uid = t2.uid
			where t2.uid is NOT NULL
			group by t1.date
		) t8
		on t7.date = t8.date
	) t9
	left join
	(select t1.date date,
			count(distinct t1.uid) count_chongzhi_yonghushu
		from
		(select fangka_uid.uid uid,
				reg_uid.date date
			from
			(select uid 
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7'
			) fangka_uid
			left join
			(select uid,
					date
				from regdb.regdetail 
				where date = '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uidto
			from paydb.basic
			where paydate >= '20180201' and paydate <= '20180331'
			group by uidto
		) t2
		on t1.uid = t2.uidto
		where t2.uidto is NOT NULL
		group by t1.date
	) t10
	on t9.date = t10.date

hive -e "
select t9.date date,
		t9.count_fangka count_fangka,
		t9.count_dakai_fangka count_dakai_fangka,
		t9.count_fangka_duiju count_fangka_duiju,
		t9.count_xiaohao_yonghushu count_xiaohao_yonghushu,
		t9.count_xiaohao_cishu count_xiaohao_cishu,
		t10.count_chongzhi_yonghushu count_chongzhi_yonghushu
	from
	(select t7.date date,
			t7.count_fangka count_fangka,
			t7.count_dakai_fangka count_dakai_fangka,
			t7.count_fangka_duiju count_fangka_duiju,
			t8.count_xiaohao_yonghushu count_xiaohao_yonghushu,
			t8.count_xiaohao_cishu count_xiaohao_cishu
		from
		(select t5.date date,
				t5.count_fangka count_fangka,
				t5.count_dakai_fangka count_dakai_fangka,
				t6.count_fangka_duiju count_fangka_duiju
			from
			(select t3.date date,
					t3.count_fangka count_fangka,
					t4.count_dakai_fangka count_dakai_fangka
				from
				(select reg_uid.date date,
					   count(distinct fangka_uid.uid) count_fangka
					from
					(select uid 
						FROM gsplaytogetherdb.tcyappactive 
						WHERE vers='5.2.7'
					) fangka_uid
					left join
					(select uid,
							date
						from regdb.regdetail 
						where date = '20180301'
					) reg_uid
					on fangka_uid.uid = reg_uid.uid
					where reg_uid.uid is NOT NULL
					group by reg_uid.date
				) t3
				left join
				(select t1.date date,
						count(distinct t1.uid) count_dakai_fangka
					from
					(select fangka_uid.uid uid,
							reg_uid.date date
						from
						(select uid 
							FROM gsplaytogetherdb.tcyappactive 
							WHERE vers='5.2.7'
						) fangka_uid
						left join
						(select uid,
								date
							from regdb.regdetail 
							where date = '20180301'
						) reg_uid
						on fangka_uid.uid = reg_uid.uid
						where reg_uid.uid is NOT NULL
					) t1
					left join
					(select uid
						from gsplaytogetherdb.gameactive 
						where date >= '20180301' and date <= '20180331'
					) t2
					on t1.uid = t2.uid
					where t2.uid is NOT NULL
					group by t1.date
				) t4
				on t3.date = t4.date
			) t5	
			left join
			(select t1.date date,
					count(distinct t1.uid) count_fangka_duiju
				from
				(select fangka_uid.uid uid,
						reg_uid.date date
					from
					(select uid 
						FROM gsplaytogetherdb.tcyappactive 
						WHERE vers='5.2.7'
					) fangka_uid
					left join
					(select uid,
							date
						from regdb.regdetail 
						where date = '20180301'
					) reg_uid
					on fangka_uid.uid = reg_uid.uid
					where reg_uid.uid is NOT NULL
				) t1
				left join
				(select uid
					from gsplaytogetherdb.endbout 
					where date >= '20180301' and date <= '20180331'
				) t2
				on t1.uid = t2.uid
				where t2.uid is NOT NULL
				group by t1.date
			) t6
			on t5.date = t6.date
		) t7
		left join
		(select t1.date date,
				count(distinct t1.uid) count_xiaohao_yonghushu,
				count(t2.operatecode) count_xiaohao_cishu
			from
			(select fangka_uid.uid uid,
					reg_uid.date date
				from
				(select uid 
					FROM gsplaytogetherdb.tcyappactive 
					WHERE vers='5.2.7'
				) fangka_uid
				left join
				(select uid,
						date
					from regdb.regdetail 
					where date = '20180301'
				) reg_uid
				on fangka_uid.uid = reg_uid.uid
				where reg_uid.uid is NOT NULL
			) t1
			left join
			(select uid, 
					operatecode
				from happycoindb.orderdetails 
				where date >= '20180301' and date <= '20180331'and substr(operatecode,0,3) like '205%'
			) t2
			on t1.uid = t2.uid
			where t2.uid is NOT NULL
			group by t1.date
		) t8
		on t7.date = t8.date
	) t9
	left join
	(select t1.date date,
			count(distinct t1.uid) count_chongzhi_yonghushu
		from
		(select fangka_uid.uid uid,
				reg_uid.date date
			from
			(select uid 
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7'
			) fangka_uid
			left join
			(select uid,
					date
				from regdb.regdetail 
				where date = '20180301'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uidto
			from paydb.basic
			where paydate >= '20180301' and paydate <= '20180331'
			group by uidto
		) t2
		on t1.uid = t2.uidto
		where t2.uidto is NOT NULL
		group by t1.date
	) t10
	on t9.date = t10.date
" > /opt/20180301.csv;


基于B2，在2月1日至3月31日期间玩家发生对局的游戏1
endbout
app
各个游戏 的用户人数
select t3.date date,
		t3.app app,
		count(distinct uid) count_game_renshu
	from
	(select t1.date date,
			t2.app app
		from
		(select fangka_uid.uid uid,
				reg_uid.date date
			from
			(select uid 
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7'
			) fangka_uid
			left join
			(select uid,
					date
				from regdb.regdetail 
				where date = '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uid,
				app
			from gsplaytogetherdb.endbout 
			where date >= '20180201' and date <= '20180331'
		) t2
		on t1.uid = t2.uid
		where t2.uid is NOT NULL
		group by t1.date, t2.app
	) t3
	left join
	(select uid,
			app
		from gsplaytogetherdb.endbout 
		where date >= '20180201' and date <= '20180331'
		group by uid, app
	) t4
	on t3.app = t4.app
	group by t3.date, t3.app

hive -e "
" > /opt/game20180201.csv;



老用户示例：
2月1日打开房卡页面的版本为5.2.7，且注册日期在2月1日前的用户数量
(select fangka_uid.date date,
	   count(distinct fangka_uid.uid) count_fangka
	from
	(select uid,
			date
		FROM gsplaytogetherdb.tcyappactive 
		WHERE vers='5.2.7' and date='20180201'
	) fangka_uid
	left join
	(select uid
		from regdb.regdetail 
		where date < '20180201'
	) reg_uid
	on fangka_uid.uid = reg_uid.uid
	where reg_uid.uid is NOT NULL
	group by fangka_uid.date	
	) t3
	
	
2月1日打开房卡页面的版本为5.2.7，且注册日期在2月1日前的用户数量
基于B2，在2月1日打开房卡房间的用户数

select t3.date date,
		t3.count_fangka count_fangka,
		t4.count_dakai_fangka count_dakai_fangka
	from
	(select fangka_uid.date date,
		   count(distinct fangka_uid.uid) count_fangka
		from
		(select uid,
				date
			FROM gsplaytogetherdb.tcyappactive 
			WHERE vers='5.2.7' and date='20180201'
		) fangka_uid
		left join
		(select uid
			from regdb.regdetail 
			where date < '20180201'
		) reg_uid
		on fangka_uid.uid = reg_uid.uid
		where reg_uid.uid is NOT NULL
		group by fangka_uid.date	
	) t3
	left join
	(select t1.date date,
			count(distinct t1.uid) count_dakai_fangka
		from
		(select fangka_uid.uid uid,
				fangka_uid.date date
			from
			(select uid,
					date
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7' and date = '20180201'
			) fangka_uid
			left join
			(select uid
				from regdb.regdetail 
				where date < '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uid
			from gsplaytogetherdb.gameactive 
			where date = '20180201'
		) t2
		on t1.uid = t2.uid
		where t2.uid is NOT NULL
		group by t1.date
	) t4
	on t3.date = t4.date
	

2月1日打开房卡页面的版本为5.2.7，且注册日期在2月1日前的用户数量	
基于B2，在2月1日完成房卡对局的用户数	
endbout
select t5.date date,
		t5.count_fangka count_fangka,
		t5.count_dakai_fangka count_dakai_fangka,
		t6.count_fangka_duiju count_fangka_duiju
	from
	(select t3.date date,
			t3.count_fangka count_fangka,
			t4.count_dakai_fangka count_dakai_fangka
		from
		(select fangka_uid.date date,
			   count(distinct fangka_uid.uid) count_fangka
			from
			(select uid,
					date
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7' and date = '20180201'
			) fangka_uid
			left join
			(select uid
				from regdb.regdetail 
				where date < '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
			group by fangka_uid.date	
		) t3
		left join
		(select t1.date date,
				count(distinct t1.uid) count_dakai_fangka
			from
			(select fangka_uid.uid uid,
					fangka_uid.date date
				from
				(select uid,
						date
					FROM gsplaytogetherdb.tcyappactive 
					WHERE vers='5.2.7' and date = '20180201'
				) fangka_uid
				left join
				(select uid
					from regdb.regdetail 
					where date < '20180201'
				) reg_uid
				on fangka_uid.uid = reg_uid.uid
				where reg_uid.uid is NOT NULL
			) t1
			left join
			(select uid
				from gsplaytogetherdb.gameactive 
				where date = '20180201'
			) t2
			on t1.uid = t2.uid
			where t2.uid is NOT NULL
			group by t1.date
		) t4
		on t3.date = t4.date
	) t5
	left join
	(select t1.date date,
			count(distinct t1.uid) count_fangka_duiju
		from
		(select fangka_uid.uid uid,
				fangka_uid.date date
			from
			(select uid,
					date
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7' and date='20180201'
			) fangka_uid
			left join
			(select uid
				from regdb.regdetail 
				where date < '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uid
			from gsplaytogetherdb.endbout 
			where date = '20180201'
		) t2
		on t1.uid = t2.uid
		where t2.uid is NOT NULL
		group by t1.date
	) t6
	on t5.date = t6.date

基于B2，在2月1日消耗欢乐币/券用户数  
基于B2，在2月1日欢乐币消耗次数

select t7.date date,
		t7.count_fangka count_fangka,
		t7.count_dakai_fangka count_dakai_fangka,
		t7.count_fangka_duiju count_fangka_duiju,
		t8.count_xiaohao_yonghushu count_xiaohao_yonghushu,
		t8.count_xiaohao_cishu count_xiaohao_cishu
	from		
	(select t5.date date,
			t5.count_fangka count_fangka,
			t5.count_dakai_fangka count_dakai_fangka,
			t6.count_fangka_duiju count_fangka_duiju
		from
		(select t3.date date,
				t3.count_fangka count_fangka,
				t4.count_dakai_fangka count_dakai_fangka
			from
			(select fangka_uid.date date,
				   count(distinct fangka_uid.uid) count_fangka
				from
				(select uid,
						date
					FROM gsplaytogetherdb.tcyappactive 
					WHERE vers='5.2.7' and date = '20180201'
				) fangka_uid
				left join
				(select uid
					from regdb.regdetail 
					where date < '20180201'
				) reg_uid
				on fangka_uid.uid = reg_uid.uid
				where reg_uid.uid is NOT NULL
				group by fangka_uid.date	
			) t3
			left join
			(select t1.date date,
					count(distinct t1.uid) count_dakai_fangka
				from
				(select fangka_uid.uid uid,
						fangka_uid.date date
					from
					(select uid,
							date
						FROM gsplaytogetherdb.tcyappactive 
						WHERE vers='5.2.7' and date = '20180201'
					) fangka_uid
					left join
					(select uid
						from regdb.regdetail 
						where date < '20180201'
					) reg_uid
					on fangka_uid.uid = reg_uid.uid
					where reg_uid.uid is NOT NULL
				) t1
				left join
				(select uid
					from gsplaytogetherdb.gameactive 
					where date = '20180201'
				) t2
				on t1.uid = t2.uid
				where t2.uid is NOT NULL
				group by t1.date
			) t4
			on t3.date = t4.date
		) t5
		left join
		(select t1.date date,
				count(distinct t1.uid) count_fangka_duiju
			from
			(select fangka_uid.uid uid,
					fangka_uid.date date
				from
				(select uid,
						date
					FROM gsplaytogetherdb.tcyappactive 
					WHERE vers='5.2.7' and date='20180201'
				) fangka_uid
				left join
				(select uid
					from regdb.regdetail 
					where date < '20180201'
				) reg_uid
				on fangka_uid.uid = reg_uid.uid
				where reg_uid.uid is NOT NULL
			) t1
			left join
			(select uid
				from gsplaytogetherdb.endbout 
				where date = '20180201'
			) t2
			on t1.uid = t2.uid
			where t2.uid is NOT NULL
			group by t1.date
		) t6
		on t5.date = t6.date
	) t7
	left join
	(select t1.date date,
			count(distinct t1.uid) count_xiaohao_yonghushu,
			count(t2.operatecode) count_xiaohao_cishu
		from
		(select fangka_uid.uid uid,
				fangka_uid.date date
			from
			(select uid,
					date
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7' and date='20180201'
			) fangka_uid
			left join
			(select uid
				from regdb.regdetail 
				where date < '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uid, 
				operatecode
			from happycoindb.orderdetails 
			where date = '20180201' and substr(operatecode,0,3) like '205%'
		) t2
		on t1.uid = t2.uid
		where t2.uid is NOT NULL
		group by t1.date
	) t8
	on t7.date = t8.date

2月1日打开房卡页面的版本为5.2.7，且注册日期在2月1日前的用户数量		
基于B2，在2月1日充值欢乐币用户数

select t9.date date,
		t9.count_fangka count_fangka,
		t9.count_dakai_fangka count_dakai_fangka,
		t9.count_fangka_duiju count_fangka_duiju,
		t9.count_xiaohao_yonghushu count_xiaohao_yonghushu,
		t9.count_xiaohao_cishu count_xiaohao_cishu,
		t10.count_chongzhi_yonghushu count_chongzhi_yonghushu
	from
	(select t7.date date,
			t7.count_fangka count_fangka,
			t7.count_dakai_fangka count_dakai_fangka,
			t7.count_fangka_duiju count_fangka_duiju,
			t8.count_xiaohao_yonghushu count_xiaohao_yonghushu,
			t8.count_xiaohao_cishu count_xiaohao_cishu
		from		
		(select t5.date date,
				t5.count_fangka count_fangka,
				t5.count_dakai_fangka count_dakai_fangka,
				t6.count_fangka_duiju count_fangka_duiju
			from
			(select t3.date date,
					t3.count_fangka count_fangka,
					t4.count_dakai_fangka count_dakai_fangka
				from
				(select fangka_uid.date date,
					   count(distinct fangka_uid.uid) count_fangka
					from
					(select uid,
							date
						FROM gsplaytogetherdb.tcyappactive 
						WHERE vers='5.2.7' and date = '20180201'
					) fangka_uid
					left join
					(select uid
						from regdb.regdetail 
						where date < '20180201'
					) reg_uid
					on fangka_uid.uid = reg_uid.uid
					where reg_uid.uid is NOT NULL
					group by fangka_uid.date	
				) t3
				left join
				(select t1.date date,
						count(distinct t1.uid) count_dakai_fangka
					from
					(select fangka_uid.uid uid,
							fangka_uid.date date
						from
						(select uid,
								date
							FROM gsplaytogetherdb.tcyappactive 
							WHERE vers='5.2.7' and date = '20180201'
						) fangka_uid
						left join
						(select uid
							from regdb.regdetail 
							where date < '20180201'
						) reg_uid
						on fangka_uid.uid = reg_uid.uid
						where reg_uid.uid is NOT NULL
					) t1
					left join
					(select uid
						from gsplaytogetherdb.gameactive 
						where date = '20180201'
					) t2
					on t1.uid = t2.uid
					where t2.uid is NOT NULL
					group by t1.date
				) t4
				on t3.date = t4.date
			) t5
			left join
			(select t1.date date,
					count(distinct t1.uid) count_fangka_duiju
				from
				(select fangka_uid.uid uid,
						fangka_uid.date date
					from
					(select uid,
							date
						FROM gsplaytogetherdb.tcyappactive 
						WHERE vers='5.2.7' and date='20180201'
					) fangka_uid
					left join
					(select uid
						from regdb.regdetail 
						where date < '20180201'
					) reg_uid
					on fangka_uid.uid = reg_uid.uid
					where reg_uid.uid is NOT NULL
				) t1
				left join
				(select uid
					from gsplaytogetherdb.endbout 
					where date = '20180201'
				) t2
				on t1.uid = t2.uid
				where t2.uid is NOT NULL
				group by t1.date
			) t6
			on t5.date = t6.date
		) t7
		left join
		(select t1.date date,
				count(distinct t1.uid) count_xiaohao_yonghushu,
				count(t2.operatecode) count_xiaohao_cishu
			from
			(select fangka_uid.uid uid,
					fangka_uid.date date
				from
				(select uid,
						date
					FROM gsplaytogetherdb.tcyappactive 
					WHERE vers='5.2.7' and date='20180201'
				) fangka_uid
				left join
				(select uid
					from regdb.regdetail 
					where date < '20180201'
				) reg_uid
				on fangka_uid.uid = reg_uid.uid
				where reg_uid.uid is NOT NULL
			) t1
			left join
			(select uid, 
					operatecode
				from happycoindb.orderdetails 
				where date = '20180201' and substr(operatecode,0,3) like '205%'
			) t2
			on t1.uid = t2.uid
			where t2.uid is NOT NULL
			group by t1.date
		) t8
		on t7.date = t8.date
	) t9
	left join
	(select t1.date date,
			count(distinct t1.uid) count_chongzhi_yonghushu
		from
		(select fangka_uid.uid uid,
				fangka_uid.date date
			from
			(select uid,
					date
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7' and date='20180201'
			) fangka_uid
			left join
			(select uid
				from regdb.regdetail 
				where date < '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uidto
			from paydb.basic
			where paydate = '20180201'
			group by uidto
		) t2
		on t1.uid = t2.uidto
		where t2.uidto is NOT NULL
		group by t1.date
	) t10
	on t9.date = t10.date	
	
	
2月2日打开房卡页面的版本为5.2.7，且注册日期在2月1日前的用户

select t9.date date,
		t9.count_fangka count_fangka,
		t9.count_dakai_fangka count_dakai_fangka,
		t9.count_fangka_duiju count_fangka_duiju,
		t9.count_xiaohao_yonghushu count_xiaohao_yonghushu,
		t9.count_xiaohao_cishu count_xiaohao_cishu,
		t10.count_chongzhi_yonghushu count_chongzhi_yonghushu
	from
	(select t7.date date,
			t7.count_fangka count_fangka,
			t7.count_dakai_fangka count_dakai_fangka,
			t7.count_fangka_duiju count_fangka_duiju,
			t8.count_xiaohao_yonghushu count_xiaohao_yonghushu,
			t8.count_xiaohao_cishu count_xiaohao_cishu
		from
		(select t5.date date,
				t5.count_fangka count_fangka,
				t5.count_dakai_fangka count_dakai_fangka,
				t6.count_fangka_duiju count_fangka_duiju
			from
			(select t3.date date,
					t3.count_fangka count_fangka,
					t4.count_dakai_fangka count_dakai_fangka
				from
				
				(select fangka_uid.date date,
					   count(distinct fangka_uid.uid) count_fangka
					from
					(select uid,
							date
						FROM gsplaytogetherdb.tcyappactive 
						WHERE vers='5.2.7' and date='20180201'
					) fangka_uid
					left join
					(select uid,
						from regdb.regdetail 
						where date > '20180201'
					) reg_uid
					on fangka_uid.uid = reg_uid.uid
					where reg_uid.uid is NOT NULL
					group by fangka_uid.date	
				) t3
				left join
				(select t1.date date,
						count(distinct t1.uid) count_dakai_fangka
					from
					(select fangka_uid.uid uid,
							fangka_uid.date date
						from
						(select uid,
								date
							FROM gsplaytogetherdb.tcyappactive 
							WHERE vers='5.2.7' and date='20180201'
						) fangka_uid
						left join
						(select uid
							from regdb.regdetail 
							where date >'20180201'
						) reg_uid
						on fangka_uid.uid = reg_uid.uid
						where reg_uid.uid is NOT NULL
					) t1
					left join
					(select uid
						from gsplaytogetherdb.gameactive 
						where date = '20180201'
					) t2
					on t1.uid = t2.uid
					where t2.uid is NOT NULL
					group by t1.date
				) t4
				on t3.date = t4.date
			) t5	
			left join
			(select t1.date date,
					count(distinct t1.uid) count_fangka_duiju
				from
				(select fangka_uid.uid uid,
						fangka_uid.date date
					from
					(select uid 
						FROM gsplaytogetherdb.tcyappactive 
						WHERE vers='5.2.7' and date='20180201'
					) fangka_uid
					left join
					(select uid
						from regdb.regdetail 
						where date > '20180201'
					) reg_uid
					on fangka_uid.uid = reg_uid.uid
					where reg_uid.uid is NOT NULL
				) t1
				left join
				(select uid
					from gsplaytogetherdb.endbout 
					where date = '20180201'
				) t2
				on t1.uid = t2.uid
				where t2.uid is NOT NULL
				group by t1.date
			) t6
			on t5.date = t6.date
		) t7
		left join
		(select t1.date date,
				count(distinct t1.uid) count_xiaohao_yonghushu,
				count(t2.operatecode) count_xiaohao_cishu
			from
			(select fangka_uid.uid uid,
					reg_uid.date date
				from
				(select uid 
					FROM gsplaytogetherdb.tcyappactive 
					WHERE vers='5.2.7'
				) fangka_uid
				left join
				(select uid,
						date
					from regdb.regdetail 
					where date = '20180201'
				) reg_uid
				on fangka_uid.uid = reg_uid.uid
				where reg_uid.uid is NOT NULL
			) t1
			left join
			(select uid, 
					operatecode
				from happycoindb.orderdetails 
				where date >= '20180201' and date <= '20180331'and substr(operatecode,0,3) like '205%'
			) t2
			on t1.uid = t2.uid
			where t2.uid is NOT NULL
			group by t1.date
		) t8
		on t7.date = t8.date
	) t9
	left join
	(select t1.date date,
			count(distinct t1.uid) count_chongzhi_yonghushu
		from
		(select fangka_uid.uid uid,
				reg_uid.date date
			from
			(select uid 
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7'
			) fangka_uid
			left join
			(select uid,
					date
				from regdb.regdetail 
				where date = '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uidto
			from paydb.basic
			where paydate >= '20180201' and paydate <= '20180331'
			group by uidto
		) t2
		on t1.uid = t2.uidto
		where t2.uidto is NOT NULL
		group by t1.date
	) t10
	on t9.date = t10.date
hive -e "
" > /opt/20180201.csv;




2月1日打开房卡页面的版本为5.2.7，且注册日期在2月1日前的用户
基于B2，在2月1日玩家发生对局的游戏1
基于B2，游戏1的用户人数

select t3.date date,
		t3.app app,
		count(distinct t4.uid) count_game_renshu
	from
	(select t1.date date,
			t2.app app
		from
		(select fangka_uid.date date,
				fangka_uid.uid uid
			from
			(select uid,
					date
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7' and date = '20180201'
			) fangka_uid
			left join
			(select uid
				from regdb.regdetail 
				where date < '20180201'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uid,
				app
			from gsplaytogetherdb.endbout 
			where date = '20180201'
		) t2
		on t1.uid = t2.uid
		where t2.uid is NOT NULL
		group by t1.date, t2.app	
	) t3
	left join
	(select uid,
			app
		from gsplaytogetherdb.endbout 
		where date = '20180201'
		group by uid, app
	) t4
	on t3.app = t4.app
	group by t3.date, t3.app

hive -e "
" > /opt/old_20180201.csv;


根据提供的附件：CtUserData040.x4lsx
匹配其中的用户是否绑定了手机，是则标记为1，否则标记为0

select t1.uid, t2.telephone
	from
	(select uid 
		from tempdb.login_uid 
		where  dt='20180409'
	) t1
	left join
	(select uid, 
			telephone 
		from tempdb.uid_telephone 
		where dt='20180327'
		group by uid, telephone
	) t2
	on t1.uid = t2.uid

	
新用户示例
游戏
select t3.date date,
		t3.app app,
		count(distinct t3.uid) count_game_renshu
	from
	(select t1.uid uid,
			t1.date date,
			t2.app app
		from
		(select fangka_uid.uid uid,
				reg_uid.date date
			from
			(select uid 
				FROM gsplaytogetherdb.tcyappactive 
				WHERE vers='5.2.7'
			) fangka_uid
			left join
			(select uid,
					date
				from regdb.regdetail 
				where date = '20180301'
			) reg_uid
			on fangka_uid.uid = reg_uid.uid
			where reg_uid.uid is NOT NULL
		) t1
		left join
		(select uid,
				app
			from gsplaytogetherdb.endbout 
			where date >= '20180301' and date <= '20180331'
		) t2
		on t1.uid = t2.uid
		where t2.uid is NOT NULL
		group by t1.uid, t1.date, t2.app
	) t3
	left join
	(select uid,
			app
		from gsplaytogetherdb.endbout 
		where date >= '20180301' and date <= '20180331'
		group by uid, app
	) t4
	on t3.app = t4.app and t3.uid = t4.uid
	group by t3.date, t3.app

	
iOS renlin账号下用户数据导出需求
1.根据运营提供的用户ID导数据；
2.时间范围：2018年1月1日开始-到目前为止；
3.每个UserID玩过的所有游戏和相应的登录次数；
select t1.uid,
		t2.game,
		count(t2.game)
	from
	(select uid 
		from  tempdb.login_uid 
		WHERE dt='20180413'
	) t1
	left join
	(select aa.uid uid,
			aa.game game
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180101' and aa.dt <= '20180413'
	) t2
	on t1.uid = t2.uid
	group by t1.uid, t2.game
	
4.每个UserID登陆过的客户端类型，分三种：pc、移动安卓、移动iOS。有就填“是”，无就填“否”；
android:
select t1.uid
	from
	(select uid 
		from  tempdb.login_uid 
		WHERE dt='20180413'
	) t1
	left join
	(select aa.uid uid
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180101' and aa.dt <= '20180413' and aa.group in (6, 66)
	) t2
	on t1.uid = t2.uid
	group by t1.uid

ios:
select t1.uid
	from
	(select uid 
		from  tempdb.login_uid 
		WHERE dt='20180413'
	) t1
	left join
	(select aa.uid uid
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180101' and aa.dt <= '20180413' and aa.group in (8, 88)
	) t2
	on t1.uid = t2.uid
	group by t1.uid
	
pc:
select t1.uid
	from
	(select uid 
		from  tempdb.login_uid 
		WHERE dt='20180413'
	) t1
	left join
	(select aa.uid uid
		from logindb.halllogin_detail aa 
		where aa.dt >= '20180101' and aa.dt <= '20180413' and aa.group > 0 and  aa.group not in (6,66,8,88,68,69,55)
	) t2
	on t1.uid = t2.uid
	group by t1.uid

	
5.每个UserID注册时记录的设备号，再通过设备号查该设备下登录过的所有UserID；
hardid string,  注册设备号
select t4.uid
    FROM
	(select t2.hardid hardid
		from
		(select uid 
			from  tempdb.login_uid 
			WHERE dt='20180413'
		) t1
		left join
		(select uid,
				hardid
			from regdb.regdetail 
			group by uid, hardid
		) t2
		on t1.uid = t2.uid
		where t2.uid is not null
		group by t2.hardid
	) t3
	left join
	(select aa.uid uid,
			aa.hardid hardid
		from logindb.halllogin_detail aa
		where aa.dt >= '20180101' and aa.dt <= '20180413'
		group by uid, hardid
	) t4
	on t3.hardid = t4.hardid
	where t4.hardid is not null
	group by t4.uid
	
	

6.表格样式：

|UserID|游戏名称-1|登录次数|游戏名称-2|登录次数|游戏名称-N|登录次数|PC|移动安卓|移动IOS|
|-|-|-|-|-|-|-|-|-|-|-|-|
| | | | | | | | | | | |

|UserID|注册时的设备号|设备下的UserID-1|设备下的UserID-2|设备下的UserID-3|设备下的UserID-N
|-|-|-|-|-|-|-|-|
| | | | | | | |

导出后表格命名字段
youxi.zongduan.0412.csv
shebei.id.0412.csv




















