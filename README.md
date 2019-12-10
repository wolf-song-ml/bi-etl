大数据离线项目之：BI上报
==========
#### 需要知识点：
* 整个hadoop技术栈，所有代码均为java
* Hdfs:文件创建、存储、管理
* Mapreduce:map split机制、shuffle机制、reduce机制、小文件合并机制、自定义输入输出机制、reduce并发输出到mysql
* Habse：列族设计机制、hbase与hive关联
* Hive：行转列、列转行、存储文件机制（重点rcfile、parquet）、分区、分桶、hive与MR调用以及优化、开发UDF
* Flume：数据采集工具
* Sqoop：hdfs、关系数据库、hive之间传输工具

# 1数据说明
## 1.1登录事件
登录事件主要就是表示用户(访客)第一次到网站的事件类型，主要应用于计算新用户等类似任务的计算。
参数名	说明
en	事件名称，launch事件为：e_l
ver	版本号
pl	平台名称，launch事件中为website
sdk	sdk版本号，website平台中为js
u_ud	用户id，唯一标识访客（用户）
u_mid	会员id，业务系统的用户id
u_sd	会话id，标识会话id
c_time	客户端时间
l	平台语言，window.navigator.language
b_iev	浏览器信息，window.navigator.userAgent
b_rst	浏览器屏幕大小，screen.width + "*" + screen.height
## 1.2 访问事件
访问事件是pc端的基本事件类型，主要是描述用户访问网站信息，应用于基本的各个不同计算任务。
参数	参数说明
en	事件名称，访问事件为：e_pv
p_url	当前页面的url
p_ref	当前一个页面的url，如果没有前一个页面，那么值为空
tt	当前页面的标题
## 1.3 事件
事件是专门记录用户对于某些特定事件/活动的触发行为，主要是用于计算各活动的活跃用户以及各个不同访问链路的转化率情况等任务。
参数	参数说明
en	事件名称，event事件为e_e
ca	事件的category值，即事件的种类名称，不为空
ac	事件的action值，即事件的活动名称，不为空
du	事件持续时间，可以为空
kv_	事件自定义属性键值对。比如kv_keyname=value，这里的keyname和value就是用户自定义，支持在事件上定义多个属性键值对
# 2数据采集
## 2.1编写Flume脚本上传日志文件到HDFS
#Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

#Describe/configure the source
a1.sources.r1.type = exec
a1.sources.r1.command = tail -F /usr/local/nginx/user_logs/access.log
a1.sources.r1.shell = /bin/bash -c

#Describe the sink
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = hdfs://192.168.0.244:9000/event-logs/%Y/%m/%d
a1.sinks.k1.hdfs.filePrefix=FlumeData
a1.sinks.k1.hdfs.fileSuffix=.log
#是否按照时间滚动文件夹
a1.sinks.k1.hdfs.round = true
#是否使用本地时间戳
a1.sinks.k1.hdfs.useLocalTimeStamp = true
#积攒多少个Event才flush到HDFS一次
a1.sinks.k1.hdfs.batchSize = 10000
#设置文件类型，可支持压缩
a1.sinks.k1.hdfs.fileType = DataStream
#多久生成一个新的文件
a1.sinks.k1.hdfs.rollInterval = 0
#设置每个文件的滚动大小
a1.sinks.k1.hdfs.rollSize = 131072000
#文件的滚动与Event数量无关
a1.sinks.k1.hdfs.rollCount = 0
#当目前被打开的临时文件在该参数指定的时间（秒）内，没有任何数据写入，则将该临时文件关闭并重命名成目标文件
a1.sinks.k1.hdfs.idleTimeout = 60
#最小冗余数
a1.sinks.k1.hdfs.minBlockReplicas = 1


# Use a channel which buffers events in memory
a1.channels.c1.type = memory
#一般设置为2 * 1024 * 1024 * 100 = 209715200
a1.channels.c1.capacity = 5120000
#单个进程的最大处理能力
a1.channels.c1.transactionCapacity = 512000
a1.channels.c1.keep-alive=60
a1.channels.c1.byteCapacityBufferPercentage=10

#Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
## 2.2关联hdfs
将core-site.xml和hdfs-site.xml文件软连接到flume的conf文件夹中
$ ln -s /opt/modules/cdh/hadoop-2.5.0-cdh5.3.6/etc/hadoop/hdfs-site.xml conf/
$ ln -s /opt/modules/cdh/hadoop-2.5.0-cdh5.3.6/etc/hadoop/core-site.xml conf/
## 2.3启动采集测试
$ bin/flume-ng agent
--conf conf/
--conf-file conf/workspace/flume-load-log-2-hdfs.conf
--name a1 -Dflume.root.logger=INFO,console > logs/flume-load-log-2-hdfs.log 2>&1 &

# 3 数据清洗
## 3.1流程
使用MapReduce通过TextInputFormat的方式将HDFS中的数据读取到map中，最终通过TableOutputFormat到HBase中。
## 3.2细节分析
* 日志解析
日志存储于HDFS中，一行一条日志，解析出操作行为中具体的key-value值，然后进行解码操作。
* IP地址解析/补全
* 浏览器信息解析
* HBase rowkey设计
注意规则：尽可能的短小，占用内存少，尽可能的均匀分布
* HBase表的创建
使用Java API创建
## 3.3代码实现
关键类：
LoggerUtil.java
详情见代码
#### 3.3.1日志解析
IP与Long的互转：
//将127.0.0.1形式的IP地址转换成十进制整数
public long IpToLong(string strIp){
    long[] ip = new long[4];
    int position1 = strIp.IndexOf(".");
    int position2 = strIp.IndexOf(".", position1 + 1);
    int position3 = strIp.IndexOf(".", position2 + 1);
    // 将每个.之间的字符串转换成整型  
    ip[0] = long.Parse(strIp.Substring(0, position1));
    ip[1] = long.Parse(strIp.Substring(position1 + 1, position2 - position1 - 1));
    ip[2] = long.Parse(strIp.Substring(position2 + 1, position3 - position2 - 1));
    ip[3] = long.Parse(strIp.Substring(position3 + 1));
    //进行左移位处理
    return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
}

//将十进制整数形式转换成127.0.0.1形式的ip地址 
public string LongToIp(long ip){
    StringBuilder sb = new StringBuilder();
    //直接右移24位
    sb.Append(ip >> 24);
    sb.Append(".");
    //将高8位置0，然后右移16
    sb.Append((ip & 0x00FFFFFF) >> 16);
    sb.Append(".");
    //将高16位置0，然后右移8位
    sb.Append((ip & 0x0000FFFF) >> 8);
    sb.Append(".");
    //将高24位置0
    sb.Append((ip & 0x000000FF));
    return sb.ToString();
}
### 3.3.2浏览器信息解析
依赖工具：uasparser第三方浏览器信息解析工具
### 3.3.3 ETL代码编写
新建类：
AnalysisDataMapper.java
AnalysisDataRunner.java
目标：读取HDFS中的数据，清洗后写入到HBase中
核心思路梳理：
* Step1、创建AnalysisDataMapper类，复写map方法

* Step2、在map方法中通过LoggerUtil.handleLogText方法将当前行数据解析成Map<String,String>集合clientInfo

* Step3、获取当前行日志信息的事件类型，并根据获取到的事件类型去枚举类型中匹配生成EventEnum对象，如果没有匹配到对应的事件类型，则返回null。

* Step4、判断如果无法处理给定的事件类型，则使用log4j输出。

* Step5、如果可以处理指定事件类型，则开始处理事件，创建handleEventData (Map<String, String> clientInfo, EventEnum event, Context context, Text value)方法处理事件。
* Step6、在handleEventData方法中，我们需要过滤掉那些数据不合法的Event事件，通过
filterEventData(Map<String, String> clientInfo, EventEnum event) 方法过滤，规律规则：如果是java_server过来的数据，则会员id必须存在，如果是website过来的数据，则会话id和用户id必须存在。

* Step7、如果没有通过过滤，则通过日志输出当前数据，如果通过过滤，则开始准备输出数据，创建方法outPutData (Map<String, String> clientInfo, Context context)

* Step8、outputData方法中，我们可以删除一些无用的数据，比如浏览器信息的原始数据（因为已经解析过了）。同时需要创建一个生成rowkey的方法generateRowKey(String uuid, long serverTime, Map<String, String> clientInfo)，通过该方法生成的rowkey之后，添加内容到HBase表中。

* Step9、generateRowKey方法主要用于rowKey的生成，通过拼接：时间+uuid的crc32编码+数据内容的hash码的crc32编码+作为rowkey，一共12个字节。
## 3.4、测试
### 13.4.1上传测试数据
$ /opt/modules/cdh/hadoop-2.5.0-cdh5.3.6/bin/hdfs dfs -mkdir -p /event-logs/2015/12/20
$ /opt/modules/cdh/hadoop-2.5.0-cdh5.3.6/bin/hdfs dfs -put ~/Desktop/20151220.log /event-logs/2015/12/20

### 3.4.2打包集群运行
* 方案一：
修改etc/hadoop/hadoop-env.sh中的HADOOP_CLASSPATH配置信息
例如：
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/opt/modules/cdh/hbase-0.98.6-cdh5.3.6/lib/*
* 方案二：
使用maven插件：maven-shade-plugin，将第三方依赖的jar全部打包进去
参数设置：
1、-P local clean package（不打包第三方jar）
2、-P dev clean package install（打包第三方jar）
# 4 数据分析
## 4.1统计表
stats_user	date_dimension_id
platform_dimension_id
new_install_users
stats_device_browser	date_dimension_id
platform_dimension_id
browser_dimension_id
new_install_users
通过表结构可以发现，只要维度id确定了，那么new_install_users也就确定了。
## 4.2目标
按照不同维度统计新增用户。
## 4.3代码实现
### 4.3.1 Mapper
* Step1、创建NewInstallUsersMapper类，OutPutKey为StatsUserDimension，OutPutValue为Text。定义全局变量，Key和Value的对象

* Step2、覆写map方法，在该方法中读取HBase中待处理的数据，分别要包含维度的字段信息以及必有的字段信息。serverTime、platformName、platformVersion、browserName、browserVersion、uuid

* Step3、数据过滤以及时间字符串转换

* Step4、构建维度信息：天维度，周维度，月维度，platform维度[(name,version)(name,all)(all,all)]，browser维度[(browser,all) (browser,version)]

* Step5、设置outputValue的值为uuid

* Step6、按照不同维度设置outputKey
### 4.3.2 Reducer
* Step1、创建NewInstallUserReducer<StatsUserDimension, Text, StatsUserDimension, MapWritableValue>类，覆写reduce方法。

* Step2、统计uuid出现的次数，并且去重。

* Step3、将数据拼装到outputValue中。

* Step4、设置数据业务KPI类型，最终输出数据。
### 4.3.3 Runner
* Step1、创建NewInstallUserRunner类，实现Tool接口

* Step2、添加时间处理函数，用来截取参数

* Step3、组装Job

* Step4、设置HBase InputFormat（设置从HBase中读取的数据都有哪些）
* Step5、自定义OutPutFormat并设置之
### 4.3.4测试：NewInstallUsers
Maven打包参数：-P dev clean package install
Hadoop环境依赖导入：
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/opt/modules/cdh/hbase-0.98.6-cdh5.3.6/lib/*
提交运行：
/opt/modules/cdh/hadoop-2.5.0-cdh5.3.6/bin/yarn jar ~/Desktop/BI-ETL.jar com.z.etl.mr.statistics.NewInstallUserRunner -date 2015-12-20

# 5 Hive维度分析
## 5.1目标
分析一天24个时间段的新增用户、活跃用户、会话个数和会话长度四个指标，最终将结果保存到HDFS中，使用sqoop导出到Mysql。
## 5.2目标解析
新增用户：分析登录事件中各个不同时间段的uuid数量
活跃用户：分析访问事件中各个不同时间段的uuid数量
会话个数：分析访问事件中各个不同时间段的会话id数量
会话长度：分析访问事件中各个不同时间段内所有会话时长的总和
## 5.3创建Mysql结果表
## 5.4 Hive分析
### 5.4.1创建Hive外部表关联HBase数据表
create external table 
event_logs20151220(
key string,
pl string,
ver string,
en string,
u_ud string,
u_sd string,
s_time bigint) 
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
with serdeproperties("hbase.columns.mapping" = ":key, info:pl,info:ver,info:en,info:u_ud,info:u_sd,info:s_time") 
tblproperties("hbase.table.name" = "event-logs20151220");
### 5.4.2创建临时表用于存放访问和登录事件的数据（即存放过滤数据）
create table stats_hourly_tmp1(pl string, ver string, en string, s_time bigint, u_ud string, u_sd string, hour int, date string) row format delimited fields terminated by '\t';;
### 5.4.3提取e_pv和e_l事件数据到临时表中
from event_logs20151220
insert overwrite table stats_hourly_tmp1
select 
pl,ver,
en,
s_time,
u_ud,
u_sd,
hour(from_unixtime(cast(s_time/1000 as int), 'yyyy-MM-dd HH:mm:ss')), 
from_unixtime(cast(s_time/1000 as int), 'yyyy-MM-dd') 
where en = 'e_pv' or en = 'e_l';
### 5.4.4创建分析结果临时保存表
create table stats_hourly_tmp2(pl string, ver string, date string,hour int, kpi string, value int) row format delimited fields terminated by '\t';;
### 5.4.5分析活跃访客数
* Step1、具体平台，具体平台版本（platform:name, version:version）
from stats_hourly_tmp1
insert overwrite table stats_hourly_tmp2
select 
pl,
ver,
date,
hour,
'active_users',
count(distinct u_ud) as active_users 
where 
en = 'e_pv' 
group by 
pl,
ver,
date,
hour;
* Step2、具体平台，所有版本（platform:name, version:all）
from stats_hourly_tmp1
insert into table stats_hourly_tmp2
select 
pl,
'all',
date,
hour,
'active_users',
count(distinct u_ud) as active_users 
where en = 'e_pv' 
group by 
pl,
date,
hour;
* Step3、所有平台，所有版本（platform:all, version:all）
from stats_hourly_tmp1
insert into table stats_hourly_tmp2
select 
'all',
'all',
date,
hour,
'active_users',
count(distinct u_ud) as active_users 
where en = 'e_pv' 
group by 
date,
hour;
### 5.4.6分析会话长度
将每个会话的长度先要计算出来，然后统计一个时间段的各个会话的总和
* Step1、具体平台，具体平台版本（platform:name, version:version）
from (
select 
pl,ver,
date,
hour,
u_sd,
(max(s_time) - min(s_time)) as s_length
from 
stats_hourly_tmp1 
where en='e_pv' 
group by 
pl,ver,
date,
u_sd,
hour
) as tmp
insert into table stats_hourly_tmp2
select 
pl,
ver,
date,
hour,
'sessions_lengths',
cast(sum(s_length) / 1000 as int) 
group by 
pl,
ver,
date,
hour;

* Step2、具体平台，所有版本（platform:name, version:all）
from (
select 
pl,
date,
hour,
u_sd,
(max(s_time) - min(s_time)) as s_length
from 
stats_hourly_tmp1 
where en='e_pv'  
group by 
pl,
date,
u_sd,
hour
) as tmp
insert into table stats_hourly_tmp2
select 
pl,
'all',
date,
hour,
'sessions_lengths',
cast(sum(s_length) / 1000 as int) 
group by 
pl,
date,
hour;

* Step3、所有平台，所有版本（platform:all, version:all）
from (
select 
date,
hour,
u_sd,
(max(s_time) - min(s_time)) as s_length
from 
stats_hourly_tmp1 
where en='e_pv'  
group by 
date,
u_sd,
hour
) as tmp
insert into table stats_hourly_tmp2
select 
'all',
'all',
date,
hour,
'sessions_lengths',
cast(sum(s_length) / 1000 as int) 
group by 
date,
hour;
### 5.4.7、创建最终结果表
我们在这里需要创建一个和Mysql表结构一致的Hive表，便于后期使用Sqoop导出数据到Mysql中。
create table stats_hourly(
platform_dimension_id int,
date_dimension_id int,
kpi_dimension_id int,
hour_00 int,
hour_01 int,
hour_02 int,
hour_03 int,
hour_04 int,
hour_05 int,
hour_06 int,
hour_07 int,
hour_08 int,
hour_09 int,
hour_10 int,
hour_11 int,
hour_12 int,
hour_13 int,
hour_14 int,
hour_15 int,
hour_16 int,
hour_17 int,
hour_18 int,
hour_19 int,
hour_20 int,
hour_21 int,
hour_22 int,
hour_23 int
) row format delimited fields terminated by '\t';

### 5.4.8向结果表中插入数据
我们需要platform_dimension_id int, date_dimension_id int, kpi_dimension_id int三个字段，所以我们需要使用UDF函数生成对应的字段。
* Step1、编写UDF函数，见代码
* Step2、编译打包UDF函数代码
编译参数：-P dev clean package install
导入HBase依赖：
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/opt/modules/cdh/hbase-0.98.6-cdh5.3.6/lib/*

* Step3、上传UDF代码jar包到HDFS
$ bin/hadoop fs -mkdir -p /event_logs/
$ bin/hadoop fs -mv /event-logs/* /event_logs/
尖叫提示：记得修改Flume的HDFS SINK路径以及手动上传脚本命令
$ bin/hadoop fs -rm -r /event-logs/
上传脚本：
$ bin/hadoop fs -mkdir -p /udf_jar/transformer 
$ bin/hadoop fs -put ~/Desktop/BI-ETL.jar  /udf_jar/transformer

* Step4、使用UDF的jar
create function date_converter as 'com.z.etl.udf.DateDimensionConverterUDF' using jar 'hdfs://node1:9000/udf_jar/BI-ETL.jar ';

create function kpi_converter as 'com.z.etl.udf.KpiDimensionConverterUDF' using jar 'hdfs://node1:9000/udf_jar/BI-ETL.jar ';

create function platform_converter as 'com.z.etl.udf.PlatformDimensionConverterUDF' using jar 'hdfs://node1:9000/udf_jar/BI-ETL.jar ';
* Step5、执行最终数据统计
insert overwrite table stats_hourly
select 
event_log.platform_converter(pl,ver), 
event_log.date_converter(date,'day'), 
event_log.kpi_converter(kpi),
max(case when hour=0 then value else 0 end) as hour_00,
max(case when hour=1 then value else 0 end) as hour_01,
max(case when hour=2 then value else 0 end) as hour_02,
max(case when hour=3 then value else 0 end) as hour_03,
max(case when hour=4 then value else 0 end) as hour_04,
max(case when hour=5 then value else 0 end) as hour_05,
max(case when hour=6 then value else 0 end) as hour_06,
max(case when hour=7 then value else 0 end) as hour_07,
max(case when hour=8 then value else 0 end) as hour_08,
max(case when hour=9 then value else 0 end) as hour_09,
max(case when hour=10 then value else 0 end) as hour_10,
max(case when hour=11 then value else 0 end) as hour_11,
max(case when hour=12 then value else 0 end) as hour_12,
max(case when hour=13 then value else 0 end) as hour_13,
max(case when hour=14 then value else 0 end) as hour_14,
max(case when hour=15 then value else 0 end) as hour_15,
max(case when hour=16 then value else 0 end) as hour_16,
max(case when hour=17 then value else 0 end) as hour_17,
max(case when hour=18 then value else 0 end) as hour_18,
max(case when hour=19 then value else 0 end) as hour_19,
max(case when hour=20 then value else 0 end) as hour_20,
max(case when hour=21 then value else 0 end) as hour_21,
max(case when hour=22 then value else 0 end) as hour_22,
max(case when hour=23 then value else 0 end) as hour_23
from stats_hourly_tmp2 group by pl,ver,date,kpi;
# 6 Sqoop导出数据到Mysql
$ bin/sqoop export --connect jdbc:mysql://node2:3306/report 
--username root 
--password 123456 
--table stats_hourly 
--num-mappers 1 
--export-dir /user/hive/warehouse/event_log.db/stats_hourly 
--input-fields-terminated-by "\t"
