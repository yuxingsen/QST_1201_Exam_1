题目3：编写MapReduce，统计这两个文件

`/user/hadoop/mapred_dev_double/ip_time`

`/user/hadoop/mapred_dev_double/ip_time_2`

当中，重复出现的IP的数量(40分)

---
加分项：

1. 写出程序里面考虑到的优化点，每个优化点+5分。
2. 额外使用Hive实现，+5分。
3. 额外使用HBase实现，+5分。
hive实现：
create external table ip_1(ip string, time string)  row format delimited fields terminated by '\t';
load data inpath  '/user/hadoop/mapred_dev_double/ip_time' into table ip_1;
create external table ip_2(ip string, time string)  row format delimited fields terminated by '\t' ;
load data inpath  '/user/hadoop/mapred_dev_double/ip_time_2' into table ip_2;
SELECT COUNT(*) FROM (SELECT DISTINCT ip  FROM  ip_1  )t ,(SELECT DISTINCT ip  FROM  ip_2  ) b WHERE t.ip=b.ip;
MR实现：
