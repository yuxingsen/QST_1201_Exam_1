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
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;


//编写MapReduce，统计/user/hadoop/mapred_dev/ip_time 中去重后的IP数，越节省性能越好
public class ip1_ip2 {
 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	 Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(ip1_ip2.class);
		job.setMapperClass(map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setCombinerClass(conbine.class);
		job.setNumReduceTasks(1);
	    job.setReducerClass(reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
//		Counters counters=job.getCounters();
//		Counter counter=counters.findCounter("ddd","vvv");
//		System.out.println("yue count"+counter.getValue());
}
 public static class map extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String str=value.toString();
		 if(((FileSplit) context.getInputSplit()).getPath().getName().equals("ip_time")){
			 String arr[]= str.split("\t",2);
			 String  ip= arr[1]+"!!!";
			 context.write(new Text("null"), new Text(ip));
		 }else{
			 String arr[]= str.split("\t",2);
			 context.write(new Text("null"), new Text(arr[1]));
		 }
	}
	 
 }
 public static class conbine extends Reducer<Text, Text, Text, Text>{
	 private HashSet<String> hash=new HashSet<String>();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for(Text ss:values){
			hash.add(ss.toString());
		}
		for (String  sf:hash){
			context.write(key, new Text(sf));
		}
	}
	 
 }
 public static class reduce extends Reducer<Text, Text, Text, Text>{
	 private HashSet<String> hash=new HashSet<String>();
	 private HashSet<String> hash2=new HashSet<String>();
	 private int countip=0;
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			      Context context)
			throws IOException, InterruptedException {
		for(Text ss:values){
			String word=ss.toString();
			if(word.endsWith("!!!")){
				String dd=word.replace("!!!", "").trim();
			hash.add(dd);
			}else{
				hash2.add(word);
			}
		}
		for (String  sf:hash){
			if(hash2.contains(sf)){
				countip++;
			}
			context.write(new Text(countip+""), new Text(""));
		}
	}
	
 }
}
