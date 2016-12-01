
题目2：编写MapReduce，统计`/user/hadoop/mapred_dev/ip_time` 中去重后的IP数，越节省性能越好。（35分）
package EXEXC_text;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;


//编写MapReduce，统计/user/hadoop/mapred_dev/ip_time 中去重后的IP数，越节省性能越好
public class DAY_IP {
 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	 Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(DAY_IP.class);
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
		Counters counters=job.getCounters();
		Counter counter=counters.findCounter("ddd","vvv");
		System.out.println("yue count"+counter.getValue());
}
 public static class map extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String str=value.toString();
		String []word=str.split("\t");
		context.write(new Text("null"), new Text(word[0]));
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
	 private int countip=0;
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			      Context context)
			throws IOException, InterruptedException {
		for(Text ss:values){
			hash.add(ss.toString());
		}
		for (String  sf:hash){
			countip++;
		}
		context.write(new Text(countip+""), new Text(""));
	}
	
 }
}


运行完之后，描述程序里所做的优化点，每点+5分。
1.加conbine先在map预去重ip，减少数据传输
