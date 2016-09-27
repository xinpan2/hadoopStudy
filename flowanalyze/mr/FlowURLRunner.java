package com.xinpaninjava.flowanalyze.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xinpaninjava.flowanalyze.bean.FlowBean;

public class FlowURLRunner extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowURLRunner.class);

		job.setMapperClass(FlowURLMapper.class);
		job.setReducerClass(FlowURLReducer.class);

		// mapper's output type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// reducer's output type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// according to the specify path to read or write data
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new FlowURLRunner(), args);
		System.exit(res);

	}
}
