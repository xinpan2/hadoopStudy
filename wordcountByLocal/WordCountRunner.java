package wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Though there are mapper and reducer in the package,In order to specify which
 * map-reduce project we desire to run on hadoop ,a job instance should be
 * created.A job instance encapsulate properties of source's path, result's
 * path,the input and output map/reduce key and value type . At the same
 * time,the jar position is also significant for the hadoop to execute the
 * runner.
 * 
 */
public class WordCountRunner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// create the job instance
		Job wcjob = Job.getInstance(new Configuration());

		// assign the jar location by class
		wcjob.setJarByClass(WordCountRunner.class);

		/*
		 * set the job to use which mapper and reducer cause there may have more
		 * the one mapper and reducer
		 */
		wcjob.setMapperClass(WordCountMapper.class);
		wcjob.setReducerClass(WordCountReducer.class);

		/*
		 * set the key/value type in the scene of the map/reduce output
		 * key/value with the same type
		 * 
		 * there still have the method wcjob.setMapOutputKeyClass(theClass);
		 * wcjob.setMapOutputValueClass(theClass); to specify the map output
		 * key/value,if there are no map method,which means that the map and the
		 * reduce has the sam type
		 */
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);

		// set the source and destination path
//		FileInputFormat.setInputPaths(wcjob, "hdfs://xinpan2:9000/wc/srcdata");
//		FileOutputFormat.setOutputPath(wcjob, new Path("hdfs://xinpan2:9000/wc/res"));
		FileInputFormat.setInputPaths(wcjob, "c:/wc/srcdata/");
		FileOutputFormat.setOutputPath(wcjob, new Path("c:/wc/res"));

		/*
		 * Submit the job to the cluster and wait for it to finish return true
		 * if the job succeed
		 */
		boolean res = wcjob.waitForCompletion(true);
		System.out.println("the result is : " + res);
	}

}
