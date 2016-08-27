package dataflowcountdescending;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * this class aims to get the previous project result
 */
public class DataFlowRunner {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(DataFlowRunner.class);

		job.setMapperClass(DataFlowMapper.class);
		job.setReducerClass(DataFlowReducer.class);

		job.setMapOutputKeyClass(DataFlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataFlowBean.class);

		// previous project's result
		FileInputFormat.setInputPaths(job, "hdfs://xinpan2:9000/dfc/res");

		// if the output path already existed deletes it
		Path output = new Path("hdfs://xinpan2:9000/dfc/sort");
		FileSystem fs = FileSystem.get(configuration);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		FileOutputFormat.setOutputPath(job, output);

		// submit the job
		job.waitForCompletion(true);
	}

}
