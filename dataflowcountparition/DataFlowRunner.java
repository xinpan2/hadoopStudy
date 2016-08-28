package dataflowcountparition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataFlowRunner {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(DataFlowRunner.class);

		job.setMapperClass(DataFlowCountPartitionMapper.class);
		job.setReducerClass(DataFlowCountReducer.class);

		// ***set the partition class to the job !!!!***
		job.setPartitionerClass(DataFlowPartition.class);

		// ***set the num of reduce tasks!without this parm which will run as
		// default with on reduce task***
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataFlowBean.class);

		FileInputFormat.setInputPaths(job, "hdfs://xinpan2:9000/fc/srcdata");

		Path output = new Path("hdfs://xinpan2:9000/partition1");
		FileSystem fileSystem = FileSystem.get(configuration);
		if (fileSystem.exists(output)) {
			fileSystem.delete(output, true);
		}

		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}

}
