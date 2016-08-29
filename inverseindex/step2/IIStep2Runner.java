package inverseindex.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IIStep2Runner {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(IIStep2Runner.class);

		job.setMapperClass(IIStep2Mapper.class);
		job.setReducerClass(IIStep2Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, "hdfs://xinpan:9000/ii/res");

		// if the output path already existed deletes it
		Path output = new Path("hdfs://xinpan:9000/ii/res2");
		FileSystem fs = FileSystem.get(configuration);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		FileOutputFormat.setOutputPath(job, output);

		// submit the job
		job.waitForCompletion(true);

	}

}
