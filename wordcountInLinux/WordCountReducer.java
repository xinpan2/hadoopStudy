package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * the WordCountReducer aims to further induction all the info with the same key
 *
 * sum all the sam class value and write the <key,value> to the context
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable value : values) {
			// get the long type data by the get()
			sum += value.get();
		}
		context.write(key, new LongWritable(sum));
	}
}
