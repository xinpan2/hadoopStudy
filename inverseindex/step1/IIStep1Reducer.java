package inverseindex.step1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * IIStepOneReducer get all the intermediate keys from shuffle and output
 * likewise {@code <hello,a.text-->3}
 * 
 * @author right
 *
 */
public class IIStep1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		int count = 0;
		for (LongWritable num : values) {
			count += num.get();
		}
		context.write(key, new LongWritable(count));

	}
}
