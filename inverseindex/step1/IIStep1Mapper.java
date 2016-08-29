package inverseindex.step1;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * IIStepOneMapper gets the source form file and output likewise
 * {@code <hello-->a.txt, 1>}
 * 
 */
public class IIStep1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// to string
		String line = value.toString();
		// get all the elements
		String[] fields = StringUtils.split(line, " ");
		for (String field : fields) {
			// get the fileName
			FileSplit split = (FileSplit) context.getInputSplit();
			String fileName = split.getPath().getName();
			// outputLongWritable
			context.write(new Text(field + "-->" + fileName), new LongWritable(1));
		}
	}
}
