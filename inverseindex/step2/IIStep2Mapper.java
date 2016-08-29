package inverseindex.step2;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * IIStep2Mapper get the source from step 1 ,each line likewise
 * {@code hello-->a.txt	3} and handle it to the pattern {@code hello,a.txt-->3}
 *
 */
public class IIStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// line pattern {hello-->a.txt 3}
		String line = value.toString();

		// split the line to array [hello-->a.txt , 3]
		String[] fields = StringUtils.split(line, "\t");
		// split again by --> get [hello a.txt]
		String[] wordAndFileName = StringUtils.split(fields[0], "-->");
		String count = fields[1];
		String word = wordAndFileName[0];
		String fileName = wordAndFileName[1];

		// change to <hello,a.txt-->3> and write to the context
		context.write(new Text(word), new Text(fileName + "-->" + count));

	}
}
