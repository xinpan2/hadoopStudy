package inverseindex.step2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * IIStep2Reducer get the line from step2Mapper likewise
 * {hello,{a.txt-->3,b.txt-->2,c.txt-->1}}. Reduce just links all the fileName
 */
public class IIStep2Reducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		StringBuffer fileNames = new StringBuffer();
		for (Text fileName : values) {
			fileNames.append(fileName + " ");
		}
		context.write(key, new Text(fileNames.toString()));
	}
}
