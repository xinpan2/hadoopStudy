package wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * the WordCountMapper aims to manipulate data from the source and send the
 * mid-result to reducer
 * 
 * measures:
 * 
 * 1¡¢set the KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * 
 * tips:hadoop has its own dataType of long,string,as longWritable,Text,in order
 * 		to transfer data more effective in the network
 * 
 * 		the keyIn is offset of the source file
 * 
 * 		the valueIn is the info that contains a line message of the file
 * 
 * 		the keyOut is the Text that the single word occurs in the file
 * 
 * 		the valueOut is a single time that recored the word occurred time in the line
 * 
 * 2¡¢override the map method to design how to handle the data
 * 
 * 3¡¢write the ultimate result to the context which will be sent to reducer
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	/*
	 * handle the line
	 * 
	 * first change the text into string and split the line to the array
	 * 
	 * then put all the element in the array to the reducer
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = StringUtils.split(line, " ");

		// assign that the word occurs in the line for 1 time
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}
	}
}
