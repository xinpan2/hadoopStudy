package dataflowcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The class aims to get the phoneNumer,upload flow,download flow of the source
 * splits and send the result info to the reducer.
 * 
 * Thoughts:
 * 
 * first turn the text from the source file to the string
 * 
 * then split the line to the array
 * 
 * according to the file's content characteristic get the useful fields and set
 * to the bean
 * 
 * in the end ,write to the context
 */
public class DataFlowCountMapper extends Mapper<LongWritable, Text, Text, DataFlowBean> {

	/*
	 * since each time call the map method the jvm will new the bean object we
	 * put the bean as a class member to prevent quantities of useless objects
	 * that burden the gc
	 */
	private DataFlowBean bean = new DataFlowBean();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataFlowBean>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		String[] fields = StringUtils.split(line, "\t");

		String phoneNum = fields[1];

		long upload_flow = Long.parseLong(fields[fields.length - 3]);

		long download_flow = Long.parseLong(fields[fields.length - 2]);

		bean.set(phoneNum, upload_flow, download_flow);

		context.write(new Text(phoneNum), bean);
	}
}
