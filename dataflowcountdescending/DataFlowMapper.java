package dataflowcountdescending;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * this class aims to handle the result from the previous project .So we get
 * fields and set to the bean then send to the reduce
 */
public class DataFlowMapper extends Mapper<LongWritable, Text, DataFlowBean, NullWritable> {
	private DataFlowBean bean = new DataFlowBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] fileds = StringUtils.split(line, "\t");
		/*
		 * cause the result of the previous project only have 4 fields,as
		 * phoneNum,upload_flow,download_flow,sum_flow
		 */
		String phoneNum = fileds[0];
		long upload_flow = Long.parseLong(fileds[1]);
		long download_flow = Long.parseLong(fileds[2]);

		bean.set(phoneNum, upload_flow, download_flow);
		context.write(bean, NullWritable.get());

	}
}
