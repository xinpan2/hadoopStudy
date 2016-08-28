package dataflowcountparition;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataFlowCountPartitionMapper extends Mapper<LongWritable, Text, Text, DataFlowBean> {
	DataFlowBean bean = new DataFlowBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		String[] fileds = StringUtils.split(line, "\t");
		String phoneNum = fileds[1];
		long upload_flow = Long.parseLong(fileds[fileds.length - 3]);
		long download_flow = Long.parseLong(fileds[fileds.length - 2]);

		bean.set(phoneNum, upload_flow, download_flow);
		context.write(new Text(phoneNum), bean);

	}

}
