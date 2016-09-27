package com.xinpaninjava.flowanalyze.mr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.xinpaninjava.flowanalyze.bean.FlowBean;

public class FlowURLMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	private FlowBean bean = new FlowBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = StringUtils.split(line, "\t");
		try {
			// to make sure that the line's message are validate.
			if (fields.length > 32 && StringUtils.isNotEmpty(fields[26]) && fields[26].startsWith("http")) {
				String url = fields[26];
				long up_flow = Long.parseLong(fields[30]);
				long d_flow = Long.parseLong(fields[31]);

				bean.set(up_flow, d_flow);
				context.write(new Text(url), bean);
			}
		} catch (Exception e) {
		}

	}
}
