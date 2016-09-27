package com.xinpaninjava.flowanalyze.mr;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.xinpaninjava.flowanalyze.bean.FlowBean;

public class FlowURLReducer extends Reducer<Text, FlowBean, Text, LongWritable> {

	private long globalCount = 0;// count all the user's flows
	private TreeMap<FlowBean, Text> treeMap = new TreeMap<>();

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		long up_sum = 0;
		long d_sum = 0;
		for (FlowBean bean : values) {
			up_sum += bean.getUpload_flow();
			d_sum += bean.getDownload_flow();
		}
		FlowBean flowBean = new FlowBean();
		flowBean.set(up_sum, d_sum);
		globalCount += flowBean.getSum_flow();

		// only get the current key's content
		Text newKey = new Text(key.toString());
		treeMap.put(flowBean, newKey);
	}

	/**
	 * after finishing all the reduce,executes this method¡£It is no sense that
	 * the context.write() executed in the reduce(),which means the map always
	 * has one element.
	 * 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {

		long temp = 0;
		for (Entry<FlowBean, Text> ent : treeMap.entrySet()) {
			// last 20% flows are utilized to the infamous websites.
			if (temp / globalCount < 0.8) {
				context.write(ent.getValue(), new LongWritable(ent.getKey().getSum_flow()));
				temp += ent.getKey().getSum_flow();
			} else {
				return;// stop the process
			}
		}

	}

}
