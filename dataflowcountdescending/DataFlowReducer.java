package dataflowcountdescending;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * this class just put the result to the context,since the map task has finish
 * the descending of the collection
 */
public class DataFlowReducer extends Reducer<DataFlowBean, NullWritable, Text, DataFlowBean> {
	@Override
	protected void reduce(DataFlowBean key, Iterable<NullWritable> values,
			Reducer<DataFlowBean, NullWritable, Text, DataFlowBean>.Context context)
			throws IOException, InterruptedException {
		context.write(new Text(key.getPhoneNum()), key);
	}
}
