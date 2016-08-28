package dataflowcountparition;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataFlowCountReducer extends Reducer<Text, DataFlowBean, Text, DataFlowBean> {
	private DataFlowBean bean = new DataFlowBean();

	@Override
	protected void reduce(Text key, Iterable<DataFlowBean> beans, Context context)
			throws IOException, InterruptedException {
		long upload_flow = 0;
		long download_flow = 0;
		for (DataFlowBean b : beans) {

			upload_flow += b.getUpload_flow();
			download_flow += b.getDownload_flow();

		}
		bean.set(key.toString(), upload_flow, download_flow);
		context.write(key, bean);

	}
}
