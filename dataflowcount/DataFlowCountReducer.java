package dataflowcount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * this class aims to manipulate the mid-consequence from the map task further.
 * 
 * thoughts:
 * 
 * get all the objects and traverse the collection to merge all the
 * mid-consequence
 */
public class DataFlowCountReducer extends Reducer<Text, DataFlowBean, Text, DataFlowBean> {
	DataFlowBean bean = new DataFlowBean();

	@Override
	protected void reduce(Text key, Iterable<DataFlowBean> beans,
			Reducer<Text, DataFlowBean, Text, DataFlowBean>.Context context) throws IOException, InterruptedException {
		// counter
		long sum_upload = 0;
		long sum_download = 0;

		for (DataFlowBean DFBean : beans) {
			sum_upload += DFBean.getUpload_flow();
			sum_download += DFBean.getDownload_flow();
		}

		// after

		bean.set(key.toString(), sum_upload, sum_download);
		context.write(new Text(key.toString()), bean);
	}
}
