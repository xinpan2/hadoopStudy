package dataflowcountparition;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * derive the partition of the reduce,the static block is imitating the source
 * from db or other source
 *
 */
public class DataFlowPartition extends Partitioner<Text, DataFlowBean> {
	private static HashMap<String, Integer> map = new HashMap<>();;
	static {

		map.put("134", 0);
		map.put("135", 1);
		map.put("136", 2);
		map.put("137", 3);
	}

	/**
	 * obtain the value by key,[0,3),if the numberList doesn't in the map,return
	 * 4
	 */
	@Override
	public int getPartition(Text key, DataFlowBean value, int numPartitions) {
		Integer num = map.get(key.toString().substring(0, 3));
		return num == null ? 4 : num;
	}

}
