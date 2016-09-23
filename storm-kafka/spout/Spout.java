package stormandkafka.storm.spout;

import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Spout act as the storm spout instead of extending the baseRichSpout,in this
 * case,we implement the backtype.storm.spout.Scheme,which reads the message
 * serialized by kafka.
 */
public class Spout implements Scheme {

	private static final long serialVersionUID = -7453940344756587898L;

	/**
	 * Say reading the serialized messages,then we need to deserialize them. If
	 * we catch exception,it will throw exception and return
	 * null.Otherwise,return the tuple.
	 */
	@Override
	public List<Object> deserialize(byte[] ser) {
		try {
			String message = new String(ser, "UTF-8");
			return new Values(message);
		} catch (Exception e) {
			new RuntimeException(e);
		}
		return null;
	}

	// declars the tuple's name
	@Override
	public Fields getOutputFields() {
		return new Fields("message");
	}
}
