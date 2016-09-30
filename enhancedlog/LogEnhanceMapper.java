package com.xinpaninjava.enhancedlog;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * to improve the info of the user,we embody the content of the log
 */
public class LogEnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private HashMap<String, String> ruleMap = new HashMap<>();

	/**
	 * load all the info from the db to append to the log.
	 * 
	 * this method is called merely the mapper's initialization stage.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		DBLoader.dbLoader(ruleMap);
	}

	/*
	 * first,the a single line of log.then splits all the fields.we can get the
	 * url. afterward,get the info of the url from the db.if the info is not
	 * null,add it to the log,otherwise to the "tocrawl" list.
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = StringUtils.split(line, "\t");
		try {
			if (fields.length > 27 && StringUtils.isNotEmpty(fields[26]) && 
					fields[26].startsWith("http")) {
				String url = fields[26];
				// get the specific record from db
				String info = ruleMap.get(url);
				String result = "";
				// judge the info
				if (info != null) {
					result = line + "\t" + info + "\n\r";
					context.write(new Text(result), NullWritable.get());
				} else {// means that have no corresponding record
					result = url + "\t" + "tocrawl" + "\n\r";
					context.write(new Text(result), NullWritable.get());
				}
			} else {
				return;
			}
		} catch (Exception e) {
			System.out.println("exception occured in mapper.....");
		}
	}
}
