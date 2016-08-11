package com.xinpaninjava.hdfs.test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * to test the HDFS working normally or not,given that we are new about
 * programming Hadoop in linuxOS, in some method like uploading or downloading
 * files , I will continue to use the traditional method,the IO technical in
 * javaSE,instead of the new knowledge of Hadoop
 * 
 */
public class HDFSTest1 {
	// extracting the common part
	private FileSystem fs = null;

	/**
	 * extracting the common block about getting the FileSystem instance,we
	 * should envision that all things are object in java at first.
	 * 
	 * the parameters relating to the hadoop: we can set the property in the
	 * client then the server will hide the same property.I will set the default
	 * FileSystem
	 * 
	 * about the annotation @Before you can read my precious articles about
	 * SSH(Spring,Struts,Hibernate)
	 * 
	 * @throws IOException
	 */
	@Before
	public void getFS() throws IOException {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://xinpan2:9000/");
		fs = FileSystem.get(configuration);
	}

	/**
	 * to upload a file to hdfs,we should ensure that the hdfs and yarn are
	 * working. Secondary,according to our experience,we need to open the
	 * InputStream and OutputStream.In this case, We can utilize the FileSystem
	 * instance to get an OutputStream and the io.fileInputStream to accomplish
	 * this task.
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void upload() throws IllegalArgumentException, IOException {
		FSDataOutputStream outputStream = fs.create(new Path("/test.upload"));
		FileInputStream inputStream = new FileInputStream("/home/xinpan2/test.txt");
		// using the utils supported by apache
		IOUtils.copy(inputStream, outputStream);
	}

	/**
	 * the chiefly measure as what have been display,differently we whereby the
	 * FileSystem instance to open an InputStream and manually create the
	 * outputStream
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void download() throws IllegalArgumentException, IOException {
		FSDataInputStream inputStream = fs.open(new Path("/test.upload"));
		FileOutputStream outputStream = new FileOutputStream("/home/xinpan2/test.upload.download");
		IOUtils.copy(inputStream, outputStream);
	}
}
