package hdfsinwindows;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

/**
 * to test the HDFS working normally or not,different from the
 * HDFSTest1.java,This class utilize the hadoop API to perform the CRUD
 * operations
 */
public class HDFSInWin {

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
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	@Before
	public void getFS() throws IOException, InterruptedException, URISyntaxException {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://xinpan2:9000/");
		/*
		 * fs = FileSystem.get(configuration); change the get method into geting
		 * by FileSystem.get(uri, conf, user)
		 */
		fs = FileSystem.get(new URI("hdfs://xinpan2:9000/"), configuration, "xinpan2");

	}

	/**
	 * we'll whereby the hadoop API to make directory
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void mkdirs() throws IllegalArgumentException, IOException {
		boolean mkdirs = fs.mkdirs(new Path("/hdfs/test2/"));
		System.out.println(mkdirs == true ? "succeed" : "fail");
	}

	/**
	 * upload file by using hadoop api
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void upload() throws IllegalArgumentException, IOException {
		fs.copyFromLocalFile(new Path("c:/test.txt"), new Path("/test.up"));
	}

	/**
	 * It's unable to modify the file in the hdfs.But to change the file's name
	 * is admission
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void rename() throws IllegalArgumentException, IOException {
		fs.rename(new Path("/hdfs/test2/test2.upload"), new Path("/hdfs/test2/test2.upload.rename"));
	}

	/**
	 * hadoop supplies 2 methods to get files in the specify directory
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	@Test
	public void listAllFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		System.out.println("--------------------1----------------------");
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus file = listFiles.next();
			System.out.println(file.getPath().getName());
		}
		System.out.println("--------------------2----------------------");
		FileStatus[] files = fs.listStatus(new Path("/"));
		for (FileStatus file : files) {
			System.out.println(file.getPath().getName());
		}
	}

	/**
	 * delete the file/directory of the hdfs,the later parameter with the type
	 * boolean indicates that recursive to delete the file or directory
	 * 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void deleteDirs() throws IllegalArgumentException, IOException {
		fs.delete(new Path("/hdfs"), true);
	}

	/**
	 * download file from the hdfs to windows
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void download() throws IllegalArgumentException, IOException {
		fs.copyToLocalFile(new Path("/test.up"), new Path("c:/test.down"));
	}
}
