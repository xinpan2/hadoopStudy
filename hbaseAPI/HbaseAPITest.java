package com.xinpaninjava.hbase.crud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * HbaseAPITest tries to call the API of HBase and distinguish the way from
 * shell to manipulate the DB
 *
 */
@SuppressWarnings( "deprecation" )
public class HbaseAPITest {
	private static Configuration conf;

	/**
	 * the hbase has its own configuration,hbaseConfiguration. Instead of adding
	 * the hbase-site.xml to the workspace,we directly set the mainly property
	 * of hbase,the zk slaves that belong to which node.
	 * 
	 */
	@Before
	public void getConf() {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "xinpan5,xinpan6,xinpan7");
	}

	@Test
	public void createTable() throws Exception {
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		
		TableName name=TableName.valueOf("friends");		
		HTableDescriptor friends = new HTableDescriptor(name);
		
		HColumnDescriptor base_info = new HColumnDescriptor("base_info");
		base_info.setMaxVersions(5);

		HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");

		friends.addFamily(base_info);
		friends.addFamily(extra_info);
		hBaseAdmin.createTable(friends);

		// close the hbaseAdmin,if we ignored ,it throws warning
		hBaseAdmin.close();
	}
	
	@Test
	public void insert() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);	
		
		HTable friends=new HTable(conf, "friends");//get the table by conf & tableName

		Put name=new Put(Bytes.toBytes("0001"));//Put object with the row to insert 
		name.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("Mary"));

		Put age=new Put(Bytes.toBytes("0001"));
		age.add(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(20));
		
		List<Put> puts=new ArrayList<>();//multi-rows are inserted into friends
		puts.add(name);
		puts.add(age);
		
		friends.put(puts);
		
		friends.close();
		hBaseAdmin.close();
		
	}
	
	
	@Test
	public void get()throws Exception{
		
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);	
		
		HTable friends=new HTable(conf, "friends");
			
		Get get=new Get(Bytes.toBytes("0001"));// instantiate a Get object with the row to get.
		
		Result row_0001 = friends.get(get);
		
		//Although this does an arraycopy of the cell[]'s value ,it is convenient to write.
		for(KeyValue kv:row_0001.list()){
			System.out.println(new String(kv.getFamily()));
			System.out.println(new String(kv.getQualifier()));
			System.out.println(new String(new String(kv.getValue())));
		}
		
		friends.close();
		hBaseAdmin.close();
	}
	
	@Test
	public void drop() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);	
		
		hBaseAdmin.disableTable("friends");
		hBaseAdmin.deleteTable("friends");
		
		hBaseAdmin.close();
	}
}
