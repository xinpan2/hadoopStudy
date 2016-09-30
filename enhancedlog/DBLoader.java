package com.xinpaninjava.enhancedlog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

/**
 * DBLoader aims to connect to the mysql and get the message from the specific
 * table to the memory.
 */
public class DBLoader {
	public static void dbLoader(HashMap<String, String> ruleMap) {
		Connection conn = null;
		Statement st = null;
		ResultSet res = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://xinpan1:3306/urlcontent", "root", "root");
			st = conn.createStatement();
			res = st.executeQuery("select url,info from urlanalyze");
			while (res.next()) {// load all records to the memory
				ruleMap.put(res.getString(1), res.getString(2));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {// close resources
			try {
				if (res != null)
					res.close();
				if (st != null)
					st.close();
				if (conn != null)
					conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
