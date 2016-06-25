/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package core;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 *
 * @author furqan
 */
public class utils {

	private static String dbURL[] = { 
			"jdbc:mysql://", 
			"jdbc:odbc:", 
			"jdbc:postgresql://", 
			"jdbc:oracle:thin:"
			};

	private static String strDriver[] = { 
			"com.mysql.jdbc.Driver", 
			"sun.jdbc.odbc.JdbcOdbcDriver",
			"org.postgresql.Driver", 
			"oracle.jdbc.driver.OracleDriver",
			};

	private static int poolSize = 5;
	
	private int connectionSize = 0;

	private Connection[] connectionPool;

	public static boolean testConnection(int type, String url, String port, String username, String password) {
		try {
			Class.forName(strDriver[type]);
			String connUrl = dbURL[type] + url + ":" + port;
			Connection conn = DriverManager.getConnection(connUrl, username, password);
			if (conn != null) {
				conn.close();
				return true;
			} else
				return false;
		} catch (Exception e) {
			return false;
		}
	}

	public static String createConnectionString(int type, String url, String port, String username, String password) {
		String connUrl = dbURL[type] + url + ":" + port;
		return connUrl;
	}

	public static Connection getConnection(int type, String url, String port, String username, String password) {
		try {
			Class.forName(strDriver[type]);
			String connUrl = createConnectionString(type,url, port,username,password);
			Connection conn = DriverManager.getConnection(connUrl, username, password);
			if (conn != null) {
				return conn;
			} else
				return null;
		} catch (Exception e) {
			return null;
		}
	}

}
