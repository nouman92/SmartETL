/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smartEtl.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import smartEtl.config.Configuration;
import smartEtl.config.PropertiesName;

/**
 *
 * @author furqan
 */
public class utils {

	private static String dbURL[] = { 
			"jdbc:mysql://", 
			"jdbc:odbc:", 
			"jdbc:postgresql://",
			"jdbc:oracle:thiLoadDatan:" };

	private static String strDriver[] = { 
			"com.mysql.jdbc.Driver", 
			"sun.jdbc.odbc.JdbcOdbcDriver",
			"org.postgresql.Driver", 
			"oracle.jdbc.driver.OracleDriver", };

	public static boolean testConnection(int type, String url, String port, String database, String username, String password) {
		boolean status = false;
		try {
			Class.forName(strDriver[type]);
			String connUrl = dbURL[type] + url + ":" + port;
			Connection conn = DriverManager.getConnection(connUrl, username, password);
			if (conn != null) {
				Statement statement = conn.createStatement();
				ResultSet res = statement.executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME = \"" + database + "\"");
				if (res.next())
					status = true;
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return status;
	}

	public static String getDriverClass(int index) {
		return strDriver[index];
	}

	public static int getDriverClassIndex(String classStr) {
		int index = 0;
		for (int i = 0; i < strDriver.length; i++) {
			if (strDriver[i].equals(classStr)) {
				index = i;
				break;
			}
		}
		return index;
	}

	public static Connection getConnection(String source) {
		int index = utils.getDriverClassIndex(Configuration.getAttribute(PropertiesName.getProperty(source+"Driver")));
		String DBUrl = Configuration.getAttribute(PropertiesName.getProperty(source+"Url"));
		String DBPort = Configuration.getAttribute(PropertiesName.getProperty(source+"Port"));
		String DBUserName = Configuration.getAttribute(PropertiesName.getProperty(source+"User"));
		String DBPassword = Configuration.getAttribute(PropertiesName.getProperty(source+"Password"));
		String DB = Configuration.getAttribute(PropertiesName.getProperty(source+"DB"));
		
		Connection conn = null;
		try {
			Class.forName(strDriver[index]);
			String connUrl = dbURL[index] + DBUrl + ":" + DBPort +"/"+DB;
			conn = DriverManager.getConnection(connUrl, DBUserName, DBPassword);
			if (conn != null) {
				return conn;
			} else
				return null;
		} catch (Exception e) {
			return null;
		}
	}
	
	public static boolean checkIfBothDbAreSame() {
		int srcindex = utils.getDriverClassIndex(Configuration.getAttribute(PropertiesName.srcDriver));
		String srcDBUrl = Configuration.getAttribute(PropertiesName.srcUrl);
		String srcDBPort = Configuration.getAttribute(PropertiesName.srcPort);
		String srcDBUserName = Configuration.getAttribute(PropertiesName.srcUser);
		String srcDBPassword = Configuration.getAttribute(PropertiesName.srcPassword);
		String srcDB= Configuration.getAttribute(PropertiesName.srcDB);
		
		int destindex = utils.getDriverClassIndex(Configuration.getAttribute(PropertiesName.destDriver));
		String destDBUrl = Configuration.getAttribute(PropertiesName.destUrl);
		String destDBPort = Configuration.getAttribute(PropertiesName.destPort);
		String destDBUserName = Configuration.getAttribute(PropertiesName.destUser);
		String destDBPassword = Configuration.getAttribute(PropertiesName.destPassword);
		String destDB = Configuration.getAttribute(PropertiesName.destDB);
		
		if( srcindex == destindex && 
				srcDBUrl.equals(destDBUrl) &&
				srcDBPort.equals(destDBPort) &&
				srcDBUserName.equals(destDBUserName) &&
				srcDBPassword.equals(destDBPassword) &&
				srcDB.equals(destDB))
			
			return true;
		else
			return false;	
	}

	@SuppressWarnings("rawtypes")
	public static ArrayList[][] listTable(Connection conn,String tableName) {
		Statement statement = null;
		try {
			statement = conn.createStatement();
		} catch (SQLException e2) {
		}
		ResultSet res = null;
		try {
			if (null != statement) {
				String query = "select * from "+tableName+";";
				res = statement.executeQuery(query);
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<Object> data = new ArrayList<Object>();
		try {
			if (res != null) {
				ResultSetMetaData md = res.getMetaData();
		        int columns = md.getColumnCount();
		        for (int i = 1; i <= columns; i++)
	            {
	                columnNames.add( md.getColumnName(i) );
	            }
		        while (res.next())
	            {
	                ArrayList<Object> row = new ArrayList<Object>(columns);
	                for (int i = 1; i <= columns; i++)
	                {
	                    row.add( res.getObject(i) );
	                }
	                data.add( row );
	            }
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		ArrayList[][] response = new ArrayList[1][2];
		response[0][0] = columnNames;
		response[0][1] = data;
		return response;
	}

	public static ArrayList<String> getTables(Connection conn) {
		Statement statement = null;
		try {
			statement = conn.createStatement();
		} catch (SQLException e2) {
		}
		ArrayList<String> data = new ArrayList<String>();
		ResultSet res = null;
		try {
			if (null != statement) {
				String query = "SHOW TABLES";
				res = statement.executeQuery(query);
			}
		} catch (SQLException e1) {
		}
		try {
			if (res != null) {
				while (res.next()) {
					String rec = res.getString(1);
					data.add(rec);
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return data;
	}
	
	public static ArrayList<String> getTableAttributes(Connection conn, String table) {
		Statement statement = null;
		try {
			statement = conn.createStatement();
		} catch (SQLException e2) {
		}
		ArrayList<String> data = new ArrayList<String>();
		ResultSet res = null;
		try {
			if (null != statement) {
				String query = "\"" + table + "\"";
				res = statement.executeQuery(query);
			}
		} catch (SQLException e1) {
		}
		try {
			if (res != null) {
				while (res.next()) {
					String rec = res.getString(1);
					data.add(rec);
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return data;
	}
	
	
	/*public static void LoadData(ArrayList<String> dataToLoad){
		Runnable op = new Operations(dataToLoad);
		new Thread(op).start();
	}*/

}
