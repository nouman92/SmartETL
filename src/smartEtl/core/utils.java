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

	public static String createConnectionString(int type, String url, String port) {
		String connUrl = dbURL[type] + url + ":" + port;
		return connUrl;
	}

	public static Connection getConnection(int type, String url, String port, String username, String password) {
		try {
			Class.forName(strDriver[type]);
			String connUrl = createConnectionString(type, url, port);
			Connection conn = DriverManager.getConnection(connUrl, username, password);
			if (conn != null) {
				return conn;
			} else
				return null;
		} catch (Exception e) {
			return null;
		}
	}
	public static Connection getConnectionWithDB(int type, String url, String port, String db ,String username, String password) {
		try {
			Class.forName(strDriver[type]);
			String connUrl = dbURL[type] + url + ":" + port +"/"+db;
			Connection conn = DriverManager.getConnection(connUrl, username, password);
			if (conn != null) {
				return conn;
			} else
				return null;
		} catch (Exception e) {
			return null;
		}
	}

	public static Connection getSourceConnection() {
		int index = utils.getDriverClassIndex(Configuration.getAttribute(PropertiesName.srcDriver));
		String srcDBUrl = Configuration.getAttribute(PropertiesName.srcUrl);
		String srcDBPort = Configuration.getAttribute(PropertiesName.srcPort);
		String srcDBUserName = Configuration.getAttribute(PropertiesName.srcUser);
		String srcDBPassword = Configuration.getAttribute(PropertiesName.srcPassword);
		Connection conn = getConnection(index, srcDBUrl, srcDBPort, srcDBUserName, srcDBPassword);
		return conn;
	}
	public static Connection getSourceConnectionWithDB() {
		int index = utils.getDriverClassIndex(Configuration.getAttribute(PropertiesName.srcDriver));
		String srcDBUrl = Configuration.getAttribute(PropertiesName.srcUrl);
		String srcDBPort = Configuration.getAttribute(PropertiesName.srcPort);
		String srcDBUserName = Configuration.getAttribute(PropertiesName.srcUser);
		String srcDBPassword = Configuration.getAttribute(PropertiesName.srcPassword);
		String srcDB = Configuration.getAttribute(PropertiesName.srcDB);
		Connection conn = getConnectionWithDB(index, srcDBUrl, srcDBPort, srcDB,srcDBUserName, srcDBPassword);
		return conn;
	}
	
	public static Connection getDestinationConnection() {
		int index = utils.getDriverClassIndex(Configuration.getAttribute(PropertiesName.destDriver));
		String destDBUrl = Configuration.getAttribute(PropertiesName.destUrl);
		String destDBPort = Configuration.getAttribute(PropertiesName.destPort);
		String destDBUserName = Configuration.getAttribute(PropertiesName.destUser);
		String destDBPassword = Configuration.getAttribute(PropertiesName.destPassword);
		String destDB = Configuration.getAttribute(PropertiesName.destDB);
		Connection conn = getConnectionWithDB(index, destDBUrl, destDBPort,destDB, destDBUserName, destDBPassword);
		return conn;
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

	public static ArrayList[][] listTable(Connection conn, String schema, String tableName) {
		Statement statement = null;
		try {
			statement = conn.createStatement();
		} catch (SQLException e2) {
		}
		ResultSet res = null;
		try {
			if (null != statement) {
				String query = "select * from "+schema+"."+tableName+";";
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

	public static ArrayList<String> getTables(Connection conn, String schema) {
		Statement statement = null;
		try {
			statement = conn.createStatement();
		} catch (SQLException e2) {
		}
		ArrayList<String> data = new ArrayList<String>();
		ResultSet res = null;
		try {
			if (null != statement) {
				String query = "select table_name from information_schema.tables where table_schema=\"" + schema + "\"";
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
	
	public static void createTable(ArrayList<String> dataToLoad){
		Operations op = new Operations();
		op.init();
		op.CreateTables(dataToLoad);
	}

}
