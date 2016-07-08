/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smartEtl.operations;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

/**
 *
 * @author furqan
 */
public class DataBaseModule {

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
			Properties props = new Properties();
			props.setProperty("user", username);
			props.setProperty("password", password);
			DriverManager.setLoginTimeout(6);
			Connection conn = DriverManager.getConnection(connUrl, props);
			if (conn != null) {
				Statement statement = conn.createStatement();
				ResultSet res = statement.executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME = \"" + database + "\"");
				if (res.next())
					status = true;
				conn.close();
			}
		} catch (Exception e) {
			//e.printStackTrace();
			
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
		int index = DataBaseModule.getDriverClassIndex(ConfigurationService.getAttribute(PropertiesName.getProperty(source+"Driver")));
		String DBUrl = ConfigurationService.getAttribute(PropertiesName.getProperty(source+"Url"));
		String DBPort = ConfigurationService.getAttribute(PropertiesName.getProperty(source+"Port"));
		String DBUserName = ConfigurationService.getAttribute(PropertiesName.getProperty(source+"User"));
		String DBPassword = ConfigurationService.getAttribute(PropertiesName.getProperty(source+"Password"));
		String DB = ConfigurationService.getAttribute(PropertiesName.getProperty(source+"DB"));
		
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
		int srcindex = DataBaseModule.getDriverClassIndex(ConfigurationService.getAttribute(PropertiesName.srcDriver));
		String srcDBUrl = ConfigurationService.getAttribute(PropertiesName.srcUrl);
		String srcDBPort = ConfigurationService.getAttribute(PropertiesName.srcPort);
		String srcDBUserName = ConfigurationService.getAttribute(PropertiesName.srcUser);
		String srcDBPassword = ConfigurationService.getAttribute(PropertiesName.srcPassword);
		String srcDB= ConfigurationService.getAttribute(PropertiesName.srcDB);
		
		int destindex = DataBaseModule.getDriverClassIndex(ConfigurationService.getAttribute(PropertiesName.destDriver));
		String destDBUrl = ConfigurationService.getAttribute(PropertiesName.destUrl);
		String destDBPort = ConfigurationService.getAttribute(PropertiesName.destPort);
		String destDBUserName = ConfigurationService.getAttribute(PropertiesName.destUser);
		String destDBPassword = ConfigurationService.getAttribute(PropertiesName.destPassword);
		String destDB = ConfigurationService.getAttribute(PropertiesName.destDB);
		
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
				String query = "DESCRIBE " + table ;
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

}
