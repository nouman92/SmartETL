package smartEtl.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class Operations {

	private Statement fromStatement;
	private Connection fromConnection;
	private ResultSet fromResultSet;

	private Statement toStatement;
	private Connection toConnection;
	private ResultSet toResultSet;
	
	public void init(){
		fromConnection = utils.getSourceConnectionWithDB();
		toConnection = utils.getDestinationConnection();
	}

	public void CreateTables(ArrayList<String> tables) {
		try{
			for(String table : tables){
				String query = "SHOW CREATE TABLE " + table;
				
				fromStatement = fromConnection.createStatement();
				fromResultSet = fromStatement.executeQuery(query);
				if(fromResultSet.isBeforeFirst())
				{
					fromResultSet.next();
					String queryToUpdate = (String) fromResultSet.getObject(2);
					toStatement = toConnection.createStatement();
					toStatement.executeUpdate("drop table if EXISTS "+table );
					try{
						int i = toStatement.executeUpdate(queryToUpdate);
					}
					catch(Exception e){
						
					}
					//System.out.println(queryToUpdate);
				}
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}

	}
}
