package smartEtl.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class Operations implements Runnable{

	public Operations(ArrayList<String> tables) {
		super();
		this.init();
		this.tables = tables;
	}

	private Statement fromStatement;
	private Connection fromConnection;
	private ResultSet fromResultSet;
	
	private String attrFrom[];
    private String typeFrom[]; 

	private Statement toStatement;
	private Connection toConnection;
	//private ResultSet toResultSet;
	
	private String attrTo[];
    public String typeTo[];
    
    ArrayList<String> tables;
   
	public void init(){
		fromConnection = utils.getConnection("src");
		toConnection = utils.getConnection("dest");
	}

	public void CreateTables() {
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
						toStatement.executeUpdate(queryToUpdate);
					}
					catch(Exception e){
						
					}
				}
			}
		}catch(Exception ex){
		}

	}
	
	public void copyData() {
		try{
			for(String table : tables){
				String query = "select * from " + table;
				fromStatement = fromConnection.createStatement();
				fromResultSet = fromStatement.executeQuery(query);
				toStatement = toConnection.createStatement();
				while(fromResultSet.next())
				{
					ResultSetMetaData metaData =fromResultSet.getMetaData();
					int columns = metaData.getColumnCount();
					attrFrom = new String[columns] ;
	                typeFrom = new String[columns] ;
	                typeTo = new String[columns] ;
	                attrTo = new String[columns] ;
					for(int lp=1;lp<=columns;lp++) {
						attrFrom[lp - 1]="";
						attrTo[lp - 1]="";
			            typeFrom[lp - 1]="";
						if (metaData.getColumnType(lp) == java.sql.Types.NULL) {
		                    typeFrom[lp - 1]= "E"; // empty
		                    typeTo[lp - 1]= "E";
		                }else if (isText(metaData.getColumnType(lp))) {
		                    typeFrom[lp - 1]= "S"; // string
		                    typeTo[lp - 1]= "S";
		                }else if (isNumeric(metaData.getColumnType(lp)) || isReal(metaData.getColumnType(lp))) {
		                    typeFrom[lp - 1]= "N"; // numeric
		                    typeTo[lp - 1]= "N";
		                }
		                else{
		                	typeFrom[lp - 1]= "O"; // numeric
		                    typeTo[lp - 1]= "O";
		                }
					}
					extract(columns);
					transform(columns);
                    load(columns , table);
				}
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}

	}
	
	public void extract(int nColumn) {
        try {
            for(int lp=1;lp<=nColumn;lp++) {
                attrFrom[lp - 1]= fromResultSet.getString(lp);
	    }
	} catch(SQLException e) {
            System.out.println(e.toString());
          }
    }
	
	public void transform(int nColumn){
		attrTo = new String[nColumn] ;
		for(int lp=1;lp<=nColumn;lp++) {
			attrTo[lp -1] = attrFrom[lp-1];
		}
	}
	
	public void load(int nColumn , String table) {
	    String cmdinc;
	   
        try {
            cmdinc = "insert into "+table+" values(";
            for(int lp=1;lp<=nColumn;lp++) {
               
                if (typeTo[lp - 1]=="E") {      // empty
                    cmdinc = cmdinc+"null";
                }else if (typeTo[lp - 1]=="S") {      // string
                    cmdinc = cmdinc+"'"+attrTo[lp - 1]+"'";
                }else if (typeTo[lp - 1]=="N") {      // numeric
                    cmdinc = cmdinc+attrTo[lp - 1];
                }else if (typeTo[lp - 1] == "B" ) {
                    cmdinc = cmdinc +attrTo[lp - 1] ;
                }
                else
                	cmdinc = cmdinc +attrTo[lp - 1] ;
                if (lp < nColumn) {
                    cmdinc = cmdinc +",";
                }
            }
            cmdinc=cmdinc+")";
            toStatement.executeUpdate(cmdinc);
                                      
        } catch(SQLException e) {
            System.out.println(e.toString());
          }
    }
	
	public boolean isReal(int type) {
        return (type == java.sql.Types.DECIMAL || type == java.sql.Types.DOUBLE || type ==java.sql.Types.FLOAT || 
		type == java.sql.Types.NUMERIC || type == java.sql.Types.REAL );
	}

    public boolean isNumeric(int type) {
        return (type == java.sql.Types.DECIMAL || type == java.sql.Types.DOUBLE || type ==java.sql.Types.FLOAT || 
		type == java.sql.Types.NUMERIC || type == java.sql.Types.REAL || type == java.sql.Types.BIGINT ||
		type == java.sql.Types.TINYINT || type == java.sql.Types.SMALLINT || type == java.sql.Types.INTEGER );
	}
	
    public boolean isText(int type) {
        return (type == java.sql.Types.CLOB || type == java.sql.Types.VARBINARY || type ==java.sql.Types.VARCHAR
		|| type == java.sql.Types.CHAR || type == java.sql.Types.LONGVARCHAR || type == java.sql.Types.DATE || type == java.sql.Types.TIMESTAMP);
	}
    
    public boolean isBoolean(int type) {
        return (type == java.sql.Types.BOOLEAN || type == java.sql.Types.TINYINT);
	}

	@Override
	public void run() {
		this.CreateTables();
		this.copyData();
	}

	public ArrayList<String> getTables() {
		return tables;
	}

	public void setTables(ArrayList<String> tables) {
		this.tables = tables;
	}
    
	
}
