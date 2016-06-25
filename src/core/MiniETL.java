package core;
import java.sql.*;

/*
 * MiniETL.java
 *
 * A simple ETL Tool...
 *
 * Herbert Laroca, 2005.
 */


public class MiniETL {
	
    private Statement  fromStatement; 
    private Connection fromConnection; 
    private ResultSet  fromResultSet;
    
    private Statement  toStatement; 
    private Connection toConnection; 
    private ResultSet  toResultSet;

    private Statement  queryStatement;
    private Connection queryConnection;
    private ResultSet  queryResultSet;
    
    private String attrFrom[];   // data from source table
    private String typeFrom[];   // type of data from source table
    private String attrTo[];     // data of data from target table - populated by transform class
    public String typeTo[];     // type of data from target table
	            
    private String error="";
		
    private String dbURL[]= {        "jdbc:odbc:" , 
                                    "jdbc:postgresql://" ,
                                    "jdbc:mysql://",
                                    "jdbc:jtds:sqlserver://",
                                    "jdbc:oracle:thin:",
                                    "jdbc:informix-sqli://," ,
                                    "jdbc:sybase:Tds:"};
                                
    private String strDriver[]= {   "sun.jdbc.odbc.JdbcOdbcDriver" , 
                                    "org.postgresql.Driver",
                                    "com.mysql.jdbc.Driver",
                                    "net.sourceforge.jtds.jdbc.Driver",
                                    "oracle.jdbc.driver.OracleDriver",
                                    "com.informix.jdbc.IfxDriver",
                                    "com.sybase.jdbc.SybDriver"};
         
    private String userFrom; // user from source db
    private String userTo;   // user from target db
    
    private String pswFrom;  // password from source db
    private String pswTo;    // password from target db 
    private String tabFrom;  // source table
    private String tabTo;    // target table
    
    private String dbURLFrom;    // URL from source db
    private String dbURLTo;      // URL from target db
    
    private boolean prtScript=false; // Does print the script ? yes or no
    private String tClass;           // name of the transformator class
    
    private int seq;         // number of the source line of the table ( sequential number )
    
    private int typeDBFrom;  // type of source db (0..6)
    
    private int typeDBTo;    // type of target db (0..6)
    
    private int  nColumnFrom ,   // number of columns - source table 
                nColumnTo,      // number of columns - target table
                retLines;       // number of lines resulted
    
    private static MiniETL objParm;  // class passed by parameter to transformator class
    
    private boolean loadLine;    // Does the line will be loaded ? 
    
    private boolean delTo;       // Does the target table will be deleted ?
           			
    public static void main(String args[]) {
      
        MiniETL objProc = new MiniETL();
                         
        System.out.println("MiniETL 0.1   Herbert Laroca, 2005 \n");
	if (! objProc.verSintax(args)) {
            System.out.println("Syntax:  -tpFrom:<0/1/2/3/4/5/6> - (*) ");
            System.out.println("         -tpTo:<0/1/2/3/4/5/6> - (*) ");
            System.out.println("         -From:<Source Database>");
            System.out.println("         -To:<Target Database>");
            System.out.println("         -usrFrom:<Source User>");
            System.out.println("         -usrTo:<Target User>");
            System.out.println("         -pswFrom:<Source Password>");
            System.out.println("         -pswTo:<Target Password>");
            System.out.println("         -tbFrom:<Source Table>");
            System.out.println("         -tbTo:<Target Table>");
            System.out.println("         -prt:<yes/no>");
            System.out.println("         -tClass:<Transformation Java Class>");
            System.out.println("         -delTo:<yes/no>");
            System.out.println(" (*) - 0:ODBC / 1:PostgreSQL / 2:MySQL / 3:Microsoft SQL Server (jTDS Driver)");
            System.out.println(" 4:Oracle 10 / 5:Informix / 6:Sybase");
        }
        else {
            objParm = objProc;
            objProc.connectFrom();
            objProc.connectTo();
            objProc.procETL();
            objProc.disconnectTo();
            objProc.disconnectFrom();
            }
    }
        
    public void procETL() {
        ResultSetMetaData rmetaDataFrom, rmetaDataTo;
	
        try {
            //--------------------------------------------------------------------------
            
            toResultSet = toStatement.executeQuery("Select * From " +tabTo);
            rmetaDataTo = toResultSet.getMetaData();
            nColumnTo = rmetaDataTo.getColumnCount();
            attrTo = new String[nColumnTo];
            typeTo = new String[nColumnTo];
                                  
            for(int lp=1;lp<=nColumnTo;lp++) {
                attrTo[lp - 1]="";
                typeTo[lp - 1]="";
                if (rmetaDataTo.getColumnType(lp) == java.sql.Types.NULL) {
                    typeTo[lp - 1]= "E";  // empty
                }
                if (isText(rmetaDataTo.getColumnType(lp))) {
                    typeTo[lp - 1]= "S"; // string
                }
                if (isNumeric(rmetaDataTo.getColumnType(lp)) || isReal(rmetaDataTo.getColumnType(lp))) {
                    typeTo[lp - 1]= "N"; // numeric
                }
            }
                               
            //-----------------------------------------------------------------------
            fromResultSet = fromStatement.executeQuery("Select * From " +tabFrom);
            rmetaDataFrom = fromResultSet.getMetaData();
            nColumnFrom = rmetaDataFrom.getColumnCount();
            attrFrom = new String[nColumnFrom];
            typeFrom = new String[nColumnFrom];
                                  
            for(int lp=1;lp<=nColumnFrom;lp++) {
                attrFrom[lp - 1]="";
                typeFrom[lp - 1]="";
                if (rmetaDataFrom.getColumnType(lp) == java.sql.Types.NULL) {
                    typeFrom[lp - 1]= "E"; // empty
                }
                if (isText(rmetaDataFrom.getColumnType(lp))) {
                    typeFrom[lp - 1]= "S"; // string
                }
                if (isNumeric(rmetaDataFrom.getColumnType(lp)) || isReal(rmetaDataFrom.getColumnType(lp))) {
                    typeFrom[lp - 1]= "N"; // numeric
                }
            }
            
            //------------------------------------------------------------------------
                
            // delete target table ?
            if (delTo) {
                retLines = toStatement.executeUpdate("delete from "+tabTo);
            }
            
            int seq=1;
            
            try {
                Class classTransf = Class.forName(tClass);     
                       
                Object objTransf = classTransf.newInstance();
                
                while (fromResultSet.next()) {
                    loadLine = true;
                    extract(nColumnFrom);
                    attrTo=((LocalModule) objTransf).transform(objParm, String.valueOf(seq)); 
                    if (loadLine) {
                       load(nColumnTo);
                    }
                    seq ++;
                }
            
            } catch (Exception e) {
                 System.out.println(e.toString());
              } 
            
            
        } catch(SQLException e) {
            System.out.println(e.toString());
        }
   }
    
    public void connectFrom() {
        try {
            Class.forName(strDriver[typeDBFrom]);
            fromConnection = DriverManager.getConnection(dbURLFrom,userFrom,pswFrom);
            fromStatement = fromConnection.createStatement();
            } catch(Exception e) {
                System.out.println(e.toString());
            }
        }
	
    public void connectTo() {
        try {
            Class.forName(strDriver[typeDBTo]);
            toConnection= DriverManager.getConnection(dbURLTo,userTo,pswTo);
            toStatement = toConnection.createStatement();
            } catch(Exception e) {
                System.out.println(e.toString());
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
           
    public void load(int nColumn) {
    String cmdinc;
        try {
            cmdinc = "insert into "+tabTo+" values(";
            int retLines;
            for(int lp=1;lp<=nColumn;lp++) {
               
                if (typeTo[lp - 1]=="E") {      // empty
                    cmdinc = cmdinc+"null";
                }
                if (typeTo[lp - 1]=="S") {      // string
                    cmdinc = cmdinc+"'"+attrTo[lp - 1]+"'";
                }
                if (typeTo[lp - 1]=="N") {      // numeric
                    cmdinc = cmdinc+attrTo[lp - 1];
                }
                if (lp < nColumn) {
                    cmdinc = cmdinc + ",";
                }
            }
            cmdinc=cmdinc+")";
            if (prtScript) System.out.println(cmdinc);
            retLines = toStatement.executeUpdate(cmdinc);
            cmdinc = "insert into "+tabTo+" values(";
                                           
        } catch(SQLException e) {
            System.out.println(e.toString());
          }
    }
	
    public void disconnectFrom() {
        try { 
            fromResultSet.close();
	    fromStatement.close();
            fromConnection.close();
	} catch(Exception e) {
            System.out.println(e.toString());
	  }    
    }
    
    public void disconnectTo() {
        try {
            toStatement.close();
            toConnection.close();
        } catch(Exception e) {
            System.out.println(e.toString());
	  }    
    }
    
    // 1 - for source tables
    // 2 - for target tables
    public String tableLookup(String strLine, int opt) {
    String strValue = new String();
    strValue = "";  
        try {
            Connection queryConnection;
            if (opt==1) {
                Class.forName(strDriver[typeDBFrom]);
                queryConnection = DriverManager.getConnection(dbURLFrom,userFrom,pswFrom);
            }
            else {
                Class.forName(strDriver[typeDBTo]);
                queryConnection = DriverManager.getConnection(dbURLTo,userTo,pswTo);
            }
            queryStatement = queryConnection.createStatement();
            if (prtScript) {
                System.out.println(strLine);
            }
            queryResultSet = queryStatement.executeQuery(strLine);
            queryResultSet.next();
            strValue = queryResultSet.getString(1);
            queryResultSet.close();
	    queryStatement.close();
            queryConnection.close();
        } catch(Exception e) {
        	System.out.println(e.toString());
        }

    return strValue;
    
     }

    public boolean verSintax(String[] parms) {
	boolean ok;
	String  aux, aux2;
	ok = true;
        if (parms.length != 13) {
            ok = false;
	}
	else {
            
            aux = new String();
            aux = parms[0];
            aux = aux.substring(0,8);					
            if (aux.equals("-tpFrom:")) {
                typeDBFrom = Integer.parseInt(parms[0].substring(8));                                                
            }
		    
            aux = parms[1];
            aux = aux.substring(0,6);					
            if (aux.equals("-tpTo:")) {
                typeDBTo = Integer.parseInt(parms[1].substring(6)); 
            }
		
            aux = parms[2];
            aux = aux.substring(0,6);					
            if (aux.equals("-From:")) {
                dbURLFrom = dbURL[typeDBFrom] + parms[2].substring(6);
            }
			
            aux = parms[3];
            aux = aux.substring(0,4);					
            if (aux.equals("-To:")) {
                dbURLTo = dbURL[typeDBTo] + parms[3].substring(4);
            }
			
            aux = parms[4];
            aux = aux.substring(0,9);					
            if (aux.equals("-usrFrom:")) {
                userFrom = parms[4].substring(9);
            }

            aux = parms[5];
            aux = aux.substring(0,7);					
            if (aux.equals("-usrTo:")) {
                userTo = parms[5].substring(7);
            }

            aux = parms[6];
            aux = aux.substring(0,9);					
            if (aux.equals("-pswFrom:")) {
                pswFrom = parms[6].substring(9);
            }
                    
            aux = parms[7];
            aux = aux.substring(0,7);					
            if (aux.equals("-pswTo:")) {
                pswTo = parms[7].substring(7);
            }
                    
            aux = parms[8];
            aux = aux.substring(0,8);					
            if (aux.equals("-tbFrom:")) {
                tabFrom = parms[8].substring(8);
            }
                    
            aux = parms[9];
            aux = aux.substring(0,6);					
            if (aux.equals("-tbTo:")) {
                tabTo = parms[9].substring(6);
            }
			
            aux = parms[10];
            aux = aux.substring(0,5);					
            if (aux.equals("-prt:")) {
                aux2 = parms[10].substring(5);
                if (aux2.equals("yes")) {
                    prtScript = true;
                }
                else {
                    prtScript = false;
                }
            }
                    
            aux = parms[11];
            aux = aux.substring(0,8);					
            if (aux.equals("-tClass:")) {
                tClass = parms[11].substring(8);
            }
            
            aux = parms[12];
            aux = aux.substring(0,7);					
            if (aux.equals("-delTo:")) {
                aux2 = parms[12].substring(7);
                if (aux2.equals("yes")) {
                    delTo = true;
                }
                else {
                    delTo = false;
                }
            }
            
            
        }        
    return ok;
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
		|| type == java.sql.Types.CHAR || type == java.sql.Types.LONGVARCHAR || type == java.sql.Types.DATE);
	}
    
}
