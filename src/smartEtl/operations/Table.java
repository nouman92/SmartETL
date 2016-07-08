package smartEtl.operations;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;

public class Table {
	
	String Name;
	ArrayList<String> Attributes;
	ArrayList<String> Type;
	boolean dropTable;
	
	public Table(String table , boolean dropTable){
		this.Name = table;
		this.Attributes = new ArrayList<String> ();
		this.Type = new ArrayList<String> ();
		this.dropTable = dropTable;
	}
	
	public void addAttribute(String attribute , String type){
		this.Attributes.add(attribute);
		this.Type.add(type);
	}
	
	public boolean creatTable( Connection conn){
		boolean status = false;
		if(dropTable)
		{
			try{
				Statement toStatement = conn.createStatement();
				toStatement.executeUpdate("DROP TABLE IF EXISTS " + this.Name);
			}catch(Exception ex){
				ex.printStackTrace();
				//System.out.println(ex.printStackTrace());
			}
		}
		String Query = "CREATE TABLE IF NOT EXISTS " + this.Name ;
		String attributes = "";
		for(int i = 0 ; i < Attributes.size() ; i++ ){
			
			attributes +=  Attributes.get(i) + " " + Type.get(i) ;
			
			if( i == 0 && Attributes.get(i).equals("id"))
				attributes += " primary key auto_increment ";
			
			if( i < Attributes.size() - 1 )
				attributes += " , ";
				
		}
		Query += " ("+attributes+"); ";
		try{
			Statement toStatement = conn.createStatement();
			toStatement.executeUpdate(Query);
			status = true;
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return status;
	}
}
