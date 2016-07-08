package smartEtl.operations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import smartEtl.Screens.Transformations;

public class TransformationModule implements Runnable{

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

	String srcTable;
	boolean orderBy;
	String sortBy;

	boolean chnageName;
	boolean dropTable;
	String destTable;

	String[] columns;

	boolean truncate;
	boolean skipNull;
	boolean updateTable;
	
	int destAttrSize;

	public TransformationModule(String srcTable, String[] columns, String destTable, String sortBy, boolean chnageName,
			boolean truncate, boolean dropTable, boolean skipNull, boolean updateTable, boolean orderBy , int destAttrSize) {
		super();
		this.srcTable = srcTable;
		this.columns = columns;
		this.destTable = destTable;
		this.sortBy = sortBy;
		this.chnageName = chnageName;
		this.truncate = truncate;
		this.dropTable = dropTable;
		this.skipNull = skipNull;
		this.updateTable = updateTable;
		this.orderBy = orderBy;
		this.destAttrSize = destAttrSize;

		if (this.chnageName)
			this.destTable = this.srcTable;
	}

	public boolean init() {
		fromConnection = DataBaseModule.getConnection("src");
		toConnection = DataBaseModule.getConnection("dest");
		if (null == fromConnection || null == toConnection)
			return false;
		else
			return true;
	}

	public boolean CreateTable() {
		try {
			String query = "describe " + this.srcTable;
			fromStatement = fromConnection.createStatement();
			fromResultSet = fromStatement.executeQuery(query);
			int index = 0;
			Table table = new Table(this.destTable, this.dropTable);

			while (fromResultSet.next()) {
				String attribute = (String) fromResultSet.getObject(1);
				String type = (String) fromResultSet.getObject(2);
				if (attribute.equals(this.columns[index++])) {
					table.addAttribute(attribute, type);
				}
			}
			if (table.creatTable(toConnection))
				return true;
			else
				return false;
		} catch (Exception ex) {
			return false;
		}
	}

	public void insertData() {
		try {
			String query = "select * from " + this.srcTable;
			if (orderBy)
				query += " ORDER BY " + sortBy;

			fromStatement = fromConnection.createStatement();
			toStatement = toConnection.createStatement();
			fromResultSet = fromStatement.executeQuery(query);
			if(this.truncate){
				try{
					toStatement.executeQuery("truncate " + this.destTable);
				}catch(Exception ex){
					ex.printStackTrace();
					//System.out.println(ex.printStackTrace());
				}
			}
			while (fromResultSet.next()) {
				ResultSetMetaData metaData = fromResultSet.getMetaData();
				int columns = metaData.getColumnCount();
				attrFrom = new String[columns];
				typeFrom = new String[columns];
				for (int lp = 1; lp <= columns; lp++) {
					attrFrom[lp - 1] = fromResultSet.getString(lp);
					if (metaData.getColumnType(lp) == java.sql.Types.NULL) {
						typeFrom[lp - 1] = "E"; // empty
					} else if (isText(metaData.getColumnType(lp))) {
						typeFrom[lp - 1] = "S"; // string
					} else if (isNumeric(metaData.getColumnType(lp)) || isReal(metaData.getColumnType(lp))) {
						typeFrom[lp - 1] = "N"; // numeric
					} else {
						typeFrom[lp - 1] = "O"; // numeric
					}
				}
				transform(columns);
				load(columns);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		this.thread = null;
		this.parent.toggleLoadButton();
	}

	public void transform(int nColumn) {
		attrTo = new String[columns.length];
		typeTo = new String[columns.length];
		int index = 0;
		for (int lp = 1; lp <= nColumn; lp++) {
			if (!columns[lp - 1 ].equals("ignore-this")) {
				if (this.skipNull && typeFrom[ lp -1 ].equals("E"))
					return;
				attrTo[index] = attrFrom[lp - 1];
				typeTo[index++] = typeFrom[lp - 1];
			}
		}
	}

	public void load(int nColumn) {
		String cmdinc = "insert into " + this.destTable + " values(";
		try {
			for (int lp = 1; lp <= nColumn; lp++) {
				if ( destAttrSize == nColumn && lp <= destAttrSize ) {
					if (typeTo[lp - 1] == "E") { // empty
						cmdinc = cmdinc + "null";
					} else if (typeTo[lp - 1] == "S") { // string
						cmdinc = cmdinc + "'" + attrTo[lp - 1] + "'";
					} else if (typeTo[lp - 1] == "N") { // numeric
						cmdinc = cmdinc + attrTo[lp - 1];
					} else if (typeTo[lp - 1] == "B") {
						cmdinc = cmdinc + attrTo[lp - 1];
					} else
						cmdinc = cmdinc + attrTo[lp - 1];
					
					if (lp < nColumn) {
						cmdinc = cmdinc + ",";
					}
				} else {
					return ;
				}

			}
			cmdinc = cmdinc + ")";
			toStatement.executeUpdate(cmdinc);

		} catch (SQLException e) {
			System.out.println(e.toString());
		}
	}

	public boolean isReal(int type) {
		return (type == java.sql.Types.DECIMAL || type == java.sql.Types.DOUBLE || type == java.sql.Types.FLOAT
				|| type == java.sql.Types.NUMERIC || type == java.sql.Types.REAL);
	}

	public boolean isNumeric(int type) {
		return (type == java.sql.Types.DECIMAL || type == java.sql.Types.DOUBLE || type == java.sql.Types.FLOAT
				|| type == java.sql.Types.NUMERIC || type == java.sql.Types.REAL || type == java.sql.Types.BIGINT
				|| type == java.sql.Types.TINYINT || type == java.sql.Types.SMALLINT || type == java.sql.Types.INTEGER);
	}

	public boolean isText(int type) {
		return (type == java.sql.Types.CLOB || type == java.sql.Types.VARBINARY || type == java.sql.Types.VARCHAR
				|| type == java.sql.Types.CHAR || type == java.sql.Types.LONGVARCHAR || type == java.sql.Types.DATE
				|| type == java.sql.Types.TIMESTAMP);
	}

	public boolean isBoolean(int type) {
		return (type == java.sql.Types.BOOLEAN || type == java.sql.Types.TINYINT);
	}

	Thread thread = null;
	
	Transformations parent = null;
	
	public void start(Transformations transformation) {
		if (thread == null) {
			parent = transformation;
			parent.toggleLoadButton();
			thread = new Thread(this);
			thread.start();
		}
	}

	public void stop() {
		thread = null;
	}
	@Override
	public void run() {
		while (thread != null) {
			this.insertData();
		}
	}
}
