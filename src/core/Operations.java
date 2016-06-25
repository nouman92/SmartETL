package core;

import java.sql.Connection;

public class Operations {

	private Connection connectionTo;
	private Connection connectionFrom;
	
	public void setConnectionTo(Connection connectionTo) {
		this.connectionTo = connectionTo;
	}
	public void setConnectionFrom(Connection connectionFrom) {
		this.connectionFrom = connectionFrom;
	}
}
