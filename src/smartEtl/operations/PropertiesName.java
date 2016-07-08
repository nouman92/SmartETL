package smartEtl.operations;

public enum PropertiesName {
	
	ProjectProperties,
	
	Project,
	
	srcDriver,
	srcUrl,
	srcPort,
	srcDB,
	srcUser,
	srcPassword,
	
	destDriver,
	destUrl,
	destPort,
	destDB,
	destUser,
	destPassword;
	
	static public PropertiesName getProperty(String property){
		return PropertiesName.valueOf(property);
	}
}

