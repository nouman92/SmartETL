package smartEtl.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

public class Configuration {

	private static Properties projectProperties ;
	
	public static void init(String ProjectName){
		
		projectProperties= new Properties();
		projectProperties.put(PropertiesName.Project.toString(), ProjectName);
		File configFile = new File("config.properties");
		if(configFile.exists())
		{
			FileReader reader = null;
			try {
				reader = new FileReader(configFile);
			} catch (IOException e) {
			}
				
			try {
				projectProperties.load(reader);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static void init(String fileName , String project){
		
		File configFile = new File(fileName);
		FileReader reader = null;
		try {
			reader = new FileReader(configFile);
		} catch (FileNotFoundException e) {
		}
		projectProperties= new Properties();
		try {
			if( reader != null )
				projectProperties.load(reader);
		} catch (IOException e) {
		}
		try {
			if( reader != null )
				reader.close();
		} catch (IOException e) {
		}
		
	}
	
	public static void addAttribute( PropertiesName property , String value ){
		if( projectProperties == null )
			projectProperties= new Properties();
		
		projectProperties.put(property.toString(), value);
	}
	
	public static String  getAttribute(PropertiesName property){
		if( projectProperties == null )
			return null;
		String pro = property.name();
		String val = (String)projectProperties.get(pro);
		return val;
	}
	
	public static void saveConfig(){
		File configFile = new File("config.properties");
		try {
			if(!configFile.exists())
				configFile.createNewFile();
		} catch (IOException e) {
			
		}
		FileWriter writer = null;
		try {
			writer = new FileWriter(configFile);
		} catch (IOException e) {
		}
		try {
			if( projectProperties == null )
				projectProperties= new Properties();
			projectProperties.store(writer, "Properties");
		} catch (IOException e) {
		}
	}
}
