package smartEtl.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class Configuration {

	private static Properties projectProperties ;
	private static String FileName; ;
	
	public static void init(String ProjectName){
		FileName = ProjectName +".properties";
		projectProperties= new Properties();
		projectProperties.put(PropertiesName.Project.toString(), ProjectName);
		File configFile = new File(FileName);
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
	
	public static Boolean init(File configFile){
		if(configFile.exists() && !configFile.isDirectory() && ( configFile.getName().contains("properties") || configFile.getName().contains("Properties")) )
		{
			FileReader reader = null;
			
			try {
				reader = new FileReader(configFile);
			} catch (FileNotFoundException e) {
			}
			FileName = configFile.getAbsolutePath();
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
			return true;
		}else{
			return false;
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
		File configFile = new File(FileName);
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
