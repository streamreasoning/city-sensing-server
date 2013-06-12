package polimi.deib.city_sensing_server.configuration;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Config {
	private static Config _instance = null;
	private static final Logger logger = LoggerFactory.getLogger(Config.class); 
	
	private Configuration config;
	
	private Config(){
		try {
			config = new PropertiesConfiguration("setup.properties");
		} catch (ConfigurationException e) {
			logger.error("Error while reading the configuration file", e);
		}
	}
	
	public int getServerPort(){
		return config.getInt("city_sensing_server.port");
	}
	
	public String getMysqlAddress(){
		return config.getString("mysql.address");
	}
	
	public String getMysqldbname(){
		return config.getString("mysql.dbname");
	}
	
	public String getMysqlUser(){
		return config.getString("mysql.username");
	}
	
	public String getMysqlPwd(){
		return config.getString("mysql.password");
	}	
			
	public static Config getInstance(){
		if(_instance==null)
			_instance=new Config();
		return _instance;
	}
	
}
