package it.polimi.deib.city_sensing_server.configuration;

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
	
	public static Config getInstance(){
		if(_instance==null)
			_instance=new Config();
		return _instance;
	}
		
	public String getServerVersion(){
		return config.getString("city_sensing_server.version");
	}
	
	public int getServerPort(){
		return config.getInt("city_sensing_server.port");
	}
	
	public boolean getCreateViewOnDB(){
		return Boolean.parseBoolean(config.getString("city_sensing_server.createviewondb"));
	}
	
	//log4j
	public String getLog4jConfFilePath(){
		return config.getString("log4j.configuration_file");
	}
	
	//MysqlServer Parameters
	
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

	// ConnectionPool Paramters
	
	public int getacquireIncrement(){
		return config.getInt("connection_pool.acquireIncrement");
	}
	
	public int getInitialPoolSize(){
		return config.getInt("connection_pool.initialPoolSize");
	}
			
	public int getMaxPoolSize(){
		return config.getInt("connection_pool.maxPoolSize");
	}
	
	public int getMinPoolSize(){
		return config.getInt("connection_pool.minPoolSize");
	}
	
	public int getMaxStatements(){
		return config.getInt("connection_pool.maxStatements");
	}	
	
	//Services shared settings
	
	public String getDefaultStart(){
		return config.getString("services.default_startTime");
	}
	
	public String getDefaultEnd(){
		return config.getString("services.default_endTime");
	}
	
	public int getDefaultNumberOfCells(){
		return config.getInt("services.default_numberOfCells");
	}
	
	//Single service settings
	
	public String getConceptNetDefaultThreshold(){
		return config.getString("concept_network.default_threshold");
	}
	
	//Sentiment
	
	public float getSentimentPositiveCoefficient(){
		return config.getFloat("sentiment.positiveCoefficient");
	}
	
	public float getSentimentNegativeCoefficient(){
		return config.getFloat("sentiment.negativeCoefficient");
	}
	
	public float getSentimentNeutralCoefficient(){
		return config.getFloat("sentiment.neutralCoefficient");
	}
	
	//Sparql endpoint
	public String getTweetsSparqlEndpointURL(){
		return config.getString("tweets.sparql.dataset.url");
	}
	
	public String getBikemiSparqlEndpointURL(){
		return config.getString("bikemi.sparql.dataset.url");
	}
	
}
