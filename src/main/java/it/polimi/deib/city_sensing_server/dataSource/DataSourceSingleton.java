package it.polimi.deib.city_sensing_server.dataSource;

import it.polimi.deib.city_sensing_server.configuration.Config;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSourceSingleton {

    private ComboPooledDataSource cpds;
    private static DataSourceSingleton datasource;
    private static Logger logger = LoggerFactory.getLogger(DataSourceSingleton.class.getName());

    private DataSourceSingleton() throws IOException, SQLException {
    	
    	cpds = new ComboPooledDataSource();
        cpds.setJdbcUrl("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname());
        cpds.setUser(Config.getInstance().getMysqlUser());
        cpds.setPassword(Config.getInstance().getMysqlPwd());

        cpds.setInitialPoolSize(Config.getInstance().getInitialPoolSize());
        cpds.setAcquireIncrement(Config.getInstance().getacquireIncrement());
        cpds.setMaxPoolSize(Config.getInstance().getMaxPoolSize());
        cpds.setMinPoolSize(Config.getInstance().getMinPoolSize());
        cpds.setMaxStatements(Config.getInstance().getMaxStatements());
    }

    public static DataSourceSingleton getInstance() throws IOException, SQLException {
        if (datasource == null) {
              datasource = new DataSourceSingleton();
              return datasource;
            } else {
              return datasource;
            }
    }

    public Connection getConnection() throws SQLException {
    	logger.debug("Connection Requested. Total connection number: {}", cpds.getNumConnections());
        return this.cpds.getConnection();
    }
}