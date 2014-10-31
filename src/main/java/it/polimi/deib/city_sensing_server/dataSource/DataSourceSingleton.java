/*******************************************************************************
 * Copyright 2014 DEIB - Politecnico di Milano
 *    
 * Marco Balduini (marco.balduini@polimi.it)
 * Emanuele Della Valle (emanuele.dellavalle@polimi.it)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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

		try {
			cpds = new ComboPooledDataSource();
			cpds.setJdbcUrl("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname());
			cpds.setUser(Config.getInstance().getMysqlUser());
			cpds.setPassword(Config.getInstance().getMysqlPwd());
			cpds.setInitialPoolSize(Config.getInstance().getInitialPoolSize());
			cpds.setAcquireIncrement(Config.getInstance().getacquireIncrement());
			cpds.setMaxPoolSize(Config.getInstance().getMaxPoolSize());
			cpds.setMinPoolSize(Config.getInstance().getMinPoolSize());
			cpds.setMaxStatements(Config.getInstance().getMaxStatements());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
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
