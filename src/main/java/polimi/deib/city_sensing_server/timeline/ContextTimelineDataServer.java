package polimi.deib.city_sensing_server.timeline;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.engine.header.Header;
import org.restlet.representation.Representation;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.restlet.util.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import polimi.deib.city_sensing_server.configuration.Config;
import polimi.deib.city_sesning_server.utilities.ResponseMapping;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;


public class ContextTimelineDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(ContextTimelineDataServer.class.getName());

	private static long minTS = 1365199200000L;
	private static long TSInterval_3h = 10800000;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Post
	public void dataServer(Representation rep) throws IOException {

		logger.debug("Context timeline request received");
		Gson gson = new Gson();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		HashMap<Integer, ResponseMapping> respMap = new HashMap<Integer, ResponseMapping>();

		Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
		if (responseHeaders == null) {
			responseHeaders = new Series(Header.class);
			getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
		}
		responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));

		try {

			JsonReader reader = null;

			reader = new JsonReader(new StringReader(rep.getText()));

			ContextTimelineRequest cReq = gson.fromJson(reader, ContextTimelineRequest.class);

			String cellList = new String();

			if(cReq.getCells() == null || cReq.getCells().size() == 0){
				for(int i = 1 ; i < 10000 ; i++)
					cellList = cellList + i + ",";
			} else {
				for(String s : cReq.getCells())
					cellList = cellList + s + ",";
			}

			cellList = cellList.substring(0, cellList.length() - 1);

			Class.forName("com.mysql.jdbc.Driver");

			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());

			String sqlQuery = "SELECT square_id,time_slot,AVG(anomaly_index) FROM anomaly WHERE square_id IN (" + cellList + ") GROUP BY time_slot ";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			ResponseMapping rm;
			String key;

			boolean next = resultSet.next();

			while(next) {

				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
					key = String.valueOf(getTsID(Long.parseLong(resultSet.getString(2)), TSInterval_3h));
					if(respMap.containsKey(key.hashCode())){
						rm = respMap.get(key.hashCode());
						if(resultSet.getString(3) != null)
							rm.setAnomaly(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setAnomaly(0);
					} else {
						rm = new ResponseMapping();
						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
						if(resultSet.getString(3) != null)
							rm.setAnomaly(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setAnomaly(0);
					}
					respMap.put(key.hashCode(), rm);
				}
				next = resultSet.next();
			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT 3h_ts_id,SUM(n_tweets) AS social_activity " +
					"FROM INFO_ABOUT_SQUARE_BY_TS " +
					"WHERE square_ID IN (" + cellList + ") " +
					"GROUP BY 3h_ts_id";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			GeneralTimelineResponse response = new GeneralTimelineResponse();
			GeneralTimelineStep step;
			ArrayList<GeneralTimelineStep> stepList = new ArrayList<GeneralTimelineStep>();


			long tsInterval = 10800000;
			//			long firstHistoricalTs = 1365199200000L;
			//			long startIntervalTS = 0;
			//			long lastEndIntervalTs = firstHistoricalTs;

			while(next){

				//				startIntervalTS = Long.parseLong(resultSet.getString(1));
				//
				//				while(lastEndIntervalTs < startIntervalTS){
				//
				//					step = new GeneralTimelineStep();
				//
				//					step.setStart(lastEndIntervalTs);
				//					lastEndIntervalTs = lastEndIntervalTs + tsInterval;
				//					step.setEnd(lastEndIntervalTs);
				//					step.setMobile_anomaly(0);
				//					step.setSocial_activity(0);
				//
				//					stepList.add(step);
				//
				//				}
				//
				//				while(startIntervalTS - lastEndIntervalTs >= tsInterval){
				//
				//					step = new GeneralTimelineStep();
				//
				//					step.setStart(lastEndIntervalTs);
				//					lastEndIntervalTs = lastEndIntervalTs + tsInterval;
				//					step.setEnd(lastEndIntervalTs);
				//					step.setMobile_anomaly(0);
				//					step.setSocial_activity(0);
				//
				//					stepList.add(step);
				//
				//				}

				step = new GeneralTimelineStep();

				//				lastEndIntervalTs = startIntervalTS + tsInterval;
				rm = respMap.get(String.valueOf(resultSet.getString(1)).hashCode());
				if(rm != null){
					step.setStart(Long.parseLong(resultSet.getString(1)));
					step.setEnd(Long.parseLong(resultSet.getString(1)) + tsInterval);
					step.setMobile_anomaly(rm.getAnomaly());
					step.setSocial_activity(Double.parseDouble(resultSet.getString(2)));


					stepList.add(step);
				}

				next = resultSet.next();

			}


			//			String sqlQuery = "SELECT 3h_ts_id,AVG(anomaly_index) AS mobily_anomaly,SUM(n_tweets) AS social_activity " +
			//					"FROM INFO_ABOUT_SQUARE_BY_TS " +
			//					"WHERE square_ID IN (" + cellList + ") " +
			//					"GROUP BY 3h_ts_id";
			//
			//			Class.forName("com.mysql.jdbc.Driver");
			//
			//			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());
			//
			//			statement = connection.createStatement();
			//
			//			resultSet = statement.executeQuery(sqlQuery);
			//
			//			boolean next = resultSet.next();
			//
			//			GeneralTimelineResponse response = new GeneralTimelineResponse();
			//			GeneralTimelineStep step;
			//			ArrayList<GeneralTimelineStep> stepList = new ArrayList<GeneralTimelineStep>();
			//
			//
			//			long tsInterval = 10800000;
			////			long firstHistoricalTs = 1365199200000L;
			////			long startIntervalTS = 0;
			////			long lastEndIntervalTs = firstHistoricalTs;
			//			
			//			while(next){
			//
			////				startIntervalTS = Long.parseLong(resultSet.getString(1));
			////
			////				while(lastEndIntervalTs < startIntervalTS){
			////
			////					step = new GeneralTimelineStep();
			////
			////					step.setStart(lastEndIntervalTs);
			////					lastEndIntervalTs = lastEndIntervalTs + tsInterval;
			////					step.setEnd(lastEndIntervalTs);
			////					step.setMobile_anomaly(0);
			////					step.setSocial_activity(0);
			////
			////					stepList.add(step);
			////
			////				}
			////
			////				while(startIntervalTS - lastEndIntervalTs >= tsInterval){
			////
			////					step = new GeneralTimelineStep();
			////
			////					step.setStart(lastEndIntervalTs);
			////					lastEndIntervalTs = lastEndIntervalTs + tsInterval;
			////					step.setEnd(lastEndIntervalTs);
			////					step.setMobile_anomaly(0);
			////					step.setSocial_activity(0);
			////
			////					stepList.add(step);
			////
			////				}
			//
			//				step = new GeneralTimelineStep();
			//
			////				lastEndIntervalTs = startIntervalTS + tsInterval;
			//				step.setStart(Long.parseLong(resultSet.getString(1)));
			//				step.setEnd(Long.parseLong(resultSet.getString(1)) + tsInterval);
			//				step.setMobile_anomaly(Double.parseDouble(resultSet.getString(2)));
			//				step.setSocial_activity(Double.parseDouble(resultSet.getString(3)));
			//
			//
			//				stepList.add(step);
			//
			//				next = resultSet.next();
			//
			//			}

			response.setSteps(stepList);
			
			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}
			if(connection != null && !connection.isClosed()){
				connection.close();
			}

			this.getResponse().setStatus(Status.SUCCESS_CREATED);
			this.getResponse().setEntity(gson.toJson(response), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();

		} catch (ClassNotFoundException e) {
			logger.error("Error while getting jdbc Driver Class", e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "Error while getting jdbc Driver Class");
			this.getResponse().setEntity(gson.toJson("error"), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();
		} catch (SQLException e) {
			try {
				if(resultSet != null && !resultSet.isClosed()){
					resultSet.close();
				}
			} catch (SQLException e1) {
				String error = "Error while connecting to mysql DB and while closing resultset";
				logger.error(error, e1);
				this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
				this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
				this.getResponse().commit();
				this.commit();	
				this.release();
			}
			try {
				if(statement != null && !statement.isClosed()){
					statement.close();
				}
			} catch (SQLException e1) {
				String error = "Error while connecting to mysql DB and while closing statement";
				logger.error(error, e1);
				this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
				this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
				this.getResponse().commit();
				this.commit();	
				this.release();
			}
			try {
				if(connection != null && !connection.isClosed()){
					connection.close();
				}
			} catch (SQLException e1) {
				String error = "Error while connecting to mysql DB and while closing database connection";
				logger.error(error, e1);
				this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
				this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
				this.getResponse().commit();
				this.commit();	
				this.release();
			}

			String error = "Error while connecting to mysql DB or retrieving data from db";
			logger.error(error, e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
			this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();

		}

	}

	private static long getTsID(long postTs, long TSInterval){
		int temp_ts_ID = -1;
		long ts_ID = -1;
		try{
			temp_ts_ID = (int)(((postTs - minTS) / TSInterval) + 1);
			ts_ID = minTS + (temp_ts_ID * TSInterval);  
		} catch (Exception e){
			return -1;
		}
		return ts_ID;
	}

}
