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
import polimi.deib.city_sensing_server.timeline.GeneralTimelineResponse;
import polimi.deib.city_sensing_server.timeline.GeneralTimelineStep;
import polimi.deib.city_sesning_server.utilities.ResponseMapping;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;


public class FocusTimelineDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(FocusTimelineDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		logger.debug("Focus timelin request received");
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

			FocusTimelineRequest tReq = gson.fromJson(reader, FocusTimelineRequest.class);

			String cellList = new String();

			if(tReq.getStart() == null || Long.parseLong(tReq.getStart()) < 0){
				tReq.setStart("1365199200000");
			}
			if(tReq.getEnd() == null || Long.parseLong(tReq.getEnd()) < 0){
				tReq.setEnd("1366927200000");
			}
			if(tReq.getCells() == null || tReq.getCells().size() == 0){
				for(int i = 1 ; i < 10000 ; i++)
					cellList = cellList + i + ",";
			} else {
				for(String s : tReq.getCells())
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
					key = resultSet.getString(2);
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

			sqlQuery = "SELECT ts_ID,AVG(anomaly_index) AS mobily_anomaly,SUM(n_tweets) AS social_activity " +
					"FROM INFO_ABOUT_SQUARE_BY_TS " +
					"WHERE square_ID IN (" + cellList + ") AND ts_id >= " + tReq.getStart() + " AND ts_id <= " + tReq.getEnd() + " " +
					"GROUP BY ts_ID";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			GeneralTimelineResponse response = new GeneralTimelineResponse();
			GeneralTimelineStep step;
			ArrayList<GeneralTimelineStep> stepList = new ArrayList<GeneralTimelineStep>();


			long tsInterval = 900000;
			long startIntervalTS = 0;
			long lastEndIntervalTs = Long.parseLong(tReq.getStart());

			while(next){

				startIntervalTS = Long.parseLong(resultSet.getString(1));

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

				rm = respMap.get(String.valueOf(resultSet.getString(1)).hashCode());
				if(rm != null){

					lastEndIntervalTs = startIntervalTS + tsInterval;
					step.setStart(startIntervalTS);
					step.setEnd(lastEndIntervalTs);
					step.setMobile_anomaly(rm.getAnomaly());
					step.setSocial_activity(Double.parseDouble(resultSet.getString(3)));


					stepList.add(step);
				}

				next = resultSet.next();

			}

			//			while(lastEndIntervalTs < Long.parseLong(tReq.getEnd())){
			//
			//				step = new GeneralTimelineStep();
			//
			//				step.setStart(lastEndIntervalTs);
			//				lastEndIntervalTs = lastEndIntervalTs + tsInterval;
			//				step.setEnd(lastEndIntervalTs);
			//				step.setMobile_anomaly(0);
			//				step.setSocial_activity(0);
			//
			//				stepList.add(step);
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

			rep.release();
			this.getResponse().setStatus(Status.SUCCESS_CREATED);
			this.getResponse().setEntity(gson.toJson(response), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();

		} catch (ClassNotFoundException e) {
			rep.release();
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
				rep.release();
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
				rep.release();
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
				rep.release();
				String error = "Error while connecting to mysql DB and while closing database connection";
				logger.error(error, e1);
				this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
				this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
				this.getResponse().commit();
				this.commit();	
				this.release();
			}

			rep.release();
			String error = "Error while connecting to mysql DB or retrieving data from db";
			logger.error(error, e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
			this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();

		} catch (Exception e) {
			rep.release();
			String error = "Generic Error";
			logger.error(error, e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
			this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();
			
		} finally {
			rep.release();
			this.getResponse().commit();
			this.commit();	
			this.release();
		}

	}

}
