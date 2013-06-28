package polimi.deib.city_sensing_server.timeline;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

import polimi.deib.city_sensing_server.Rest_Server;
import polimi.deib.city_sesning_server.utilities.ResponseMapping;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;


public class FocusTimelineDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(FocusTimelineDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		logger.debug("Focus timeline request received");
		Gson gson = new Gson();
		Connection connection = null;
		ResultSet resultSet = null;
		HashMap<Integer, ResponseMapping> respMap = new HashMap<Integer, ResponseMapping>();

		int lastIndex = 0;

		PreparedStatement p1 = null;
		PreparedStatement p2 = null;

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

			ArrayList<Integer> cellList = new ArrayList<Integer>();
			String prepStmt = new String();
			
			if(tReq.getStart() == null || Long.parseLong(tReq.getStart()) < 0){
				tReq.setStart("1365199200000");
			}
			if(tReq.getEnd() == null || Long.parseLong(tReq.getEnd()) < 0){
				tReq.setEnd("1366927200000");
			}
			if(tReq.getCells() == null || tReq.getCells().size() == 0){
				for(int i = 1 ; i < 9999 ; i++){
					cellList.add(i);
					prepStmt = prepStmt + "?,";
				}
			} else {
				for(String s : tReq.getCells()){
					cellList.add(Integer.parseInt(s));
					prepStmt = prepStmt + "?,";
				}
			}

			prepStmt = prepStmt.substring(0, prepStmt.length() - 1);

//			Class.forName("com.mysql.jdbc.Driver");
//
//			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());
			connection = Rest_Server.bds.getConnection();
			connection.setAutoCommit(false);

			String sqlQuery = "SELECT square_id,time_slot,AVG(anomaly_index) FROM anomaly WHERE square_id IN (" + prepStmt + ") AND time_slot >= ? AND time_slot <= ? GROUP BY time_slot ";

			p1 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p1, 1, cellList);
			p1.setObject(lastIndex + 1, Long.parseLong(tReq.getStart()));
			p1.setObject(lastIndex + 2, Long.parseLong(tReq.getEnd()));
			resultSet = p1.executeQuery();

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
			
			sqlQuery = "SELECT ts_ID,AVG(anomaly_index) AS mobily_anomaly,SUM(n_tweets) AS social_activity " +
					"FROM INFO_ABOUT_SQUARE_BY_TS " +
					"WHERE square_ID IN (" + prepStmt + ") AND ts_id >= ? AND ts_id <= ? " +
					"GROUP BY ts_ID";

			p2 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p2, 1, cellList);
			p2.setObject(lastIndex + 1, Long.parseLong(tReq.getStart()));
			p2.setObject(lastIndex + 2, Long.parseLong(tReq.getEnd()));
			resultSet = p2.executeQuery();

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
			connection.commit();
			rep.release();
			this.getResponse().setStatus(Status.SUCCESS_CREATED);
			this.getResponse().setEntity(gson.toJson(response), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();

		} 
//		catch (ClassNotFoundException e) {
//			rep.release();
//			String error = "Error while getting jdbc Driver Class";
//			logger.error(error, e);
//			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
//			this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
//			this.getResponse().commit();
//			this.commit();	
//			this.release();
//
//		} 
		catch (SQLException e) {
			try {
				if(connection != null && !connection.isClosed()){
					connection.rollback();
					connection.close();
				}
			} catch (SQLException e1) {
				logger.error("Error during rollback operation");
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
			try {
				if(connection != null && !connection.isClosed()){
					connection.rollback();
					connection.close();
				}
			} catch (SQLException e1) {
				logger.error("Error during rollback operation");
			}
			rep.release();
			String error = "Server error or malformed input parameters";
			logger.error(error, e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
			this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();
		} finally {
			rep.release();
			try {
				if(p1 != null && !p1.isClosed()){ p1.close();}
				if(p2 != null && !p2.isClosed()){ p2.close();}
				if(connection != null && !connection.isClosed()){
					connection.rollback();
					connection.close();
				}
			} catch (SQLException e) {
				rep.release();
				String error = "Error while connecting to mysql DB and while closing resultset";
				logger.error(error, e);
				this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
				this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
				this.getResponse().commit();
				this.commit();	
				this.release();
			}
		} 

	}
	
	private int insertValues(PreparedStatement st, int startIndex, ArrayList<Integer> cellList){

		int i = 0;
		int j = 0;
		for(i = startIndex ; i< startIndex + cellList.size() ; i++){
			try {
				st.setInt(i, cellList.get(j));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			j++;
		}

		return i - 1;

	}

}
