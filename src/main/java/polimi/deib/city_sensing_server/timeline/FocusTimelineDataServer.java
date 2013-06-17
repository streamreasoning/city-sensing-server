package polimi.deib.city_sensing_server.timeline;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;


public class FocusTimelineDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(FocusTimelineDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new Gson();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		
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
			
			String sqlQuery = "SELECT ts_ID,AVG(anomaly_index) AS mobily_anomaly,SUM(n_tweets) AS social_activity " +
					"FROM INFO_ABOUT_SQUARE_BY_TS " +
					"WHERE square_ID IN (" + cellList + ") AND ts_id >= " + tReq.getStart() + " AND ts_id <= " + tReq.getEnd() + " " +
					"GROUP BY ts_ID";
						
			Class.forName("com.mysql.jdbc.Driver");
			
			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());
			
			statement = connection.createStatement();
			
			resultSet = statement.executeQuery(sqlQuery);
			
			boolean next = resultSet.next();
			
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

				lastEndIntervalTs = startIntervalTS + tsInterval;
				step.setStart(startIntervalTS);
				step.setEnd(lastEndIntervalTs);
				step.setMobile_anomaly(Double.parseDouble(resultSet.getString(2)));
				step.setSocial_activity(Double.parseDouble(resultSet.getString(3)));


				stepList.add(step);

				next = resultSet.next();

			}
			
			while(lastEndIntervalTs < Long.parseLong(tReq.getEnd())){

				step = new GeneralTimelineStep();

				step.setStart(lastEndIntervalTs);
				lastEndIntervalTs = lastEndIntervalTs + tsInterval;
				step.setEnd(lastEndIntervalTs);
				step.setMobile_anomaly(0);
				step.setSocial_activity(0);

				stepList.add(step);

			}

			response.setSteps(stepList);

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
			logger.error("Error while connecting to mysql DB", e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "Error while connecting to mysql DB");
			this.getResponse().setEntity(gson.toJson("error"), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();

		} finally {
			try {
				if(resultSet != null && !resultSet.isClosed()){
					resultSet.close();
				}
			} catch (SQLException e) {
				logger.error("Error while closing resultset", e);
			}
			try {
				if(statement != null && !statement.isClosed()){
					statement.close();
				}
			} catch (SQLException e) {
				logger.error("Error while closing statement", e);
			}
			try {
				if(connection != null && !connection.isClosed()){
					connection.close();
				}
			} catch (SQLException e) {
				logger.error("Error while closing database connection", e);
			}
		}

	}

}
