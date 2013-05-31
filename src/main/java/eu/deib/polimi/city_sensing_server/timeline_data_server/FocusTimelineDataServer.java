package eu.deib.polimi.city_sensing_server.timeline_data_server;

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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

public class FocusTimelineDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(FocusTimelineDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes", "null" })
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
			
			String sqlQuery = "SELECT ts_interval_start, ts_interval_end,SUM(anomaly_index) AS mobily_anomaly,SUM(n_tweets) AS social_activity " +
					"FROM INFO_ABOUT_SQUARE_BY_TS, TIMESTAMPS " +
					"WHERE square_ID IN (" + cellList + ") AND INFO_ABOUT_SQUARE_BY_TS.ts_ID = TIMESTAMPS.ts_id AND " +
					"ts_interval_start > " + tReq.getStart() + " AND ts_interval_end < " + tReq.getEnd() + " " +
					"GROUP BY INFO_ABOUT_SQUARE_BY_TS.ts_ID";
						
			Class.forName("com.mysql.jdbc.Driver");
			
			connection = DriverManager.getConnection("jdbc:mysql://156.54.107.76:8001/milano_city_sensing?user=mbalduini&password=mbalduini");
			
			statement = connection.createStatement();
			
			resultSet = statement.executeQuery(sqlQuery);
			
			boolean next = resultSet.next();
			
			GeneralTimelineResponse response = new GeneralTimelineResponse();
			GeneralTimelineStep step;
			ArrayList<GeneralTimelineStep> stepList = new ArrayList<GeneralTimelineStep>();

			
			do{
				step = new GeneralTimelineStep();
				
				step.setStart(Long.parseLong(resultSet.getString(1)));
				step.setEnd(Long.parseLong(resultSet.getString(2)));
				step.setMobile_anomaly(Double.parseDouble(resultSet.getString(3)));
				step.setSocial_activity(Double.parseDouble(resultSet.getString(4)));

				stepList.add(step);
				
				next = resultSet.next();
				
			}while(next);
			
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
				if(resultSet != null || !resultSet.isClosed()){
					resultSet.close();
				}
			} catch (SQLException e) {
				logger.error("Error while closing resultset", e);
			}
			try {
				if(statement != null || !statement.isClosed()){
					statement.close();
				}
			} catch (SQLException e) {
				logger.error("Error while closing statement", e);
			}
			try {
				if(connection != null || !connection.isClosed()){
					connection.close();
				}
			} catch (SQLException e) {
				logger.error("Error while closing database connection", e);
			}
		}

	}

}
