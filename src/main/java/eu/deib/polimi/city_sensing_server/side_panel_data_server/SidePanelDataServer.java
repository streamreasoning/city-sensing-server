package eu.deib.polimi.city_sensing_server.side_panel_data_server;

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
import com.google.gson.stream.MalformedJsonException;

import eu.deib.polimi.city_sensing_server.configuration.Config;

public class SidePanelDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(SidePanelDataServer.class.getName());

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

			SidePanelRequest spReq = gson.fromJson(reader, SidePanelRequest.class);

			String cellList = new String();
			
			if(spReq.getStart() == null || Long.parseLong(spReq.getStart()) < 0){
				spReq.setStart("1365199200000");
			}
			if(spReq.getEnd() == null || Long.parseLong(spReq.getEnd()) < 0){
				spReq.setEnd("1366927200000");
			}
			if(spReq.getCells() == null || spReq.getCells().size() == 0){
				for(int i = 1 ; i < 10000 ; i++)
					cellList = cellList + i + ",";
			} else {
				for(String s : spReq.getCells())
					cellList = cellList + s + ",";
			}

			cellList = cellList.substring(0, cellList.length() - 1);

			String sqlQuery = "SELECT outcoming_call_number,incoming_call_number,outcoming_sms_number,incoming_sms_number,data_cdr_number,hashtag,n_occurrences " +
					"FROM INFO_ABOUT_SQUARE_BY_TS, HASHTAG_SQUARE " +
					"WHERE square_ID IN (" + cellList + ") AND ts_ID >= " + spReq.getStart() + " AND ts_ID <= " + spReq.getEnd() + " " +
					"AND INFO_ABOUT_SQUARE_BY_TS.square_ID = HASHTAG_SQUARE.ht_square_ID AND INFO_ABOUT_SQUARE_BY_TS.ts_ID = HASHTAG_SQUARE.ht_ts_ID";
			
			Class.forName("com.mysql.jdbc.Driver");

			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			boolean next = resultSet.next();
			
			SidePanelResponse response = new SidePanelResponse();
			SidePanelHashtag hashtag;
			ArrayList<SidePanelHashtag> hashtagList = new ArrayList<SidePanelHashtag>();
			
			double totalOutcomingCalls = 0;
			double totalIncomingCalls = 0;
			double totalOutcomingSMS = 0;
			double totalIncomingSMS = 0;
			double totalDataCdr = 0;

			while(next){
				hashtag = new SidePanelHashtag();

				totalOutcomingCalls = totalOutcomingCalls + Double.parseDouble(resultSet.getString(1));
				totalIncomingCalls = totalIncomingCalls + Double.parseDouble(resultSet.getString(2));
				totalOutcomingCalls = totalOutcomingSMS + Double.parseDouble(resultSet.getString(3));
				totalIncomingSMS = totalIncomingSMS + Double.parseDouble(resultSet.getString(4));
				totalDataCdr = totalDataCdr + Double.parseDouble(resultSet.getString(5));
				
				hashtag.setLabel(resultSet.getString(6));
				hashtag.setValue(Long.parseLong(resultSet.getString(7)));

				hashtagList.add(hashtag);

				next = resultSet.next();

			}

			response.setCalls_out(totalOutcomingCalls);
			response.setCalls_in(totalIncomingCalls);
			response.setMessages_out(totalOutcomingSMS);
			response.setMessages_in(totalIncomingSMS);
			response.setData_traffic(totalDataCdr);
			response.setHashtags(hashtagList);
					
			this.getResponse().setStatus(Status.SUCCESS_CREATED);
			this.getResponse().setEntity(gson.toJson(response), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();

		} catch (ClassNotFoundException e) {
			logger.error("Error while getting jdbc Driver Class", e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "Error while getting jdbc Driver Class");
			this.getResponse().setEntity(gson.toJson("Error while getting jdbc Driver Class"), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();
		} catch (SQLException e) {
			logger.error("Error while connecting to mysql DB", e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "Error while connecting to mysql DB");
			this.getResponse().setEntity(gson.toJson("Error while connecting to mysql DB"), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();

		} catch (MalformedJsonException e) {
			logger.error("Error while serializing json, malformed json", e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "Error while serializing json, malformed json");
			this.getResponse().setEntity(gson.toJson("Error while serializing json, malformed json"), MediaType.APPLICATION_JSON);
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
