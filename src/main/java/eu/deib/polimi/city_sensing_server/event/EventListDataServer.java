package eu.deib.polimi.city_sensing_server.event;

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

public class EventListDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(EventListDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes", "null" })
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new Gson();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		ResultSet innerResultSet = null;

		
		Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
		if (responseHeaders == null) {
		    responseHeaders = new Series(Header.class);
		    getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
		}
		responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));

		try {

			JsonReader reader = null;

			reader = new JsonReader(new StringReader(rep.getText()));

			EventListRequest mReq = gson.fromJson(reader, EventListRequest.class);

			String cellList = new String();
			
			if(mReq.getStart() == null || Long.parseLong(mReq.getStart()) < 0){
				mReq.setStart("1365199200000");
			}
			if(mReq.getEnd() == null || Long.parseLong(mReq.getEnd()) < 0){
				mReq.setEnd("1366927200000");
			}
			if(mReq.getCells() == null || mReq.getCells().size() == 0){
				for(int i = 1 ; i < 10000 ; i++)
					cellList = cellList + i + ",";
			} else {
				for(String s : mReq.getCells())
					cellList = cellList + s + ",";
			}

			cellList = cellList.substring(0, cellList.length() - 1);

			String sqlQuery = "SELECT EVENT.event_ID, EVENT.name, VENUE.address, EVENT.start_date, EVENT.end_date, EVENT.link, EVENT.venue_ID " +
					"FROM EVENT,VENUE " +
					"WHERE event_venue_ID = venue_ID AND ( start_date >= " + mReq.getStart() + " OR end_date <= " + mReq.getEnd() + " ) AND " +
					"venue_square_ID IN  (" + cellList + ") ";

			Class.forName("com.mysql.jdbc.Driver");

			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			boolean next = resultSet.next();
			boolean innerNext;


			EventListResponse response = new EventListResponse();
			Event event;
			ArrayList<Long> dateList;
			ArrayList<Event> eventList = new ArrayList<Event>();
			EventListCategory category;
			ArrayList<EventListCategory> categoryList;
			EventListVenue venue;


			while(next){
				event = new Event();
				dateList = new ArrayList<Long>();
				categoryList = new ArrayList<EventListCategory>();
				venue = new EventListVenue();

				event.setId(Long.parseLong(resultSet.getString(1)));
				event.setName(resultSet.getString(2));
				event.setAddress(resultSet.getString(3));
				dateList.add(Long.parseLong(resultSet.getString(4)));
				dateList.add(Long.parseLong(resultSet.getString(5)));
				event.setDate(dateList);
				event.setLink(resultSet.getString(6));
				
				sqlQuery = "SELECT event_fs_cat_ID, FUORISALONE_CAT.name" +
						"FROM EVENT, FUORISALONE_CAT " +
						"WHERE EVENT.event_fs_cat_ID = FUORISALONE_CAT.fs_cat_ID AND EVENT.event_fs_cat_ID = " + resultSet.getString(7);
				
				statement = connection.createStatement();

				innerResultSet = statement.executeQuery(sqlQuery);
				
				innerNext = innerResultSet.next();
				
				while(innerNext){
					
					category = new EventListCategory();
					
					category.setId(Long.parseLong(innerResultSet.getString(1)));
					category.setLabel(innerResultSet.getString(2));
					
					categoryList.add(category);
					
					innerNext = innerResultSet.next();
				}
				
				venue.setCategories(categoryList);
				event.setVenues(venue);
				
				eventList.add(event);
				
				next = resultSet.next();

			}

			response.setCells(eventList);
					
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
				if(resultSet == null && !resultSet.isClosed()){
					resultSet.close();
				}
			} catch (SQLException e) {
				logger.error("Error while closing resultset", e);
			}
			try {
				if(innerResultSet != null && !innerResultSet.isClosed()){
					innerResultSet.close();
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
