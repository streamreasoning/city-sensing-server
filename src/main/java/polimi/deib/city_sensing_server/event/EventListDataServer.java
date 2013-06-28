package polimi.deib.city_sensing_server.event;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

import polimi.deib.city_sensing_server.Rest_Server;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

public class EventListDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(EventListDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		logger.debug("Event List request received");
		Gson gson = new Gson();
		Connection connection = null;
		PreparedStatement p1 = null;
		PreparedStatement p2 = null;
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

			ArrayList<Integer> cellList = new ArrayList<Integer>();
			String prepStmt = new String();
			
			if(mReq.getStart() == null || Long.parseLong(mReq.getStart()) < 0){
				mReq.setStart("1365199200000");
			}
			if(mReq.getEnd() == null || Long.parseLong(mReq.getEnd()) < 0){
				mReq.setEnd("1366927200000");
			}
			if(mReq.getCells() == null || mReq.getCells().size() == 0){
				for(int i = 1 ; i < 9999 ; i++){
					cellList.add(i);
					prepStmt = prepStmt + "?,";
				}
			} else {
				for(String s : mReq.getCells()){
					cellList.add(Integer.parseInt(s));
					prepStmt = prepStmt + "?,";
				}
			}

			prepStmt = prepStmt.substring(0, prepStmt.length() - 1);

			String sqlQuery = "SELECT EVENT.event_ID, EVENT.name, VENUE.address, EVENT.start_date, EVENT.end_date, EVENT.link, EVENT.event_venue_ID " +
					"FROM EVENT,VENUE " +
					"WHERE event_venue_ID = venue_ID AND ( start_date >= ? OR end_date <= ? ) AND " +
					"venue_square_ID IN  (" + prepStmt + ") ";

//			Class.forName("com.mysql.jdbc.Driver");
//
//			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());

			connection = Rest_Server.bds.getConnection();
			connection.setAutoCommit(false);

			p1 = connection.prepareStatement(sqlQuery);
			p1.setObject(1, Long.parseLong(mReq.getStart()));
			p1.setObject(2, Long.parseLong(mReq.getEnd()));
			insertValues(p1, 3, cellList);
			resultSet = p1.executeQuery();
			
			sqlQuery = "SELECT event_fs_cat_ID, FUORISALONE_CAT.name " +
					"FROM EVENT, FUORISALONE_CAT " +
					"WHERE EVENT.event_fs_cat_ID = FUORISALONE_CAT.fs_cat_ID AND EVENT.event_fs_cat_ID = ?";
			p2 = connection.prepareStatement(sqlQuery);


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

				p2.setObject(1, Integer.parseInt(resultSet.getString(7)));

				innerResultSet = p2.executeQuery();

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

				if(innerResultSet != null && !innerResultSet.isClosed()){
					innerResultSet.close();
				}

				next = resultSet.next();

			}

			response.setCells(eventList);
			
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
