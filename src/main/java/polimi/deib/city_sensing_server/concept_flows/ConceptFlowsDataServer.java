package polimi.deib.city_sensing_server.concept_flows;

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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;


public class ConceptFlowsDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(ConceptFlowsDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		logger.debug("Concept flows request received");
		Gson gson = new GsonBuilder()
		.registerTypeAdapter(ConceptFlowsNode.class, new ConceptFlowsNodeSerializer())
		.create();

		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;

		ConceptFlowsNode node;
		ConceptFlowsLink link;
		ArrayList<ConceptFlowsNode> nodeList = new ArrayList<ConceptFlowsNode>();
		ArrayList<ConceptFlowsLink> linkList = new ArrayList<ConceptFlowsLink>();

		Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
		if (responseHeaders == null) {
			responseHeaders = new Series(Header.class);
			getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
		}
		responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));

		try {

			JsonReader reader = null;

			reader = new JsonReader(new StringReader(rep.getText()));

			ConceptFlowsRequest cReq = gson.fromJson(reader, ConceptFlowsRequest.class);

			String cellList = new String();

			if(cReq.getStart() == null || Long.parseLong(cReq.getStart()) < 0){
				cReq.setStart("1365199200000");
			}
			if(cReq.getEnd() == null || Long.parseLong(cReq.getEnd()) < 0){
				cReq.setEnd("1366927200000");
			}
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

			String sqlQuery = "CREATE OR REPLACE VIEW `foursquare_cat_in_square` AS " +
					"SELECT DISTINCT FOURSQUARE_CAT.name AS name, VENUE.venue_square_ID AS square_ID " +
					"FROM FOURSQUARE_CAT,VENUE_FOURSQUARE,VENUE " +
					"WHERE VENUE.venue_square_ID in (" + cellList + ") AND " +
					"VENUE_FOURSQUARE.venue_fsq_ID = VENUE.venue_ID AND VENUE_FOURSQUARE.fsq_cat_ID = FOURSQUARE_CAT.fsq_cat_ID";

			statement = connection.createStatement();

			statement.executeUpdate(sqlQuery);

			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "CREATE OR REPLACE VIEW `fuorisalone_cat_in_square` AS " +
					"SELECT DISTINCT FUORISALONE_CAT.name AS name, VENUE.venue_square_ID AS square_ID " +
					"FROM FUORISALONE_CAT,VENUE,EVENT " +
					"WHERE VENUE.venue_square_ID in (" + cellList + ") " +
					"AND VENUE.venue_ID = EVENT.event_venue_ID AND " +
					"EVENT.event_fs_cat_ID = FUORISALONE_CAT.fs_cat_ID";

			statement = connection.createStatement();

			statement.executeUpdate(sqlQuery);

			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "CREATE OR REPLACE VIEW `hashtag_in_square_by_ts` AS " +
					"SELECT DISTINCT hashtag, ht_square_ID, ht_ts_ID " +
					"FROM HASHTAG_SQUARE " +
					"WHERE ht_square_ID in (" + cellList + ") AND ht_ts_ID >=" + cReq.getStart() + " AND ht_ts_ID <=" + cReq.getEnd();

			statement = connection.createStatement();

			statement.executeUpdate(sqlQuery);

			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT name " +
					"FROM foursquare_cat_in_square ";
			statement = connection.createStatement();
			resultSet = statement.executeQuery(sqlQuery);

			boolean next = resultSet.next();

			while(next){
				node = new ConceptFlowsNode();

				node.setId(resultSet.getString(1));
				node.setLabel(resultSet.getString(1));
				node.setGroup("foursquare");

				nodeList.add(node);

				next = resultSet.next();

			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT name " +
					"FROM fuorisalone_cat_in_square ";
			statement = connection.createStatement();
			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next){
				node = new ConceptFlowsNode();

				node.setId(resultSet.getString(1));
				node.setLabel(resultSet.getString(1));
				node.setGroup("fuorisalone");

				nodeList.add(node);

				next = resultSet.next();

			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT hashtag " +
					"FROM hashtag_in_square_by_ts ";
			statement = connection.createStatement();
			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next){
				node = new ConceptFlowsNode();

				node.setId(resultSet.getString(1));
				node.setLabel(resultSet.getString(1));
				node.setGroup("hashtag");
				node.setSentiment(0);

				nodeList.add(node);

				next = resultSet.next();

			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT c1.name as fuorisalone_cat_name,c2.name as foursquare_cat_name,COUNT(*) as count " +
					"FROM fuorisalone_cat_in_square as c1, foursquare_cat_in_square as c2 " +
					"WHERE c1.square_ID IN (" + cellList + ") AND c2.square_ID IN (" + cellList + ") " +
					"GROUP BY c1.name,c2.name " +
					"ORDER BY count";
			statement = connection.createStatement();
			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next){
				link = new ConceptFlowsLink();

				link.setSource(resultSet.getString(2));
				link.setTarget(resultSet.getString(1));
				link.setValue(Long.parseLong(resultSet.getString(3)));

				linkList.add(link);

				next = resultSet.next();

			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT c1.name as fuorisalone_cat_name,h2.hashtag,COUNT(*) as count " +
					"FROM fuorisalone_cat_in_square as c1, hashtag_in_square_by_ts as h2 " +
					"WHERE c1.square_ID IN (" + cellList + ") AND h2.ht_square_ID IN (" + cellList + ") AND " +
					"h2.ht_ts_ID >=" + cReq.getStart() + " AND h2.ht_ts_ID <=" + cReq.getEnd()  + " " +
					"GROUP BY c1.name,h2.hashtag " +
					"ORDER BY count";
			statement = connection.createStatement();
			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next){
				link = new ConceptFlowsLink();

				link.setSource(resultSet.getString(2));
				link.setTarget(resultSet.getString(1));
				link.setValue(Long.parseLong(resultSet.getString(3)));

				linkList.add(link);

				next = resultSet.next();

			}

			ConceptFlowsResponse response = new ConceptFlowsResponse();
			response.setNodes(nodeList);
			response.setLinks(linkList);


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
			
			String error = "Error while getting jdbc Driver Class";
			logger.error(error, e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
			this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
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

}
