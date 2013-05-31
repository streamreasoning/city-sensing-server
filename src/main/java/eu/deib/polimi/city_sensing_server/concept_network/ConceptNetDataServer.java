package eu.deib.polimi.city_sensing_server.concept_network;

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

public class ConceptNetDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(ConceptNetDataServer.class.getName());

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

			ConceptNetRequest cnReq = gson.fromJson(reader, ConceptNetRequest.class);

			String cellList = new String();

			if(cnReq.getStart() == null || Long.parseLong(cnReq.getStart()) < 0){
				cnReq.setStart("1365199200000");
			}
			if(cnReq.getEnd() == null || Long.parseLong(cnReq.getEnd()) < 0){
				cnReq.setEnd("1366927200000");
			}
			if(cnReq.getThreshold() == null || Integer.parseInt(cnReq.getThreshold()) < 0){
				cnReq.setThreshold("50");
			}
			if(cnReq.getCells() == null || cnReq.getCells().size() == 0){
				for(int i = 1 ; i < 10000 ; i++)
					cellList = cellList + i + ",";
			} else {
				for(String s : cnReq.getCells())
					cellList = cellList + s + ",";
			}

			cellList = cellList.substring(0, cellList.length() - 1);

			ConceptNetResponse response = new ConceptNetResponse();
			ConceptNetNode node;
			ArrayList<ConceptNetNode> nodeList = new ArrayList<ConceptNetNode>();
			ConceptNetLink link;
			ArrayList<ConceptNetLink> linkList = new ArrayList<ConceptNetLink>();

			String sqlQuery = "SELECT DISTINCT hashtag " +
					"FROM HASHTAG_SQUARE,TIMESTAMPS AS TS1, TIMESTAMPS AS TS2 " +
					"WHERE TS1.ts_ID = ht_ts_ID AND TS2.ts_ID = ht_ts_ID AND ht_square_ID in (" + cellList + ") AND " +
					"TS1.ts_interval_start >= " + cnReq.getStart() + " AND TS2.ts_interval_end <= " + cnReq.getEnd() + " " +
					"GROUP BY hashtag";

			Class.forName("com.mysql.jdbc.Driver");

			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			boolean next = resultSet.next();

			do{
				node = new ConceptNetNode();

				node.setId(resultSet.getString(1));
				node.setLabel(resultSet.getString(1));
				node.setGroup("keyword");

				nodeList.add(node);

				next = resultSet.next();

			}while(next);

			if(resultSet != null || !resultSet.isClosed()){
				try {
					resultSet.close();
				} catch (SQLException e) {
					logger.error("Error while closing resultset", e);
				}
			}
			if(statement != null || !statement.isClosed()){
				try {
					statement.close();
				} catch (SQLException e) {
					logger.error("Error while closing statement", e);
				}
			}

			sqlQuery = "SELECT HT1.hashtag,HT2.hashtag,COUNT(*) as count " +
					"FROM HASHTAG_SQUARE as HT1, HASHTAG_SQUARE as HT2, TIMESTAMPS AS TS1, TIMESTAMPS AS TS2 " +
					"WHERE TS1.ts_ID = HT1.ht_ts_ID AND TS2.ts_ID = HT1.ht_ts_ID AND " +
					"TS1.ts_ID = HT2.ht_ts_ID AND TS2.ts_ID = HT2.ht_ts_ID AND " +
					"HT1.ht_square_ID in  (" + cellList + ") AND HT2.ht_square_ID in  (" + cellList + ") AND " +
					"TS1.ts_interval_start >= " + cnReq.getStart() + " AND TS2.ts_interval_end <= " + cnReq.getEnd() + " AND " +
					"HT1.hashtag != HT2.hashtag " +
					"GROUP BY HT1.hashtag,HT2.hashtag " +
					"HAVING(count > " + cnReq.getThreshold() + ") " +
					"ORDER BY count";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			do{
				link = new ConceptNetLink();

				link.setSource(resultSet.getString(1));
				link.setTarget(resultSet.getString(2));
				link.setValue(Double.parseDouble(resultSet.getString(3)));

				linkList.add(link);

				next = resultSet.next();

			}while(next);

			response.setNodes(nodeList);
			response.setLinks(linkList);

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
