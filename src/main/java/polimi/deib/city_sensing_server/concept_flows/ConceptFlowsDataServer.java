package polimi.deib.city_sensing_server.concept_flows;

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

import polimi.deib.city_sensing_server.configuration.Config;
import polimi.deib.city_sensing_server.dataSource.DataSourceSingleton;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;


public class ConceptFlowsDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(ConceptFlowsDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder()
		.serializeSpecialFloatingPointValues()
		.serializeNulls()
		.registerTypeAdapter(ConceptFlowsNode.class, new ConceptFlowsNodeSerializer())
		.create();

		Connection connection = null;
		ResultSet resultSet = null;

		PreparedStatement p1 = null;
		PreparedStatement p2 = null;
		PreparedStatement p3 = null;
		PreparedStatement p4 = null;
		PreparedStatement p5 = null;

		try {

			logger.debug("Concept flows request received");
			String parameters = rep.getText();
			logger.debug("parameters: {}",parameters);

			ConceptFlowsNode node;
			ConceptFlowsLink link;
			ArrayList<ConceptFlowsNode> nodeList = new ArrayList<ConceptFlowsNode>();
			ArrayList<ConceptFlowsLink> linkList = new ArrayList<ConceptFlowsLink>();
			int lastIndex = 0;

			Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
			if (responseHeaders == null) {
				responseHeaders = new Series(Header.class);
				getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
			}
			responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));

			JsonReader reader = null;

			reader = new JsonReader(new StringReader(parameters));

			ConceptFlowsRequest cReq = gson.fromJson(reader, ConceptFlowsRequest.class);

			//			String cellList = new String();
			ArrayList<Integer> cellList = new ArrayList<Integer>();
			String prepStmt = new String();

			if(cReq.getStart() == null || Long.parseLong(cReq.getStart()) < 0){
				cReq.setStart(Config.getInstance().getDefaultStart());
			}
			if(cReq.getEnd() == null || Long.parseLong(cReq.getEnd()) < 0){
				cReq.setEnd(Config.getInstance().getDefaultEnd());
			}
			if(cReq.getCells() == null || cReq.getCells().size() == 0){
				for(int i = 1 ; i < Config.getInstance().getDefaultNumberOfCells() ; i++){
					cellList.add(i);
					prepStmt = prepStmt + "?,";
				}
			} else {
				for(String s : cReq.getCells()){
					cellList.add(Integer.parseInt(s));
					prepStmt = prepStmt + "?,";
				}
			}

			prepStmt = prepStmt.substring(0, prepStmt.length() - 1);

			connection = DataSourceSingleton.getInstance().getConnection();
			connection.setAutoCommit(false);

			String sqlQuery = "SELECT c1.name as fuorisalone_cat_name,h2.hashtag,COUNT(*) as count " +
					"FROM fuorisalone_cat_in_square as c1, HASHTAG_SQUARE as h2 " +
					"WHERE c1.square_ID IN (" + prepStmt + ") AND h2.ht_square_ID = c1.square_ID AND " +
					"h2.ht_ts_ID >=? AND h2.ht_ts_ID <=? " +
					"GROUP BY h2.hashtag " +
					"ORDER BY count DESC " +
					"LIMIT 50";
			
			//			sqlQuery = "SELECT c1.name as fuorisalone_cat_name,h2.hashtag,COUNT(*) as count " +
			//					"FROM fuorisalone_cat_in_square as c1, HASHTAG_SQUARE as h2 " +
			//					"WHERE c1.square_ID IN (" + prepStmt + ") AND h2.ht_square_ID IN (" + prepStmt + ") AND " +
			//					"h2.ht_ts_ID >=? AND h2.ht_ts_ID <=? " +
			//					"GROUP BY c1.name,h2.hashtag " +
			//					"ORDER BY count DESC " +
			//					"LIMIT 20";

			long startTs = System.currentTimeMillis();
			p5 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p5, 1, cellList);
			//			lastIndex = insertValues(p5, lastIndex + 1, cellList);
			p5.setObject(lastIndex + 1, Long.parseLong(cReq.getStart()));
			p5.setObject(lastIndex + 2, Long.parseLong(cReq.getEnd()));
			resultSet= p5.executeQuery();
			long endTs = System.currentTimeMillis();

			logger.debug("Concept flows hashtag-fuorisalone links query done, time: {} ms",endTs - startTs);

			boolean next = resultSet.next();

			while(next){

				node = new ConceptFlowsNode();

				node.setId(resultSet.getString(2));
				node.setLabel(resultSet.getString(2));
				node.setGroup("hashtag");

				if(!nodeList.contains(node))
					nodeList.add(node);

				node = new ConceptFlowsNode();

				node.setId(resultSet.getString(1));
				node.setLabel(resultSet.getString(1));
				node.setGroup("fuorisalone");

				if(!nodeList.contains(node))
					nodeList.add(node);

				link = new ConceptFlowsLink();

				link.setSource(resultSet.getString(2));
				link.setTarget(resultSet.getString(1));
				link.setValue(Long.parseLong(resultSet.getString(3)));

				linkList.add(link);

				next = resultSet.next();

			}

			if(resultSet != null){
				resultSet.close();
			}

			sqlQuery = "SELECT c1.name as fuorisalone_cat_name,c2.name as foursquare_cat_name,COUNT(*) as count " +
					"FROM fuorisalone_cat_in_square as c1, foursquare_cat_in_square as c2 " +
					"WHERE c1.square_ID IN (" + prepStmt + ") AND c2.square_ID = c1.square_ID " +
					"GROUP BY c1.name " +
					"ORDER BY count DESC " +
					"LIMIT 50";
			//			sqlQuery = "SELECT c1.name as fuorisalone_cat_name,c2.name as foursquare_cat_name,COUNT(*) as count " +
			//					"FROM fuorisalone_cat_in_square as c1, foursquare_cat_in_square as c2 " +
			//					"WHERE c1.square_ID IN (" + prepStmt + ") AND c2.square_ID IN (" + prepStmt + ") " +
			//					"GROUP BY c1.name,c2.name " +
			//					"ORDER BY count DESC " +
			//					"LIMIT 20";

			startTs = System.currentTimeMillis();
			p4 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p4, 1, cellList);
			//			lastIndex = insertValues(p4, lastIndex + 1, cellList);
			resultSet= p4.executeQuery();
			endTs = System.currentTimeMillis();

			logger.debug("Concept flows fuorisalone-foursquare links query done, time: {} ms",endTs - startTs);

			next = resultSet.next();

			while(next){
				
				node = new ConceptFlowsNode();

				node.setId(resultSet.getString(1));
				node.setLabel(resultSet.getString(1));
				node.setGroup("fuorisalone");

				if(!nodeList.contains(node))
					nodeList.add(node);
				
				node = new ConceptFlowsNode();

				node.setId(resultSet.getString(2));
				node.setLabel(resultSet.getString(2));
				node.setGroup("foursquare");

				if(!nodeList.contains(node))
					nodeList.add(node);
				
				link = new ConceptFlowsLink();

				link.setSource(resultSet.getString(1));
				link.setTarget(resultSet.getString(2));
				link.setValue(Long.parseLong(resultSet.getString(3)));

				linkList.add(link);

				next = resultSet.next();

			}

//			Collections.sort(linkList);

			ConceptFlowsResponse response = new ConceptFlowsResponse();
			response.setNodes(nodeList);
			response.setLinks(linkList);

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(response);
			endTs = System.currentTimeMillis();

			logger.debug("Concept flow json serialization done, time: {} ms",endTs - startTs);

			connection.commit();
			rep.release();
			this.getResponse().setStatus(Status.SUCCESS_CREATED);
			this.getResponse().setEntity(respString, MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();

		} catch (SQLException e) {

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
				if(p1 != null){ p1.close();}
				if(p2 != null){ p2.close();}
				if(p3 != null){ p3.close();}
				if(p4 != null){ p4.close();}
				if(p5 != null){ p5.close();}
				if(connection != null && !connection.isClosed()){
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


//String sqlQuery = "SELECT DISTINCT name " +
//					"FROM foursquare_cat_in_square " +
//					"WHERE square_ID in (" + prepStmt + ")";
//
//			long startTs = System.currentTimeMillis();
//			p1 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p1, 1, cellList);
//			resultSet= p1.executeQuery();
//			long endTs = System.currentTimeMillis();
//
//			logger.debug("Concept flows foursquare categories query done, time: {} ms",endTs - startTs);
//
//			boolean next = resultSet.next();
//
//			while(next){
//				node = new ConceptFlowsNode();
//
//				node.setId(resultSet.getString(1));
//				node.setLabel(resultSet.getString(1));
//				node.setGroup("foursquare");
//
//				nodeList.add(node);
//
//				next = resultSet.next();
//
//			}
//
//			if(resultSet != null){
//				resultSet.close();
//			}
//
//			sqlQuery = "SELECT DISTINCT name " +
//					"FROM fuorisalone_cat_in_square " +
//					"WHERE square_ID in (" + prepStmt + ")";
//
//			startTs = System.currentTimeMillis();
//			p2 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p2, 1, cellList);
//			resultSet= p2.executeQuery();
//			endTs = System.currentTimeMillis();
//
//			logger.debug("Concept flows fuorisalone categories query done, time: {} ms",endTs - startTs);
//			next = resultSet.next();
//
//			while(next){
//				node = new ConceptFlowsNode();
//
//				node.setId(resultSet.getString(1));
//				node.setLabel(resultSet.getString(1));
//				node.setGroup("fuorisalone");
//
//				nodeList.add(node);
//
//				next = resultSet.next();
//
//			}
//
//			if(resultSet != null){
//				resultSet.close();
//			}
//
//			sqlQuery = "SELECT hashtag, SUM(n_occurrences) as count " +
//					"FROM HASHTAG_SQUARE " +
//					"WHERE ht_square_ID in (" + prepStmt + ") AND ht_ts_ID >=? AND ht_ts_ID <=? " +
//					"GROUP BY hashtag " +
//					"ORDER BY count DESC " +
//					"LIMIT 50";
//
//			startTs = System.currentTimeMillis();
//			p3 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p3, 1, cellList);
//			p3.setObject(lastIndex + 1, Long.parseLong(cReq.getStart()));
//			p3.setObject(lastIndex + 2, Long.parseLong(cReq.getEnd()));
//			resultSet= p3.executeQuery();
//			endTs = System.currentTimeMillis();
//
//			logger.debug("Concept flows hashtags query done, time: {} ms",endTs - startTs);
//
//			next = resultSet.next();
//
//			while(next){
//
//				if(!resultSet.getString(1).equals("rt")){
//					node = new ConceptFlowsNode();
//
//					node.setId(resultSet.getString(1));
//					node.setLabel(resultSet.getString(1));
//					node.setGroup("hashtag");
//					node.setSentiment(0);
//
//					nodeList.add(node);
//				}
//
//				next = resultSet.next();
//
//			}
//
//			if(resultSet != null){
//				resultSet.close();
//			}
