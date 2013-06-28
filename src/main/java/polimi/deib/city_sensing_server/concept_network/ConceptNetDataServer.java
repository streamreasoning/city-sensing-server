package polimi.deib.city_sensing_server.concept_network;

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


public class ConceptNetDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(ConceptNetDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		logger.debug("Concept network request received");
		Gson gson = new Gson();
		Connection connection = null;
		PreparedStatement p1 = null;
		PreparedStatement p2 = null;
		ResultSet resultSet = null;
		int lastIndex = 0;

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

			ArrayList<Integer> cellList = new ArrayList<Integer>();
			String prepStmt = new String();

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
				for(int i = 1 ; i < 9999 ; i++){
					cellList.add(i);
					prepStmt = prepStmt + "?,";
				}
			} else {
				for(String s : cnReq.getCells()){
					cellList.add(Integer.parseInt(s));
					prepStmt = prepStmt + "?,";
				}
			}

			prepStmt = prepStmt.substring(0, prepStmt.length() - 1);

			ConceptNetResponse response = new ConceptNetResponse();
			ConceptNetNode node;
			ArrayList<ConceptNetNode> nodeList = new ArrayList<ConceptNetNode>();
			ConceptNetLink link;
			ArrayList<ConceptNetLink> linkList = new ArrayList<ConceptNetLink>();

			String sqlQuery = "SELECT DISTINCT hashtag " +
					"FROM HASHTAG_SQUARE " +
					"WHERE ht_square_ID in (" + prepStmt + ") AND ht_ts_ID >=? AND ht_ts_ID <=? " +
					"GROUP BY hashtag";

//			Class.forName("com.mysql.jdbc.Driver");
//
//			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());

			connection = Rest_Server.bds.getConnection();
			connection.setAutoCommit(false);

			p1 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p1, 1, cellList);
			p1.setObject(lastIndex + 1, Long.parseLong(cnReq.getStart()));
			p1.setObject(lastIndex + 2, Long.parseLong(cnReq.getEnd()));
			resultSet= p1.executeQuery();

			resultSet= p1.executeQuery();

			boolean next = resultSet.next();

			while(next){
				node = new ConceptNetNode();

				node.setId(resultSet.getString(1));
				node.setLabel(resultSet.getString(1));
				node.setGroup("keyword");

				nodeList.add(node);

				next = resultSet.next();

			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}

			sqlQuery = "SELECT DISTINCT HT1.hashtag,HT2.hashtag,COUNT(*) as count " +
					"FROM HASHTAG_SQUARE as HT1, HASHTAG_SQUARE as HT2 " +
					"WHERE HT1.ht_square_ID in  (" + prepStmt + ") AND HT2.ht_square_ID in  (" + prepStmt + ") AND " +
					"HT1.ht_ts_ID >= ? AND HT1.ht_ts_ID <= ? AND " +
					"HT2.ht_ts_ID >= ? AND HT2.ht_ts_ID <= ? AND " +
					"HT1.hashtag != HT2.hashtag " +
					"GROUP BY HT1.hashtag,HT2.hashtag " +
					"HAVING(count > ?) " +
					"ORDER BY count";

			p2 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p2, 1, cellList);
			lastIndex = insertValues(p2, lastIndex + 1, cellList);
			p2.setObject(lastIndex + 1, Long.parseLong(cnReq.getStart()));
			p2.setObject(lastIndex + 2, Long.parseLong(cnReq.getEnd()));
			p2.setObject(lastIndex + 3, Long.parseLong(cnReq.getStart()));
			p2.setObject(lastIndex + 4, Long.parseLong(cnReq.getEnd()));
			p2.setObject(lastIndex + 5, Long.parseLong(cnReq.getThreshold()));
			resultSet= p2.executeQuery();

			next = resultSet.next();

			while(next){
				link = new ConceptNetLink();

				link.setSource(resultSet.getString(1));
				link.setTarget(resultSet.getString(2));
				link.setValue(Double.parseDouble(resultSet.getString(3)));

				linkList.add(link);

				next = resultSet.next();

			}

			response.setNodes(nodeList);
			response.setLinks(linkList);

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
