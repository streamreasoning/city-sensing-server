/*******************************************************************************
 * Copyright 2014 DEIB - Politecnico di Milano
 *    
 * Marco Balduini (marco.balduini@polimi.it)
 * Emanuele Della Valle (emanuele.dellavalle@polimi.it)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package it.polimi.deib.city_sensing_server.concept_network;

import it.polimi.deib.city_sensing_server.configuration.Config;
import it.polimi.deib.city_sensing_server.dataSource.DataSourceSingleton;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;

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
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;


public class ConceptNetDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(ConceptNetDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();
		Connection connection = null;
		PreparedStatement p1 = null;
		//		PreparedStatement p2 = null;
		ResultSet resultSet = null;
		int lastIndex = 0;

		try {

			logger.debug("Concept network request received");
			String parameters = rep.getText();
			logger.debug("parameters: {}",parameters);

			Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
			if (responseHeaders == null) {
				responseHeaders = new Series(Header.class);
				getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
			}
			responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));

			JsonReader reader = null;

			reader = new JsonReader(new StringReader(parameters));

			ConceptNetRequest cnReq = gson.fromJson(reader, ConceptNetRequest.class);

			ArrayList<Integer> cellList = new ArrayList<Integer>();
			String prepStmt = new String();

			if(cnReq.getStart() == null || Long.parseLong(cnReq.getStart()) < 0){
				cnReq.setStart(Config.getInstance().getDefaultStart());
			}
			if(cnReq.getEnd() == null || Long.parseLong(cnReq.getEnd()) < 0){
				cnReq.setEnd(Config.getInstance().getDefaultEnd());
			}
			if(cnReq.getThreshold() == null || Integer.parseInt(cnReq.getThreshold()) < 0){
				cnReq.setThreshold(Config.getInstance().getConceptNetDefaultThreshold());
			}
			if(cnReq.getCells() == null || cnReq.getCells().size() == 0){
				for(int i = 1 ; i < Config.getInstance().getDefaultNumberOfCells() ; i++){
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
			ConceptNetExtendedNode extNode;
			ArrayList<ConceptNetNode> nodeList = new ArrayList<ConceptNetNode>();
			ArrayList<ConceptNetExtendedNode> extendedNodeList = new ArrayList<ConceptNetExtendedNode>();

			ConceptNetLink link;
			ArrayList<ConceptNetLink> linkList = new ArrayList<ConceptNetLink>();

			String sqlQuery = "SELECT hashtag, SUM(n_occurrences) as count " +
					"FROM HASHTAG_SQUARE " +
					"WHERE ht_square_ID in (" + prepStmt + ") AND ht_ts_ID >=? AND ht_ts_ID <=? " +
					"GROUP BY hashtag " +
					"HAVING(count >= ?) " +
					"ORDER BY count DESC " +
					"LIMIT 100";

			connection = DataSourceSingleton.getInstance().getConnection();
			connection.setAutoCommit(false);

			long startTs = System.currentTimeMillis();
			p1 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p1, 1, cellList);
			p1.setObject(lastIndex + 1, Long.parseLong(cnReq.getStart()));
			p1.setObject(lastIndex + 2, Long.parseLong(cnReq.getEnd()));
			p1.setObject(lastIndex + 3, Long.parseLong(cnReq.getThreshold()));
			resultSet = p1.executeQuery();
			long endTs = System.currentTimeMillis();

			logger.debug("Concept network hashtag query done, time: {} ms",endTs - startTs);

			boolean next = resultSet.next();

			while(next){
				if(!resultSet.getString(1).equals("rt")){
					node = new ConceptNetNode();
					extNode = new ConceptNetExtendedNode();

					node.setId(resultSet.getString(1));
					extNode.setId(resultSet.getString(1));
					node.setLabel(resultSet.getString(1));
					extNode.setLabel(resultSet.getString(1));
					node.setGroup("keyword");
					extNode.setCount(Integer.parseInt(resultSet.getString(2)));

					extendedNodeList.add(extNode);
					nodeList.add(node);
				}

				next = resultSet.next();

			}

			if(resultSet != null){
				resultSet.close();
			}

			for(ConceptNetExtendedNode externalNode : extendedNodeList){
				for(ConceptNetExtendedNode internalNode : extendedNodeList){
					if(!externalNode.getId().equals(internalNode.getId())){

						link = new ConceptNetLink();

						link.setSource(externalNode.getId());
						link.setTarget(internalNode.getId());
						link.setValue(externalNode.getCount() + internalNode.getCount());

						if(!linkList.contains(link))
							linkList.add(link);						
					}

				}

			}

			Collections.sort(linkList);

			//Cut number of link
			int linkNumberLimit = 1000;
			if(linkList.size() > linkNumberLimit){
				ArrayList<ConceptNetLink> tempSubList = new ArrayList<ConceptNetLink>();
				tempSubList.addAll(linkList.subList(linkNumberLimit + 1, linkList.size()));
				linkList.removeAll(tempSubList);
			}

			//			sqlQuery = "SELECT DISTINCT HT1.hashtag,HT2.hashtag,COUNT(*) as count " +
			//					"FROM HASHTAG_SQUARE as HT1, HASHTAG_SQUARE as HT2 " +
			//					"WHERE HT1.ht_square_ID in  (" + prepStmt + ") AND HT2.ht_square_ID in  (" + prepStmt + ") AND " +
			//					"HT1.ht_ts_ID >= ? AND HT1.ht_ts_ID <= ? AND " +
			//					"HT2.ht_ts_ID >= ? AND HT2.ht_ts_ID <= ? AND " +
			//					"HT1.hashtag != HT2.hashtag " +
			//					"GROUP BY HT1.hashtag,HT2.hashtag " +
			//					"HAVING(count >= ?) " +
			//					"ORDER BY count DESC";
			//
			//			p2 = connection.prepareStatement(sqlQuery);
			//			lastIndex = insertValues(p2, 1, cellList);
			//			lastIndex = insertValues(p2, lastIndex + 1, cellList);
			//			p2.setObject(lastIndex + 1, Long.parseLong(cnReq.getStart()));
			//			p2.setObject(lastIndex + 2, Long.parseLong(cnReq.getEnd()));
			//			p2.setObject(lastIndex + 3, Long.parseLong(cnReq.getStart()));
			//			p2.setObject(lastIndex + 4, Long.parseLong(cnReq.getEnd()));
			//			p2.setObject(lastIndex + 5, Long.parseLong(cnReq.getThreshold()));
			//			resultSet= p2.executeQuery();
			//
			//			next = resultSet.next();
			//
			//			while(next){
			//				link = new ConceptNetLink();
			//
			//				link.setSource(resultSet.getString(1));
			//				link.setTarget(resultSet.getString(2));
			//				link.setValue(Double.parseDouble(resultSet.getString(3)));
			//
			//				linkList.add(link);
			//
			//				next = resultSet.next();
			//
			//			}

			response.setNodes(nodeList);
			response.setLinks(linkList);

			if(resultSet != null){
				resultSet.close();
			}

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(response);
			endTs = System.currentTimeMillis();

			logger.debug("Concept network json serialization done, time: {} ms",endTs - startTs);

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
				//				if(p2 != null && !p2.isClosed()){ p2.close();}
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
