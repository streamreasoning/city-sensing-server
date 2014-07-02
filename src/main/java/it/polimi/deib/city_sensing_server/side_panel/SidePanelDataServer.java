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
package it.polimi.deib.city_sensing_server.side_panel;

import it.polimi.deib.city_sensing_server.configuration.Config;
import it.polimi.deib.city_sensing_server.dataSource.DataSourceSingleton;

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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;


public class SidePanelDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(SidePanelDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();
		Connection connection = null;
		ResultSet resultSet = null;

		int lastIndex = 0;

		PreparedStatement p1 = null;
		PreparedStatement p2 = null;

		try {

			logger.debug("Side panel request received");
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

			SidePanelRequest spReq = gson.fromJson(reader, SidePanelRequest.class);

			ArrayList<Integer> cellList = new ArrayList<Integer>();
			String prepStmt = new String();

			if(spReq.getStart() == null || Long.parseLong(spReq.getStart()) < 0){
				spReq.setStart(Config.getInstance().getDefaultStart());
			}
			if(spReq.getEnd() == null || Long.parseLong(spReq.getEnd()) < 0){
				spReq.setEnd(Config.getInstance().getDefaultEnd());
			}
			if(spReq.getCells() == null || spReq.getCells().size() == 0){
				for(int i = 1 ; i < Config.getInstance().getDefaultNumberOfCells() ; i++){
					cellList.add(i);
					prepStmt = prepStmt + "?,";
				}
			} else {
				for(String s : spReq.getCells()){
					cellList.add(Integer.parseInt(s));
					prepStmt = prepStmt + "?,";
				}
			}

			prepStmt = prepStmt.substring(0, prepStmt.length() - 1);

			connection = DataSourceSingleton.getInstance().getConnection();
			connection.setAutoCommit(false);

			String sqlQuery = "SELECT ROUND(SUM(outgoing_call_number),0),ROUND(SUM(incoming_call_number),0),ROUND(SUM(outgoing_sms_number),0),ROUND(SUM(incoming_sms_number),0),ROUND(SUM(data_cdr_number),0) " +
					"FROM NEW_MYISAM_INF_ABOUT_SQUARE_BY_TS_2_tmp " +
					"WHERE square_ID IN (" + prepStmt + ") AND ts_ID >= ? AND ts_ID <= ? ";

			long startTs = System.currentTimeMillis();
			p1 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p1, 1, cellList);
			p1.setObject(lastIndex + 1, Long.parseLong(spReq.getStart()));
			p1.setObject(lastIndex + 2, Long.parseLong(spReq.getEnd()));
			resultSet = p1.executeQuery();
			long endTs = System.currentTimeMillis();

			logger.debug("Side panel telco informations query done, time: {} ms",endTs - startTs);

			boolean next = resultSet.next();

			SidePanelResponse response = new SidePanelResponse();
			SidePanelHashtag hashtag;

			while(next){
				response.setCalls_out(Double.parseDouble(resultSet.getString(1)));
				response.setCalls_in(Double.parseDouble(resultSet.getString(2)));
				response.setMessages_out(Double.parseDouble(resultSet.getString(3)));
				response.setMessages_in(Double.parseDouble(resultSet.getString(4)));
				response.setData_traffic(Double.parseDouble(resultSet.getString(5)));

				next = resultSet.next();

			}

			if(resultSet != null){
				resultSet.close();
			}

			sqlQuery = "SELECT DISTINCT hashtag,SUM(n_occurrences) AS total_occurrencies " +
					"FROM HASHTAG_SQUARE " +
					"WHERE ht_square_ID IN (" + prepStmt + ") AND ht_ts_ID >= ? AND ht_ts_ID <= ? " +
					"GROUP BY hashtag " +
					"ORDER BY total_occurrencies DESC " +
					"LIMIT 100";  

			startTs = System.currentTimeMillis();
			p2 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p2, 1, cellList);
			p2.setObject(lastIndex + 1, Long.parseLong(spReq.getStart()));
			p2.setObject(lastIndex + 2, Long.parseLong(spReq.getEnd()));
			resultSet = p2.executeQuery();
			endTs = System.currentTimeMillis();

			logger.debug("Side Panel hashtag query done, time: {} ms",endTs - startTs);

			ArrayList<SidePanelHashtag> hashtagList = new ArrayList<SidePanelHashtag>();

			next = resultSet.next();

			while(next){
				if(!resultSet.getString(1).equals("rt")){
					hashtag = new SidePanelHashtag();

					hashtag.setLabel(resultSet.getString(1));
					hashtag.setValue(Long.parseLong(resultSet.getString(2)));

					hashtagList.add(hashtag);
				}

				next = resultSet.next();

			}

			if(resultSet != null){
				resultSet.close();
			}

			response.setHashtags(hashtagList);

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(response);
			endTs = System.currentTimeMillis();

			logger.debug("Side panel json serialization done, time: {} ms",endTs - startTs);

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
