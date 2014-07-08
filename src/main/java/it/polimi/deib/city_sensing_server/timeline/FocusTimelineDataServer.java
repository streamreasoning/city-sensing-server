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
package it.polimi.deib.city_sensing_server.timeline;

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


public class FocusTimelineDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(FocusTimelineDataServer.class.getName());

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

			logger.debug("Focus timeline request received");
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

			FocusTimelineRequest tReq = gson.fromJson(reader, FocusTimelineRequest.class);

			ArrayList<Integer> cellList = new ArrayList<Integer>();
			String prepStmt = new String();
			String anomalyColumnName = new String();

			if(tReq.getStart() == null || Long.parseLong(tReq.getStart()) < 0){
				tReq.setStart(Config.getInstance().getDefaultStart());
			}
			if(tReq.getEnd() == null || Long.parseLong(tReq.getEnd()) < 0){
				tReq.setEnd(Config.getInstance().getDefaultEnd());
			}
			if(tReq.getCells() == null || tReq.getCells().size() == 0){
				for(int i = 1 ; i < Config.getInstance().getDefaultNumberOfCells() ; i++){
					cellList.add(i);
					prepStmt = prepStmt + "?,";
				}
			} else {
				for(String s : tReq.getCells()){
					cellList.add(Integer.parseInt(s));
					prepStmt = prepStmt + "?,";
				}
			}
			if(tReq.getAnomalyColumnName() == null){
				anomalyColumnName = "anomaly_index";
			} else {
				anomalyColumnName = tReq.getAnomalyColumnName();
			}
			
			prepStmt = prepStmt.substring(0, prepStmt.length() - 1);

			connection = DataSourceSingleton.getInstance().getConnection();
			connection.setAutoCommit(false);

			String sqlQuery = "SELECT ts_ID,AVG(" + anomalyColumnName + ") AS mobily_anomaly,SUM(n_tweets) AS social_activity, " +
					"(SUM(incoming_call_number) + SUM(outgoing_call_number) + SUM(incoming_sms_number) + SUM(outgoing_sms_number) + SUM(data_cdr_number)) AS mobily_activity, " +
					"((SUM(positive_tweets_number) * " + Config.getInstance().getSentimentPositiveCoefficient() + ") - (SUM(negative_tweets_number) * " + Config.getInstance().getSentimentNegativeCoefficient() + ") + (SUM(neutral_tweets_number)  * " + Config.getInstance().getSentimentNeutralCoefficient() + " )) AS social_sentiment , " + 
					"((SUM(positive_tweets_number) * " + Config.getInstance().getSentimentPositiveCoefficient() + ") + (SUM(negative_tweets_number) * " + Config.getInstance().getSentimentNegativeCoefficient() + ") + (SUM(neutral_tweets_number)  * " + Config.getInstance().getSentimentNeutralCoefficient() + " )) AS weightedSocialActivity " +					
					"FROM NEW_MYISAM_INF_ABOUT_SQUARE_BY_TS_2 " +
					"WHERE square_ID IN (" + prepStmt + ") AND ts_id >= ? AND ts_id <= ? " +
					"GROUP BY ts_ID";

			long startTs = System.currentTimeMillis();
			p1 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p1, 1, cellList);
			p1.setObject(lastIndex + 1, Long.parseLong(tReq.getStart()));
			p1.setObject(lastIndex + 2, Long.parseLong(tReq.getEnd()));
			resultSet = p1.executeQuery();
			long endTs = System.currentTimeMillis();

			logger.debug("Context timeline query done, time: {} ms",endTs - startTs);
			
			boolean next = resultSet.next();

			GeneralTimelineResponse response = new GeneralTimelineResponse();
			GeneralTimelineStep step;
			ArrayList<GeneralTimelineStep> stepList = new ArrayList<GeneralTimelineStep>();

			long tsInterval = 900000;

			while(next){

				step = new GeneralTimelineStep();

				step.setStart(Long.parseLong(resultSet.getString(1)));
				step.setEnd(Long.parseLong(resultSet.getString(1)) + tsInterval);
				step.setMobily_anomaly(Double.parseDouble(resultSet.getString(2)));
				step.setSocial_activity(Double.parseDouble(resultSet.getString(3)));
				step.setMobily_activity(Double.parseDouble(resultSet.getString(4)));
				double weightedSocialActivity = Double.parseDouble(resultSet.getString(6));
				if(weightedSocialActivity != 0)
					step.setSocial_sentiment((Double.parseDouble(resultSet.getString(5)) / weightedSocialActivity));
				else
					step.setSocial_sentiment(Double.parseDouble(resultSet.getString(5)));
//				if(step.getSocial_activity() != 0)
//					step.setSocial_sentiment((Double.parseDouble(resultSet.getString(5)) / step.getSocial_activity()));
//				else
//					step.setSocial_sentiment(Double.parseDouble(resultSet.getString(5)));
				
				stepList.add(step);

				next = resultSet.next();

			}

			response.setSteps(stepList);

			if(resultSet != null){
				resultSet.close();
			}

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(response);
			endTs = System.currentTimeMillis();

			logger.debug("Focus timeline json serialization done, time: {} ms",endTs - startTs);

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
