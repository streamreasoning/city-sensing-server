package polimi.deib.city_sensing_server.map;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

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
import polimi.deib.city_sesning_server.utilities.ResponseMapping;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.MalformedJsonException;


public class MapDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(MapDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		logger.debug("Map request received");
		Gson gson = new Gson();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		HashMap<Integer, ResponseMapping> respMap = new HashMap<Integer, ResponseMapping>();

		Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
		if (responseHeaders == null) {
			responseHeaders = new Series(Header.class);
			getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
		}
		responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));

		try {

			JsonReader reader = null;

			reader = new JsonReader(new StringReader(rep.getText()));

			MapRequest mReq = gson.fromJson(reader, MapRequest.class);

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

			Class.forName("com.mysql.jdbc.Driver");

			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());

			ResponseMapping rm;
			String key;

			String sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_call_in WHERE square_id IN (" + cellList + ") AND quarter >= " + mReq.getStart() + " AND quarter <= " + mReq.getEnd() + " GROUP BY square_id ";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			boolean next = resultSet.next();

			while(next) {

				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
					rm = new ResponseMapping();
					key = resultSet.getString(1);
					rm.setCell_id(Long.parseLong(resultSet.getString(1)));
					rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
					if(resultSet.getString(3) != null)
						rm.setCall_in(Double.parseDouble(resultSet.getString(3)));
					else
						rm.setCall_in(0);

					respMap.put(key.hashCode(), rm);
				}
				next = resultSet.next();
			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_call_out WHERE square_id IN (" + cellList + ") AND quarter >= " + mReq.getStart() + " AND quarter <= " + mReq.getEnd() + " GROUP BY square_id ";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next) {

				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
					key = resultSet.getString(1);
					if(respMap.containsKey(key.hashCode())){
						rm = respMap.get(key.hashCode());
						if(resultSet.getString(3) != null)
							rm.setCall_out(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setCall_out(0);
					} else {
						rm = new ResponseMapping();
						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
						if(resultSet.getString(3) != null)
							rm.setCall_out(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setCall_out(0);
					}
					respMap.put(key.hashCode(), rm);
				}
				next = resultSet.next();
			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_internet WHERE square_id IN (" + cellList + ") AND quarter >= " + mReq.getStart() + " AND quarter <= " + mReq.getEnd() + " GROUP BY square_id ";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next) {

				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
					key = resultSet.getString(1);
					if(respMap.containsKey(key.hashCode())){
						rm = respMap.get(key.hashCode());
						if(resultSet.getString(3) != null)
							rm.setInternet(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setInternet(0);
					} else {
						rm = new ResponseMapping();
						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
						if(resultSet.getString(3) != null)
							rm.setInternet(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setInternet(0);
					}
					respMap.put(key.hashCode(), rm);
				}
				next = resultSet.next();
			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_rec_in WHERE square_id IN (" + cellList + ") AND quarter >= " + mReq.getStart() + " AND quarter <= " + mReq.getEnd() + " GROUP BY square_id ";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next) {

				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
					key = resultSet.getString(1);
					if(respMap.containsKey(key.hashCode())){
						rm = respMap.get(key.hashCode());
						if(resultSet.getString(3) != null)
							rm.setRec_in(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setRec_in(0);
					} else {
						rm = new ResponseMapping();
						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
						if(resultSet.getString(3) != null)
							rm.setRec_in(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setRec_in(0);
					}
					respMap.put(key.hashCode(), rm);
				}
				next = resultSet.next();
			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_rec_out WHERE square_id IN (" + cellList + ") AND quarter >= " + mReq.getStart() + " AND quarter <= " + mReq.getEnd() + " GROUP BY square_id ";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next) {

				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
					key = resultSet.getString(1);
					if(respMap.containsKey(key.hashCode())){
						rm = respMap.get(key.hashCode());
						if(resultSet.getString(3) != null)
							rm.setRec_out(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setRec_out(0);
					} else {
						rm = new ResponseMapping();
						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
						if(resultSet.getString(3) != null)
							rm.setRec_out(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setRec_out(0);
					}
					respMap.put(key.hashCode(), rm);
				}
				next = resultSet.next();
			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_sms_in WHERE square_id IN (" + cellList + ") AND quarter >= " + mReq.getStart() + " AND quarter <= " + mReq.getEnd() + " GROUP BY square_id ";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next) {

				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
					key = resultSet.getString(1);
					if(respMap.containsKey(key.hashCode())){
						rm = respMap.get(key.hashCode());
						if(resultSet.getString(3) != null)
							rm.setSms_in(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setSms_in(0);
					} else {
						rm = new ResponseMapping();
						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
						if(resultSet.getString(3) != null)
							rm.setSms_in(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setSms_in(0);
					}
					respMap.put(key.hashCode(), rm);
				}
				next = resultSet.next();
			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_sms_out WHERE square_id IN (" + cellList + ") AND quarter >= " + mReq.getStart() + " AND quarter <= " + mReq.getEnd() + " GROUP BY square_id ";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next) {

				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
					key = resultSet.getString(1);
					if(respMap.containsKey(key.hashCode())){
						rm = respMap.get(key.hashCode());
						if(resultSet.getString(3) != null)
							rm.setSms_out(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setSms_out(0);
					} else {
						rm = new ResponseMapping();
						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
						if(resultSet.getString(3) != null)
							rm.setSms_out(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setSms_out(0);
					}
					respMap.put(key.hashCode(), rm);
				}
				next = resultSet.next();
			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT square_id,time_slot,AVG(anomaly_index) FROM anomaly WHERE square_id IN (" + cellList + ") AND time_slot >= " + mReq.getStart() + " AND time_slot <= " + mReq.getEnd() + " GROUP BY square_id ";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			while(next) {

				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
					key = resultSet.getString(1);
					if(respMap.containsKey(key.hashCode())){
						rm = respMap.get(key.hashCode());
						if(resultSet.getString(3) != null)
							rm.setAnomaly(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setAnomaly(0);
					} else {
						rm = new ResponseMapping();
						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
						if(resultSet.getString(3) != null)
							rm.setAnomaly(Double.parseDouble(resultSet.getString(3)));
						else
							rm.setAnomaly(0);
					}
					respMap.put(key.hashCode(), rm);
				}
				next = resultSet.next();
			}

			if(resultSet != null && !resultSet.isClosed()){
				resultSet.close();
			}
			if(statement != null && !statement.isClosed()){
				statement.close();
			}

			sqlQuery = "SELECT square_id, SUM(n_tweets) AS social_activity, (SUM(positive_tweets_number) - SUM(negative_tweets_number)) AS social_sentiment, ts_ID " +
					"FROM INFO_ABOUT_SQUARE_BY_TS " +
					"WHERE square_ID IN (" + cellList + ") AND ts_ID > " + mReq.getStart() + " AND ts_ID < " + mReq.getEnd() + " " +
					"GROUP BY square_ID";

			statement = connection.createStatement();

			resultSet = statement.executeQuery(sqlQuery);

			next = resultSet.next();

			MapResponse response = new MapResponse();
			MapCell mapCell;
			ArrayList<MapCell> mapCellList = new ArrayList<MapCell>();


			while(next){
				mapCell = new MapCell();
				key = resultSet.getString(1);

				rm = respMap.get(key.hashCode());
				
				respMap.put(key.hashCode(), new ResponseMapping(Long.parseLong(resultSet.getString(1)), Long.parseLong(resultSet.getString(4)),0d,0d,0d,0d,0d,0d,0d,0d,Double.parseDouble(resultSet.getString(2)),Double.parseDouble(resultSet.getString(3))));

				if(rm != null){

					mapCell.setId(Long.parseLong(resultSet.getString(1)));
					mapCell.setMobily_activity(rm.getCall_in() + rm.getCall_out() + rm.getInternet() + rm.getRec_in() + rm.getRec_out() + rm.getSms_in() + rm.getSms_out());
					mapCell.setMobily_anomaly(rm.getAnomaly());
					mapCell.setSocial_activity(Double.parseDouble(resultSet.getString(2)));
					mapCell.setSocial_sentiment(Double.parseDouble(resultSet.getString(3)));

					mapCellList.add(mapCell);
					respMap.remove(key.hashCode());
				}
				next = resultSet.next();

			}

			Set<Integer> keyset = respMap.keySet();

			for(int k : keyset){
				rm = respMap.get(k);
				mapCell = new MapCell();
				mapCell.setId(rm.getCell_id());
				mapCell.setMobily_activity(rm.getCall_in() + rm.getCall_out() + rm.getInternet() + rm.getRec_in() + rm.getRec_out() + rm.getSms_in() + rm.getSms_out());
				mapCell.setMobily_anomaly(rm.getAnomaly());
				mapCell.setSocial_activity(rm.getSocial_activity());
				mapCell.setSocial_sentiment(rm.getSocial_sentiment());

				mapCellList.add(mapCell);
			}

			//			sqlQuery = "SELECT square_id,(SUM(incoming_call_number) + SUM(outcoming_call_number) + SUM(incoming_sms_number) + SUM(outcoming_call_number) + SUM(data_cdr_number)) AS mobily_activity, " +
			//					"COUNT(anomaly_index) AS mobily_anomaly , SUM(n_tweets) AS social_activity, (SUM(positive_tweets_number) - SUM(negative_tweets_number)) AS social_sentiment " +
			//					"FROM INFO_ABOUT_SQUARE_BY_TS " +
			//					"WHERE square_ID IN (" + cellList + ") AND ts_ID > " + mReq.getStart() + " AND ts_ID < " + mReq.getEnd() + " " +
			//					"GROUP BY square_ID";
			//
			//			statement = connection.createStatement();
			//
			//			resultSet = statement.executeQuery(sqlQuery);
			//
			//			next = resultSet.next();
			//
			//			MapResponse response = new MapResponse();
			//			MapCell mapCell;
			//			ArrayList<MapCell> mapCellList = new ArrayList<MapCell>();
			//
			//
			//			while(next){
			//				mapCell = new MapCell();
			//
			//				mapCell.setId(Long.parseLong(resultSet.getString(1)));
			//				mapCell.setMobily_activity(Double.parseDouble(resultSet.getString(2)));
			//				mapCell.setMobily_anomaly(Double.parseDouble(resultSet.getString(3)));
			//				mapCell.setSocial_activity(Double.parseDouble(resultSet.getString(4)));
			//				mapCell.setSocial_sentiment(Double.parseDouble(resultSet.getString(5)));
			//
			//				mapCellList.add(mapCell);
			//
			//				next = resultSet.next();
			//
			//			}

			response.setCells(mapCellList);
			
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
			logger.error("Error while getting jdbc Driver Class", e);
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, "Error while getting jdbc Driver Class");
			this.getResponse().setEntity(gson.toJson("Error while getting jdbc Driver Class"), MediaType.APPLICATION_JSON);
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
