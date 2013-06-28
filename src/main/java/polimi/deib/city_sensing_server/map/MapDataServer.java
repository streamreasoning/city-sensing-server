package polimi.deib.city_sensing_server.map;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

import polimi.deib.city_sensing_server.Rest_Server;
import polimi.deib.city_sesning_server.utilities.ResponseMapping;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;


public class MapDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(MapDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		logger.debug("Map request received");
		Gson gson = new Gson();
		Connection connection = null;
		ResultSet resultSet = null;
		HashMap<Integer, ResponseMapping> respMap = new HashMap<Integer, ResponseMapping>();
		int lastIndex = 0;

		PreparedStatement p1 = null;
		PreparedStatement p2 = null;
		PreparedStatement p3 = null;
		PreparedStatement p4 = null;
		PreparedStatement p5 = null;
		PreparedStatement p6 = null;
		PreparedStatement p7 = null;
		PreparedStatement p8 = null;
		PreparedStatement p9 = null;

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

//			Class.forName("com.mysql.jdbc.Driver");
//
//			connection = DriverManager.getConnection("jdbc:mysql://" + Config.getInstance().getMysqlAddress() + "/" + Config.getInstance().getMysqldbname() + "?user=" + Config.getInstance().getMysqlUser() + "&password=" + Config.getInstance().getMysqlPwd());
			connection = Rest_Server.bds.getConnection();
			connection.setAutoCommit(false);

			ResponseMapping rm;
			String key;

//			String sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_call_in WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? GROUP BY square_id ";
//
//			p1 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p1, 1, cellList);
//			p1.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
//			p1.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
//			resultSet = p1.executeQuery();
//
//			boolean next = resultSet.next();
//
//			while(next) {
//
//				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
//					rm = new ResponseMapping();
//					key = resultSet.getString(1);
//					rm.setCell_id(Long.parseLong(resultSet.getString(1)));
//					rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
//					if(resultSet.getString(3) != null)
//						rm.setCall_in(Double.parseDouble(resultSet.getString(3)));
//					else
//						rm.setCall_in(0);
//
//					respMap.put(key.hashCode(), rm);
//				}
//				next = resultSet.next();
//			}
//
//			if(resultSet != null && !resultSet.isClosed()){
//				resultSet.close();
//			}
//
//			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_call_out WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? GROUP BY square_id ";
//
//			p2 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p2, 1, cellList);
//			p2.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
//			p2.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
//			resultSet = p2.executeQuery();
//
//			next = resultSet.next();
//
//			while(next) {
//
//				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
//					key = resultSet.getString(1);
//					if(respMap.containsKey(key.hashCode())){
//						rm = respMap.get(key.hashCode());
//						if(resultSet.getString(3) != null)
//							rm.setCall_out(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setCall_out(0);
//					} else {
//						rm = new ResponseMapping();
//						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
//						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
//						if(resultSet.getString(3) != null)
//							rm.setCall_out(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setCall_out(0);
//					}
//					respMap.put(key.hashCode(), rm);
//				}
//				next = resultSet.next();
//			}
//
//			if(resultSet != null && !resultSet.isClosed()){
//				resultSet.close();
//			}
//
//			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_internet WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? GROUP BY square_id ";
//
//			p3 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p3, 1, cellList);
//			p3.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
//			p3.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
//			resultSet = p3.executeQuery();
//
//			next = resultSet.next();
//
//			while(next) {
//
//				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
//					key = resultSet.getString(1);
//					if(respMap.containsKey(key.hashCode())){
//						rm = respMap.get(key.hashCode());
//						if(resultSet.getString(3) != null)
//							rm.setInternet(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setInternet(0);
//					} else {
//						rm = new ResponseMapping();
//						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
//						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
//						if(resultSet.getString(3) != null)
//							rm.setInternet(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setInternet(0);
//					}
//					respMap.put(key.hashCode(), rm);
//				}
//				next = resultSet.next();
//			}
//
//			if(resultSet != null && !resultSet.isClosed()){
//				resultSet.close();
//			}
//
//			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_rec_in WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? GROUP BY square_id ";
//
//			p4 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p4, 1, cellList);
//			p4.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
//			p4.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
//			resultSet = p4.executeQuery();
//
//			next = resultSet.next();
//
//			while(next) {
//
//				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
//					key = resultSet.getString(1);
//					if(respMap.containsKey(key.hashCode())){
//						rm = respMap.get(key.hashCode());
//						if(resultSet.getString(3) != null)
//							rm.setRec_in(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setRec_in(0);
//					} else {
//						rm = new ResponseMapping();
//						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
//						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
//						if(resultSet.getString(3) != null)
//							rm.setRec_in(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setRec_in(0);
//					}
//					respMap.put(key.hashCode(), rm);
//				}
//				next = resultSet.next();
//			}
//
//			if(resultSet != null && !resultSet.isClosed()){
//				resultSet.close();
//			}
//			
//			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_rec_out WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? GROUP BY square_id ";
//
//			p5 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p5, 1, cellList);
//			p5.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
//			p5.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
//			resultSet = p5.executeQuery();
//
//			next = resultSet.next();
//
//			while(next) {
//
//				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
//					key = resultSet.getString(1);
//					if(respMap.containsKey(key.hashCode())){
//						rm = respMap.get(key.hashCode());
//						if(resultSet.getString(3) != null)
//							rm.setRec_out(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setRec_out(0);
//					} else {
//						rm = new ResponseMapping();
//						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
//						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
//						if(resultSet.getString(3) != null)
//							rm.setRec_out(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setRec_out(0);
//					}
//					respMap.put(key.hashCode(), rm);
//				}
//				next = resultSet.next();
//			}
//
//			if(resultSet != null && !resultSet.isClosed()){
//				resultSet.close();
//			}
//
//			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_sms_in WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? GROUP BY square_id ";
//
//			p6 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p6, 1, cellList);
//			p6.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
//			p6.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
//			resultSet = p6.executeQuery();
//
//			next = resultSet.next();
//
//			while(next) {
//
//				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
//					key = resultSet.getString(1);
//					if(respMap.containsKey(key.hashCode())){
//						rm = respMap.get(key.hashCode());
//						if(resultSet.getString(3) != null)
//							rm.setSms_in(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setSms_in(0);
//					} else {
//						rm = new ResponseMapping();
//						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
//						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
//						if(resultSet.getString(3) != null)
//							rm.setSms_in(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setSms_in(0);
//					}
//					respMap.put(key.hashCode(), rm);
//				}
//				next = resultSet.next();
//			}
//
//			if(resultSet != null && !resultSet.isClosed()){
//				resultSet.close();
//			}
//
//			sqlQuery = "SELECT square_id,quarter,SUM(count) FROM skil_sms_out WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? GROUP BY square_id ";
//
//			p7 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p7, 1, cellList);
//			p7.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
//			p7.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
//			resultSet = p7.executeQuery();
//
//			next = resultSet.next();
//
//			while(next) {
//
//				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
//					key = resultSet.getString(1);
//					if(respMap.containsKey(key.hashCode())){
//						rm = respMap.get(key.hashCode());
//						if(resultSet.getString(3) != null)
//							rm.setSms_out(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setSms_out(0);
//					} else {
//						rm = new ResponseMapping();
//						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
//						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
//						if(resultSet.getString(3) != null)
//							rm.setSms_out(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setSms_out(0);
//					}
//					respMap.put(key.hashCode(), rm);
//				}
//				next = resultSet.next();
//			}
//
//			if(resultSet != null && !resultSet.isClosed()){
//				resultSet.close();
//			}
//			
//			sqlQuery = "SELECT square_id,time_slot,AVG(anomaly_index) FROM anomaly WHERE square_id IN (" + prepStmt + ") AND time_slot >= ? AND time_slot <= ? GROUP BY square_id ";
//
//			p8 = connection.prepareStatement(sqlQuery);
//			lastIndex = insertValues(p8, 1, cellList);
//			p8.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
//			p8.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
//			resultSet = p8.executeQuery();
//
//			next = resultSet.next();
//
//			while(next) {
//
//				if(resultSet.getString(1) != null && resultSet.getString(2) != null){
//					key = resultSet.getString(1);
//					if(respMap.containsKey(key.hashCode())){
//						rm = respMap.get(key.hashCode());
//						if(resultSet.getString(3) != null)
//							rm.setAnomaly(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setAnomaly(0);
//					} else {
//						rm = new ResponseMapping();
//						rm.setCell_id(Long.parseLong(resultSet.getString(1)));
//						rm.setStart_ts(Long.parseLong(resultSet.getString(2)));
//						if(resultSet.getString(3) != null)
//							rm.setAnomaly(Double.parseDouble(resultSet.getString(3)));
//						else
//							rm.setAnomaly(0);
//					}
//					respMap.put(key.hashCode(), rm);
//				}
//				next = resultSet.next();
//			}
//
//			if(resultSet != null && !resultSet.isClosed()){
//				resultSet.close();
//			}

			String sqlQuery = "SELECT square_id, SUM(n_tweets) AS social_activity, (SUM(positive_tweets_number) - SUM(negative_tweets_number)) AS social_sentiment, ts_ID " +
					"FROM INFO_ABOUT_SQUARE_BY_TS " +
					"WHERE square_ID IN (" + prepStmt + ") AND ts_ID > ? AND ts_ID < ? " +
					"GROUP BY square_ID";

			p9 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p9, 1, cellList);
			p9.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
			p9.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
			resultSet = p9.executeQuery();

			boolean next = resultSet.next();

			MapResponse response = new MapResponse();
			MapCell mapCell;
			ArrayList<MapCell> mapCellList = new ArrayList<MapCell>();

			while(next){
				mapCell = new MapCell();
				
				mapCell.setId(Long.parseLong(resultSet.getString(1)));
				mapCell.setMobily_activity(0);
				mapCell.setMobily_anomaly(0);
				mapCell.setSocial_activity(Double.parseDouble(resultSet.getString(2)));
				mapCell.setSocial_sentiment(Double.parseDouble(resultSet.getString(3)));

				mapCellList.add(mapCell);
				
//				key = resultSet.getString(1);
//
//				rm = respMap.get(key.hashCode());
//
//				respMap.put(key.hashCode(), new ResponseMapping(Long.parseLong(resultSet.getString(1)), Long.parseLong(resultSet.getString(4)),0d,0d,0d,0d,0d,0d,0d,0d,Double.parseDouble(resultSet.getString(2)),Double.parseDouble(resultSet.getString(3))));
//
//				if(rm != null){
//
//					mapCell.setId(Long.parseLong(resultSet.getString(1)));
//					mapCell.setMobily_activity(rm.getCall_in() + rm.getCall_out() + rm.getInternet() + rm.getRec_in() + rm.getRec_out() + rm.getSms_in() + rm.getSms_out());
//					mapCell.setMobily_anomaly(rm.getAnomaly());
//					mapCell.setSocial_activity(Double.parseDouble(resultSet.getString(2)));
//					mapCell.setSocial_sentiment(Double.parseDouble(resultSet.getString(3)));
//
//					mapCellList.add(mapCell);
//					respMap.remove(key.hashCode());
//				}
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
				if(p3 != null && !p3.isClosed()){ p3.close();}
				if(p4 != null && !p4.isClosed()){ p4.close();}
				if(p5 != null && !p5.isClosed()){ p5.close();}
				if(p6 != null && !p6.isClosed()){ p6.close();}
				if(p7 != null && !p7.isClosed()){ p7.close();}
				if(p8 != null && !p8.isClosed()){ p8.close();}
				if(p9 != null && !p9.isClosed()){ p9.close();}
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
