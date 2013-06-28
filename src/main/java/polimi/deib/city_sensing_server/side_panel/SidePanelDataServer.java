package polimi.deib.city_sensing_server.side_panel;

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


public class SidePanelDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(SidePanelDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		logger.debug("Side panel request received");
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

		Series<Header> responseHeaders = (Series<Header>) getResponse().getAttributes().get("org.restlet.http.headers");
		if (responseHeaders == null) {
			responseHeaders = new Series(Header.class);
			getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
		}
		responseHeaders.add(new Header("Access-Control-Allow-Origin", "*"));

		try {

			JsonReader reader = null;

			reader = new JsonReader(new StringReader(rep.getText()));

			SidePanelRequest spReq = gson.fromJson(reader, SidePanelRequest.class);

			ArrayList<Integer> cellList = new ArrayList<Integer>();
			String prepStmt = new String();
			
			if(spReq.getStart() == null || Long.parseLong(spReq.getStart()) < 0){
				spReq.setStart("1365199200000");
			}
			if(spReq.getEnd() == null || Long.parseLong(spReq.getEnd()) < 0){
				spReq.setEnd("1366927200000");
			}
			if(spReq.getCells() == null || spReq.getCells().size() == 0){
				for(int i = 1 ; i < 9999 ; i++){
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
//			p1.setObject(lastIndex + 1, Long.parseLong(spReq.getStart()));
//			p1.setObject(lastIndex + 2, Long.parseLong(spReq.getEnd()));
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
//			p2.setObject(lastIndex + 1, Long.parseLong(spReq.getStart()));
//			p2.setObject(lastIndex + 2, Long.parseLong(spReq.getEnd()));
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
//			p3.setObject(lastIndex + 1, Long.parseLong(spReq.getStart()));
//			p3.setObject(lastIndex + 2, Long.parseLong(spReq.getEnd()));
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
//			p4.setObject(lastIndex + 1, Long.parseLong(spReq.getStart()));
//			p4.setObject(lastIndex + 2, Long.parseLong(spReq.getEnd()));
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
//			p5.setObject(lastIndex + 1, Long.parseLong(spReq.getStart()));
//			p5.setObject(lastIndex + 2, Long.parseLong(spReq.getEnd()));
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
//			p6.setObject(lastIndex + 1, Long.parseLong(spReq.getStart()));
//			p6.setObject(lastIndex + 2, Long.parseLong(spReq.getEnd()));
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
//			p7.setObject(lastIndex + 1, Long.parseLong(spReq.getStart()));
//			p7.setObject(lastIndex + 2, Long.parseLong(spReq.getEnd()));
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
			
			String sqlQuery = "SELECT DISTINCT hashtag,n_occurrences " +
					"FROM HASHTAG_SQUARE " +
					"WHERE ht_square_ID IN (" + prepStmt + ") AND ht_ts_ID >= ? AND ht_ts_ID <= ? " +
					"ORDER BY n_occurrences DESC ";

			p8 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p8, 1, cellList);
			p8.setObject(lastIndex + 1, Long.parseLong(spReq.getStart()));
			p8.setObject(lastIndex + 2, Long.parseLong(spReq.getEnd()));
			resultSet = p8.executeQuery();

			boolean next = resultSet.next();

			SidePanelResponse response = new SidePanelResponse();
			SidePanelHashtag hashtag;
			ArrayList<SidePanelHashtag> hashtagList = new ArrayList<SidePanelHashtag>();

			while(next){
				hashtag = new SidePanelHashtag();

				hashtag.setLabel(resultSet.getString(1));
				hashtag.setValue(Long.parseLong(resultSet.getString(2)));

				hashtagList.add(hashtag);

				next = resultSet.next();

			}

//			Set<Integer> keyset = respMap.keySet();
//
//			for(int k : keyset){
//				rm = respMap.get(k);
//				response.setCalls_out(response.getCalls_out() + rm.getCall_out() + rm.getRec_out());
//				response.setCalls_in(response.getCalls_in() + rm.getCall_in() + rm.getRec_in());
//				response.setMessages_out(response.getMessages_out() + rm.getSms_out());
//				response.setMessages_in(response.getMessages_in() + rm.getSms_in());
//				response.setData_traffic(response.getData_traffic() + rm.getInternet());
//				response.setHashtags(hashtagList);
//			}
			
			response.setCalls_out(0);
			response.setCalls_in(0);
			response.setMessages_out(0);
			response.setMessages_in(0);
			response.setData_traffic(0);
			response.setHashtags(hashtagList);

			//			sqlQuery = "SELECT outcoming_call_number,incoming_call_number,outcoming_sms_number,incoming_sms_number,data_cdr_number,hashtag,n_occurrences " +
			//					"FROM INFO_ABOUT_SQUARE_BY_TS, HASHTAG_SQUARE " +
			//					"WHERE square_ID IN (" + cellList + ") AND ts_ID >= " + spReq.getStart() + " AND ts_ID <= " + spReq.getEnd() + " " +
			//					"AND INFO_ABOUT_SQUARE_BY_TS.square_ID = HASHTAG_SQUARE.ht_square_ID AND INFO_ABOUT_SQUARE_BY_TS.ts_ID = HASHTAG_SQUARE.ht_ts_ID";
			//			
			//			statement = connection.createStatement();
			//
			//			resultSet = statement.executeQuery(sqlQuery);
			//
			//			next = resultSet.next();
			//			
			//			SidePanelResponse response = new SidePanelResponse();
			//			SidePanelHashtag hashtag;
			//			ArrayList<SidePanelHashtag> hashtagList = new ArrayList<SidePanelHashtag>();
			//			
			//			double totalOutcomingCalls = 0;
			//			double totalIncomingCalls = 0;
			//			double totalOutcomingSMS = 0;
			//			double totalIncomingSMS = 0;
			//			double totalDataCdr = 0;
			//
			//			while(next){
			//				hashtag = new SidePanelHashtag();
			//
			//				totalOutcomingCalls = totalOutcomingCalls + Double.parseDouble(resultSet.getString(1));
			//				totalIncomingCalls = totalIncomingCalls + Double.parseDouble(resultSet.getString(2));
			//				totalOutcomingCalls = totalOutcomingSMS + Double.parseDouble(resultSet.getString(3));
			//				totalIncomingSMS = totalIncomingSMS + Double.parseDouble(resultSet.getString(4));
			//				totalDataCdr = totalDataCdr + Double.parseDouble(resultSet.getString(5));
			//				
			//				hashtag.setLabel(resultSet.getString(6));
			//				hashtag.setValue(Long.parseLong(resultSet.getString(7)));
			//
			//				hashtagList.add(hashtag);
			//
			//				next = resultSet.next();
			//
			//			}
			//
			//			response.setCalls_out(totalOutcomingCalls);
			//			response.setCalls_in(totalIncomingCalls);
			//			response.setMessages_out(totalOutcomingSMS);
			//			response.setMessages_in(totalIncomingSMS);
			//			response.setData_traffic(totalDataCdr);
			//			response.setHashtags(hashtagList);

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
