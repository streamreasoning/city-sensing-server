package polimi.deib.city_sensing_server.map;

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

import polimi.deib.city_sensing_server.dataSource.DataSourceSingleton;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;


public class MapDataServer extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(MapDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();
		Connection connection = null;
		ResultSet resultSet = null;
		int lastIndex = 0;

		PreparedStatement p1 = null;

		try {

			logger.debug("Map request received");
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

			connection = DataSourceSingleton.getInstance().getConnection();
			connection.setAutoCommit(false);

			String sqlQuery = "SELECT square_ID,(SUM(incoming_call_number) + SUM(outgoing_call_number) + SUM(incoming_sms_number) + SUM(outgoing_sms_number) + SUM(data_cdr_number)) AS mobily_activity, " +
					"AVG(anomaly_index) AS mobily_anomaly , SUM(n_tweets) AS social_activity, (SUM(positive_tweets_number) - SUM(negative_tweets_number) + (SUM(neutral_tweets_number) * 0.1)) AS social_sentiment " +
					"FROM NEW_MYISAM_INF_ABOUT_SQUARE_BY_TS_2 " +
					"WHERE square_ID IN (" + prepStmt + ") AND ts_ID > ? AND ts_ID < ? " +
					"GROUP BY square_ID";

			long startTs = System.currentTimeMillis();
			p1 = connection.prepareStatement(sqlQuery);
			lastIndex = insertValues(p1, 1, cellList);
			p1.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
			p1.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
			resultSet = p1.executeQuery();
			long endTs = System.currentTimeMillis();

			logger.debug("Map query done, time: {} ms",endTs - startTs);

			boolean next = resultSet.next();

			MapResponse response = new MapResponse();
			MapCell mapCell;
			ArrayList<MapCell> mapCellList = new ArrayList<MapCell>();

			while(next){
				mapCell = new MapCell();

				mapCell.setId(Long.parseLong(resultSet.getString(1)));
				mapCell.setMobily_activity(Double.parseDouble(resultSet.getString(2)));
				mapCell.setMobily_anomaly(Double.parseDouble(resultSet.getString(3)));
				mapCell.setSocial_activity(Double.parseDouble(resultSet.getString(4)));
				if(mapCell.getSocial_activity() != 0)
					mapCell.setSocial_sentiment(Double.parseDouble(resultSet.getString(5)) / mapCell.getSocial_activity());
				else
					mapCell.setSocial_sentiment(Double.parseDouble(resultSet.getString(5)));

				mapCellList.add(mapCell);

				next = resultSet.next();

			}

			response.setCells(mapCellList);

			if(resultSet != null){
				resultSet.close();
			}

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(response);
			endTs = System.currentTimeMillis();

			logger.debug("Map json serialization done, time: {} ms",endTs - startTs);

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
