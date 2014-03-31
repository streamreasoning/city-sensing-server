package it.polimi.deib.city_sensing_server.most_contacted_chart;

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

public class MostContactedChartDataServer  extends ServerResource{

	private Logger logger = LoggerFactory.getLogger(MostContactedChartDataServer.class.getName());

	@SuppressWarnings({ "unchecked", "rawtypes"})
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().serializeNulls().create();
		Connection connection = null;
		int lastIndex = 0;
		
		ArrayList<MostContactedChartCell> mostContactedCellList = new ArrayList<MostContactedChartCell>();
		MostContactedChartResponse mostContactedResponse = new MostContactedChartResponse();
				
		PreparedStatement statement = null;

		try {

			logger.debug("Contact Chart request received");
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

			MostContactedChartRequest mReq = gson.fromJson(reader, MostContactedChartRequest.class);

			ArrayList<Integer> cellList = new ArrayList<Integer>();
			String prepStmt = new String();

			if(mReq.getStart() == null || Long.parseLong(mReq.getStart()) < 0){
				mReq.setStart(Config.getInstance().getDefaultStart());
			}
			if(mReq.getEnd() == null || Long.parseLong(mReq.getEnd()) < 0){
				mReq.setEnd(Config.getInstance().getDefaultEnd());
			}
			if(mReq.getCells() == null || mReq.getCells().size() == 0){
				for(int i = 1 ; i < Config.getInstance().getDefaultNumberOfCells() ; i++){
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

			String query = "SELECT country, SUM(sum) AS overall_sum " +
					"FROM ( SELECT country, SUM(count) as sum FROM skil_call_in " +
					"WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? " +
					"GROUP BY country " +
					"UNION ALL " +
					"SELECT country, SUM(count) as sum FROM skil_sms_in " +
					"WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? " +
					"GROUP BY country ) q " +
					"GROUP BY country " +
					"ORDER BY overall_sum DESC " +
					"LIMIT 10";
			
			ResultSet resultset = null;

			long startTs = System.currentTimeMillis();
			statement = connection.prepareStatement(query);
			lastIndex = insertValues(statement, 1, cellList);
			statement.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
			statement.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
			lastIndex = insertValues(statement, lastIndex + 3, cellList);
			statement.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
			statement.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
			resultset = statement.executeQuery();
			long endTs = System.currentTimeMillis();

			logger.debug("International communication query done, time: {} ms",endTs - startTs);
			
			boolean next = resultset.next();
			
			MostContactedChartCell cell;
			
			while(next){
				cell = new MostContactedChartCell();
				cell.setCountryCode(resultset.getString(1));
				cell.setCount(Double.parseDouble(resultset.getString(2)));
				cell.setLocation("international");
				cell.setType("in");
				mostContactedCellList.add(cell);
				next = resultset.next();
			}

			if(resultset != null){
				resultset.close();
			}
			
			query = "SELECT country, SUM(sum) AS overall_sum FROM " +
					"( SELECT country, SUM(count) as sum FROM skil_call_out " +
					"WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? " +
					"GROUP BY country " +
					"UNION ALL " +
					"SELECT country, SUM(count) as sum FROM skil_sms_out " +
					"WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? " +
					"GROUP BY country ) q " +
					"GROUP BY country " +
					"ORDER BY overall_sum DESC " +
					"LIMIT 10";
			
			resultset = null;

			startTs = System.currentTimeMillis();
			statement = connection.prepareStatement(query);
			lastIndex = insertValues(statement, 1, cellList);
			statement.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
			statement.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
			lastIndex = insertValues(statement, lastIndex + 3, cellList);
			statement.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()));
			statement.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()));
			resultset = statement.executeQuery();
			endTs = System.currentTimeMillis();

			logger.debug("International communication query done, time: {} ms",endTs - startTs);
			
			next = resultset.next();
						
			while(next){
				cell = new MostContactedChartCell();
				cell.setCountryCode(resultset.getString(1));
				cell.setCount(Double.parseDouble(resultset.getString(2)));
				cell.setLocation("international");
				cell.setType("out");
				mostContactedCellList.add(cell);
				next = resultset.next();
			}

			if(resultset != null){
				resultset.close();
			}
			
			query = "SELECT province, SUM(count) as sum FROM skil_rec_in " +
					"WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? " +
					"GROUP BY province " +
					"ORDER BY sum DESC " +
					"LIMIT 10";
			
			resultset = null;

			startTs = System.currentTimeMillis();
			statement = connection.prepareStatement(query);
			lastIndex = insertValues(statement, 1, cellList);
			statement.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()) / 1000);
			statement.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()) / 1000);
			resultset = statement.executeQuery();
			endTs = System.currentTimeMillis();

			logger.debug("National communication query done, time: {} ms",endTs - startTs);
			
			next = resultset.next();
						
			while(next){
				cell = new MostContactedChartCell();
				cell.setCountryCode(resultset.getString(1));
				cell.setCount(Double.parseDouble(resultset.getString(2)));		
				cell.setLocation("national");
				cell.setType("in");
				mostContactedCellList.add(cell);
				next = resultset.next();
			}

			if(resultset != null){
				resultset.close();
			}
			
			query = "SELECT province, SUM(count) as sum FROM skil_rec_out " +
					"WHERE square_id IN (" + prepStmt + ") AND quarter >= ? AND quarter <= ? " +
					"GROUP BY province " +
					"ORDER BY sum DESC " +
					"LIMIT 10";
			
			resultset = null;

			startTs = System.currentTimeMillis();
			statement = connection.prepareStatement(query);
			lastIndex = insertValues(statement, 1, cellList);
			statement.setObject(lastIndex + 1, Long.parseLong(mReq.getStart()) / 1000);
			statement.setObject(lastIndex + 2, Long.parseLong(mReq.getEnd()) / 1000);
			resultset = statement.executeQuery();
			endTs = System.currentTimeMillis();

			logger.debug("National communication query done, time: {} ms",endTs - startTs);
			
			next = resultset.next();
						
			while(next){
				cell = new MostContactedChartCell();
				cell.setCountryCode(resultset.getString(1));
				cell.setCount(Double.parseDouble(resultset.getString(2)));
				cell.setLocation("national");
				cell.setType("out");
				mostContactedCellList.add(cell);
				next = resultset.next();
			}

			if(resultset != null){
				resultset.close();
			}
			
			mostContactedResponse.setContactsChart(mostContactedCellList);

			startTs = System.currentTimeMillis();
			String respString = gson.toJson(mostContactedResponse);
			endTs = System.currentTimeMillis();

			logger.debug("Contact chart json serialization done, time: {} ms",endTs - startTs);

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
				if(statement != null){ statement.close();}
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
