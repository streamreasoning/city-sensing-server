package it.polimi.deib.city_sensing_server.hashtag_heatmap;

import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import it.polimi.city_sensing_server.topic_utilities.Cluster;
import it.polimi.city_sensing_server.topic_utilities.ClusterList;
import it.polimi.city_sensing_server.topic_utilities.CooccourenceMatrix;
import it.polimi.city_sensing_server.topic_utilities.TopicLogic;
import it.polimi.deib.city_sensing_server.configuration.Config;
import it.polimi.city_sensing_server.topic_utilities.Logic;

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

public class HashtagHeatmapDataServer extends ServerResource{
	
	private Logger logger = LoggerFactory.getLogger(HashtagHeatmapDataServer.class.getName());
	
	
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();
		try {

			logger.debug("Hashtag heatmap request received");
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

			HashtagHeatmapRequest cnReq = gson.fromJson(reader, HashtagHeatmapRequest.class);

			ArrayList<Integer> cellList = new ArrayList<Integer>();

			if(cnReq.getStart() == null || Long.parseLong(cnReq.getStart()) < 0){
				cnReq.setStart(Config.getInstance().getDefaultStart());
			}
			if(cnReq.getEnd() == null || Long.parseLong(cnReq.getEnd()) < 0){
				cnReq.setEnd(Config.getInstance().getDefaultEnd());
			}
			if(cnReq.getCells() == null || cnReq.getCells().size() == 0){
				for(int i = 1 ; i < Config.getInstance().getDefaultNumberOfCells() ; i++){
					cellList.add(i);
				}
			} else {
				for(String s : cnReq.getCells()){
					cellList.add(Integer.parseInt(s));
				}
			}
			
			Logic logic = new Logic();
			HashtagHeatmapResponse response = new HashtagHeatmapResponse();
			DateFormat outFormat = new SimpleDateFormat("dd/MM"); 
			
			String[] topHashtag = logic.getTopHashtag(longToDateFormat(Long.parseLong(cnReq.getStart())), longToDateFormat(Long.parseLong(cnReq.getEnd())));
			ArrayList<Hashtag> hashtags = new ArrayList<Hashtag>();
			for(int i=0; i<topHashtag.length; i++){
				logic.buildAggregatePattern(topHashtag[i], logic.getDateFormat().parse(longToDate(Long.parseLong(cnReq.getStart()))), logic.getDateFormat().parse(longToDate(Long.parseLong(cnReq.getEnd()))), 0, 1, logic.dayDifference(logic.getDateFormat().parse(longToDate(Long.parseLong(cnReq.getStart()))), logic.getDateFormat().parse(longToDate(Long.parseLong(cnReq.getEnd())))) + 1, 5);
				Hashtag hashtag = new Hashtag();
				hashtag.setId(Integer.toString(i));
				hashtag.setLabel(topHashtag[i]);
				ArrayList<HeatmapSlot> data = new ArrayList<HeatmapSlot>();
				for(int j=0; j<logic.getAggregatePatternLength(); j++){
					String day = outFormat.format(logic.getDatePattern(j));
					for(int k=0; k<logic.getAggregatePatternNumRow(); k++){
						HeatmapSlot slot = new HeatmapSlot();
						slot.setDate(day);
						slot.setInterval(getTimeSlotLabel(k));
						slot.setValue(Double.toString(logic.getAggregatePattern(j,k)));
						data.add(slot);
					}
				hashtag.setData(data);
				}
				hashtags.add(hashtag);
			}
			
			response.setHashtags(hashtags);	
			
			String respString = gson.toJson(response);
			
			rep.release();
			this.getResponse().setStatus(Status.SUCCESS_CREATED);
			this.getResponse().setEntity(respString, MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			logic.closeConnection();
			this.commit();	
			this.release();
			
		} catch (Exception ex){
			logger.error(ex.getMessage(), ex);
			String error = "Generic error";
			rep.release();
			this.getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, error);
			this.getResponse().setEntity(gson.toJson(error), MediaType.APPLICATION_JSON);
			this.getResponse().commit();
			this.commit();	
			this.release();
		} finally {
			rep.release();
		}
	}
	
	private String getTimeSlotLabel(int index){
		switch(index) {
		case 0: {
			return "0-7";
		}
		case 1: {
			return "7-11";
		}
		case 2: {
			return "11-14";
		}
		case 3: {
			return "14-19";
		}
		case 4: {
			return "19-24";
		}
		}
		return null;
	}
	

		
	private String longToDate(long timeStamp) throws ParseException, DatatypeConfigurationException{
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTimeInMillis(timeStamp);
		SimpleDateFormat dateAndTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date d = dateAndTime.parse(DatatypeFactory.newInstance().newXMLGregorianCalendar(calendar).toXMLFormat());
		SimpleDateFormat onlyDate = new SimpleDateFormat("dd/MM/yyyy"); 
		return onlyDate.format(d);
	}
	
	private Date longToDateFormat(long timeStamp) throws ParseException, DatatypeConfigurationException{
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTimeInMillis(timeStamp);
		SimpleDateFormat dateAndTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date d = dateAndTime.parse(DatatypeFactory.newInstance().newXMLGregorianCalendar(calendar).toXMLFormat());
		SimpleDateFormat onlyDate = new SimpleDateFormat("dd/MM/yyyy"); 
		String stringDate = onlyDate.format(d);
		return onlyDate.parse(stringDate);
	}


}
