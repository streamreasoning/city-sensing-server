package it.polimi.deib.city_sensing_server.topic_network;

import java.io.IOException;
import java.io.StringReader;
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

public class TopicNetworkDataServer extends ServerResource{
	
	private Logger logger = LoggerFactory.getLogger(TopicNetworkDataServer.class.getName());
	
	private final double THRESHOLD_VAL = 5;
	private final double SCALE_FACTOR = 10.0; // fattore di scala per distanze tra centri di cluster

	private ClusterList clusterList;
	private TopicLogic logic;

	private CooccourenceMatrix m;

	private int DAYMODE = 0;
	private int TIMEMODE;
	
	@Post
	public void dataServer(Representation rep) throws IOException {

		Gson gson = new GsonBuilder().serializeNulls().serializeSpecialFloatingPointValues().create();
		try {

			logger.debug("Topic network request received");
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

			TopicNetworkRequest cnReq = gson.fromJson(reader, TopicNetworkRequest.class);

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

			clusterList = new ClusterList();
			logic = new TopicLogic(clusterList);
			int numDay = -1;
			TIMEMODE = setTimeMode(logic.getDateFormat().parse(longToDate(Long.parseLong(cnReq.getStart()))), logic.getDateFormat().parse(longToDate(Long.parseLong(cnReq.getEnd()))));
			numDay = logic.dayDifference(logic.getDateFormat().parse(longToDate(Long.parseLong(cnReq.getStart()))), logic.getDateFormat().parse(longToDate(Long.parseLong(cnReq.getEnd()))));
			logic.setParameters(setKParameter(logic.getDateFormat().parse(longToDate(Long.parseLong(cnReq.getStart()))), logic.getDateFormat().parse(longToDate(Long.parseLong(cnReq.getEnd())))), THRESHOLD_VAL);
			logic.buildClusters(longToDate(Long.parseLong(cnReq.getStart())), longToDate(Long.parseLong(cnReq.getEnd())), DAYMODE, TIMEMODE, numDay, getNumTimeSlot(TIMEMODE));
			Cluster[] allClusters = clusterList.getClusterVector();
			int dim = 0;
			if(allClusters.length > 20){
				dim = 20;
			} else {
				dim = allClusters.length;
			}
			Cluster[] clusters = new Cluster[dim];
			for(int i=0; i<clusters.length; i++) {
				clusters[i] = allClusters[i];
			}
			
			ArrayList<TopicNetworkNode> topicNetNodes = new ArrayList<TopicNetworkNode>();
			ArrayList<TopicNetworkLink> topicNetLinks = new ArrayList<TopicNetworkLink>();
			int id = 0;
			
			for(int i=0; i<clusters.length; i++){
				for(int j=0; j<clusters[i].getDim(); j++){
					TopicNetworkNode node = new TopicNetworkNode();
					TopicNetworkLink link = new TopicNetworkLink();
					node.setLabel(clusters[i].getTag(j));
					node.setGroup(Integer.toString(i));
					node.setId(Integer.toString(id));
					node.setValue(Double.toString(clusters[i].getElementRelevance(j)));
					topicNetNodes.add(node);
					int source = getUniqueId(topicNetNodes, clusters[i].getClusterCenter().getTag());
					if(source != id){
						link.setSource(getUniqueId(topicNetNodes, clusters[i].getClusterCenter().getTag()));
						link.setTarget(id);
						link.setValue(clusters[i].getDistance(j));
						topicNetLinks.add(link);
					}
					id++;
				}
			}
			
			String[] tag;
			ArrayList<String> tagList = new ArrayList<String>();
			for(int i=0; i<clusterList.getNumClusters()+1 && i < 20; i++){
				tagList.add(new String(clusterList.getClusterClass(i).getClusterCenter().getTag()));
			}
			m = new CooccourenceMatrix(tagList.size());
			tag = logic.listToVector(tagList);
			logic.buildMatrix(m, tag);
			for(int i=0; i<m.getMatrixDimension(); i++){
				for(int j=i+1; j<m.getMatrixDimension(); j++){
					TopicNetworkLink link = new TopicNetworkLink();
					if(10 * (1 - m.getMatrixValue(i, j)) < 10.0){
						link.setSource(getUniqueId(topicNetNodes, clusters[i].getClusterCenter().getTag()));
						link.setTarget(getUniqueId(topicNetNodes, clusters[j].getClusterCenter().getTag()));
						link.setValue(10 * (1 - m.getMatrixValue(i, j)));
						topicNetLinks.add(link);
					}
				}
			}
			
			TopicNetworkResponse response = new TopicNetworkResponse();
			response.setNodes(topicNetNodes);
			response.setLinks(topicNetLinks);
			
			
			String respString = gson.toJson(response);
			
			rep.release();
			this.getResponse().setStatus(Status.SUCCESS_CREATED);
			this.getResponse().setEntity(respString, MediaType.APPLICATION_JSON);
			this.getResponse().commit();
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
	
	private int getUniqueId(ArrayList<TopicNetworkNode> node, String nodeLabel){
		for(int i=0; i<node.size(); i++){
			if(node.get(i).getLabel().equals(nodeLabel)){
				return Integer.parseInt(node.get(i).getId());
			}
		}
		return -1;
	}

	private int setTimeMode(Date startDate, Date endDate){
		int dayDistance = logic.dayDifference(startDate, endDate);
		if(dayDistance == 0){
			return 0;
		} else if (dayDistance > 0 && dayDistance < 5){
			return 1;
		} else if (dayDistance > 4 && dayDistance < 13){
			return 2;
		} else {
			return 3;
		}
	}

	private int setKParameter(Date startDate, Date endDate){
		int dayDistance = logic.dayDifference(startDate, endDate);
		if(dayDistance > 7){
			return 2;
		} else {
			return 1;
		}
	}

	private int getNumTimeSlot(int timeMode){
		switch (timeMode){
		case 0: return 24;
		case 1: return 5;
		case 2: return 2;
		case 3: return 1;
		default: return -1;
		}
	}
	
	private String longToDate(long timeStamp) throws ParseException, DatatypeConfigurationException{
		GregorianCalendar calendar = new GregorianCalendar();
		calendar.setTimeInMillis(timeStamp);
		SimpleDateFormat dateAndTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Date d = dateAndTime.parse(DatatypeFactory.newInstance().newXMLGregorianCalendar(calendar).toXMLFormat());
		SimpleDateFormat onlyDate = new SimpleDateFormat("dd/MM/yyyy"); 
		return onlyDate.format(d);
	}


}
