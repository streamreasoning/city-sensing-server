package it.polimi.deib.city_sensing_server.topic_heatmap;

import it.polimi.deib.city_sensing_server.hashtag_heatmap.Hashtag;
import it.polimi.deib.city_sensing_server.hashtag_heatmap.HeatmapSlot;

import java.util.Collection;

public class TopicHeatmapResponse {
	
private Collection<Topic> topics;
	

	public Collection<Topic> getTopics() {
		return topics;
	}

	public void setTopics(Collection<Topic> topics) {
		this.topics = topics;
	}

}
