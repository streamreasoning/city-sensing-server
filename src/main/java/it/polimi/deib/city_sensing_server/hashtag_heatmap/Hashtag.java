package it.polimi.deib.city_sensing_server.hashtag_heatmap;

import java.util.ArrayList;

public class Hashtag {
	
	String id;
	String label;
	ArrayList<HeatmapSlot> data;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public ArrayList<HeatmapSlot> getData() {
		return data;
	}
	public void setData(ArrayList<HeatmapSlot> data) {
		this.data = data;
	}

}
