package it.polimi.deib.city_sensing_server.timeline;

import java.util.ArrayList;

public class ContextTimelineRequest {
	
	private ArrayList<String> cells;
	private String anomalyColumnName;

	public ArrayList<String> getCells() {
		return cells;
	}
	public void setCells(ArrayList<String> cells) {
		this.cells = cells;
	}
	public String getAnomalyColumnName() {
		return anomalyColumnName;
	}
	public void setAnomalyColumnName(String anomalyColumnName) {
		this.anomalyColumnName = anomalyColumnName;
	}
	
}
