package it.polimi.deib.city_sensing_server.timeline;

import java.util.ArrayList;

public class FocusTimelineRequest {
	
	private String start;
	private String end;
	private ArrayList<String> cells;
	private String anomalyColumnName;
	
	public String getStart() {
		return start;
	}
	public void setStart(String start) {
		this.start = start;
	}
	public String getEnd() {
		return end;
	}
	public void setEnd(String end) {
		this.end = end;
	}
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
