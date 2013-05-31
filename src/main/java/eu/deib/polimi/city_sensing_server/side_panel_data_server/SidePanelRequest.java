package eu.deib.polimi.city_sensing_server.side_panel_data_server;

import java.util.ArrayList;

public class SidePanelRequest {
	
	String start;
	String end;
	ArrayList<String> cells;
	
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
}
