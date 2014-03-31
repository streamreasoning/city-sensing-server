package it.polimi.city_sensing_server.bikes;

import java.util.ArrayList;

public class StallsRequest {
	
	private String when;
	private ArrayList<String> cells;
	
	public String getWhen() {
		return when;
	}

	public void setWhen(String when) {
		this.when = when;
	}

	public ArrayList<String> getCells() {
		return cells;
	}

	public void setCells(ArrayList<String> cells) {
		this.cells = cells;
	}
	
}
