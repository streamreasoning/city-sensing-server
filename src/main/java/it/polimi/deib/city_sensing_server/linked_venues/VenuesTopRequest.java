package it.polimi.deib.city_sensing_server.linked_venues;

import java.util.ArrayList;

public class VenuesTopRequest {
	
	private String start;
	private String end;
	private ArrayList<String> cells;
	private String threshold;
	
	public VenuesTopRequest() {
		super();
	}

	public VenuesTopRequest(String start, String end, ArrayList<String> cells,
			String threshold) {
		super();
		this.start = start;
		this.end = end;
		this.cells = cells;
		this.threshold = threshold;
	}

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

	public String getThreshold() {
		return threshold;
	}

	public void setThreshold(String threshold) {
		this.threshold = threshold;
	}
	
}
