package polimi.deib.city_sensing_server.concept_network;

import java.util.ArrayList;

public class ConceptNetRequest {
	
	String start;
	String end;
	ArrayList<String> cells;
	String threshold;
	
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
