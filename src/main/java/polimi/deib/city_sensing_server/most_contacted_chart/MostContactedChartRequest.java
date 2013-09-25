package polimi.deib.city_sensing_server.most_contacted_chart;

import java.util.ArrayList;

public class MostContactedChartRequest {
	
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
