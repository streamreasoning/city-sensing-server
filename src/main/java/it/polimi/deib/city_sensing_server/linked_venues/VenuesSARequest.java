package it.polimi.deib.city_sensing_server.linked_venues;

import java.util.ArrayList;

public class VenuesSARequest {
	
	String start;
	String end;
	ArrayList<String> venues;
	
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
	public ArrayList<String> getVenues() {
		return venues;
	}
	public void setVenues(ArrayList<String> venues) {
		this.venues = venues;
	}
	
}
