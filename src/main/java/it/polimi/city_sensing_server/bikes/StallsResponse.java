package it.polimi.city_sensing_server.bikes;

import it.polimi.city_sensing_server.bikes.utilities.Stall;

import java.util.ArrayList;
import java.util.Collection;

public class StallsResponse {
	
	private ArrayList<Stall> stalls;
	
	public StallsResponse() {
		stalls = new ArrayList<Stall>();
	}

	public ArrayList<Stall> getStalls() {
		return stalls;
	}

	public void setStalls(ArrayList<Stall> stalls) {
		this.stalls = stalls;
	}
	
	public void addElementToList(Stall stall){
		stalls.add(stall);
	}
	
	public void addElementsToList(Collection<Stall> stallList){
		stalls.addAll(stallList);
	}
	
}
