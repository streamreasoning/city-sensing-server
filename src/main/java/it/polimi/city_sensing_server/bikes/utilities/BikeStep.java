package it.polimi.city_sensing_server.bikes.utilities;

public class BikeStep {
	
//	"start":1365469200000,
//    "end":1365470100000,
//    "availableBikes": 1000,
//    "docks": 2000,
//    "inUseBikes": 1000,
	
	private long start;
	private long end;
	private long availableBikes;
	private long docks;
	private long inUseBikes;
	
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	public long getAvailableBikes() {
		return availableBikes;
	}
	public void setAvailableBikes(long availableBikes) {
		this.availableBikes = availableBikes;
	}
	public long getDocks() {
		return docks;
	}
	public void setDocks(long docks) {
		this.docks = docks;
	}
	public long getInUseBikes() {
		return inUseBikes;
	}
	public void setInUseBikes(long inUseBikes) {
		this.inUseBikes = inUseBikes;
	}
	
	

}
