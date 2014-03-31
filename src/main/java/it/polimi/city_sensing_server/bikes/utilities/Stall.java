package it.polimi.city_sensing_server.bikes.utilities;

public class Stall {
	
	private String id;
	private String name;
	private double latitude;
	private double longitude;
	private int availableBikes;
	private int docks;
	private double percentageOfAvailableBikes;
	
	public double getPercentageOfAvailableBikes() {
		return percentageOfAvailableBikes;
	}
	public void setPercentageOfAvailableBikes(double percentageOfAvailableBikes) {
		this.percentageOfAvailableBikes = percentageOfAvailableBikes;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public int getAvailableBikes() {
		return availableBikes;
	}
	public void setAvailableBikes(int availableBikes) {
		this.availableBikes = availableBikes;
	}
	public int getDocks() {
		return docks;
	}
	public void setDocks(int docks) {
		this.docks = docks;
	}

	

}
