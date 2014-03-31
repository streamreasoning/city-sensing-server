package it.polimi.deib.city_sensing_server.linked_venues.utilities;

public class Venue {
	
	private String id;
	private String name;
	private double latitude;
	private double longitude;
	private long socialActivity;
	
	public Venue() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Venue(String id, String name, double latitude, double longitude,
			long socialActivity) {
		super();
		this.id = id;
		this.name = name;
		this.latitude = latitude;
		this.longitude = longitude;
		this.socialActivity = socialActivity;
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

	public long getSocialActivity() {
		return socialActivity;
	}

	public void setSocialActivity(long socialActivity) {
		this.socialActivity = socialActivity;
	}
	
	

}
