package it.polimi.deib.city_sensing_server.linked_venues.utilities;

public class SimplifiedVenue {
	
	private String id;
	private long socialActivity;
	
	public SimplifiedVenue() {
		super();
		// TODO Auto-generated constructor stub
	}

	public SimplifiedVenue(String id,long socialActivity) {
		super();
		this.id = id;
		this.socialActivity = socialActivity;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getSocialActivity() {
		return socialActivity;
	}

	public void setSocialActivity(long socialActivity) {
		this.socialActivity = socialActivity;
	}
	
	

}
