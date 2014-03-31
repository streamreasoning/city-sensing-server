package it.polimi.deib.city_sensing_server.users.utilities;

public class User {
	
	private String id;
	private String name;
	private String imageUrl;
	private long socialActivity;
	
	public User() {
		super();
	}

	public User(String id, String name, String imageUrl, long socialActivity) {
		super();
		this.id = id;
		this.name = name;
		this.imageUrl = imageUrl;
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

	public String getImageUrl() {
		return imageUrl;
	}

	public void setImageUrl(String imageUrl) {
		this.imageUrl = imageUrl;
	}

	public long getSocialActivity() {
		return socialActivity;
	}

	public void setSocialActivity(long socialActivity) {
		this.socialActivity = socialActivity;
	}
	
}
