package eu.deib.polimi.city_sensing_server.map_data_server;

public class MapCell {
	
	long id;
	double mobily_activity;
	double mobily_anomaly;
	double social_activity;
	double social_sentiment;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public double getMobily_activity() {
		return mobily_activity;
	}
	public void setMobily_activity(double mobily_activity) {
		this.mobily_activity = mobily_activity;
	}
	public double getMobily_anomaly() {
		return mobily_anomaly;
	}
	public void setMobily_anomaly(double mobily_anomaly) {
		this.mobily_anomaly = mobily_anomaly;
	}
	public double getSocial_activity() {
		return social_activity;
	}
	public void setSocial_activity(double social_activity) {
		this.social_activity = social_activity;
	}
	public double getSocial_sentiment() {
		return social_sentiment;
	}
	public void setSocial_sentiment(double social_sentiment) {
		this.social_sentiment = social_sentiment;
	}

}
