package polimi.deib.city_sensing_server.timeline;

public class GeneralTimelineStep {
	
	long start;
	long end;
	double mobily_anomaly;
	double social_activity;
	double mobily_activity;
	double social_sentiment;
	
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
	public double getMobily_anomaly() {
		return mobily_anomaly;
	}
	public void setMobily_anomaly(double mobile_anomaly) {
		this.mobily_anomaly = mobile_anomaly;
	}
	public double getSocial_activity() {
		return social_activity;
	}
	public void setSocial_activity(double social_activity) {
		this.social_activity = social_activity;
	}
	public double getMobily_activity() {
		return mobily_activity;
	}
	public void setMobily_activity(double mobily_activity) {
		this.mobily_activity = mobily_activity;
	}
	public double getSocial_sentiment() {
		return social_sentiment;
	}
	public void setSocial_sentiment(double social_sentiment) {
		this.social_sentiment = social_sentiment;
	}
	
}
