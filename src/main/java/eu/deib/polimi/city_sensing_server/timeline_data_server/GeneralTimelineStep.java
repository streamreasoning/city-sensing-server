package eu.deib.polimi.city_sensing_server.timeline_data_server;

public class GeneralTimelineStep {
	
	long start;
	long end;
	double mobily_anomaly;
	double social_activity;
	
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
	public double getMobile_anomaly() {
		return mobily_anomaly;
	}
	public void setMobile_anomaly(double mobile_anomaly) {
		this.mobily_anomaly = mobile_anomaly;
	}
	public double getSocial_activity() {
		return social_activity;
	}
	public void setSocial_activity(double social_activity) {
		this.social_activity = social_activity;
	}
	
}
