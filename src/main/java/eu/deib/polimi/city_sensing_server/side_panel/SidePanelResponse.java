package eu.deib.polimi.city_sensing_server.side_panel;

import java.util.Collection;

public class SidePanelResponse {
	
	private double calls_out;
	private double calls_in;
	private double messages_out;
	private double messages_in;
	private double data_traffic;
	private Collection<SidePanelHashtag> hashtags;
	
	public double getCalls_out() {
		return calls_out;
	}
	public void setCalls_out(double calls_out) {
		this.calls_out = calls_out;
	}
	public double getCalls_in() {
		return calls_in;
	}
	public void setCalls_in(double calls_in) {
		this.calls_in = calls_in;
	}
	public double getMessages_out() {
		return messages_out;
	}
	public void setMessages_out(double messages_out) {
		this.messages_out = messages_out;
	}
	public double getMessages_in() {
		return messages_in;
	}
	public void setMessages_in(double messages_in) {
		this.messages_in = messages_in;
	}
	public double getData_traffic() {
		return data_traffic;
	}
	public void setData_traffic(double data_traffic) {
		this.data_traffic = data_traffic;
	}
	public Collection<SidePanelHashtag> getHashtags() {
		return hashtags;
	}
	public void setHashtags(Collection<SidePanelHashtag> hashtags) {
		this.hashtags = hashtags;
	}	

}
