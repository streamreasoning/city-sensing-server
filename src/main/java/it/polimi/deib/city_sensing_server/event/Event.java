package it.polimi.deib.city_sensing_server.event;

import java.util.Collection;

public class Event {
	
	private long id;
	private String name;
	private EventListVenue venues;
	private String address;
	private Collection<Long> date;
	private String link;
	private long squareID; 
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public EventListVenue getVenues() {
		return venues;
	}
	public void setVenues(EventListVenue venues) {
		this.venues = venues;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public Collection<Long> getDate() {
		return date;
	}
	public void setDate(Collection<Long> date) {
		this.date = date;
	}
	public String getLink() {
		return link;
	}
	public void setLink(String link) {
		this.link = link;
	}
	public long getSquareID() {
		return squareID;
	}
	public void setSquareID(long squareID) {
		this.squareID = squareID;
	}
	
}
