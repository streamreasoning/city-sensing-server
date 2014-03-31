package it.polimi.deib.city_sensing_server.event;

import java.util.Collection;

public class EventListResponse {
	
	Collection<Event> events;

	public Collection<Event> getCells() {
		return events;
	}

	public void setCells(Collection<Event> events) {
		this.events = events;
	}
	
}
