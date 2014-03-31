package it.polimi.deib.city_sensing_server.linked_venues;

import it.polimi.deib.city_sensing_server.linked_venues.utilities.Venue;

import java.util.ArrayList;
import java.util.Collection;

public class VenuesTopResponse {
	
	private Collection<Venue> venues;

	public VenuesTopResponse() {
		venues = new ArrayList<Venue>();
	}

	public VenuesTopResponse(Collection<Venue> venues) {
		super();
		this.venues = venues;
	}

	public Collection<Venue> getVenues() {
		return venues;
	}

	public void setVenues(Collection<Venue> venues) {
		this.venues = venues;
	}

	public void addElementToList(Venue venue){
		venues.add(venue);
	}
	
	public void addElementsToList(Collection<Venue> venueList){
		venues.addAll(venueList);
	}

}
