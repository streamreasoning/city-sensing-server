package it.polimi.deib.city_sensing_server.linked_venues;

import it.polimi.deib.city_sensing_server.linked_venues.utilities.SimplifiedVenue;

import java.util.ArrayList;
import java.util.Collection;

public class VenuesSAResponse {
	
	private Collection<SimplifiedVenue> venues;

	public VenuesSAResponse() {
		venues = new ArrayList<SimplifiedVenue>();
	}
	
	public void addElementToList(SimplifiedVenue venue){
		venues.add(venue);
	}
	
	public void addElementsToList(Collection<SimplifiedVenue> venueList){
		venues.addAll(venueList);
	}	
	
		
}
