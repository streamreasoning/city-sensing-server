/*******************************************************************************
 * Copyright 2014 DEIB - Politecnico di Milano
 *    
 * Marco Balduini (marco.balduini@polimi.it)
 * Emanuele Della Valle (emanuele.dellavalle@polimi.it)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
