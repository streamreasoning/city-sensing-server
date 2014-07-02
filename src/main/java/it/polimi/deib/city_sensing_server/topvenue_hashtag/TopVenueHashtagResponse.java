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
package it.polimi.deib.city_sensing_server.topvenue_hashtag;

import java.util.ArrayList;
import java.util.List;

public class TopVenueHashtagResponse {
	
	private List<TopVenue> topVenues;
	private List<TopHashtag> topHashtags;
	
	public TopVenueHashtagResponse(){
		topVenues = new ArrayList<TopVenue>();
		topHashtags = new ArrayList<TopHashtag>();
	}
	
	public List<TopVenue> getTopVenues() {
		return topVenues;
	}
	public void setTopVenues(List<TopVenue> topVenues) {
		this.topVenues = topVenues;
	}
	public List<TopHashtag> getTopHashtags() {
		return topHashtags;
	}
	public void setTopHashtag(List<TopHashtag> topHashtags) {
		this.topHashtags = topHashtags;
	}
	public void addVenue(TopVenue venue){
		topVenues.add(venue);
	}
	public void addHashtag(TopHashtag hashtag){
		topHashtags.add(hashtag);
	}

	
}
