package polimi.deib.city_sensing_server.event;

import java.util.Collection;

public class EventListVenue {
	
	Collection<EventListCategory> categories;

	public Collection<EventListCategory> getCategories() {
		return categories;
	}

	public void setCategories(Collection<EventListCategory> categories) {
		this.categories = categories;
	}

}
