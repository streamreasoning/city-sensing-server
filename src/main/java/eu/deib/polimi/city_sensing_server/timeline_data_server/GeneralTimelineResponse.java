package eu.deib.polimi.city_sensing_server.timeline_data_server;

import java.util.Collection;

public class GeneralTimelineResponse {
	
	Collection<GeneralTimelineStep> steps;

	public Collection<GeneralTimelineStep> getSteps() {
		return steps;
	}

	public void setSteps(Collection<GeneralTimelineStep> steps) {
		this.steps = steps;
	}
	
	public void addElementToList(GeneralTimelineStep parStep){
		steps.add(parStep);
	}
	
	public void addElementsToList(Collection<GeneralTimelineStep> parSteps){
		steps.addAll(parSteps);
	}

}
