package it.polimi.city_sensing_server.bikes;

import it.polimi.city_sensing_server.bikes.utilities.BikeStep;

import java.util.ArrayList;
import java.util.Collection;

public class BikeTimeLineResponse {
	
	private Collection<BikeStep> steps;
	
	public BikeTimeLineResponse() {
		steps = new ArrayList<BikeStep>();
	}

	public Collection<BikeStep> getSteps() {
		return steps;
	}

	public void setSteps(Collection<BikeStep> steps) {
		this.steps = steps;
	}
	
	public void addElementToList(BikeStep step){
		steps.add(step);
	}
	
	public void addElementsToList(Collection<BikeStep> stepList){
		steps.addAll(stepList);
	}

}
