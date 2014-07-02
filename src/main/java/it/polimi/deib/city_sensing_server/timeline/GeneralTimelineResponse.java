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
package it.polimi.deib.city_sensing_server.timeline;

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
