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
package it.polimi.deib.city_sensing_server.concept_network;

import java.util.Collection;

public class ConceptNetResponse {
	
	private Collection<ConceptNetNode> nodes;
	private Collection<ConceptNetLink> links;
	
	public Collection<ConceptNetNode> getNodes() {
		return nodes;
	}
	public void setNodes(Collection<ConceptNetNode> nodes) {
		this.nodes = nodes;
	}
	public Collection<ConceptNetLink> getLinks() {
		return links;
	}
	public void setLinks(Collection<ConceptNetLink> links) {
		this.links = links;
	}
	
}
