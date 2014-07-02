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
package it.polimi.deib.city_sensing_server.users;

import it.polimi.deib.city_sensing_server.users.utilities.UsersSALink;
import it.polimi.deib.city_sensing_server.users.utilities.UsersSANode;

import java.util.ArrayList;
import java.util.Collection;

public class UsersSAResponse {
	
	private Collection<UsersSANode> nodes;
	private Collection<UsersSALink> links;
	
	public UsersSAResponse() {
		nodes = new ArrayList<UsersSANode>();
		links = new ArrayList<UsersSALink>();
	}
	public Collection<UsersSANode> getNodes() {
		return nodes;
	}
	public void setNodes(Collection<UsersSANode> nodes) {
		this.nodes = nodes;
	}
	public Collection<UsersSALink> getLinks() {
		return links;
	}
	public void setLinks(Collection<UsersSALink> links) {
		this.links = links;
	}
	public void addNode(UsersSANode node){
		nodes.add(node);
	}
	public void addLink(UsersSALink link){
		links.add(link);
	}	
}
