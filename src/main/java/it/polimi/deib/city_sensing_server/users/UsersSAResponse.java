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
