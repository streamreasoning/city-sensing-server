package polimi.deib.city_sensing_server.concept_network;

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
