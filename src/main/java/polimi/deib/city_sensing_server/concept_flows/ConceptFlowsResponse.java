package polimi.deib.city_sensing_server.concept_flows;

import java.util.Collection;

public class ConceptFlowsResponse {
	
	private Collection<ConceptFlowsNode> nodes;
	private Collection<ConceptFlowsLink> links;
	
	public Collection<ConceptFlowsNode> getNodes() {
		return nodes;
	}
	public void setNodes(Collection<ConceptFlowsNode> nodes) {
		this.nodes = nodes;
	}
	public Collection<ConceptFlowsLink> getLinks() {
		return links;
	}
	public void setLinks(Collection<ConceptFlowsLink> links) {
		this.links = links;
	}
	
}
