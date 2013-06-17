package polimi.deib.city_sensing_server.concept_flows;

public class ConceptFlowsNode {
	
	private String id;
	private String label;
	private String group;
	private long sentiment;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public long getSentiment() {
		return sentiment;
	}
	public void setSentiment(long sentiment) {
		this.sentiment = sentiment;
	}

}