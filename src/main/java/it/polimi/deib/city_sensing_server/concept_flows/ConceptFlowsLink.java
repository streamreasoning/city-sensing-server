package it.polimi.deib.city_sensing_server.concept_flows;


public class ConceptFlowsLink  implements Comparable<ConceptFlowsLink> {
	
	private String source;
	private String target;
	private double value;
	
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	
	public int compareTo(ConceptFlowsLink o) {
		if(o.value < value)
			return -1;
		else if(o.value > value)
			return 1;
		return 0;
	}
	
}
