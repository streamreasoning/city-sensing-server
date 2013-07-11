package polimi.deib.city_sensing_server.concept_network;

public class ConceptNetLink implements Comparable<ConceptNetLink>{
	
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
	
	public int compareTo(ConceptNetLink o) {
		if(o.value < value)
			return -1;
		else if(o.value > value)
			return 1;
		return 0;
	}
	
}
