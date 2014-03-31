package it.polimi.deib.city_sensing_server.concept_network;

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

	@Override
	public boolean equals(Object other){
		ConceptNetLink o = (ConceptNetLink) other;
		if((o.source.equals(source) && o.target.equals(target)) || (o.source.equals(target) && o.target.equals(source))){
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public int compareTo(ConceptNetLink arg0) {
		if(arg0.value < value)
			return -1;
		else if(arg0.value > value)
			return 1;
		return 0;
	}



}
