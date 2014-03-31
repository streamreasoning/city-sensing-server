package it.polimi.deib.city_sensing_server.concept_network;

public class ConceptNetExtendedNode implements Comparable<ConceptNetExtendedNode>{
	
	private String id;
	private String label;
	private int count;
	
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
	public int getCount() {
		return count;
	}
	public void setCount(int group) {
		this.count = group;
	}
	public int compareTo(ConceptNetExtendedNode o) {
		if(o.getCount() < count)
			return 1;
		else if(o.getCount() > count)
			return -1;
		return 0;
	}

}
