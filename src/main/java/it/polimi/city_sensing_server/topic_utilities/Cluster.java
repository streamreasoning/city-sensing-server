package it.polimi.city_sensing_server.topic_utilities;

import java.util.ArrayList;

public class Cluster  {
	
	public class ClusterElement {
		
		private String tag;
		private double[][] pattern;
		private double distance;
		
		public ClusterElement(){}
		
		public ClusterElement(String tag, double[][] values){
			this.tag = tag;
			this.pattern = values;
		}
		
		public ClusterElement(String tag, double[][] values, double distance){
			this.tag = tag;
			this.pattern = values;
			this.distance = distance;
		}
		
		public String getTag(){
			return this.tag;
		}
		
		public void setTag(String tag){
			this.tag = tag;
		}
		
		public double[][] getPattern(){
			return this.pattern;
		}
		
		public void setPattern(double[][] pattern){
			this.pattern = pattern;
			
		}
		
		public double getDistance(){
			return this.distance;
		}
		
		public void setDistance(double distance){
			this.distance = distance;
		}
		
		public double getElementRelevance(){
			double sum = 0.0;
			for(int i=0; i<pattern.length; i++){
				for(int j=0; j<pattern[0].length; j++){
					sum += pattern[i][j];
				}
			}
			return sum;
		}
	}
	
	private ArrayList<ClusterElement> tagList = new ArrayList<ClusterElement>();
	private int dim = 0;
	private int indexClusterCenter;
	double[][] pattern;
	
	public Cluster(){
		dim = 0;
	}
	
	public Cluster(String tag, double[][] values){  // questa si usa!!!!
		this.tagList.add(new ClusterElement(tag, values));
		dim++;
	}
	
	public Cluster(String tag, double[][] values, double distance){
		this.tagList.add(new ClusterElement(tag, values, distance));
		dim++;
	}
	
	public int getDim(){
		return this.dim;
	}
	
/**	public ClusterElement getClusterCenter(){
		double max = 0.0;
		double relevance = 0.0;
		ClusterElement center = null;
		for(int i=0; i<tagList.size(); i++){
			if((relevance = tagList.get(i).getElementRelevance()) > max){
				max = relevance;
				center = tagList.get(i);
			}
		}
		return center;
	}*/
	
	public void setIndexClusterCenter(int index){
		this.indexClusterCenter = index;
	}
	
	public ClusterElement getClusterCenter() {
		return this.tagList.get(indexClusterCenter);
	}
	
	public void setDistance(int index, double distance){
		tagList.get(index).setDistance(distance);
	}
	
	public double getDistance(int index){
		return tagList.get(index).getDistance();
	}
	
	
	public String[] getTagList() {
		String[] tagList = new String[this.tagList.size()];
		for(int i=0; i<this.tagList.size(); i++){
			tagList[i] = this.tagList.get(i).getTag();
		}
		return tagList;
	}
	
	public String getTagListAsString() {
		String tagList = new String(this.tagList.get(0).getTag());
		for(int i=1; i<this.tagList.size(); i++){
			tagList = tagList + "," + this.tagList.get(i).getTag();
		}
		return tagList;
	}
	

	public String getTag(int index){
		return this.tagList.get(index).getTag();
	}
	
	public void setTag(String tag, double[][] pattern){
		tagList.add(new ClusterElement(tag, pattern));
		dim++;
	}
	
	public void setTag(String tag, double[][] pattern, double distance){
		tagList.add(new ClusterElement(tag, pattern, distance));
		dim++;
	}
	
	public void print(){
		for(int i=0; i<dim; i++){
			System.out.println(tagList.get(i));
		}
	}
	
	public void replaceTagList(Cluster newList){
		this.tagList.clear();
		this.dim = 0;
		for(int i=0; i<newList.getDim(); i++){
			this.setTag(newList.getTag(i), newList.getPattern(i), newList.getDistance(i));
		}
	}
	
	public void orderClusterElement() throws NullPointerException {
		for(int heapsize = 0; heapsize < tagList.size(); heapsize++){
			int n = heapsize;
			while(n > 0){
				int p = (n-1)/2;
				if((tagList.get(n).getDistance()) > (tagList.get(p).getDistance())){
					swap(n, p);
					n = p;
				} else {
					break;
				}
			}
		}
		
		for(int heapsize = tagList.size(); heapsize>0; ){
			swap(0, --heapsize);
			int n = 0;
			while(true){
				int left = (n*2) + 1;
				if(left >= heapsize){
					break;
				}
				int right = left + 1;
				if(right >= heapsize){
					if((tagList.get(left).getDistance()) > (tagList.get(n).getDistance())){
						swap (left, n);
					}
					break;
				}
				if((tagList.get(left).getDistance()) > (tagList.get(n).getDistance())){
					if((tagList.get(left).getDistance()) > (tagList.get(right).getDistance())){
						swap(left, n);
						n = left;
						continue;
					} else {
						swap (right, n);
						n = right;
						continue;
					}
				} else {
					if((tagList.get(right).getDistance()) > (tagList.get(n).getDistance())){
						swap (right, n);
						n = right;
						continue;
					} else {
						break;
					}
				}
			}
		}
	}
	
	private void swap (int x, int y) {
		if(x != y){
			ClusterElement temp = new ClusterElement();
			temp.setTag(tagList.get(x).getTag());
			temp.setPattern(tagList.get(x).getPattern());
			temp.setDistance(tagList.get(x).getDistance());
			tagList.get(x).setTag(tagList.get(y).getTag());
			tagList.get(x).setPattern(tagList.get(y).getPattern());
			tagList.get(x).setDistance(tagList.get(y).getDistance());
			tagList.get(y).setTag(temp.getTag());
			tagList.get(y).setPattern(temp.getPattern());
			tagList.get(y).setDistance(temp.getDistance());
		}
	}
		
	public void setPatternDimension(int x, int y){
		pattern = new double[x][y];
		for(int i=0; i<x; i++){
			for(int j=0; j<y; j++){
				pattern[i][j] = 0.0;
			}
		}
	}
	
	public int getPatternDimension(){
		return pattern.length * pattern[0].length;
	}
	
	public void setPattern(double[][] pattern){
		this.pattern = pattern;
	}
	
	public double[][] getPattern(){
		return this.pattern;
	}
	
	public double[][] getPattern(int index){
		return this.tagList.get(index).getPattern();
	}
	
	public double getRelevance(){
		double sum = 0.0;
		for(int i=0; i<pattern.length; i++){
			for(int j=0; j<pattern[0].length; j++){
				sum += pattern[i][j];
			}
		}
		return sum;
	}
	
	public double getElementRelevance(int index){
		return this.tagList.get(index).getElementRelevance();
	}
	

}