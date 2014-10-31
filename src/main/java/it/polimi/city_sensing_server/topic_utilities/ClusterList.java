package it.polimi.city_sensing_server.topic_utilities;

import java.util.ArrayList;

public class ClusterList {
	
	private ArrayList<Cluster> clusterList = new ArrayList<Cluster>();
	
	public ClusterList(){
		
	}
	
	public void startNew(String tag, double[][] pattern, double distance){
		clusterList.add(new Cluster(tag, pattern, distance));
	}
	
	public int getNumClusters(){
		return clusterList.size() - 1;
	}
	
	public void update(int index, String tag, double[][] pattern, double distance){
		clusterList.get(index).setTag(tag, pattern, distance);
	}
	
	public String[] getCluster(int index){
		return this.clusterList.get(index).getTagList();
	}
	
	public Cluster[] getClusterVector() {
		Cluster[] clusterList = new Cluster[this.clusterList.size()];
		for(int i=0; i<clusterList.length; i++) {
			clusterList[i] = this.clusterList.get(i);
		}
		return clusterList;
	}
	
	public String[] getClusterList(){
		ArrayList<String> list = new ArrayList<String>();
		for(int i=0; i<clusterList.size(); i++){
			list.add(new String((i+1) + " - " + clusterToString(clusterList.get(i))));
		}
		return listToVector(list);
	}
	
	public String[] getCSVClusterList(){
		ArrayList<String> list = new ArrayList<String>();
		for(int i=0; i<clusterList.size(); i++){
			list.add(new String(Integer.toString(i+1) + "," + clusterToString(clusterList.get(i))));
		}
		return listToVector(list);
	}
	
	private String clusterToString(Cluster cluster){
		String out = new String();
		for(int i=0; i<cluster.getDim(); i++){
			out += cluster.getTag(i);
			if(i != cluster.getDim()-1){
				out += ",";
			}
		}
		return out;
	}
	
	public Cluster getClusterClass(int index){
		return this.clusterList.get(index);
	}
	
	private String[] listToVector(ArrayList<String> list){
		String[] vector = new String[list.size()];
		for(int i=0; i<list.size(); i++){
			vector[i] = list.get(i);
		}
		return vector;
	}
	
	public void removeAll(){
		clusterList.clear();
	}
	
	public void orderByRelevance() throws NullPointerException {
		for(int heapsize = 0; heapsize < clusterList.size(); heapsize++){
			int n = heapsize;
			while(n > 0){
				int p = (n-1)/2;
				if((clusterList.get(n).getRelevance()) < (clusterList.get(p).getRelevance())){
					swap(n, p);
					n = p;
				} else {
					break;
				}
			}
		}
		
		for(int heapsize = clusterList.size(); heapsize>0; ){
			swap(0, --heapsize);
			int n = 0;
			while(true){
				int left = (n*2) + 1;
				if(left >= heapsize){
					break;
				}
				int right = left + 1;
				if(right >= heapsize){
					if((clusterList.get(left).getRelevance()) < (clusterList.get(n).getRelevance())){
						swap (left, n);
					}
					break;
				}
				if((clusterList.get(left).getRelevance()) < (clusterList.get(n).getRelevance())){
					if((clusterList.get(left).getRelevance()) < (clusterList.get(right).getRelevance())){
						swap(left, n);
						n = left;
						continue;
					} else {
						swap (right, n);
						n = right;
						continue;
					}
				} else {
					if((clusterList.get(right).getRelevance()) < (clusterList.get(n).getRelevance())){
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
			Cluster temp = new Cluster();
			double[][] tempPattern;
			for(int i=0; i<clusterList.get(x).getDim(); i++){
				temp.setTag(clusterList.get(x).getTag(i), clusterList.get(x).getPattern(i), clusterList.get(x).getDistance(i));
			}
			tempPattern = clusterList.get(x).getPattern();
		
		
		clusterList.get(x).replaceTagList(clusterList.get(y));
		clusterList.get(x).setPattern(clusterList.get(y).getPattern());
		clusterList.get(y).replaceTagList(temp);
		clusterList.get(y).setPattern(tempPattern);
		
		}
	}
}
