package it.polimi.city_sensing_server.topic_utilities;

import java.util.ArrayList;


public class Breaker {
	
	private CooccourenceMatrix matrix;
	private Cluster cluster;
	private double threshold;
	
	private ArrayList<Cluster> clusterList = new ArrayList<Cluster>();
	
	public Breaker (CooccourenceMatrix matrix, Cluster cluster, double threshold){
		this.matrix = matrix;
		this.cluster = cluster;
		this.threshold = threshold;
	}
	
	public ArrayList<Cluster> clustering(){
		int[] index = new int[2];
		System.out.println("Cluster dim: " + cluster.getDim());
		for(int i=0; i<cluster.getDim(); i++){
			clusterList.add(new Cluster(cluster.getTag(i), cluster.getPattern(i)));
		}
		boolean flag;
		do {
			
			flag = false;
			index = getMinDistance();
			if(index[0]!= -1){
				mergeCluster(index);
				flag = true;
			}	
		} while(flag && clusterList.size() > 1);
		return clusterList;
	}
	
	private int[] getMinDistance(){
		double minDistance = 0d;
		int[] index = new int[2];
		index[0] = -1;
		index[1] = -1;
		for(int i=0; i<clusterList.size(); i++){
			for(int j=i+1; j<clusterList.size(); j++){
				double distance = clusterDistance(i, j);
				if(distance > minDistance  &&  distance >= threshold){
					minDistance = distance;
					index[0] = i;
					index[1] = j;
				}
			}
		}
		return index;
	}
	
	private void mergeCluster(int[] index){
		for(int i=0; i<clusterList.get(index[1]).getDim(); i++){
			clusterList.get(index[0]).setTag(clusterList.get(index[1]).getTag(i), clusterList.get(index[1]).getPattern(i));
		}
		clusterList.remove(index[1]);
	}
	
	private double clusterDistance(int arg0, int arg1){
		double maxDistance = 0d;
		for(int i=0; i<clusterList.get(arg0).getDim(); i++){
			for(int j=0; j<clusterList.get(arg1).getDim(); j++){
				if(maxDistance < matrix.getMatrixValue(clusterList.get(arg0).getTag(i), clusterList.get(arg1).getTag(j))){
					maxDistance = matrix.getMatrixValue(clusterList.get(arg0).getTag(i), clusterList.get(arg1).getTag(j));
				}
			}
		}
		return maxDistance;
	}

}
