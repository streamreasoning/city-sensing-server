package it.polimi.city_sensing_server.topic_utilities;

public class CooccourenceMatrix {
	
	private double matrix[][];
	private String labels[];
	
	public CooccourenceMatrix(int dim){
		matrix = new double[dim][dim];
		labels = new String[dim];
	}
	
	public void setLabels(String[] labels){
		this.labels = labels;
	}
	
	public void setLabel(String label, int index){
		labels[index] = label;
	}

		
	public void setMatrixValue(int row, int column, double value){
		if(Double.isNaN(value)){
			matrix[row][column] = 0;
			matrix[column][row] = 0;
		} else {
			matrix[row][column] = value;
			matrix[column][row] = value;
		}
	}
	
	public void setMatrixValue(String firstTag, String secondTag, double value){
		setMatrixValue(getLabelIndex(firstTag), getLabelIndex(secondTag), value);
	}
	
	public double getMatrixValue(int row, int column){
		if (row < column){
			return this.matrix[row][column];
		} else 
			return this.matrix[column][row];
	}
	
	public double getMatrixValue(String firstTag, String secondTag){
		return getMatrixValue(getLabelIndex(firstTag), getLabelIndex(secondTag));
	}
	
	public String getLabel(int index){
		return this.labels[index];
	}
	
	private int getLabelIndex(String label){
		for(int i=0; i<labels.length; i++){
			if(labels[i].equals(label)){
				return i;
			}
		} 
		return -1;
	}
	
	public int getMatrixDimension(){
		return matrix.length;
	}

}
