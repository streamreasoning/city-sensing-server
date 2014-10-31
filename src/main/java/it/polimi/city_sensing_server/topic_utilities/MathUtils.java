package it.polimi.city_sensing_server.topic_utilities;

public class MathUtils {
	
	public static int max(int x, int y){
		if(x > y){
			return x;
		} else {
			return y;
		}
	}
	
	public static double max(double x, double y){
		if(x > y){
			return x;
		} else {
			return y;
		}
	}
	
	public static int min(int x, int y){
		if (x < y){
			return x;
		} else {
			return y;
		}
	}
	
	public static double getMaxValue(double[][] values){
		double max = 0.0;
		for(int i=0; i<values.length; i++){
			for(int j=0; j<values[0].length; j++){
				if(values[i][j] > max){
					max = values[i][j];
				}
			}
		}
		return max;
	}
	
	public static double getMinValue(double[][] values){
		double min = 0.0;
		for(int i=0; i<values.length; i++){
			for(int j=0; j<values[0].length; j++){
				if(values[i][j] < min){
					min = values[i][j];
				}
			}
		}
		return min;
	}
	
	public static double cosineDistance(double[][] v1, double[][] v2){
		double[] x = matrixToVector(v1);
		double[] y = matrixToVector(v2);
		if(x.length == y.length){
			return dotProduct(x,y) / (norm(x) * norm(y));
		} else 
			return Double.NaN;
	}
	
	private static double norm (double[] x){
		double norm = 0.0;
		for(int i=0; i<x.length; i++){
			norm += x[i]*x[i];
		}
		return Math.sqrt(norm);
	}
	
	private static double dotProduct(double[] x, double[] y){
		double dot = 0.0;
		for(int i=0; i<x.length; i++){
			dot += x[i]*y[i];
		}
		return dot;
	}
	
	private static double[] matrixToVector(double[][] m){
		double[] v = new double[m.length * m[0].length];
		for(int i=0; i<m.length; i++){
			for(int j=0; j<m[0].length; j++){
				v[i*m[0].length + j] = m[i][j];
			}
		}
		return v;
	}
	
	public static double mean(double[] v){
		double sum = 0.0;
		for(int i=0; i<v.length; i++){
			sum += v[i];
		}
		return sum / v.length;
	}

}
